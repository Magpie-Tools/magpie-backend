package proxyqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"magpie/internal/domain"
	"magpie/internal/security"

	"github.com/redis/go-redis/v9"
)

var benchmarkDecodedProxy domain.Proxy

// BenchmarkProxyQueueLegacyRekey measures the exceptional compatibility cycle
// with distinct aliases and eight existing owners per canonical route. The
// alias adds a ninth owner, so atomic migration must encode the merged payload.
// Requires an empty disposable Redis database like the normal cycle benchmark.
func BenchmarkProxyQueueLegacyRekey(b *testing.B) {
	redisURL := os.Getenv("MAGPIE_BENCH_REDIS_URL")
	if redisURL == "" {
		b.Skip("MAGPIE_BENCH_REDIS_URL is not set")
	}
	b.Setenv(envEncryptQueueCredentials, "false")
	b.Setenv("PROXY_ENCRYPTION_KEY", strings.Repeat("11", 32))
	security.ResetProxyCipherForTests()
	b.Cleanup(security.ResetProxyCipherForTests)
	options, err := redis.ParseURL(redisURL)
	if err != nil {
		b.Fatal(err)
	}
	client := redis.NewClient(options)
	b.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	if size, err := client.DBSize(ctx).Result(); err != nil || size != 0 {
		b.Fatalf("benchmark requires an empty disposable Redis database: size=%d, error=%v", size, err)
	}
	queue := NewRedisProxyQueue(client)
	keys := append([]string{proxyQueueHeadKey, queueRescheduleStateKey}, queue.popKeys()...)
	b.Cleanup(func() {
		for start := 0; start < len(keys); start += 1000 {
			if err := client.Del(ctx, keys[start:min(start+1000, len(keys))]...).Err(); err != nil {
				b.Error(err)
			}
		}
	})
	if err := client.Set(ctx, queueRescheduleStateKey, "3600000", 0).Err(); err != nil {
		b.Fatal(err)
	}
	canonicalDue := float64(time.Now().Add(processingLease / 2).UnixMilli())
	pipe := client.Pipeline()
	for i := 0; i <= b.N; i++ {
		proxy := domain.Proxy{
			ID: uint64(i + 1), IP: fmt.Sprintf("route-%d.provider.example", i), Port: 8080,
			Username: "proxy-user", Password: "proxy-password",
		}
		if err := proxy.GenerateHash(); err != nil {
			b.Fatal(err)
		}
		for owner := 1; owner <= 8; owner++ {
			proxy.Workspaces = append(proxy.Workspaces, domain.Workspace{ID: uint(owner)})
		}
		current, err := marshalQueuedProxy(proxy)
		if err != nil {
			b.Fatal(err)
		}
		alias := fmt.Sprintf("legacy-benchmark-%012d", i)
		old, err := json.Marshal(queuedProxy{
			ID: proxy.ID, IP: proxy.IP, Port: proxy.Port, Username: proxy.Username, Password: proxy.Password,
			Hash: []byte(alias), UserIDs: []uint{9},
		})
		if err != nil {
			b.Fatal(err)
		}
		keys = append(keys, proxyKeyPrefix+string(proxy.Hash), proxyKeyPrefix+alias)
		pipe.Set(ctx, proxyKeyPrefix+string(proxy.Hash), current, 0)
		pipe.ZAdd(ctx, queue.queueKeyForMember(string(proxy.Hash)), redis.Z{Score: canonicalDue, Member: string(proxy.Hash)})
		pipe.Set(ctx, proxyKeyPrefix+alias, old, 0)
		pipe.ZAdd(ctx, legacyQueueKey, redis.Z{Score: 0, Member: alias})
		if (i+1)%500 == 0 {
			if _, err := pipe.Exec(ctx); err != nil {
				b.Fatal(err)
			}
		}
	}
	if _, err := pipe.Exec(ctx); err != nil {
		b.Fatal(err)
	}
	if err := queue.refreshQueueHeads(); err != nil {
		b.Fatal(err)
	}
	// One untimed alias warms every script in both implementations.
	warm, _, err := queue.GetNextProxyContext(ctx)
	if err != nil {
		b.Fatal(err)
	}
	if err := queue.RequeueProxy(warm, time.Now()); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		proxy, _, err := queue.GetNextProxyContext(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if len(proxy.Workspaces) != 9 {
			b.Fatalf("migration lost owners: %d", len(proxy.Workspaces))
		}
		if err := queue.RequeueProxy(proxy, time.Now()); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkProxyLeaseRenewal measures the additional command required only
// when a route is still running within two minutes of its lease expiry.
func BenchmarkProxyLeaseRenewal(b *testing.B) {
	redisURL := os.Getenv("MAGPIE_BENCH_REDIS_URL")
	if redisURL == "" {
		b.Skip("MAGPIE_BENCH_REDIS_URL is not set")
	}
	options, err := redis.ParseURL(redisURL)
	if err != nil {
		b.Fatal(err)
	}
	client := redis.NewClient(options)
	b.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	const key = "magpie-benchmark-lease-renewal"
	if exists, err := client.Exists(ctx, key).Result(); err != nil || exists != 0 {
		b.Fatalf("benchmark key already exists: %d %v", exists, err)
	}
	b.Cleanup(func() { _ = client.Del(ctx, key).Err() })
	score := time.Now().UnixMilli() + int64(processingLease/time.Millisecond)
	if err := client.ZAdd(ctx, key, redis.Z{Score: float64(score) + leaseScoreMarker, Member: "route"}).Err(); err != nil {
		b.Fatal(err)
	}
	if err := renewScript.Load(ctx, client).Err(); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		newScore, err := renewScript.Run(ctx, client, []string{key}, "route", score, score-1000, score+1).Int64()
		if err != nil || newScore != score+1 {
			b.Fatalf("renewal: score=%d error=%v", newScore, err)
		}
		score = newScore
	}
}

func BenchmarkCurrentPlaintextProxyDecode(b *testing.B) {
	b.Setenv(envEncryptQueueCredentials, "false")
	b.Setenv("PROXY_ENCRYPTION_KEY", "")
	security.ResetProxyCipherForTests()
	b.Cleanup(security.ResetProxyCipherForTests)

	for _, workspaceCount := range []int{1, 8} {
		b.Run(fmt.Sprintf("workspaces=%d", workspaceCount), func(b *testing.B) {
			proxy := domain.Proxy{
				ID: 1, IP: "gateway.provider.example", Port: 8080,
				Username: "proxy-user", Password: "proxy-password",
				Hash: []byte("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"),
			}
			for i := 0; i < workspaceCount; i++ {
				proxy.Workspaces = append(proxy.Workspaces, domain.Workspace{ID: uint(i + 1)})
			}
			raw, err := marshalQueuedProxy(proxy)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.SetBytes(int64(len(raw)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var payload queuedProxy
				if err := json.Unmarshal(raw, &payload); err != nil {
					b.Fatal(err)
				}
				decoded, err := payload.toDomainProxy()
				if err != nil {
					b.Fatal(err)
				}
				benchmarkDecodedProxy = decoded
			}
		})
	}
}

// BenchmarkProxyQueueCycle requires an empty, disposable Redis database. Seed
// one ready route per iteration so scheduling is measured without artificial
// score-reset commands or sleeps in the timed loop.
func BenchmarkProxyQueueCycle(b *testing.B) {
	benchmarkProxyQueueCycle(b, false)
}

// BenchmarkProxyQueueCycleConcurrent uses eight workers per GOMAXPROCS value,
// sharing the same queue and client as concurrent production checkers.
func BenchmarkProxyQueueCycleConcurrent(b *testing.B) {
	b.SetParallelism(8)
	benchmarkProxyQueueCycle(b, true)
}

func benchmarkProxyQueueCycle(b *testing.B, concurrent bool) {
	redisURL := os.Getenv("MAGPIE_BENCH_REDIS_URL")
	if redisURL == "" {
		b.Skip("MAGPIE_BENCH_REDIS_URL is not set")
	}
	b.Setenv(envEncryptQueueCredentials, "false")
	b.Setenv("PROXY_ENCRYPTION_KEY", "")
	security.ResetProxyCipherForTests()
	b.Cleanup(security.ResetProxyCipherForTests)
	options, err := redis.ParseURL(redisURL)
	if err != nil {
		b.Fatal(err)
	}
	client := redis.NewClient(options)
	b.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	if size, err := client.DBSize(ctx).Result(); err != nil || size != 0 {
		b.Fatalf("benchmark requires an empty disposable Redis database: size=%d, error=%v", size, err)
	}
	queue := NewRedisProxyQueue(client)
	keys := append([]string{proxyQueueHeadKey, queueRescheduleStateKey}, queue.popKeys()...)
	b.Cleanup(func() {
		for start := 0; start < len(keys); start += 1000 {
			end := min(start+1000, len(keys))
			if err := client.Del(ctx, keys[start:end]...).Err(); err != nil {
				b.Error(err)
			}
		}
	})
	// Keep completed routes behind every still-ready route throughout the run.
	if err := client.Set(ctx, queueRescheduleStateKey, "3600000", 0).Err(); err != nil {
		b.Fatal(err)
	}
	pipe := client.Pipeline()
	for i := 0; i < b.N; i++ {
		member := fmt.Sprintf("benchmark-route-%064d", i)
		proxy := domain.Proxy{
			ID: uint64(i + 1), IP: "gateway.provider.example", Port: 8080,
			Username: "proxy-user", Password: "proxy-password", Hash: []byte(member),
			Workspaces: []domain.Workspace{{ID: 1}, {ID: 2}},
		}
		raw, err := marshalQueuedProxy(proxy)
		if err != nil {
			b.Fatal(err)
		}
		key := proxyKeyPrefix + member
		keys = append(keys, key)
		pipe.Set(ctx, key, raw, 0)
		pipe.ZAdd(ctx, queue.queueKeyForMember(member), redis.Z{Score: 0, Member: member})
		if (i+1)%1000 == 0 {
			if _, err := pipe.Exec(ctx); err != nil {
				b.Fatal(err)
			}
		}
	}
	if _, err := pipe.Exec(ctx); err != nil {
		b.Fatal(err)
	}
	if err := queue.refreshQueueHeads(); err != nil {
		b.Fatal(err)
	}
	if err := queue.popScript.Load(ctx, client).Err(); err != nil {
		b.Fatal(err)
	}
	if err := completeScript.Load(ctx, client).Err(); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	if concurrent {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				proxy, _, err := queue.GetNextProxyContext(ctx)
				if err != nil {
					b.Error(err)
					return
				}
				if err := queue.RequeueProxy(proxy, time.Now()); err != nil {
					b.Error(err)
					return
				}
			}
		})
		return
	}
	for i := 0; i < b.N; i++ {
		proxy, _, err := queue.GetNextProxyContext(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if err := queue.RequeueProxy(proxy, time.Now()); err != nil {
			b.Fatal(err)
		}
	}
}
