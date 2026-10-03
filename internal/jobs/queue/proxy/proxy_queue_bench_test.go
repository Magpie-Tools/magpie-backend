package proxyqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"magpie/internal/domain"
	"magpie/internal/security"

	"github.com/redis/go-redis/v9"
)

var benchmarkDecodedProxy domain.Proxy

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
