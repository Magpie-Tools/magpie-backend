package checker

import (
	"fmt"
	"net/http"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"magpie/internal/domain"
)

func TestCheckerHTTPClientCacheEntriesForThreads(t *testing.T) {
	tests := []struct {
		name    string
		threads uint32
		want    int
	}{
		{name: "minimum clamp at low thread count", threads: 1, want: 2048},
		{name: "2000 threads", threads: 2000, want: 8192},
		{name: "5000 threads", threads: 5000, want: 16384},
		{name: "max clamp at very high thread count", threads: 7000, want: 16384},
	}

	for _, tc := range tests {
		if got := checkerHTTPClientCacheEntriesForThreads(tc.threads); got != tc.want {
			t.Fatalf("%s: got %d, want %d", tc.name, got, tc.want)
		}
	}
}

func TestNextPow2(t *testing.T) {
	tests := []struct {
		in   uint64
		want uint64
	}{
		{in: 0, want: 1},
		{in: 1, want: 1},
		{in: 2, want: 2},
		{in: 3, want: 4},
		{in: 7, want: 8},
		{in: 8, want: 8},
		{in: 9, want: 16},
	}

	for _, tc := range tests {
		if got := nextPow2(tc.in); got != tc.want {
			t.Fatalf("nextPow2(%d) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestFullClientCacheHitDoesNotEvictTransport(t *testing.T) {
	resetCheckerHTTPClientCacheForTests()
	oldThreads := currentThreads.Load()
	currentThreads.Store(1)
	oldFactory := checkerTransportFactory
	t.Cleanup(func() {
		currentThreads.Store(oldThreads)
		checkerTransportFactory = oldFactory
		resetCheckerHTTPClientCacheForTests()
	})
	var created, closed atomic.Int64
	checkerTransportFactory = func(domain.Proxy, *domain.Judge, string, string, ...uint16) (http.RoundTripper, func(), error) {
		created.Add(1)
		return http.DefaultTransport, func() { closed.Add(1) }, nil
	}
	judge := &domain.Judge{FullString: "http://127.0.0.1/"}
	firstProxy := domain.Proxy{IP: "192.0.2.1", Port: 1}
	first, err := getCheckerHTTPClient(firstProxy, judge, "http", "tcp", 1000)
	if err != nil {
		t.Fatal(err)
	}
	for i := 2; i <= checkerHTTPClientCacheMinEntries; i++ {
		if _, err := getCheckerHTTPClient(domain.Proxy{IP: "192.0.2.1", Port: uint16(i)}, judge, "http", "tcp", 1000); err != nil {
			t.Fatal(err)
		}
	}
	hit, err := getCheckerHTTPClient(firstProxy, judge, "http", "tcp", 1000)
	if err != nil || hit != first || created.Load() != checkerHTTPClientCacheMinEntries || closed.Load() != 0 {
		t.Fatalf("full-cache hit evicted its client: created=%d closed=%d error=%v", created.Load(), closed.Load(), err)
	}
	if _, err := getCheckerHTTPClient(domain.Proxy{IP: "192.0.2.1", Port: 65535}, judge, "http", "tcp", 1000); err != nil {
		t.Fatal(err)
	}
	if closed.Load() != 1 {
		t.Fatalf("miss did not evict exactly one client: %d", closed.Load())
	}
	if hit, err := getCheckerHTTPClient(firstProxy, judge, "http", "tcp", 1000); err != nil || hit != first {
		t.Fatalf("recently used transport was evicted: %v", err)
	}
}

func TestClientCacheExpirationCleanupIsBounded(t *testing.T) {
	resetCheckerHTTPClientCacheForTests()
	t.Cleanup(resetCheckerHTTPClientCacheForTests)
	now := time.Now()
	for i := 0; i < 100; i++ {
		key := checkerHTTPClientCacheKey{proxyAddr: strconv.Itoa(i)}
		checkerHTTPClientCache[key] = &cachedCheckerHTTPClient{lastUsed: now.Add(-2 * checkerHTTPClientCacheTTL), position: checkerHTTPClientLRU.PushBack(key)}
	}
	checkerHTTPClientCacheMu.Lock()
	runCheckerHTTPClientCacheMaintenanceLocked(now)
	checkerHTTPClientCacheMu.Unlock()
	if len(checkerHTTPClientCache) != 84 {
		t.Fatalf("one miss cleaned %d entries, want bounded cleanup of 16", 100-len(checkerHTTPClientCache))
	}
}

func BenchmarkCheckerClientCacheChurn(b *testing.B) {
	for _, concurrent := range []bool{false, true} {
		for _, threads := range []uint32{500, 2000, 5000} {
			b.Run(fmt.Sprintf("threads=%d/concurrent=%t", threads, concurrent), func(b *testing.B) {
				resetCheckerHTTPClientCacheForTests()
				oldThreads := currentThreads.Load()
				currentThreads.Store(threads)
				b.Cleanup(func() { currentThreads.Store(oldThreads); resetCheckerHTTPClientCacheForTests() })
				now := time.Now()
				for i := 0; i < checkerHTTPClientCacheMaxEntries(); i++ {
					key := checkerHTTPClientCacheKey{proxyAddr: "seed-" + strconv.Itoa(i)}
					checkerHTTPClientCache[key] = &cachedCheckerHTTPClient{lastUsed: now, position: checkerHTTPClientLRU.PushBack(key)}
				}
				var serial atomic.Uint64
				churn := func() {
					key := checkerHTTPClientCacheKey{proxyAddr: strconv.FormatUint(serial.Add(1), 10)}
					checkerHTTPClientCacheMu.Lock()
					runCheckerHTTPClientCacheMaintenanceLocked(now)
					checkerHTTPClientCache[key] = &cachedCheckerHTTPClient{lastUsed: now, position: checkerHTTPClientLRU.PushBack(key)}
					checkerHTTPClientCacheMu.Unlock()
				}
				b.ReportAllocs()
				b.ResetTimer()
				if concurrent {
					b.SetParallelism(8)
					b.RunParallel(func(pb *testing.PB) {
						for pb.Next() {
							churn()
						}
					})
				} else {
					for i := 0; i < b.N; i++ {
						churn()
					}
				}
			})
		}
	}
}
