package support

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func TestExpiryCacheBoundsChurnAndPrunesUnusedEntries(t *testing.T) {
	cache := NewExpiryCache[int, string](100)
	now := time.Now()
	for key := 0; key < 10_000; key++ {
		cache.Set(key, fmt.Sprint(key), now.Add(time.Hour))
	}
	if cache.Len() != 100 {
		t.Fatalf("entries = %d", cache.Len())
	}
	cache.Get(9900, now)
	cache.Set(10_000, "new", now.Add(time.Hour))
	if _, ok := cache.Get(9900, now); !ok {
		t.Fatal("recently used entry evicted")
	}
	if _, ok := cache.Get(9901, now); ok {
		t.Fatal("least recently used entry retained")
	}
	cache.Prune(now.Add(time.Hour))
	if cache.Len() != 0 {
		t.Fatal("unused expired entries retained")
	}
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Go(func() {
			for key := 0; key < 1000; key++ {
				cache.Set(key, "value", now.Add(time.Hour))
				cache.Get(key, now)
				cache.Prune(now)
			}
		})
	}
	wg.Wait()
	if cache.Len() > 100 {
		t.Fatal("concurrent stores exceeded capacity")
	}
}

func TestLeaderRoutineRecoversFromInitialRedisOutage(t *testing.T) {
	resetRedisClientTestState()
	t.Cleanup(resetRedisClientTestState)
	server := miniredis.RunT(t)
	addr := server.Addr()
	server.Close()
	t.Setenv(envRedisMode, redisModeSingle)
	t.Setenv(envRedisURL, "redis://"+addr+"?dial_timeout=10ms&read_timeout=10ms&max_retries=0")
	t.Setenv(envRedisPassword, "")
	t.Setenv(envRedisConnectRetryBackoffMS, "1")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	called := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- RunWithLeader(ctx, "test:startup-recovery", time.Second, func(leaderCtx context.Context) { close(called); <-leaderCtx.Done() })
	}()
	deadline := time.Now().Add(time.Second)
	for GetRedisClientStatus().LastError == "" && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-done:
		t.Fatalf("routine exited before Redis recovery: %v", err)
	default:
	}
	if err := server.StartAddr(addr); err != nil {
		t.Fatal(err)
	}
	select {
	case <-called:
	case <-ctx.Done():
		t.Fatal("leader task did not recover")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("leader task did not stop")
	}
}
