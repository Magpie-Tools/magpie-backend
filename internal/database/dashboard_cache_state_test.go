package database

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type dashboardObservedContext struct {
	context.Context
	once    sync.Once
	waiting chan struct{}
}

func (c *dashboardObservedContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

func TestDashboardDemandClockAndSelection(t *testing.T) {
	now := time.Date(2026, 10, 24, 10, 0, 0, 0, time.UTC)
	var removed []dashboardCacheKey
	c := newDashboardCacheCoordinator(func() time.Time { return now }, func(key dashboardCacheKey) { removed = append(removed, key) })
	a := dashboardCacheKey{workspaceID: 1, component: dashboardInfoComponent}
	b := dashboardCacheKey{workspaceID: 2, component: dashboardRecentComponent, limit: 3}
	if len(c.requestedKeys()) != 0 {
		t.Fatal("empty cache created work")
	}
	c.touch(a)
	if keys := c.requestedKeys(); len(keys) != 1 || keys[0] != a {
		t.Fatal(keys)
	}
	// An explicit refresh/revision creates storage without creating demand.
	c.revision(b)
	if c.requested(b) {
		t.Fatal("internal work created demand")
	}
	for _, stamp := range []time.Time{time.Time{}, now.Add(time.Second), now.Add(-dashboardCacheFreshness)} {
		if c.fresh(stamp) {
			t.Fatal("invalid or expired stamp counted as fresh", stamp)
		}
	}
	if !c.fresh(now.Add(-dashboardCacheFreshness + time.Nanosecond)) {
		t.Fatal("fresh boundary rejected")
	}
	now = now.Add(dashboardDemandIdle)
	if len(c.requestedKeys()) != 0 || len(removed) != 2 {
		t.Fatal("idle components survived", removed)
	}
}

func TestDashboardDemandFlightsJoinAndCanceledWaiterDoesNotCancelLeader(t *testing.T) {
	c := newDashboardCacheCoordinator(time.Now, func(dashboardCacheKey) {})
	key := dashboardCacheKey{workspaceID: 1}
	started, release, leaderDone := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	var calls atomic.Int32
	load := func(ctx context.Context) (any, error) {
		calls.Add(1)
		close(started)
		select {
		case <-release:
			return "loaded", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	go func() { _, err := c.run(context.Background(), key, load); leaderDone <- err }()
	<-started
	waitCtx, cancel := context.WithCancel(context.Background())
	observed := &dashboardObservedContext{Context: waitCtx, waiting: make(chan struct{})}
	waiterDone := make(chan error, 1)
	go func() { _, err := c.run(observed, key, load); waiterDone <- err }()
	<-observed.waiting
	cancel()
	if err := <-waiterDone; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	select {
	case err := <-leaderDone:
		t.Fatal("waiter stopped leader", err)
	default:
	}
	// A second waiter must receive the leader's result, with no new load.
	joined := make(chan any, 1)
	joinedCtx := &dashboardObservedContext{Context: context.Background(), waiting: make(chan struct{})}
	go func() {
		value, err := c.run(joinedCtx, key, func(context.Context) (any, error) { t.Error("duplicate loader"); return nil, nil })
		if err != nil {
			joined <- err
		} else {
			joined <- value
		}
	}()
	<-joinedCtx.waiting
	// Keep the leader pinned through a sweep and invalidation.
	c.mu.Lock()
	c.entries[key].requestedAt, c.entries[key].usedAt = time.Time{}, time.Time{}
	c.mu.Unlock()
	c.requestedKeys()
	c.mu.Lock()
	if c.entries[key] == nil || c.entries[key].flight == nil {
		t.Fatal("sweep discarded active flight")
	}
	c.mu.Unlock()
	close(release)
	if err := <-leaderDone; err != nil {
		t.Fatal(err)
	}
	if value := <-joined; value != "loaded" {
		t.Fatal(value)
	}
	if calls.Load() != 1 {
		t.Fatal(calls.Load())
	}
}

func TestDashboardDemandCanceledLeaderCanBeRetriedByLiveCaller(t *testing.T) {
	c := newDashboardCacheCoordinator(time.Now, func(dashboardCacheKey) {})
	key := dashboardCacheKey{workspaceID: 1}
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	first := make(chan error, 1)
	go func() {
		_, err := c.run(ctx, key, func(ctx context.Context) (any, error) { close(started); <-ctx.Done(); return nil, ctx.Err() })
		first <- err
	}()
	<-started
	second := make(chan error, 1)
	go func() {
		value, err := c.run(context.Background(), key, func(context.Context) (any, error) { return 42, nil })
		if err == nil && value != 42 {
			err = errors.New("wrong retried value")
		}
		second <- err
	}()
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := <-second; err != nil {
		t.Fatal(err)
	}
}

func TestDashboardDemandTouchSweepAndInvalidationPublication(t *testing.T) {
	var clock atomic.Int64
	clock.Store(time.Now().UnixNano())
	c := newDashboardCacheCoordinator(func() time.Time { return time.Unix(0, clock.Load()) }, func(dashboardCacheKey) {})
	key := dashboardCacheKey{workspaceID: 2, component: dashboardRecentComponent, limit: 7}
	var wg sync.WaitGroup
	for n := 0; n < 8; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				c.touch(key)
				c.requestedKeys()
			}
		}()
	}
	wg.Wait()
	if !c.requested(key) {
		t.Fatal("sweep lost renewed demand")
	}
	revision := c.revision(key)
	c.mu.Lock()
	c.entries[key].revision++
	c.mu.Unlock()
	published := false
	c.publish(key, revision, func() { published = true })
	if published {
		t.Fatal("invalidated leader published old value")
	}
	c.publish(key, revision+1, func() { published = true })
	if !published {
		t.Fatal("current revision did not publish")
	}
}

func TestDashboardDemandCanceledGetDoesNotTouchOrLoad(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	key := dashboardCacheKey{workspaceID: 984235, component: dashboardInfoComponent}
	if _, err := GetDashboardInfoContext(ctx, key.workspaceID); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if dashboardCaches.requested(key) {
		t.Fatal("canceled request registered demand")
	}
}

func TestDashboardDemandExpiredInterestEvictsAtFlightCompletion(t *testing.T) {
	for _, renew := range []bool{false, true} {
		t.Run(map[bool]string{false: "expired", true: "renewed"}[renew], func(t *testing.T) {
			var clock atomic.Int64
			clock.Store(time.Now().UnixNano())
			var evictions atomic.Int32
			c := newDashboardCacheCoordinator(func() time.Time { return time.Unix(0, clock.Load()) }, func(dashboardCacheKey) { evictions.Add(1) })
			key := dashboardCacheKey{workspaceID: 1}
			c.touch(key)
			started, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			go func() {
				defer close(done)
				_, _ = c.run(context.Background(), key, func(context.Context) (any, error) { close(started); <-release; return nil, nil })
			}()
			<-started
			clock.Add(int64(dashboardDemandIdle))
			if len(c.requestedKeys()) != 0 {
				t.Fatal("expired flight remained requested")
			}
			if renew {
				c.touch(key)
			}
			close(release)
			<-done
			c.mu.Lock()
			entry := c.entries[key]
			c.mu.Unlock()
			if renew && (entry == nil || evictions.Load() != 0) {
				t.Fatal("completion evicted renewed request")
			}
			if !renew && (entry != nil || evictions.Load() != 1) {
				t.Fatal("completion extended idle retention", evictions.Load())
			}
		})
	}
}

func TestDashboardDemandPanicReleasesFlightAndPreservesRecovery(t *testing.T) {
	c := newDashboardCacheCoordinator(time.Now, func(dashboardCacheKey) {})
	key := dashboardCacheKey{workspaceID: 1}
	started, release, recovered := make(chan struct{}), make(chan struct{}), make(chan any, 1)
	go func() {
		defer func() { recovered <- recover() }()
		_, _ = c.run(context.Background(), key, func(context.Context) (any, error) {
			close(started)
			<-release
			panic("loader failed")
		})
	}()
	<-started
	ctx := &dashboardObservedContext{Context: context.Background(), waiting: make(chan struct{})}
	waiter := make(chan error, 1)
	go func() {
		_, err := c.run(ctx, key, func(context.Context) (any, error) { t.Error("duplicate leader"); return nil, nil })
		waiter <- err
	}()
	<-ctx.waiting
	close(release)
	if value := <-recovered; value != "loader failed" {
		t.Fatal("panic was swallowed", value)
	}
	if err := <-waiter; err == nil || err.Error() != "dashboard cache: refresh aborted" {
		t.Fatal("follower stayed pinned or missed failure", err)
	}
	if value, err := c.run(context.Background(), key, func(context.Context) (any, error) { return 42, nil }); err != nil || value != 42 {
		t.Fatal("failed flight prevented a later refresh", value, err)
	}
}
