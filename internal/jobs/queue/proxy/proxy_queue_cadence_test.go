package proxyqueue

import (
	"context"
	"errors"
	"testing"
	"time"

	"magpie/internal/checkpace"
	"magpie/internal/domain"

	"github.com/redis/go-redis/v9"
)

// An import must not postpone a route that waits in another shard than the
// one this queue would put it in: the legacy queue, or a shard of another
// shard count. The route moves to its shard and keeps its earlier due time.
func TestImportKeepsEarlierDueTimeOfRouteWaitingInAnotherShard(t *testing.T) {
	for _, location := range []string{"legacy", "different_shard"} {
		t.Run(location, func(t *testing.T) {
			queue, client, due, proxy := cadenceFixture(t)
			ctx := context.Background()
			member := string(proxy.Hash)
			target := queue.queueKeyForMember(member)
			elsewhere := legacyQueueKey
			if location == "different_shard" {
				for _, candidate := range queue.queueShardKeys {
					if candidate != target {
						elsewhere = candidate
						break
					}
				}
			}
			// The route waits for a time one minute before its owner's next
			// point, so the import computes a later due time.
			point := queue.pace.FirstRouteCheck(queue.pace.Current(), proxy.ID, proxy.Workspaces, time.Now())
			waiting := point.Add(-time.Minute).UnixMilli()
			if err := client.ZAdd(ctx, elsewhere, redis.Z{Score: float64(waiting), Member: member}).Err(); err != nil {
				t.Fatal(err)
			}
			if err := queue.AddToQueue([]domain.Proxy{proxy}); err != nil {
				t.Fatal(err)
			}
			if got := due(proxy).UnixMilli(); got != waiting {
				t.Fatalf("the import moved the route's due time from %d to %d", waiting, got)
			}
			if err := client.ZScore(ctx, elsewhere, member).Err(); !errors.Is(err, redis.Nil) {
				t.Fatalf("the route is still queued in %s: %v", elsewhere, err)
			}
		})
	}
}

// cadenceFixture gives the queue a Pacer on which workspace 1 checks every
// six hours and workspace 2 every minute.
func cadenceFixture(t *testing.T) (*RedisProxyQueue, *redis.Client, func(domain.Proxy) time.Time, domain.Proxy) {
	t.Helper()
	queue, client, proxy := queueConcurrencyFixture(t)
	every := func(workspaceID uint, interval time.Duration) checkpace.Allowance {
		return checkpace.Allowance{WorkspaceID: workspaceID, UnlimitedChecks: true, RequestedCheckInterval: interval, ChecksPerCycle: 1}
	}
	pace := checkpace.NewPacer()
	pace.Publish(checkpace.Compute(time.Now(), []checkpace.Allowance{every(1, 6*time.Hour), every(2, time.Minute)}))
	queue.pace = pace
	due := func(proxy domain.Proxy) time.Time {
		t.Helper()
		score, err := client.ZScore(context.Background(), queue.queueKeyForMember(string(proxy.Hash)), string(proxy.Hash)).Result()
		if err != nil {
			t.Fatal(err)
		}
		return time.UnixMilli(int64(score))
	}
	return queue, client, due, proxy
}

// An imported route waits for its owner's cadence point. Routes of one import
// are spread over the cadence, and none is due at once.
func TestImportedRoutesAreFirstDueAtTheirOwnersCadencePoints(t *testing.T) {
	queue, _, due, _ := cadenceFixture(t)
	const routes = 240
	batch := make([]domain.Proxy, 0, routes)
	for index := 0; index < routes; index++ {
		batch = append(batch, domain.Proxy{
			ID: uint64(100 + index), IP: "192.0.2.9", Port: uint16(9000 + index),
			Hash: []byte{'i', byte(index), byte(index >> 8)}, Workspaces: []domain.Workspace{{ID: 1}},
		})
	}
	before := time.Now()
	if err := queue.AddToQueue(batch); err != nil {
		t.Fatal(err)
	}
	after := time.Now()
	snapshot := queue.pace.Current()
	var hours [6]int
	for _, proxy := range batch {
		// The import read the clock between before and after, so the route
		// waits for its owner's first point after one of the two.
		got := due(proxy)
		first := queue.pace.FirstRouteCheck(snapshot, proxy.ID, proxy.Workspaces, before)
		if !got.Equal(first) && !got.Equal(queue.pace.FirstRouteCheck(snapshot, proxy.ID, proxy.Workspaces, after)) {
			t.Fatalf("route %d is first due at %s, want its owner's point %s", proxy.ID, got, first)
		}
		wait := got.Sub(before)
		if wait <= 0 || wait > 6*time.Hour+time.Second {
			t.Fatalf("route %d is first due after %s, want within its owner's six hours", proxy.ID, wait)
		}
		hours[min(int(wait/time.Hour), 5)]++
	}
	for hour, count := range hours {
		if count < routes/6/2 || count > routes/6*2 {
			t.Fatalf("hour %d holds %d of %d first checks: %v", hour, count, routes, hours)
		}
	}
	// Nothing is handed out before its point. A route whose point came while
	// this test ran may be.
	limited, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	claimed, scheduled, err := queue.GetNextProxyContext(limited)
	if err == nil {
		if early := time.Until(scheduled); early > 0 {
			t.Fatalf("route %d was handed out %s before its point", claimed.ID, early)
		}
	} else if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
}

// A route queued for a slow owner is pulled forward when an import adds an
// owner with a faster cadence. Importing it again never moves it.
func TestImportWithFasterOwnerPullsQueuedRouteForward(t *testing.T) {
	queue, _, due, proxy := cadenceFixture(t)
	if err := queue.AddToQueue([]domain.Proxy{proxy}); err != nil {
		t.Fatal(err)
	}
	slow := due(proxy)
	// A point within the next two minutes would make the route look pulled
	// forward already; pick a route that waits longer.
	for attempt := 0; slow.Sub(time.Now()) < 2*time.Minute; attempt++ {
		if attempt > 100 {
			t.Fatal("no route waits longer than two minutes for a six-hour owner")
		}
		proxy.ID, proxy.Hash = proxy.ID+1, append([]byte("stored-route-hash-"), byte(attempt))
		if err := queue.AddToQueue([]domain.Proxy{proxy}); err != nil {
			t.Fatal(err)
		}
		slow = due(proxy)
	}
	if err := queue.AddToQueue([]domain.Proxy{proxy}); err != nil {
		t.Fatal(err)
	}
	if again := due(proxy); !again.Equal(slow) {
		t.Fatalf("importing the same owners moved the route from %s to %s", slow, again)
	}

	shared := proxy
	shared.Workspaces = []domain.Workspace{{ID: 1}, {ID: 2}}
	before := time.Now()
	if err := queue.AddToQueue([]domain.Proxy{shared}); err != nil {
		t.Fatal(err)
	}
	pulled := due(proxy)
	if wait := pulled.Sub(before); wait <= 0 || wait > time.Minute+time.Second {
		t.Fatalf("with a one-minute owner the route is due after %s, want within a minute", wait)
	}
	// The fast owner leaves again: an import never postpones a queued route.
	if err := queue.AddToQueue([]domain.Proxy{proxy}); err != nil {
		t.Fatal(err)
	}
	if after := due(proxy); !after.Equal(pulled) {
		t.Fatalf("an import postponed the route from %s to %s", pulled, after)
	}
}

// An explicit requeue that arrives while a worker is checking the route must
// not be lost. AddToQueueNow takes the route from the worker, in whichever
// shard its lease is: the check in progress cannot reschedule the route any
// more, and the route is due at once for every owner. The administrator's
// requeue removes the route before it adds it, which changes nothing here.
func TestExplicitRequeueDuringACheckTakesTheRouteFromItsWorker(t *testing.T) {
	for _, test := range []struct {
		name         string
		anotherShard bool // the worker holds the route in a shard other than its current one
		removeFirst  bool // the administrator's requeue
	}{
		{name: "lease in the route's shard"},
		{name: "lease in the route's shard, removed first", removeFirst: true},
		{name: "lease in another shard", anotherShard: true},
		{name: "lease in another shard, removed first", anotherShard: true, removeFirst: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			queue, client, _, proxy := cadenceFixture(t)
			proxy.Workspaces = []domain.Workspace{{ID: 1}, {ID: 2}}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			member := string(proxy.Hash)
			if err := queue.AddToQueueNow([]domain.Proxy{proxy}); err != nil {
				t.Fatal(err)
			}
			if test.anotherShard {
				// As after a change of the shard count: the route still
				// waits where the previous configuration put it.
				target := queue.queueKeyForMember(member)
				elsewhere := legacyQueueKey
				for _, candidate := range queue.queueShardKeys {
					if candidate != target {
						elsewhere = candidate
						break
					}
				}
				if err := client.ZRem(ctx, target, member).Err(); err != nil {
					t.Fatal(err)
				}
				if err := client.ZAdd(ctx, elsewhere, redis.Z{Score: 1, Member: member}).Err(); err != nil {
					t.Fatal(err)
				}
				if err := queue.refreshQueueHeads(); err != nil {
					t.Fatal(err)
				}
			}
			worker, _, err := queue.GetNextProxyContext(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if test.anotherShard == (worker.QueueLease.QueueKey == queue.queueKeyForMember(member)) {
				t.Fatalf("the worker holds the route in %s", worker.QueueLease.QueueKey)
			}

			if test.removeFirst {
				if err := queue.RemoveFromQueue([]domain.Proxy{proxy}); err != nil {
					t.Fatal(err)
				}
			}
			if err := queue.AddToQueueNow([]domain.Proxy{proxy}); err != nil {
				t.Fatal(err)
			}
			if err := queue.RequeueProxy(worker, time.Now().Add(time.Hour)); !errors.Is(err, ErrProxyLeaseLost) {
				t.Fatalf("the interrupted check rescheduled the route: %v", err)
			}

			again, scheduled, err := queue.GetNextProxyContext(ctx)
			if err != nil {
				t.Fatalf("the requeued route is not due at once: %v", err)
			}
			cycle := queue.pace.BeginRouteCycle(queue.pace.Current(), again.ID, scheduled, time.Now())
			var served checkpace.Selection
			if cycle.Select(again.Workspaces, &served); !served.All {
				t.Fatalf("the requeued route is not checked for every owner: %+v", served)
			}
		})
	}
}

// An import, unlike an explicit requeue, leaves a route to the worker that
// holds it: the worker's completion schedules it.
func TestImportDuringACheckLeavesTheRouteToItsWorker(t *testing.T) {
	queue, _, due, proxy := cadenceFixture(t)
	if err := queue.AddToQueueNow([]domain.Proxy{proxy}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	worker, _, err := queue.GetNextProxyContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.AddToQueue([]domain.Proxy{proxy}); err != nil {
		t.Fatal(err)
	}
	next := time.Now().Add(time.Hour).Truncate(time.Millisecond)
	if err := queue.RequeueProxy(worker, next); err != nil {
		t.Fatalf("an import took the route from its worker: %v", err)
	}
	if got := due(proxy); !got.Equal(next) {
		t.Fatalf("after the check the route is due at %s, want the worker's %s", got, next)
	}
}

// An explicit request to check a route again makes it due for every owner,
// whatever their cadence points.
func TestAddToQueueNowMakesTheRouteDueForEveryOwner(t *testing.T) {
	queue, _, _, proxy := cadenceFixture(t)
	proxy.Workspaces = []domain.Workspace{{ID: 1}, {ID: 2}}
	if err := queue.AddToQueue([]domain.Proxy{proxy}); err != nil {
		t.Fatal(err)
	}
	if err := queue.AddToQueueNow([]domain.Proxy{proxy}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	worker, scheduled, err := queue.GetNextProxyContext(ctx)
	if err != nil {
		t.Fatalf("the route is not due at once: %v", err)
	}
	now := time.Now()
	cycle := queue.pace.BeginRouteCycle(queue.pace.Current(), worker.ID, scheduled, now)
	for _, owner := range worker.Workspaces {
		if !cycle.Due(owner.ID) {
			t.Fatalf("owner %d is not checked by an explicit requeue", owner.ID)
		}
		cycle.Reschedule(owner.ID, now)
	}
	// Afterwards the route is back on its owners' cadences.
	next := cycle.Next(now)
	if wait := next.Sub(now); wait <= 0 || wait > time.Minute {
		t.Fatalf("after the explicit check the route is due in %s, want its fastest owner's next point", wait)
	}
	if err := queue.RequeueProxy(worker, next); err != nil {
		t.Fatal(err)
	}
	limited, stop := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer stop()
	if _, _, err := queue.GetNextProxyContext(limited); !errors.Is(err, context.DeadlineExceeded) && time.Until(next) > 100*time.Millisecond {
		t.Fatalf("the checked route was due again at once: %v", err)
	}
}
