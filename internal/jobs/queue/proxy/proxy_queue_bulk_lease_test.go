package proxyqueue

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"magpie/internal/domain"

	"github.com/redis/go-redis/v9"
)

func assertRouteStillLeased(t *testing.T, queue *RedisProxyQueue, client *redis.Client, worker domain.Proxy) {
	t.Helper()
	ctx := context.Background()
	for _, key := range queue.popKeys() {
		score, err := client.ZScore(ctx, key, string(worker.Hash)).Result()
		if key == worker.QueueLease.QueueKey {
			if err != nil || score != float64(worker.QueueLease.ScoreMS)+leaseScoreMarker {
				t.Fatalf("owned lease changed in %s: score=%f error=%v", key, score, err)
			}
		} else if !errors.Is(err, redis.Nil) {
			t.Fatalf("route duplicated into %s: score=%f error=%v", key, score, err)
		}
	}
	if err := queue.RenewLeaseIfNeeded(ctx, worker); err != nil {
		t.Fatal(err)
	}
	limited, cancel := context.WithTimeout(ctx, 75*time.Millisecond)
	defer cancel()
	if other, _, err := queue.GetNextProxyContext(limited); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("active route leased twice: %+v error=%v", other.QueueLease, err)
	}
}

func TestBulkRequeuePreservesActiveLease(t *testing.T) {
	queue, client, proxy := queueConcurrencyFixture(t)
	ctx := context.Background()
	if err := queue.AddToQueue([]domain.Proxy{proxy}); err != nil {
		t.Fatal(err)
	}
	worker, scheduled, err := queue.GetNextProxyContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		count, err := queue.RequeueAll()
		if err != nil || count != 0 {
			t.Fatalf("bulk scheduling counted an active lease: count=%d error=%v", count, err)
		}
		assertRouteStillLeased(t, queue, client, worker)
	}
	if err := queue.RequeueProxy(worker, scheduled); err != nil {
		t.Fatal(err)
	}
	if count, err := queue.RequeueAll(); err != nil || count != 1 {
		t.Fatalf("completed route was not rescheduled: count=%d error=%v", count, err)
	}
}

type afterQueueListingHook struct {
	once  sync.Once
	key   string
	claim func()
}

func (h *afterQueueListingHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *afterQueueListingHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h *afterQueueListingHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		if err == nil && cmd.Name() == "zrange" && cmd.Args()[1] == h.key {
			h.once.Do(h.claim)
		}
		return err
	}
}

func TestBulkRequeuePreservesLeaseClaimedAfterListing(t *testing.T) {
	queue, client, proxy := queueConcurrencyFixture(t)
	ctx := context.Background()
	if err := queue.AddToQueue([]domain.Proxy{proxy}); err != nil {
		t.Fatal(err)
	}
	writer := redis.NewClient(client.Options())
	t.Cleanup(func() { _ = writer.Close() })
	otherQueue := NewRedisProxyQueue(writer)
	var worker domain.Proxy
	client.AddHook(&afterQueueListingHook{key: queue.queueKeyForMember(string(proxy.Hash)), claim: func() {
		limited, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		var err error
		worker, _, err = otherQueue.GetNextProxyContext(limited)
		if err != nil {
			t.Fatal(err)
		}
	}})
	if count, err := queue.RequeueAll(); err != nil || count != 0 {
		t.Fatalf("concurrent lease overwritten: count=%d error=%v", count, err)
	}
	if worker.QueueLease == nil {
		t.Fatal("fixture did not claim between listing and update")
	}
	assertRouteStillLeased(t, queue, client, worker)
}

func TestImportPreservesActiveLeaseInEverySupportedShard(t *testing.T) {
	for _, location := range []string{"current", "legacy", "different_shard"} {
		t.Run(location, func(t *testing.T) {
			queue, client, proxy := queueConcurrencyFixture(t)
			ctx := context.Background()
			member := string(proxy.Hash)
			key := queue.queueKeyForMember(member)
			switch location {
			case "legacy":
				key = legacyQueueKey
			case "different_shard":
				for _, candidate := range queue.queueShardKeys {
					if candidate != key {
						key = candidate
						break
					}
				}
			}
			raw, err := marshalQueuedProxy(proxy)
			if err != nil {
				t.Fatal(err)
			}
			if err := client.Set(ctx, proxyKeyPrefix+member, raw, 0).Err(); err != nil {
				t.Fatal(err)
			}
			if err := client.ZAdd(ctx, key, redis.Z{Score: 0, Member: member}).Err(); err != nil {
				t.Fatal(err)
			}
			if err := queue.refreshQueueHeads(); err != nil {
				t.Fatal(err)
			}
			worker, scheduled, err := queue.GetNextProxyContext(ctx)
			if err != nil {
				t.Fatal(err)
			}
			proxy.Workspaces = append(proxy.Workspaces, domain.Workspace{ID: 2})
			if err := queue.AddToQueue([]domain.Proxy{proxy}); err != nil {
				t.Fatal(err)
			}
			assertRouteStillLeased(t, queue, client, worker)
			// A changed payload must not prevent normal scheduling completion,
			// and the new owner must survive when the old worker completes.
			if err := client.Set(ctx, queueRescheduleStateKey, "1", 0).Err(); err != nil {
				t.Fatal(err)
			}
			if err := queue.RequeueProxy(worker, scheduled); err != nil {
				t.Fatal(err)
			}
			limited, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			next, _, err := queue.GetNextProxyContext(limited)
			if err != nil || len(next.Workspaces) != 2 || next.Workspaces[1].ID != 2 {
				t.Fatalf("imported ownership lost: %+v error=%v", next.Workspaces, err)
			}
		})
	}
}

func TestBulkSchedulingLeavesExpiredLeaseRecoverable(t *testing.T) {
	queue, client, proxy := queueConcurrencyFixture(t)
	ctx := context.Background()
	if err := queue.AddToQueue([]domain.Proxy{proxy}); err != nil {
		t.Fatal(err)
	}
	worker, scheduled, err := queue.GetNextProxyContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	worker.QueueLease.ScoreMS = time.Now().Add(-time.Second).UnixMilli()
	if err := client.ZAdd(ctx, worker.QueueLease.QueueKey, redis.Z{
		Score: float64(worker.QueueLease.ScoreMS) + leaseScoreMarker, Member: string(worker.Hash),
	}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := queue.AddToQueue([]domain.Proxy{proxy}); err != nil {
		t.Fatal(err)
	}
	if count, err := queue.RequeueAll(); err != nil || count != 0 {
		t.Fatalf("expired lease rewritten: count=%d error=%v", count, err)
	}
	limited, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if _, _, err := queue.GetNextProxyContext(limited); err != nil {
		t.Fatalf("expired lease not reclaimed: %v", err)
	}
	if err := queue.RequeueProxy(worker, scheduled); !errors.Is(err, ErrProxyLeaseLost) {
		t.Fatalf("expired worker changed new lease: %v", err)
	}
}
