package proxyqueue

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"magpie/internal/domain"
	"magpie/internal/security"

	"github.com/redis/go-redis/v9"
)

func migrationLeaseFixture(t *testing.T) (*RedisProxyQueue, *redis.Client, domain.Proxy) {
	t.Helper()
	queue, client, proxy := queueConcurrencyFixture(t)
	configureProxyQueueEncryption(t)
	proxy.Username, proxy.Password = "CaseUser", "CaseSecret"
	if err := proxy.GenerateHash(); err != nil {
		t.Fatal(err)
	}
	return queue, client, proxy
}

func seedLegacyAlias(t *testing.T, queue *RedisProxyQueue, client *redis.Client, proxy domain.Proxy, alias string, ids []uint, encrypted bool) string {
	t.Helper()
	payload := queuedProxy{ID: proxy.ID, IP: proxy.IP, Port: proxy.Port, Hash: []byte(alias), UserIDs: ids}
	if encrypted {
		payload.Version = 2
		var err error
		payload.UsernameEncrypted, err = security.EncryptProxySecret(proxy.Username)
		if err != nil {
			t.Fatal(err)
		}
		payload.PasswordEncrypted, err = security.EncryptProxySecret(proxy.Password)
		if err != nil {
			t.Fatal(err)
		}
	} else {
		payload.Username, payload.Password = proxy.Username, proxy.Password
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := client.Set(ctx, proxyKeyPrefix+alias, raw, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.ZAdd(ctx, legacyQueueKey, redis.Z{Score: 0, Member: alias}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := queue.refreshQueueHeads(); err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func readCanonicalOwners(t *testing.T, client *redis.Client, member string) []uint {
	t.Helper()
	raw, err := client.Get(context.Background(), proxyKeyPrefix+member).Result()
	if err != nil {
		t.Fatal(err)
	}
	var payload queuedProxy
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		t.Fatal(err)
	}
	ids := payload.WorkspaceIDs
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func assertAliasConsumed(t *testing.T, queue *RedisProxyQueue, client *redis.Client, alias string) {
	t.Helper()
	ctx := context.Background()
	if client.Exists(ctx, proxyKeyPrefix+alias).Val() != 0 {
		t.Fatal("consumed alias payload retained")
	}
	for _, key := range queue.popKeys() {
		if err := client.ZScore(ctx, key, alias).Err(); !errors.Is(err, redis.Nil) {
			t.Fatalf("consumed alias still scheduled in %s: %v", key, err)
		}
	}
}

func TestLegacyAliasMigrationPreservesActiveCanonicalLeaseAcrossShards(t *testing.T) {
	for _, location := range []string{"current", "legacy", "different_shard"} {
		t.Run(location, func(t *testing.T) {
			queue, client, proxy := migrationLeaseFixture(t)
			ctx := context.Background()
			if err := queue.AddToQueue([]domain.Proxy{proxy}); err != nil {
				t.Fatal(err)
			}
			key := queue.queueKeyForMember(string(proxy.Hash))
			if location != "current" {
				newKey := legacyQueueKey
				if location == "different_shard" {
					for _, candidate := range queue.queueShardKeys {
						if candidate != key {
							newKey = candidate
							break
						}
					}
				}
				if err := client.ZRem(ctx, key, string(proxy.Hash)).Err(); err != nil {
					t.Fatal(err)
				}
				if err := client.ZAdd(ctx, newKey, redis.Z{Score: 0, Member: string(proxy.Hash)}).Err(); err != nil {
					t.Fatal(err)
				}
				if err := queue.refreshQueueHeads(); err != nil {
					t.Fatal(err)
				}
			}
			worker, scheduled, err := queue.GetNextProxyContext(ctx)
			if err != nil {
				t.Fatal(err)
			}
			seedLegacyAlias(t, queue, client, proxy, "old-hash", []uint{2}, false)
			assertRouteStillLeased(t, queue, client, worker)
			assertAliasConsumed(t, queue, client, "old-hash")
			if owners := readCanonicalOwners(t, client, string(proxy.Hash)); !reflect.DeepEqual(owners, []uint{1, 2}) {
				t.Fatalf("legacy owners lost: %v", owners)
			}
			worker.Workspaces = nil
			if err := queue.RequeueProxyWithPayload(worker, scheduled); err != nil {
				t.Fatalf("original lease could not complete: %v", err)
			}
			if owners := readCanonicalOwners(t, client, string(proxy.Hash)); !reflect.DeepEqual(owners, []uint{2}) {
				t.Fatalf("original completion erased alias owners: %v", owners)
			}
		})
	}
}

type beforeMigrationHook struct {
	once   sync.Once
	mutate func()
}

func (h *beforeMigrationHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *beforeMigrationHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h *beforeMigrationHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if (cmd.Name() == "evalsha" && cmd.Args()[1] == migrateScript.Hash()) || (cmd.Name() == "eval" && cmd.Args()[1] == luaMigrateScript) {
			h.once.Do(h.mutate)
		}
		return next(ctx, cmd)
	}
}

func TestLegacyMigrationPreservesCanonicalClaimAndImportDuringDecode(t *testing.T) {
	queue, client, proxy := migrationLeaseFixture(t)
	ctx := context.Background()
	seedLegacyAlias(t, queue, client, proxy, "old-hash", []uint{3}, false)
	writer := redis.NewClient(client.Options())
	t.Cleanup(func() { _ = writer.Close() })
	writerQueue := NewRedisProxyQueue(writer)
	var worker domain.Proxy
	client.AddHook(&beforeMigrationHook{mutate: func() {
		if err := writerQueue.AddToQueue([]domain.Proxy{proxy}); err != nil {
			t.Fatal(err)
		}
		limited, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		var err error
		worker, _, err = writerQueue.GetNextProxyContext(limited)
		if err != nil {
			t.Fatal(err)
		}
		proxy.Workspaces = append(proxy.Workspaces, domain.Workspace{ID: 2})
		if err := writerQueue.AddToQueue([]domain.Proxy{proxy}); err != nil {
			t.Fatal(err)
		}
	}})
	limited, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if _, _, err := queue.GetNextProxyContext(limited); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("migration returned a second check after concurrent claim: %v", err)
	}
	if worker.QueueLease == nil {
		t.Fatal("fixture did not claim during migration preparation")
	}
	assertRouteStillLeased(t, queue, client, worker)
	assertAliasConsumed(t, queue, client, "old-hash")
	if owners := readCanonicalOwners(t, client, string(proxy.Hash)); !reflect.DeepEqual(owners, []uint{1, 2, 3}) {
		t.Fatalf("concurrent import or legacy owners erased: %v", owners)
	}
}

func TestConcurrentLegacyAliasesGrantOneCanonicalWorker(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(fmt.Sprintf("encrypted=%t", encrypted), func(t *testing.T) {
			queue, client, proxy := migrationLeaseFixture(t)
			if encrypted {
				t.Setenv(envEncryptQueueCredentials, "true")
			}
			for i := 1; i <= 3; i++ {
				seedLegacyAlias(t, queue, client, proxy, fmt.Sprintf("alias-%d", i), []uint{uint(i)}, encrypted)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()
			type result struct {
				proxy domain.Proxy
				err   error
			}
			results := make(chan result, 3)
			for i := 0; i < 3; i++ {
				go func() {
					worker, _, err := queue.GetNextProxyContext(ctx)
					results <- result{worker, err}
				}()
			}
			var winner domain.Proxy
			claims := 0
			for i := 0; i < 3; i++ {
				r := <-results
				if r.err == nil {
					claims++
					winner = r.proxy
				} else if !errors.Is(r.err, context.DeadlineExceeded) {
					t.Fatal(r.err)
				}
			}
			if claims != 1 || !bytes.Equal(winner.Hash, proxy.Hash) || winner.Username != proxy.Username || winner.Password != proxy.Password {
				t.Fatalf("aliases granted %d checks or changed route identity: %+v", claims, winner)
			}
			assertRouteStillLeased(t, queue, client, winner)
			for i := 1; i <= 3; i++ {
				assertAliasConsumed(t, queue, client, fmt.Sprintf("alias-%d", i))
			}
			if owners := readCanonicalOwners(t, client, string(proxy.Hash)); !reflect.DeepEqual(owners, []uint{1, 2, 3}) {
				t.Fatalf("concurrent aliases lost owners: %v", owners)
			}
		})
	}
}

func TestLegacyMigrationCannotUseStaleSourceLeaseOrPayload(t *testing.T) {
	for _, mutation := range []string{"reclaimed", "deleted", "updated"} {
		t.Run(mutation, func(t *testing.T) {
			queue, client, proxy := migrationLeaseFixture(t)
			ctx := context.Background()
			original := seedLegacyAlias(t, queue, client, proxy, "old-hash", []uint{1}, false)
			writer := redis.NewClient(client.Options())
			t.Cleanup(func() { _ = writer.Close() })
			client.AddHook(&beforeMigrationHook{mutate: func() {
				switch mutation {
				case "reclaimed":
					if err := writer.ZAdd(ctx, legacyQueueKey, redis.Z{
						Score: float64(time.Now().Add(2*processingLease).UnixMilli()) + leaseScoreMarker, Member: "old-hash",
					}).Err(); err != nil {
						t.Fatal(err)
					}
				case "deleted":
					if err := NewRedisProxyQueue(writer).RemoveFromQueue([]domain.Proxy{{Hash: []byte("old-hash")}}); err != nil {
						t.Fatal(err)
					}
				case "updated":
					var payload queuedProxy
					if err := json.Unmarshal([]byte(original), &payload); err != nil {
						t.Fatal(err)
					}
					payload.UserIDs = []uint{2}
					raw, err := json.Marshal(payload)
					if err != nil {
						t.Fatal(err)
					}
					if err := writer.Set(ctx, proxyKeyPrefix+"old-hash", raw, 0).Err(); err != nil {
						t.Fatal(err)
					}
				}
			}})
			limited, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
			defer cancel()
			worker, _, err := queue.GetNextProxyContext(limited)
			if mutation == "updated" {
				if err != nil || len(worker.Workspaces) != 1 || worker.Workspaces[0].ID != 2 {
					t.Fatalf("migration overwrote changed source: %+v error=%v", worker.Workspaces, err)
				}
				assertAliasConsumed(t, queue, client, "old-hash")
			} else {
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("stale source granted a canonical check: %v", err)
				}
				if client.Exists(ctx, proxyKeyPrefix+string(proxy.Hash)).Val() != 0 {
					t.Fatal("stale source recreated canonical payload")
				}
			}
		})
	}
}

func TestLegacyAliasMigrationRecoversExpiredCanonicalLease(t *testing.T) {
	queue, client, proxy := migrationLeaseFixture(t)
	ctx := context.Background()
	if err := queue.AddToQueue([]domain.Proxy{proxy}); err != nil {
		t.Fatal(err)
	}
	oldWorker, scheduled, err := queue.GetNextProxyContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	oldWorker.QueueLease.ScoreMS = time.Now().Add(-time.Second).UnixMilli()
	if err := client.ZAdd(ctx, oldWorker.QueueLease.QueueKey, redis.Z{
		Score: float64(oldWorker.QueueLease.ScoreMS) + leaseScoreMarker, Member: string(proxy.Hash),
	}).Err(); err != nil {
		t.Fatal(err)
	}
	// Make the alias the oldest ready member so it performs the reclamation.
	seedLegacyAlias(t, queue, client, proxy, "old-hash", []uint{2}, false)
	worker, _, err := queue.GetNextProxyContext(ctx)
	if err != nil || worker.ID != proxy.ID || !bytes.Equal(worker.Hash, proxy.Hash) || len(worker.Workspaces) != 2 {
		t.Fatalf("expired canonical route not recovered with merged owners: %+v error=%v", worker, err)
	}
	assertAliasConsumed(t, queue, client, "old-hash")
	if err := queue.RequeueProxy(oldWorker, scheduled); !errors.Is(err, ErrProxyLeaseLost) {
		t.Fatalf("expired canonical worker was not fenced: %v", err)
	}
}
