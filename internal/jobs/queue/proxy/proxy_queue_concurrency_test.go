package proxyqueue

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"magpie/internal/domain"
	"magpie/internal/security"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func queueConcurrencyFixture(t *testing.T) (*RedisProxyQueue, *redis.Client, domain.Proxy) {
	t.Helper()
	t.Setenv(envEncryptQueueCredentials, "false")
	var options *redis.Options
	if url := os.Getenv("MAGPIE_TEST_QUEUE_REDIS_URL"); url != "" {
		var err error
		options, err = redis.ParseURL(url)
		if err != nil {
			t.Fatal(err)
		}
	} else {
		server := miniredis.RunT(t)
		options = &redis.Options{Addr: server.Addr()}
	}
	client := redis.NewClient(options)
	// The optional real-Redis fixture requires its own disposable database,
	// separate from concurrently running statistics tests and benchmarks.
	if err := client.FlushDB(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = client.FlushDB(context.Background()).Err()
		_ = client.Close()
	})
	queue := NewRedisProxyQueue(client)
	proxy := domain.Proxy{ID: 1, IP: "192.0.2.1", Port: 8080, Hash: []byte("stored-route-hash"), Workspaces: []domain.Workspace{{ID: 1}}}
	return queue, client, proxy
}

type beforeHeadRepairHook struct {
	once    sync.Once
	enqueue func()
}

func (h *beforeHeadRepairHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *beforeHeadRepairHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() == "evalsha" || cmd.Name() == "eval" {
			h.once.Do(h.enqueue)
		}
		return next(ctx, cmd)
	}
}
func (h *beforeHeadRepairHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		if len(cmds) > 0 && cmds[0].Name() == "del" {
			h.once.Do(h.enqueue)
		}
		return next(ctx, cmds)
	}
}

func TestQueueHeadRepairPreservesConcurrentEnqueue(t *testing.T) {
	queue, client, proxy := queueConcurrencyFixture(t)
	writer := redis.NewClient(client.Options())
	t.Cleanup(func() { _ = writer.Close() })
	writerQueue := NewRedisProxyQueue(writer)
	client.AddHook(&beforeHeadRepairHook{enqueue: func() {
		if err := writerQueue.AddToQueue([]domain.Proxy{proxy}); err != nil {
			t.Fatal(err)
		}
	}})
	if err := queue.refreshQueueHeads(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, _, err := queue.GetNextProxyContext(ctx)
	if err != nil || got.ID != proxy.ID {
		t.Fatalf("concurrent addition stranded: %+v %v", got, err)
	}
}

func TestOwnershipChangingCompletionPreservesNewWorkspace(t *testing.T) {
	queue, client, proxy := queueConcurrencyFixture(t)
	ctx := context.Background()
	if err := client.Set(ctx, queueRescheduleStateKey, "1", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := queue.AddToQueue([]domain.Proxy{proxy}); err != nil {
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
	if err := queue.RemoveFromQueue([]domain.Proxy{worker}); !errors.Is(err, ErrProxyLeaseLost) {
		t.Fatalf("stale orphan removal erased a new owner: %v", err)
	}
	worker.Workspaces = nil
	if err := queue.RequeueProxyWithPayload(worker, scheduled); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	next, _, err := queue.GetNextProxyContext(ctx)
	if err != nil || len(next.Workspaces) != 1 || next.Workspaces[0].ID != 2 {
		t.Fatalf("new workspace erased: %+v %v", next.Workspaces, err)
	}
}

func TestCurrentPayloadInLegacyShardCompletesWithItsOwnLease(t *testing.T) {
	queue, client, proxy := queueConcurrencyFixture(t)
	ctx := context.Background()
	raw, err := marshalQueuedProxy(proxy)
	if err != nil {
		t.Fatal(err)
	}
	member := string(proxy.Hash)
	if err := client.Set(ctx, proxyKeyPrefix+member, raw, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.ZAdd(ctx, legacyQueueKey, redis.Z{Score: 0, Member: member}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := queue.refreshQueueHeads(); err != nil {
		t.Fatal(err)
	}
	worker, scheduled, err := queue.GetNextProxyContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if worker.QueueLease.QueueKey != legacyQueueKey {
		t.Fatalf("wrong lease shard: %s", worker.QueueLease.QueueKey)
	}
	if err := queue.RequeueProxy(worker, scheduled); err != nil {
		t.Fatal(err)
	}
	if err := client.ZScore(ctx, legacyQueueKey, member).Err(); err != redis.Nil {
		t.Fatalf("legacy scheduling retained: %v", err)
	}
	if err := client.ZScore(ctx, queue.queueKeyForMember(member), member).Err(); err != nil {
		t.Fatalf("sharded scheduling absent: %v", err)
	}
}

func TestProxyLeaseRenewalAndStaleCompletionFencing(t *testing.T) {
	queue, client, proxy := queueConcurrencyFixture(t)
	ctx := context.Background()
	if err := queue.AddToQueue([]domain.Proxy{proxy}); err != nil {
		t.Fatal(err)
	}
	worker, scheduled, err := queue.GetNextProxyContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	oldExpiry := time.Now().Add(time.Minute).UnixMilli()
	worker.QueueLease.ScoreMS = oldExpiry
	if err := client.ZAdd(ctx, worker.QueueLease.QueueKey, redis.Z{Score: float64(oldExpiry) + leaseScoreMarker, Member: string(worker.Hash)}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := queue.refreshQueueHeads(); err != nil {
		t.Fatal(err)
	}
	if err := queue.RenewLeaseIfNeeded(ctx, worker); err != nil {
		t.Fatal(err)
	}
	if worker.QueueLease.ScoreMS <= oldExpiry {
		t.Fatal("lease was not extended")
	}
	result, err := queue.popScript.Run(ctx, client, []string{proxyQueueHeadKey}, oldExpiry+1, int64(processingLease/time.Millisecond), proxyKeyPrefix).Result()
	if err != nil {
		t.Fatal(err)
	}
	pop, err := parseProxyPopResult(result)
	if err != nil || pop.Found {
		t.Fatalf("renewed check was leased concurrently: %+v %v", pop, err)
	}
	newClaimTime := worker.QueueLease.ScoreMS + 1
	result, err = queue.popScript.Run(ctx, client, []string{proxyQueueHeadKey}, newClaimTime, int64(processingLease/time.Millisecond), proxyKeyPrefix).Result()
	if err != nil {
		t.Fatal(err)
	}
	pop, err = parseProxyPopResult(result)
	if err != nil || !pop.Found {
		t.Fatalf("expired check was not recovered: %+v %v", pop, err)
	}
	if err := queue.RequeueProxy(worker, scheduled); !errors.Is(err, ErrProxyLeaseLost) {
		t.Fatalf("stale completion accepted: %v", err)
	}
	if err := queue.RequeueProxyWithPayload(worker, scheduled); !errors.Is(err, ErrProxyLeaseLost) {
		t.Fatalf("stale payload replacement accepted: %v", err)
	}
	if err := queue.RemoveFromQueue([]domain.Proxy{worker}); !errors.Is(err, ErrProxyLeaseLost) {
		t.Fatalf("stale removal accepted: %v", err)
	}
	worker.QueueLease.ScoreMS = oldExpiry
	if err := queue.RenewLeaseIfNeeded(ctx, worker); !errors.Is(err, ErrProxyLeaseLost) {
		t.Fatalf("stale renewal accepted: %v", err)
	}
	score, err := client.ZScore(ctx, worker.QueueLease.QueueKey, string(worker.Hash)).Result()
	if err != nil || score != float64(newClaimTime+int64(processingLease/time.Millisecond))+leaseScoreMarker {
		t.Fatalf("stale worker changed newer scheduling: %.0f %v", score, err)
	}
}

type queueCommandHook struct{ commands []string }

func (h *queueCommandHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *queueCommandHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.commands = append(h.commands, cmd.Name())
		return next(ctx, cmd)
	}
}
func (h *queueCommandHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, cmd := range cmds {
			h.commands = append(h.commands, cmd.Name())
		}
		return next(ctx, cmds)
	}
}

func TestPlaintextProxyCycleCommandBudgetAndPayload(t *testing.T) {
	queue, client, proxy := queueConcurrencyFixture(t)
	t.Setenv("PROXY_ENCRYPTION_KEY", "")
	security.ResetProxyCipherForTests()
	t.Cleanup(security.ResetProxyCipherForTests)
	ctx := context.Background()
	if err := queue.AddToQueue([]domain.Proxy{proxy}); err != nil {
		t.Fatal(err)
	}
	if err := queue.popScript.Load(ctx, client).Err(); err != nil {
		t.Fatal(err)
	}
	if err := completeScript.Load(ctx, client).Err(); err != nil {
		t.Fatal(err)
	}
	original, err := client.Get(ctx, proxyKeyPrefix+string(proxy.Hash)).Result()
	if err != nil {
		t.Fatal(err)
	}
	hook := &queueCommandHook{}
	client.AddHook(hook)
	worker, scheduled, err := queue.GetNextProxyContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.RenewLeaseIfNeeded(ctx, worker); err != nil {
		t.Fatal(err)
	}
	if err := queue.RequeueProxy(worker, scheduled); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(hook.commands, []string{"evalsha", "get", "evalsha"}) {
		t.Fatalf("hot cycle commands = %v", hook.commands)
	}
	stored, err := client.Get(ctx, proxyKeyPrefix+string(proxy.Hash)).Result()
	if err != nil || stored != original {
		t.Fatalf("normal requeue rewrote payload: %v", err)
	}
	var payload queuedProxy
	if err := json.Unmarshal([]byte(stored), &payload); err != nil || string(payload.Hash) != string(proxy.Hash) {
		t.Fatalf("stored hash changed: %v", err)
	}
}
