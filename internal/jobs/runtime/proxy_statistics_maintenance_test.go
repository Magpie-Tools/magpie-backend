package runtime

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"magpie/internal/database"
	"magpie/internal/domain"
	"magpie/internal/support"
)

func TestStreamMaintenancePreservesPendingAndRejectsRetiredReplay(t *testing.T) {
	db, client, cfg := statisticsRecoveryFixture(t)
	ctx := context.Background()
	seedPendingStatistics(t, client, cfg, 3)
	pending, err := client.XPendingExt(ctx, &redis.XPendingExtArgs{Stream: cfg.streamKey, Group: cfg.groupName, Start: "-", End: "+", Count: 10}).Result()
	if err != nil || len(pending) != 3 {
		t.Fatalf("pending=%v err=%v", pending, err)
	}
	stats := []domain.ProxyStatistic{}
	for _, event := range pending {
		stats = append(stats, domain.ProxyStatistic{ProxyID: 1, ProtocolID: 1, JudgeID: 1, Alive: true, EventStream: cfg.streamKey, EventID: event.ID})
	}
	if err := database.InsertProxyStatistics(ctx, stats, 3); err != nil {
		t.Fatal(err)
	}
	// An older backend acknowledged one entry but failed to delete it.
	if err := client.XAck(ctx, cfg.streamKey, cfg.groupName, pending[0].ID).Err(); err != nil {
		t.Fatal(err)
	}
	if err := maintainProxyStatisticStream(ctx, client, cfg); err != nil {
		t.Fatal(err)
	}
	var ledgerCount int64
	if err := db.Model(&domain.ProxyStatisticEvent{}).Count(&ledgerCount).Error; err != nil || ledgerCount != 2 {
		t.Fatalf("ledger rows=%d err=%v", ledgerCount, err)
	}
	if entries, err := client.XPending(ctx, cfg.streamKey, cfg.groupName).Result(); err != nil || entries.Count != 2 {
		t.Fatalf("pending removed: %+v %v", entries, err)
	}
	if _, err := ackAndDeleteStreamMessageIDs(ctx, []string{pending[1].ID, pending[2].ID}, client, cfg); err != nil {
		t.Fatal(err)
	}
	// New entries remain unread and must survive maintenance as well.
	newID, err := client.XAdd(ctx, &redis.XAddArgs{Stream: cfg.streamKey, Values: map[string]any{"stat": "new"}}).Result()
	if err != nil {
		t.Fatal(err)
	}
	if err := maintainProxyStatisticStream(ctx, client, cfg); err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&domain.ProxyStatisticEvent{}).Count(&ledgerCount).Error; err != nil || ledgerCount != 0 {
		t.Fatalf("ledger rows=%d err=%v", ledgerCount, err)
	}
	if messages, err := client.XRange(ctx, cfg.streamKey, newID, newID).Result(); err != nil || len(messages) != 1 {
		t.Fatalf("unread entry removed: %+v %v", messages, err)
	}
	if err := database.InsertProxyStatistics(ctx, stats, 3); err != nil {
		t.Fatal(err)
	}
	var count int64
	if err := db.Model(&domain.ProxyStatistic{}).Count(&count).Error; err != nil || count != 3 {
		t.Fatalf("retired replay counted: %d %v", count, err)
	}
}

type rejectStatisticAckScriptHook struct{ reject bool }

func (h *rejectStatisticAckScriptHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *rejectStatisticAckScriptHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h *rejectStatisticAckScriptHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if h.reject && isStatisticAckCommand(cmd) {
			return fmt.Errorf("injected script transport error")
		}
		return next(ctx, cmd)
	}
}

func TestFailedAtomicAckKeepsRetryState(t *testing.T) {
	_, client, cfg := statisticsRecoveryFixture(t)
	seedPendingStatistics(t, client, cfg, 1)
	ctx := context.Background()
	pending, err := client.XPendingExt(ctx, &redis.XPendingExtArgs{Stream: cfg.streamKey, Group: cfg.groupName, Start: "-", End: "+", Count: 10}).Result()
	if err != nil {
		t.Fatal(err)
	}
	hook := &rejectStatisticAckScriptHook{reject: true}
	client.AddHook(hook)
	ids := []string{pending[0].ID}
	buffer := []domain.ProxyStatistic{}
	if flushStreamStatisticsBuffer(ctx, &buffer, &ids, client, cfg) || len(ids) != 1 {
		t.Fatal("failed acknowledgment discarded retry state")
	}
	if size, err := client.XLen(ctx, cfg.streamKey).Result(); err != nil || size != 1 {
		t.Fatalf("entry lost: %d %v", size, err)
	}
	hook.reject = false
	if !flushStreamStatisticsBuffer(ctx, &buffer, &ids, client, cfg) || len(ids) != 0 {
		t.Fatal("acknowledgment did not recover")
	}
	if size, err := client.XLen(ctx, cfg.streamKey).Result(); err != nil || size != 0 {
		t.Fatalf("entry retained: %d %v", size, err)
	}
}

func TestWorkspaceTrafficRetiresOldPeriodsAfterSuccessfulFlush(t *testing.T) {
	oldRecord := recordWorkspaceManagedTrafficBatch
	t.Cleanup(func() { recordWorkspaceManagedTrafficBatch = oldRecord; workspaceTrafficCounters.Clear() })
	workspaceTrafficCounters.Clear()
	key := workspaceTrafficKey{WorkspaceID: 7, PeriodStart: time.Now().AddDate(0, -1, 0).Unix()}
	counter := &workspaceTrafficCounter{}
	counter.Requests.Store(3)
	workspaceTrafficCounters.Store(key, counter)
	recordWorkspaceManagedTrafficBatch = func(context.Context, []database.WorkspaceManagedTrafficSample) error {
		return fmt.Errorf("temporary failure")
	}
	if err := flushWorkspaceManagedTraffic(context.Background()); err == nil {
		t.Fatal("expected failure")
	}
	if _, ok := workspaceTrafficCounters.Load(key); !ok {
		t.Fatal("unflushed period retired")
	}
	recordWorkspaceManagedTrafficBatch = func(context.Context, []database.WorkspaceManagedTrafficSample) error { return nil }
	if err := flushWorkspaceManagedTraffic(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok := workspaceTrafficCounters.Load(key); ok {
		t.Fatal("drained old period retained")
	}
}

func TestStatisticsStreamStartsAfterInitialRedisOutage(t *testing.T) {
	db, _, cfg := statisticsRecoveryFixture(t)
	if err := db.AutoMigrate(&domain.ProxyReputationRefresh{}); err != nil {
		t.Fatal(err)
	}
	oldReady := proxyStatisticStreamReady.Load()
	oldQueue := proxyStatisticQueue
	proxyStatisticStreamReady.Store(false)
	proxyStatisticQueue = make(chan domain.ProxyStatistic, 10)
	support.CloseRedisClient()
	t.Cleanup(func() {
		support.CloseRedisClient()
		proxyStatisticStreamReady.Store(oldReady)
		proxyStatisticQueue = oldQueue
	})
	t.Setenv("REDIS_MODE", "single")
	t.Setenv("REDIS_URL", "redis://127.0.0.1:1?dial_timeout=10ms&read_timeout=10ms&max_retries=0")
	t.Setenv("REDIS_PASSWORD", "")
	t.Setenv("REDIS_CONNECT_RETRY_BACKOFF_MS", "1")
	t.Setenv(envProxyStatisticsRedisStreamEnabled, "true")
	t.Setenv(envProxyStatisticsRedisStreamKey, cfg.streamKey)
	t.Setenv(envProxyStatisticsRedisStreamGroup, cfg.groupName)
	t.Setenv(envProxyStatisticsIngestWorkers, "1")
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); StartProxyStatisticsRoutine(ctx) }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("statistics routine did not stop")
		}
	}()
	deadline := time.Now().Add(3 * time.Second)
	for support.GetRedisClientStatus().LastError == "" && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if proxyStatisticStreamReady.Load() {
		t.Fatal("stream ready while Redis unavailable")
	}
	server := miniredis.RunT(t)
	t.Setenv("REDIS_URL", "redis://"+server.Addr())
	for !proxyStatisticStreamReady.Load() {
		select {
		case <-ctx.Done():
			t.Fatal("stream never recovered")
		case <-time.After(10 * time.Millisecond):
		}
	}
	client, current := proxyStatisticStreamSnapshot()
	groups, err := client.XInfoGroups(ctx, current.streamKey).Result()
	if err != nil || len(groups) != 1 {
		t.Fatalf("consumer group not initialized: %+v %v", groups, err)
	}
}
