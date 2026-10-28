package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"magpie/internal/database"
	"magpie/internal/domain"

	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

func sharedStatistics(count, owners int) []domain.ProxyStatistic {
	evidence := make([]domain.WorkspaceCheckEvidence, owners)
	workspaceIDs := make([]uint, owners)
	for index := range evidence {
		evidence[index] = domain.WorkspaceCheckEvidence{WorkspaceID: uint(index + 1), ConfigKey: "current", Alive: true}
		workspaceIDs[index] = uint(index + 1)
	}
	stats := make([]domain.ProxyStatistic, count)
	for index := range stats {
		stats[index] = domain.ProxyStatistic{ProxyID: 1, ProtocolID: 1, JudgeID: 1, Alive: true,
			CheckEvidence: evidence, WorkspaceIDs: workspaceIDs, CreatedAt: time.Now().UTC().Add(time.Duration(index)), ResponseBody: fmt.Sprintf("input-%d", index)}
	}
	return stats
}

func statisticsPrefixFixture(t *testing.T) (*gorm.DB, *redis.Client, proxyStatisticStreamConfig) {
	t.Helper()
	db, client, cfg := statisticsRecoveryFixture(t)
	if err := db.AutoMigrate(&domain.WorkspaceUsagePeriod{}, &domain.ProxyReputationRefresh{}); err != nil {
		t.Fatal(err)
	}
	return db, client, cfg
}

func assertStatisticsEffects(t *testing.T, db *gorm.DB, want int64, owners int) {
	t.Helper()
	var rows, checks int64
	if err := db.Model(&domain.ProxyStatistic{}).Count(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&domain.ProxyDailyCheck{}).Select("COALESCE(SUM(checks_count),0)").Scan(&checks).Error; err != nil {
		t.Fatal(err)
	}
	if rows != want || checks != want {
		t.Fatalf("history=%d checks=%d want=%d", rows, checks, want)
	}
	var usage []domain.WorkspaceUsagePeriod
	if err := db.Order("workspace_id").Find(&usage).Error; err != nil {
		t.Fatal(err)
	}
	if want == 0 {
		owners = 0
	}
	if len(usage) != owners {
		t.Fatalf("usage owners=%d want=%d", len(usage), owners)
	}
	for index, period := range usage {
		if period.WorkspaceID != uint(index+1) || period.CheckAttempts != uint64(want) {
			t.Fatalf("usage[%d]=%+v want attempts=%d", index, period, want)
		}
	}
	var latest int64
	if err := db.Model(&domain.ProxyLatestStatistic{}).Count(&latest).Error; err != nil || latest != int64(owners) {
		t.Fatalf("latest rows=%d want=%d error=%v", latest, owners, err)
	}
	var refreshes []domain.ProxyReputationRefresh
	if err := db.Find(&refreshes).Error; err != nil {
		t.Fatal(err)
	}
	if want == 0 {
		if len(refreshes) != 0 {
			t.Fatalf("uncommitted results queued reputation work: %+v", refreshes)
		}
	} else if len(refreshes) != 1 || refreshes[0].ProxyID != 1 || refreshes[0].Version == 0 {
		t.Fatalf("durable results did not queue reputation work: %+v", refreshes)
	}
}

func TestStatisticsPrefixUsesExpandedEvidenceBudget(t *testing.T) {
	for _, tc := range []struct{ count, owners, end, cost int }{
		{5001, 0, 5000, 5000}, {5000, 1, 5000, 5000}, {630, 8, 625, 5000}, {2, 5001, 1, 5001},
	} {
		end, cost := boundedStatisticsPrefix(sharedStatistics(tc.count, tc.owners))
		if end != tc.end || cost != tc.cost {
			t.Fatalf("count=%d owners=%d prefix=%d/%d want=%d/%d", tc.count, tc.owners, end, cost, tc.end, tc.cost)
		}
	}
	invalid := sharedStatistics(5001, 0)
	invalid[0].CheckEvidence = []domain.WorkspaceCheckEvidence{{WorkspaceID: 0, ConfigKey: "bad"}, {WorkspaceID: 1}}
	if end, cost := boundedStatisticsPrefix(invalid); end != 5000 || cost != 5000 {
		t.Fatalf("invalid evidence bypassed physical bound: %d/%d", end, cost)
	}
}

func TestStatisticsMemoryPrefixCommitSurvivesSuffixFailure(t *testing.T) {
	for _, deleted := range []bool{false, true} {
		t.Run(fmt.Sprintf("deleted-%t", deleted), func(t *testing.T) {
			db, _, _ := statisticsPrefixFixture(t)
			buffer := sharedStatistics(630, 8)
			original := buffer
			if deleted {
				buffer[10].ProxyID = 999
			}
			calls := 0
			if err := db.Callback().Create().Before("gorm:create").Register("fail_second_statistics_prefix", func(tx *gorm.DB) {
				if tx.Statement.Table == "proxy_statistics" {
					calls++
					if calls == 2 {
						tx.AddError(errors.New("injected suffix failure"))
					}
				}
			}); err != nil {
				t.Fatal(err)
			}
			if _, flushed := flushProxyStatisticsContext(context.Background(), &buffer); flushed || len(buffer) != 5 {
				t.Fatalf("failed suffix buffer=%d flushed=%t", len(buffer), flushed)
			}
			if buffer[0].ResponseBody != "input-625" || original[0].ResponseBody != "" || original[0].CheckEvidence != nil {
				t.Fatal("original-prefix progress or payload release changed")
			}
			want := int64(625)
			if deleted {
				want--
			}
			assertStatisticsEffects(t, db, want, 8)
			if _, flushed := flushProxyStatisticsContext(context.Background(), &buffer); !flushed || len(buffer) != 0 {
				t.Fatal("failed memory suffix did not recover")
			}
			assertStatisticsEffects(t, db, want+5, 8)
		})
	}
}

func TestStatisticsPrefixSharesDeadlineAndRetainsCanceledSuffix(t *testing.T) {
	db, _, _ := statisticsPrefixFixture(t)
	buffer := sharedStatistics(630, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var deadlines []time.Time
	var prepareDeadline time.Time
	if err := db.Callback().Query().Before("gorm:query").Register("observe_statistics_prepare_deadline", func(tx *gorm.DB) {
		if prepareDeadline.IsZero() {
			prepareDeadline, _ = tx.Statement.Context.Deadline()
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Callback().Create().Before("gorm:create").Register("cancel_second_statistics_prefix", func(tx *gorm.DB) {
		if tx.Statement.Table == "proxy_statistics" {
			deadline, ok := tx.Statement.Context.Deadline()
			if !ok {
				t.Fatal("statistics transaction lacks a deadline")
			}
			deadlines = append(deadlines, deadline)
			if len(deadlines) == 2 {
				cancel()
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	if _, flushed := flushProxyStatisticsContext(ctx, &buffer); flushed || len(buffer) != 5 {
		t.Fatalf("canceled suffix buffer=%d flushed=%t", len(buffer), flushed)
	}
	if len(deadlines) != 2 || !deadlines[0].Equal(deadlines[1]) || !prepareDeadline.Equal(deadlines[0]) {
		t.Fatalf("chunking reset the flush deadline: prepare=%v transactions=%v", prepareDeadline, deadlines)
	}
	assertStatisticsEffects(t, db, 625, 8)
	if _, flushed := flushProxyStatisticsContext(context.Background(), &buffer); !flushed {
		t.Fatal("canceled suffix did not recover")
	}
	assertStatisticsEffects(t, db, 630, 8)
}

func TestStatisticsOversizedEventRemainsAtomic(t *testing.T) {
	db, _, _ := statisticsPrefixFixture(t)
	if db.Dialector.Name() != "postgres" {
		t.Skip("5001 distinct latest-evidence owners require PostgreSQL parameter capacity")
	}
	buffer := sharedStatistics(2, statisticsLatestRowBudget+1)
	var historyCreates []int
	if err := db.Callback().Create().Before("gorm:create").Register("observe_indivisible_statistics", func(tx *gorm.DB) {
		if tx.Statement.Table == "proxy_statistics" {
			historyCreates = append(historyCreates, tx.Statement.ReflectValue.Len())
		}
	}); err != nil {
		t.Fatal(err)
	}
	if _, flushed := flushProxyStatisticsContext(context.Background(), &buffer); !flushed {
		t.Fatal("indivisible events did not persist")
	}
	if fmt.Sprint(historyCreates) != "[1 1]" {
		t.Fatalf("oversized events shared a transaction: %v", historyCreates)
	}
	assertStatisticsEffects(t, db, 2, statisticsLatestRowBudget+1)
}

type prefixAckHook struct {
	reject    atomic.Bool
	succeeded atomic.Int32
}

func (h *prefixAckHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *prefixAckHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h *prefixAckHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if isStatisticAckCommand(cmd) && h.reject.Load() {
			return errors.New("injected prefix acknowledgement failure")
		}
		err := next(ctx, cmd)
		if isStatisticAckCommand(cmd) && err == nil {
			h.succeeded.Add(1)
		}
		return err
	}
}

func TestStatisticsStreamPrefixAcknowledgesOnlyDurableOriginalIDs(t *testing.T) {
	db, client, cfg := statisticsPrefixFixture(t)
	ctx := context.Background()
	if err := client.XGroupCreateMkStream(ctx, cfg.streamKey, cfg.groupName, "0").Err(); err != nil {
		t.Fatal(err)
	}
	stats := sharedStatistics(630, 8)
	stats[10].ProxyID = 999 // Deleted within the first original prefix.
	for index, stat := range stats {
		if index == 3 || index == 627 {
			if err := client.XAdd(ctx, &redis.XAddArgs{Stream: cfg.streamKey, Values: map[string]any{"stat": "{"}}).Err(); err != nil {
				t.Fatal(err)
			}
		}
		payload, _ := json.Marshal(stat)
		if err := client.XAdd(ctx, &redis.XAddArgs{Stream: cfg.streamKey, Values: map[string]any{"stat": string(payload)}}).Err(); err != nil {
			t.Fatal(err)
		}
	}
	streams, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{Group: cfg.groupName, Consumer: "worker", Streams: []string{cfg.streamKey, ">"}, Count: 1000}).Result()
	if err != nil {
		t.Fatal(err)
	}
	hook := &prefixAckHook{}
	hook.reject.Store(true)
	client.AddHook(hook)
	var buffer []domain.ProxyStatistic
	var ids []string
	appendStreamMessagesToBuffer(ctx, streams[0].Messages, &buffer, &ids, client, cfg)
	if len(buffer) != 630 || len(ids) != 632 {
		t.Fatalf("malformed IDs lost their independent state: stats=%d ids=%d", len(buffer), len(ids))
	}
	hook.reject.Store(false)
	calls := 0
	if err := db.Callback().Create().Before("gorm:create").Register("fail_stream_suffix_and_ack", func(tx *gorm.DB) {
		if tx.Statement.Table == "proxy_statistics" {
			calls++
			if calls == 2 {
				hook.reject.Store(true)
				tx.AddError(errors.New("injected stream suffix failure"))
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	if flushStreamStatisticsBuffer(ctx, &buffer, &ids, client, cfg) || len(buffer) != 5 || len(ids) != 630 {
		t.Fatalf("partial state stats=%d ids=%d", len(buffer), len(ids))
	}
	assertStatisticsEffects(t, db, 624, 8)
	// ACK failure must be retried before the uncommitted suffix is inserted.
	if flushStreamStatisticsBuffer(ctx, &buffer, &ids, client, cfg) || calls != 2 {
		t.Fatal("pending ACK-only prefix was bypassed")
	}
	hook.reject.Store(false)
	if !flushStreamStatisticsBuffer(ctx, &buffer, &ids, client, cfg) {
		t.Fatal("stream prefix/suffix did not recover")
	}
	assertStatisticsEffects(t, db, 629, 8)
	pending, err := client.XPending(ctx, cfg.streamKey, cfg.groupName).Result()
	if err != nil || pending.Count != 0 || len(ids) != 0 {
		t.Fatalf("recovery left pending IDs: %+v error=%v", pending, err)
	}
}

func TestStatisticsStreamRejectsMissingMetadataBeforeAnySideEffect(t *testing.T) {
	for _, mode := range []string{"missing-id", "wrong-stream", "untracked-id"} {
		t.Run(mode, func(t *testing.T) {
			db, client, cfg := statisticsPrefixFixture(t)
			seedPendingStatistics(t, client, cfg, 1)
			messages, err := client.XRange(context.Background(), cfg.streamKey, "-", "+").Result()
			if err != nil {
				t.Fatal(err)
			}
			buffer := sharedStatistics(1, 8)
			buffer[0].EventStream, buffer[0].EventID = cfg.streamKey, messages[0].ID
			ids := []string{messages[0].ID}
			switch mode {
			case "missing-id":
				buffer[0].EventID = ""
			case "wrong-stream":
				buffer[0].EventStream = "another"
			case "untracked-id":
				ids = []string{"other"}
			}
			if flushStreamStatisticsBuffer(context.Background(), &buffer, &ids, client, cfg) || len(buffer) != 1 || len(ids) != 1 {
				t.Fatal("invalid metadata was consumed")
			}
			assertStatisticsEffects(t, db, 0, 8)
			pending, err := client.XPending(context.Background(), cfg.streamKey, cfg.groupName).Result()
			if err != nil || pending.Count != 1 {
				t.Fatalf("invalid metadata acknowledged pending ID: %+v error=%v", pending, err)
			}
		})
	}
}

func pendingSharedStatistics(t *testing.T, client *redis.Client, cfg proxyStatisticStreamConfig, stats []domain.ProxyStatistic) ([]domain.ProxyStatistic, []string) {
	t.Helper()
	ctx := context.Background()
	if err := client.XGroupCreateMkStream(ctx, cfg.streamKey, cfg.groupName, "0").Err(); err != nil {
		t.Fatal(err)
	}
	pipe := client.Pipeline()
	for _, stat := range stats {
		payload, err := json.Marshal(stat)
		if err != nil {
			t.Fatal(err)
		}
		pipe.XAdd(ctx, &redis.XAddArgs{Stream: cfg.streamKey, Values: map[string]any{"stat": string(payload)}})
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	streams, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{Group: cfg.groupName, Consumer: "worker", Streams: []string{cfg.streamKey, ">"}, Count: int64(len(stats)), Block: -1}).Result()
	if err != nil || len(streams) != 1 {
		t.Fatalf("stream fixture=%v error=%v", streams, err)
	}
	var buffer []domain.ProxyStatistic
	var ids []string
	if added := appendStreamMessagesToBuffer(ctx, streams[0].Messages, &buffer, &ids, client, cfg); added != len(stats) {
		t.Fatalf("decoded=%d want=%d", added, len(stats))
	}
	return buffer, ids
}

func recoverSharedStatistics(t *testing.T, client *redis.Client, cfg proxyStatisticStreamConfig) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := startRecoveryWorker(t, ctx, cancel)
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		pending, err := client.XPending(context.Background(), cfg.streamKey, cfg.groupName).Result()
		if err != nil {
			t.Fatal(err)
		}
		if pending.Count == 0 {
			break
		}
		select {
		case <-deadline.C:
			t.Fatal("shared statistics did not recover after restart")
		case <-ticker.C:
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("recovery worker did not stop")
	}
}

func TestStatisticsStreamReplayPrefixRetiresSafelyBeforeFailedSuffixRecovers(t *testing.T) {
	db, client, cfg := statisticsPrefixFixture(t)
	ctx := context.Background()
	buffer, ids := pendingSharedStatistics(t, client, cfg, sharedStatistics(630, 8))
	replay := append([]domain.ProxyStatistic(nil), buffer[:625]...)
	if err := database.InsertProxyStatistics(ctx, replay, database.CalculateProxyStatisticBatchSize(len(replay))); err != nil {
		t.Fatal(err)
	}
	failed := false
	if err := db.Callback().Create().Before("gorm:create").Register("fail_first_new_statistics_after_replay", func(tx *gorm.DB) {
		if tx.Statement.Table == "proxy_statistics" && !failed {
			failed = true
			tx.AddError(errors.New("injected new suffix failure"))
		}
	}); err != nil {
		t.Fatal(err)
	}
	if flushStreamStatisticsBuffer(ctx, &buffer, &ids, client, cfg) || len(buffer) != 5 || len(ids) != 5 {
		t.Fatalf("replayed original prefix was not handled: stats=%d ids=%d", len(buffer), len(ids))
	}
	assertStatisticsEffects(t, db, 625, 8)
	// Retirement between commits must use the oldest pending suffix event,
	// even though the earlier page accepted no new ledger/history rows.
	if err := maintainProxyStatisticStream(ctx, client, cfg); err != nil {
		t.Fatal(err)
	}
	var floor domain.ProxyStatisticStream
	if err := db.First(&floor, "stream=?", cfg.streamKey).Error; err != nil || floor.ReplayBefore != database.ProxyStatisticEventSortID(ids[0]) {
		t.Fatalf("retirement crossed pending suffix: floor=%+v error=%v", floor, err)
	}
	var ledger int64
	if err := db.Model(&domain.ProxyStatisticEvent{}).Count(&ledger).Error; err != nil || ledger != 0 {
		t.Fatalf("committed prefix identities did not retire: ledger=%d error=%v", ledger, err)
	}
	recoverSharedStatistics(t, client, cfg)
	assertStatisticsEffects(t, db, 630, 8)
	if err := database.InsertProxyStatistics(ctx, replay, database.CalculateProxyStatisticBatchSize(len(replay))); err != nil {
		t.Fatal(err)
	}
	assertStatisticsEffects(t, db, 630, 8)
}

func TestStatisticsStreamCanceledPrefixAckRecoversAfterRestart(t *testing.T) {
	db, client, cfg := statisticsPrefixFixture(t)
	buffer, ids := pendingSharedStatistics(t, client, cfg, sharedStatistics(630, 8))
	calls := 0
	if err := db.Callback().Create().Before("gorm:create").Register("fail_suffix_before_canceled_ack", func(tx *gorm.DB) {
		if tx.Statement.Table == "proxy_statistics" {
			calls++
			if calls == 2 {
				tx.AddError(errors.New("injected suffix failure before canceled acknowledgement"))
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client.AddHook(&cancelStatisticsAckHook{cancel: cancel})
	if flushStreamStatisticsBuffer(ctx, &buffer, &ids, client, cfg) || len(buffer) != 5 || len(ids) != 630 || ctx.Err() == nil {
		t.Fatalf("canceled prefix acknowledgement state stats=%d ids=%d error=%v", len(buffer), len(ids), ctx.Err())
	}
	assertStatisticsEffects(t, db, 625, 8)
	// The new worker sees both the durable prefix and uncommitted suffix in
	// Redis. The ledger must charge and count each original exactly once.
	recoverSharedStatistics(t, client, cfg)
	assertStatisticsEffects(t, db, 630, 8)
}

func TestStatisticsStreamDeletedOnlyPrefixFinishesOriginals(t *testing.T) {
	db, client, cfg := statisticsPrefixFixture(t)
	stats := sharedStatistics(630, 8)
	for index := 0; index < 625; index++ {
		stats[index].ProxyID = 999
	}
	buffer, ids := pendingSharedStatistics(t, client, cfg, stats)
	if !flushStreamStatisticsBuffer(context.Background(), &buffer, &ids, client, cfg) || len(buffer) != 0 || len(ids) != 0 {
		t.Fatal("deleted-only original prefix was not consumed")
	}
	assertStatisticsEffects(t, db, 5, 8)
	var ledger int64
	if err := db.Model(&domain.ProxyStatisticEvent{}).Count(&ledger).Error; err != nil || ledger != 5 {
		t.Fatalf("deleted events entered ledger: count=%d error=%v", ledger, err)
	}
}

func TestStatisticsStreamPrefixKeepsAmortizedAckCount(t *testing.T) {
	for _, owners := range []int{1, 8} {
		t.Run(fmt.Sprint(owners), func(t *testing.T) {
			db, client, cfg := statisticsPrefixFixture(t)
			if db.Dialector.Name() != "postgres" {
				t.Skip("5000-event command-count measurement requires PostgreSQL parameter capacity")
			}
			buffer, ids := pendingSharedStatistics(t, client, cfg, sharedStatistics(statisticsBatchThreshold, owners))
			// Separate cold script loading from the normal consumer command count.
			if err := client.ScriptLoad(context.Background(), ackDeleteStatisticScriptSource).Err(); err != nil {
				t.Fatal(err)
			}
			hook := &prefixAckHook{}
			client.AddHook(hook)
			if !flushStreamStatisticsBuffer(context.Background(), &buffer, &ids, client, cfg) {
				t.Fatal("full consumer flush failed")
			}
			if calls := hook.succeeded.Load(); calls != statisticsBatchThreshold/statisticsStreamAckBatchCap {
				t.Fatalf("owners=%d added acknowledgement calls: %d", owners, calls)
			}
			assertStatisticsEffects(t, db, statisticsBatchThreshold, owners)
			t.Logf("owners=%d physical_events=%d successful_ack_lua_calls=%d", owners, statisticsBatchThreshold, hook.succeeded.Load())
		})
	}
}
