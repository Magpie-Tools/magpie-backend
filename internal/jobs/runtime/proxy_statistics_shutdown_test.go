package runtime

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"magpie/internal/domain"

	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

type cancelStatisticsAckHook struct {
	once   sync.Once
	cancel context.CancelFunc
}

func (h *cancelStatisticsAckHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *cancelStatisticsAckHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h *cancelStatisticsAckHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() == "xack" {
			h.once.Do(h.cancel)
			if ctx.Err() != nil {
				return ctx.Err()
			}
		}
		return next(ctx, cmd)
	}
}

func startRecoveryWorker(t *testing.T, ctx context.Context, cancel context.CancelFunc) <-chan struct{} {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		runProxyStatisticsStreamWorker(ctx, "worker")
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("statistics worker did not stop")
		}
	})
	return done
}

func TestStatisticsCancellationDuringAckLeavesReplayableEvents(t *testing.T) {
	for _, count := range []int{1, 37, statisticsBatchThreshold + 1} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			if count > statisticsBatchThreshold && (os.Getenv("MAGPIE_TEST_POSTGRES_DSN") == "" || os.Getenv("MAGPIE_TEST_REDIS_URL") == "") {
				t.Skip("multi-page recovery requires disposable PostgreSQL and Redis")
			}
			db, client, cfg := statisticsRecoveryFixture(t)
			seedPendingStatistics(t, client, cfg, count)
			ctx, cancel := context.WithCancel(context.Background())
			client.AddHook(&cancelStatisticsAckHook{cancel: cancel})
			done := startRecoveryWorker(t, ctx, cancel)
			select {
			case <-ctx.Done():
			case <-time.After(15 * time.Second):
				t.Fatal("fixture did not reach acknowledgement")
			}
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("worker retried acknowledgements after cancellation")
			}
			var rows int64
			if err := db.Model(&domain.ProxyStatistic{}).Count(&rows).Error; err != nil || rows != int64(min(count, statisticsBatchThreshold)) {
				t.Fatalf("first batch did not commit before interrupted acknowledgement: rows=%d error=%v", rows, err)
			}
			pending, err := client.XPending(context.Background(), cfg.streamKey, cfg.groupName).Result()
			if err != nil || pending.Count != int64(count) {
				t.Fatalf("unacknowledged events lost: %+v error=%v", pending, err)
			}

			// Replay both the committed page and any uncommitted tail with the
			// same consumer. Each event must count exactly once after recovery.
			replayCtx, stopReplay := context.WithCancel(context.Background())
			replayed := startRecoveryWorker(t, replayCtx, stopReplay)
			deadline := time.NewTimer(5 * time.Second)
			defer deadline.Stop()
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			for {
				pending, err = client.XPending(context.Background(), cfg.streamKey, cfg.groupName).Result()
				if err != nil {
					t.Fatal(err)
				}
				if pending.Count == 0 {
					break
				}
				select {
				case <-deadline.C:
					t.Fatal("pending events did not recover after restart")
				case <-ticker.C:
				}
			}
			stopReplay()
			select {
			case <-replayed:
			case <-time.After(5 * time.Second):
				t.Fatal("recovered worker did not stop")
			}
			if err := db.Model(&domain.ProxyStatistic{}).Count(&rows).Error; err != nil || rows != int64(count) {
				t.Fatalf("replay duplicated or lost history: rows=%d error=%v", rows, err)
			}
			if err := db.Model(&domain.ProxyStatisticEvent{}).Count(&rows).Error; err != nil || rows != int64(count) {
				t.Fatalf("event ledger count=%d error=%v", rows, err)
			}
			if err := db.Model(&domain.ProxyDailyCheck{}).Select("SUM(checks_count)").Scan(&rows).Error; err != nil || rows != int64(count) {
				t.Fatalf("replay double-counted daily checks: count=%d error=%v", rows, err)
			}
		})
	}
}

func TestCancelledStatisticsFlushDoesNotAcknowledgeUncommittedEvents(t *testing.T) {
	db, client, cfg := statisticsRecoveryFixture(t)
	seedPendingStatistics(t, client, cfg, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	buffer := []domain.ProxyStatistic{{ProxyID: 1, ProtocolID: 1, Alive: true, CreatedAt: time.Now()}}
	ids := []string{"uncommitted"}
	if flushStreamStatisticsBuffer(ctx, &buffer, &ids, client, cfg) {
		t.Fatal("cancelled persistence succeeded")
	}
	if len(buffer) != 1 || len(ids) != 1 {
		t.Fatal("cancelled persistence discarded the pending batch")
	}
	var rows int64
	if err := db.Model(&domain.ProxyStatistic{}).Count(&rows).Error; err != nil || rows != 0 {
		t.Fatalf("cancelled persistence wrote history: rows=%d error=%v", rows, err)
	}
	pending, err := client.XPending(context.Background(), cfg.streamKey, cfg.groupName).Result()
	if err != nil || pending.Count != 1 {
		t.Fatalf("uncommitted event was acknowledged: %+v error=%v", pending, err)
	}
}

func TestStatisticsMemoryWorkerStopsAfterFailedBatchAndCancellation(t *testing.T) {
	db, _, _ := statisticsRecoveryFixture(t)
	oldQueue := proxyStatisticQueue
	proxyStatisticQueue = make(chan domain.ProxyStatistic, statisticsBatchThreshold)
	t.Cleanup(func() { proxyStatisticQueue = oldQueue })
	for i := 0; i < statisticsBatchThreshold; i++ {
		proxyStatisticQueue <- domain.ProxyStatistic{ProxyID: 1}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var queries atomic.Int32
	if err := db.Callback().Query().Before("gorm:query").Register("cancel_statistics_query", func(tx *gorm.DB) {
		queries.Add(1)
		cancel()
		tx.AddError(errors.New("database unavailable"))
	}); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { defer close(done); runProxyStatisticsWorker(ctx) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("memory worker retried a failed batch after cancellation")
	}
	if queries.Load() != 2 {
		t.Fatalf("expected failed batch and one final flush, got %d queries", queries.Load())
	}
}
