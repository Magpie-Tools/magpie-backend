package runtime

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"magpie/internal/domain"
)

type longevityRejectDeleteHook struct{}

func (longevityRejectDeleteHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (longevityRejectDeleteHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (longevityRejectDeleteHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() == "xdel" {
			return fmt.Errorf("injected XDEL transport failure")
		}
		return next(ctx, cmd)
	}
}

func TestBackendLongevityAcknowledgedStreamEntryDeleteRecovery(t *testing.T) {
	_, client, cfg := statisticsRecoveryFixture(t)
	seedPendingStatistics(t, client, cfg, 1)
	ctx := context.Background()
	pending, err := client.XPendingExt(ctx, &redis.XPendingExtArgs{Stream: cfg.streamKey, Group: cfg.groupName, Start: "-", End: "+", Count: 10}).Result()
	if err != nil || len(pending) != 1 {
		t.Fatalf("seed pending: %v %v", pending, err)
	}
	client.AddHook(longevityRejectDeleteHook{})
	completed, err := ackAndDeleteStreamMessageIDs(ctx, []string{pending[0].ID}, client, cfg)
	if err != nil {
		t.Fatal(err)
	}
	remaining, err := client.XPending(ctx, cfg.streamKey, cfg.groupName).Result()
	if err != nil {
		t.Fatal(err)
	}
	length, err := client.XLen(ctx, cfg.streamKey).Result()
	if err != nil {
		t.Fatal(err)
	}
	readErr := client.XReadGroup(ctx, &redis.XReadGroupArgs{Group: cfg.groupName, Consumer: "new-worker", Streams: []string{cfg.streamKey, ">"}, Count: 1, Block: -1}).Err()
	t.Logf("reported_completed_ids=%d pending=%d retained_stream_entries=%d next_group_read=%v", completed, remaining.Count, length, readErr)
	if completed == 1 && remaining.Count == 0 && length == 1 {
		t.Fatal("failed XDEL leaves an acknowledged entry that normal consumers cannot reclaim, while its retry ID is discarded")
	}
}

func TestBackendLongevityStatisticsConsumerGroupRecovery(t *testing.T) {
	_, client, cfg := statisticsRecoveryFixture(t)
	ctx := context.Background()
	if err := client.XGroupCreateMkStream(ctx, cfg.streamKey, cfg.groupName, "0").Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.Del(ctx, cfg.streamKey).Err(); err != nil {
		t.Fatal(err)
	}
	if _, handled := enqueueProxyStatisticToStream(domain.ProxyStatistic{ProxyID: 1, ProtocolID: 1, JudgeID: 1, CreatedAt: time.Now().UTC()}, statisticsOverloadPolicyBlock); !handled {
		t.Fatal("producer failed to recreate stream")
	}
	workerCtx, cancel := context.WithTimeout(ctx, 2200*time.Millisecond)
	defer cancel()
	runProxyStatisticsStreamWorker(workerCtx, "longevity-worker")
	groups, err := client.XInfoGroups(ctx, cfg.streamKey).Result()
	if err != nil {
		t.Fatal(err)
	}
	length, err := client.XLen(ctx, cfg.streamKey).Result()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("redis_ping=%v consumer_groups_after_retries=%d queued_results=%d", client.Ping(ctx).Err(), len(groups), length)
	if len(groups) == 0 {
		t.Fatal("statistics consumer never recreated its lost Redis group")
	}
}
