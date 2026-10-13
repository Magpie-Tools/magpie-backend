package runtime

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"magpie/internal/database"
	"magpie/internal/support"

	"github.com/charmbracelet/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

const ackDeleteStatisticScriptSource = `
redis.call('XDEL', KEYS[1], unpack(ARGV, 2))
redis.call('XACK', KEYS[1], ARGV[1], unpack(ARGV, 2))
return #ARGV - 1
`

var ackDeleteStatisticScript = redis.NewScript(ackDeleteStatisticScriptSource)

var statisticReplayFloorTime = prometheus.NewGauge(prometheus.GaugeOpts{
	Name: "magpie_statistics_replay_floor_timestamp_seconds", Help: "Stream timestamp before which individual event identities can be retired. A stalled pending event holds this floor back.",
})

func init() { prometheus.MustRegister(statisticReplayFloorTime) }

// Observe pending and delivered progress in one Redis operation. Separate
// reads could miss a worker that takes a new batch between those reads.
var statisticReplayFloorScript = redis.NewScript(`
local pending = redis.call('XPENDING', KEYS[1], ARGV[1])
if pending[1] > 0 then return {pending[2], 'pending'} end
local groups = redis.call('XINFO', 'GROUPS', KEYS[1])
for _, group in ipairs(groups) do
  local name, delivered
  for i = 1, #group, 2 do
    if group[i] == 'name' then name = group[i+1] end
    if group[i] == 'last-delivered-id' then delivered = group[i+1] end
  end
  if name == ARGV[1] then return {delivered, 'delivered'} end
end
return redis.error_reply('NOGROUP statistics group missing')
`)

func repairProxyStatisticGroup(ctx context.Context, client *redis.Client, cfg proxyStatisticStreamConfig, cause error) bool {
	if cause == nil || !strings.Contains(strings.ToUpper(cause.Error()), "NOGROUP") {
		return false
	}
	opCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := client.XGroupCreateMkStream(opCtx, cfg.streamKey, cfg.groupName, "0").Err(); err != nil && !isBusyGroupError(err) {
		log.Error("Failed to restore statistics stream group", "error", err)
		return false
	}
	log.Warn("Restored statistics stream consumer group", "stream", cfg.streamKey, "group", cfg.groupName)
	return true
}

func runProxyStatisticStreamMaintenance(ctx context.Context) {
	client, cfg := proxyStatisticStreamSnapshot()
	err := support.RunWithLeader(ctx, "magpie:leader:statistics_stream_maintenance:"+cfg.streamKey, support.DefaultLeadershipTTL, func(leaderCtx context.Context) {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			err := maintainProxyStatisticStream(leaderCtx, client, cfg)
			support.RecordMaintenanceRun("statistics_stream", err)
			if err != nil && leaderCtx.Err() == nil {
				repairProxyStatisticGroup(leaderCtx, client, cfg, err)
				log.Error("Statistics stream maintenance failed", "error", err)
			}
			select {
			case <-leaderCtx.Done():
				return
			case <-ticker.C:
			}
		}
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Error("Statistics stream maintenance stopped", "error", err)
	}
}

func maintainProxyStatisticStream(ctx context.Context, client *redis.Client, cfg proxyStatisticStreamConfig) error {
	opCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	result, err := statisticReplayFloorScript.Run(opCtx, client, []string{cfg.streamKey}, cfg.groupName).StringSlice()
	if err != nil {
		return err
	}
	if len(result) != 2 {
		return fmt.Errorf("invalid statistics stream replay boundary")
	}
	before := result[0]
	if before == "0" {
		before = "0-0"
	}
	if result[1] == "delivered" {
		parts := strings.Split(before, "-")
		if len(parts) != 2 {
			return fmt.Errorf("invalid delivered stream ID %q", before)
		}
		seq, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil || seq == ^uint64(0) {
			return fmt.Errorf("invalid delivered stream sequence %q", before)
		}
		before = parts[0] + "-" + strconv.FormatUint(seq+1, 10)
	}
	// Also removes acknowledged entries stranded by older backend versions.
	// Unread entries and the oldest pending entry are never trimmed.
	if err := client.Do(opCtx, "XTRIM", cfg.streamKey, "MINID", before).Err(); err != nil {
		return err
	}
	batchSize := max(1, support.GetEnvInt("PROXY_STATISTICS_LEDGER_RETENTION_BATCH_SIZE", 50_000))
	maxBatches := max(1, support.GetEnvInt("PROXY_STATISTICS_LEDGER_RETENTION_MAX_BATCHES", 240))
	for batch := 0; batch < maxBatches; batch++ {
		deleted, err := database.RetireProxyStatisticEvents(opCtx, cfg.streamKey, before, batchSize)
		if err != nil {
			return err
		}
		if deleted < int64(batchSize) {
			break
		}
		if deadline, ok := opCtx.Deadline(); ok && time.Until(deadline) < 5*time.Second {
			break
		}
	}
	ms, _ := strconv.ParseUint(strings.Split(before, "-")[0], 10, 64)
	statisticReplayFloorTime.Set(float64(ms) / 1000)
	return nil
}
