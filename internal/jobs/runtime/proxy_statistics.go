package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"magpie/internal/database"
	"magpie/internal/domain"
	"magpie/internal/instanceid"
	"magpie/internal/support"

	"github.com/charmbracelet/log"
	"github.com/redis/go-redis/v9"
)

const (
	statisticsFlushInterval           = 15 * time.Second
	statisticsBatchThreshold          = 5000
	statisticsQueueCapacity           = 20_000
	statisticsBackpressureLogEvery    = 15 * time.Second
	statisticsInsertTimeout           = 30 * time.Second
	statisticsProducerRetryDelay      = 250 * time.Millisecond
	defaultStatisticsProducerMaxBlock = 500 * time.Millisecond
	defaultStatisticsIngestWorkers    = 4
	maxStatisticsIngestWorkers        = 32
	envProxyStatisticsIngestWorkers   = "PROXY_STATISTICS_INGEST_WORKERS"
	envProxyStatisticsProducerBlockMS = "PROXY_STATISTICS_PRODUCER_BLOCK_TIMEOUT_MS"

	envProxyStatisticsRedisStreamEnabled        = "PROXY_STATISTICS_REDIS_STREAM_ENABLED"
	envProxyStatisticsRedisStreamKey            = "PROXY_STATISTICS_REDIS_STREAM_KEY"
	envProxyStatisticsRedisStreamGroup          = "PROXY_STATISTICS_REDIS_STREAM_GROUP"
	envProxyStatisticsRedisStreamMaxLen         = "PROXY_STATISTICS_REDIS_STREAM_MAXLEN"
	envProxyStatisticsRedisStreamOverloadPolicy = "PROXY_STATISTICS_REDIS_STREAM_OVERLOAD_POLICY"
	// Keep the historical environment name; policy keys are workspace IDs.
	envProxyStatisticsWorkspaceOverloadPolicies = "PROXY_STATISTICS_TENANT_OVERLOAD_POLICIES"

	defaultProxyStatisticsRedisStreamKey       = "magpie:proxy_statistics:stream"
	defaultProxyStatisticsRedisStreamGroup     = "magpie:proxy_statistics:consumers"
	defaultProxyStatisticsRedisStreamMaxLen    = 1_000_000
	defaultProxyStatisticsStreamOverloadPolicy = statisticsOverloadPolicyBlock

	statisticsStreamAddTimeout   = 150 * time.Millisecond
	statisticsStreamReadBlock    = 2 * time.Second
	statisticsStreamErrorDelay   = 1 * time.Second
	statisticsStreamAckBatchCap  = 1000
	statisticsStreamClaimEvery   = 15 * time.Second
	statisticsStreamClaimMinIdle = 1 * time.Minute
	statisticsStreamClaimBatch   = 500
	statisticsStreamClaimStart   = "0-0"

	statisticsOverloadPolicyBlock       = "block"
	statisticsOverloadPolicyEvictOldest = "evict_oldest"
	statisticsOverloadPolicyDropNew     = "drop_new"
)

var (
	proxyStatisticQueue               = make(chan domain.ProxyStatistic, statisticsQueueCapacity)
	proxyStatisticDroppedCount        atomic.Uint64
	proxyStatisticLastBackpressureLog atomic.Int64

	proxyStatisticStreamInitMu  sync.Mutex
	proxyStatisticStreamStateMu sync.RWMutex
	proxyStatisticStreamClient  *redis.Client
	proxyStatisticStreamCfg     proxyStatisticStreamConfig
	proxyStatisticStreamReady   atomic.Bool
)

type proxyStatisticStreamConfig struct {
	enabled                   bool
	streamKey                 string
	groupName                 string
	maxLen                    int64
	overloadPolicy            string
	workspaceOverloadPolicies map[uint]string
}

// AddProxyStatistic hands off a result using any WorkspaceIDs already attached
// to it. It has the same asynchronous acceptance contract as the attributed form.
func AddProxyStatistic(proxyStatistic domain.ProxyStatistic) {
	AddProxyStatisticForWorkspaces(proxyStatistic, nil)
}

// AddProxyStatisticForWorkspaces hands off one physical check result for
// asynchronous persistence attributed to the supplied workspace IDs.
// The payload shares slices with its caller; do not mutate them after handoff.
// Returning reports neither acceptance nor a PostgreSQL commit: policy can drop
// the result, Redis acceptance depends on Redis persistence, and memory fallback
// is volatile. Block policy retries stream admission within the configured wait
// budget, then tries the memory queue with any remaining time. Rejected new
// results are counted and logged instead of returned to the checker.
func AddProxyStatisticForWorkspaces(proxyStatistic domain.ProxyStatistic, workspaceIDs []uint) {
	if len(workspaceIDs) > 0 {
		proxyStatistic.WorkspaceIDs = workspaceIDs
	} else {
		workspaceIDs = proxyStatistic.WorkspaceIDs
	}
	policy := resolveStatisticsOverloadPolicy(workspaceIDs)
	blockDeadline := time.Time{}
	if policy != statisticsOverloadPolicyDropNew {
		maxBlock := resolveStatisticsProducerMaxBlock()
		if maxBlock > 0 {
			blockDeadline = time.Now().Add(maxBlock)
		}
	}

	if proxyStatisticStreamReady.Load() {
		for {
			enqueued, handled := enqueueProxyStatisticToStream(proxyStatistic, policy)
			if enqueued || handled {
				return
			}
			if policy != statisticsOverloadPolicyBlock {
				break
			}
			if !blockDeadline.IsZero() && !time.Now().Before(blockDeadline) {
				break
			}
			recordProxyStatisticBackpressure(policy)
			time.Sleep(statisticsProducerRetryDelay)
		}
	}

	if enqueueProxyStatistic(proxyStatistic, policy, blockDeadline) {
		return
	}

	dropped := proxyStatisticDroppedCount.Add(1)
	recordProxyStatisticDrop(dropped, policy)
}

// AddProxyStatisticForUsers is the compatibility name for workspace attribution.
// Its IDs identify workspaces, not user accounts.
//
// Deprecated: Use AddProxyStatisticForWorkspaces.
func AddProxyStatisticForUsers(proxyStatistic domain.ProxyStatistic, workspaceIDs []uint) {
	AddProxyStatisticForWorkspaces(proxyStatistic, workspaceIDs)
}

// enqueueProxyStatisticToStream returns (enqueued, handled). An intentional
// drop_new rejection is handled but not enqueued, preventing memory fallback
// from bypassing the policy. An unavailable or failing stream leaves fallback
// to the producer. evict_oldest may trim unread or pending entries via XADD.
func enqueueProxyStatisticToStream(proxyStatistic domain.ProxyStatistic, policy string) (bool, bool) {
	client, cfg := proxyStatisticStreamSnapshot()
	if client == nil || !cfg.enabled {
		return false, false
	}

	if (policy == statisticsOverloadPolicyDropNew || policy == statisticsOverloadPolicyBlock) && cfg.maxLen > 0 {
		addCtx, cancel := context.WithTimeout(context.Background(), statisticsStreamAddTimeout)
		streamLen, err := client.XLen(addCtx, cfg.streamKey).Result()
		cancel()
		if err == nil && streamLen >= cfg.maxLen {
			if policy == statisticsOverloadPolicyDropNew {
				dropped := proxyStatisticDroppedCount.Add(1)
				recordProxyStatisticDrop(dropped, policy)
				return false, true
			}
			recordProxyStatisticBackpressure(policy)
			return false, false
		}
	}

	encoded, err := json.Marshal(proxyStatistic)
	if err != nil {
		log.Error("Failed to encode proxy statistic for stream ingest", "error", err)
		return false, false
	}

	addArgs := &redis.XAddArgs{
		Stream: cfg.streamKey,
		Values: map[string]any{"stat": string(encoded)},
	}
	if policy == statisticsOverloadPolicyEvictOldest && cfg.maxLen > 0 {
		addArgs.MaxLen = cfg.maxLen
		addArgs.Approx = true
	}

	addCtx, cancel := context.WithTimeout(context.Background(), statisticsStreamAddTimeout)
	defer cancel()
	if _, err := client.XAdd(addCtx, addArgs).Result(); err != nil {
		log.Warn("Failed to enqueue proxy statistic to Redis stream", "error", err, "policy", policy)
		return false, false
	}

	return true, true
}

// enqueueProxyStatistic makes one immediate admission attempt, then waits only
// until a nonzero deadline. Only the stream path evicts old entries; the memory
// fallback treats evict_oldest like block. False means the caller must count a
// drop, and true means the volatile queue accepted the result, not that it saved it.
func enqueueProxyStatistic(proxyStatistic domain.ProxyStatistic, policy string, blockDeadline time.Time) bool {
	if policy == statisticsOverloadPolicyDropNew {
		select {
		case proxyStatisticQueue <- proxyStatistic:
			return true
		default:
			return false
		}
	}

	select {
	case proxyStatisticQueue <- proxyStatistic:
		return true
	default:
		recordProxyStatisticBackpressure(policy)
		if blockDeadline.IsZero() {
			return false
		}

		remaining := time.Until(blockDeadline)
		if remaining <= 0 {
			return false
		}

		timer := time.NewTimer(remaining)
		defer timer.Stop()
		select {
		case proxyStatisticQueue <- proxyStatistic:
			return true
		case <-timer.C:
			return false
		}
	}
}

func recordProxyStatisticBackpressure(policy string) {
	nowUnix := time.Now().Unix()
	last := proxyStatisticLastBackpressureLog.Load()
	if nowUnix-last < int64(statisticsBackpressureLogEvery/time.Second) {
		return
	}
	if !proxyStatisticLastBackpressureLog.CompareAndSwap(last, nowUnix) {
		return
	}

	log.Warn(
		"Proxy statistics ingestion applying backpressure",
		"policy", policy,
		"queue_len", len(proxyStatisticQueue),
		"queue_capacity", cap(proxyStatisticQueue),
	)
}

func recordProxyStatisticDrop(totalDropped uint64, policy string) {
	log.Warn("Dropped proxy statistic due overload policy", "policy", policy, "dropped_total", totalDropped)
}

// StartProxyStatisticsRoutine runs stream and memory consumers alongside the
// reputation coordinator and waits for them on cancellation. Memory consumers
// remain available while stream initialization recovers from an outage. Each
// consumer owns its batch; stream batches can replay after shutdown, while a
// failed final memory flush loses that consumer's buffered results.
func StartProxyStatisticsRoutine(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}

	workerCount := resolveStatisticsIngestWorkers()
	initializeProxyStatisticStream(ctx)

	var reputationWG sync.WaitGroup
	reputationWG.Add(1)
	go func() {
		defer reputationWG.Done()
		runProxyReputationCoordinator(ctx)
	}()

	var workerWG sync.WaitGroup
	memoryWorkerCount := workerCount
	_, streamCfg := proxyStatisticStreamSnapshot()
	if streamCfg.enabled {
		workerWG.Add(1)
		go func() {
			defer workerWG.Done()
			for !proxyStatisticStreamReady.Load() {
				if !waitForStatisticsRetry(ctx) {
					return
				}
				initializeProxyStatisticStream(ctx)
			}
			var streams sync.WaitGroup
			for i := 0; i < workerCount; i++ {
				streams.Add(1)
				go func(i int) {
					defer streams.Done()
					runProxyStatisticsStreamWorker(ctx, fmt.Sprintf("%s-%d", instanceid.Get(), i))
				}(i)
			}
			streams.Add(1)
			go func() { defer streams.Done(); runProxyStatisticStreamMaintenance(ctx) }()
			streams.Wait()
		}()
	}

	for i := 0; i < memoryWorkerCount; i++ {
		workerWG.Add(1)
		go func() {
			defer workerWG.Done()
			runProxyStatisticsWorker(ctx)
		}()
	}

	<-ctx.Done()
	workerWG.Wait()
	reputationWG.Wait()
}

func initializeProxyStatisticStream(ctx context.Context) {
	proxyStatisticStreamInitMu.Lock()
	defer proxyStatisticStreamInitMu.Unlock()
	if proxyStatisticStreamReady.Load() {
		return
	}
	cfg := resolveProxyStatisticStreamConfig()
	proxyStatisticStreamStateMu.Lock()
	proxyStatisticStreamCfg = cfg
	proxyStatisticStreamStateMu.Unlock()
	if !cfg.enabled {
		return
	}

	client, err := support.GetRedisClientContext(ctx)
	if err != nil {
		log.Warn("Proxy statistics Redis stream unavailable; using in-memory backpressure path", "error", err)
		return
	}

	opCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	err = client.XGroupCreateMkStream(opCtx, cfg.streamKey, cfg.groupName, "0").Err()
	if err != nil && !isBusyGroupError(err) {
		log.Warn("Proxy statistics Redis stream unavailable; failed to create consumer group", "error", err)
		return
	}

	proxyStatisticStreamStateMu.Lock()
	proxyStatisticStreamClient = client
	proxyStatisticStreamStateMu.Unlock()
	proxyStatisticStreamReady.Store(true)
	// Keep tenant_overload_policies as the historical log field for workspace overrides.
	log.Info(
		"Proxy statistics ingest using Redis stream",
		"stream", cfg.streamKey,
		"group", cfg.groupName,
		"default_overload_policy", cfg.overloadPolicy,
		"tenant_overload_policies", len(cfg.workspaceOverloadPolicies),
		"max_len", cfg.maxLen,
	)
}

func proxyStatisticStreamSnapshot() (*redis.Client, proxyStatisticStreamConfig) {
	proxyStatisticStreamStateMu.RLock()
	defer proxyStatisticStreamStateMu.RUnlock()
	return proxyStatisticStreamClient, proxyStatisticStreamCfg
}

func resolveProxyStatisticStreamConfig() proxyStatisticStreamConfig {
	enabled := support.GetEnvBool(envProxyStatisticsRedisStreamEnabled, true)

	streamKey := strings.TrimSpace(support.GetEnv(envProxyStatisticsRedisStreamKey, defaultProxyStatisticsRedisStreamKey))
	if streamKey == "" {
		streamKey = defaultProxyStatisticsRedisStreamKey
	}

	groupName := strings.TrimSpace(support.GetEnv(envProxyStatisticsRedisStreamGroup, defaultProxyStatisticsRedisStreamGroup))
	if groupName == "" {
		groupName = defaultProxyStatisticsRedisStreamGroup
	}

	maxLen := int64(support.GetEnvInt(envProxyStatisticsRedisStreamMaxLen, defaultProxyStatisticsRedisStreamMaxLen))
	if maxLen < 1 {
		maxLen = defaultProxyStatisticsRedisStreamMaxLen
	}

	overloadPolicy := normalizeOverloadPolicy(support.GetEnv(envProxyStatisticsRedisStreamOverloadPolicy, defaultProxyStatisticsStreamOverloadPolicy))
	workspacePolicies := parseWorkspaceOverloadPolicies(support.GetEnv(envProxyStatisticsWorkspaceOverloadPolicies, ""))

	return proxyStatisticStreamConfig{
		enabled:                   enabled,
		streamKey:                 streamKey,
		groupName:                 groupName,
		maxLen:                    maxLen,
		overloadPolicy:            overloadPolicy,
		workspaceOverloadPolicies: workspacePolicies,
	}
}

func parseWorkspaceOverloadPolicies(raw string) map[uint]string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}

	entries := strings.Split(raw, ",")
	policies := make(map[uint]string, len(entries))
	for _, entry := range entries {
		parts := strings.SplitN(strings.TrimSpace(entry), "=", 2)
		if len(parts) != 2 {
			continue
		}
		workspaceID, err := strconv.ParseUint(strings.TrimSpace(parts[0]), 10, 64)
		if err != nil || workspaceID == 0 {
			continue
		}
		policy := normalizeOverloadPolicy(parts[1])
		policies[uint(workspaceID)] = policy
	}

	if len(policies) == 0 {
		return nil
	}
	return policies
}

func normalizeOverloadPolicy(raw string) string {
	policy := strings.ToLower(strings.TrimSpace(raw))
	switch policy {
	case statisticsOverloadPolicyDropNew, statisticsOverloadPolicyEvictOldest, statisticsOverloadPolicyBlock:
		return policy
	default:
		return defaultProxyStatisticsStreamOverloadPolicy
	}
}

// resolveStatisticsOverloadPolicy selects the highest-priority policy among the
// default and workspace overrides. Block outranks drop_new, then evict_oldest;
// an override cannot weaken the default for any result sharing that stream entry.
func resolveStatisticsOverloadPolicy(workspaceIDs []uint) string {
	_, cfg := proxyStatisticStreamSnapshot()
	selected := normalizeOverloadPolicy(cfg.overloadPolicy)
	if len(workspaceIDs) == 0 || len(cfg.workspaceOverloadPolicies) == 0 {
		return selected
	}

	for _, workspaceID := range workspaceIDs {
		override, ok := cfg.workspaceOverloadPolicies[workspaceID]
		if !ok {
			continue
		}
		override = normalizeOverloadPolicy(override)
		if overloadPolicyPriority(override) > overloadPolicyPriority(selected) {
			selected = override
		}
	}

	return selected
}

func overloadPolicyPriority(policy string) int {
	switch policy {
	case statisticsOverloadPolicyBlock:
		return 3
	case statisticsOverloadPolicyDropNew:
		return 1
	case statisticsOverloadPolicyEvictOldest:
		return 0
	default:
		return 2
	}
}

func isBusyGroupError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToUpper(err.Error()), "BUSYGROUP")
}

func runProxyStatisticsWorker(ctx context.Context) {
	var buffer []domain.ProxyStatistic
	flushTimer := time.NewTimer(statisticsFlushInterval)
	defer flushTimer.Stop()
	defer func() {
		drainProxyStatisticQueue(&buffer)
		// The volatile fallback gets one final attempt, bounded by the normal
		// database insert timeout. A failed attempt must not restart the loop.
		_ = flushStatisticsBuffer(&buffer)
	}()

	for {
		if ctx.Err() != nil {
			return
		}
		if len(buffer) >= statisticsBatchThreshold {
			if _, flushed := flushProxyStatisticsContext(ctx, &buffer); !flushed {
				if !waitForStatisticsRetry(ctx) {
					return
				}
				continue
			}
			resetTimer(flushTimer)
		}

		select {
		case <-ctx.Done():
			return
		case stat := <-proxyStatisticQueue:
			buffer = append(buffer, stat)
		case <-flushTimer.C:
			_, _ = flushProxyStatisticsContext(ctx, &buffer)
			flushTimer.Reset(statisticsFlushInterval)
		}
	}
}

// runProxyStatisticsStreamWorker owns the paired result and message-ID buffers.
// IDs remain pending until persistence and acknowledgement finish. Recovery reads
// this consumer's pending entries and claims idle entries from other consumers;
// the database event ledger handles overlap and replay across workers.
func runProxyStatisticsStreamWorker(ctx context.Context, consumerName string) {
	client, cfg := proxyStatisticStreamSnapshot()
	if client == nil || !cfg.enabled {
		return
	}

	var (
		buffer     []domain.ProxyStatistic
		messageIDs []string
		streamID   = "0" // consume pending entries for this consumer first, then switch to new entries.
		claimStart = statisticsStreamClaimStart
		lastClaim  time.Time
	)
	flushTimer := time.NewTimer(statisticsFlushInterval)
	defer flushTimer.Stop()

	for {
		// Buffered and committed-but-unacknowledged stream entries remain
		// pending. A later consumer safely replays them through the event ledger.
		// Check before retrying, including when cancellation interrupted XACK.
		if ctx.Err() != nil {
			return
		}
		if len(messageIDs) > 0 && len(buffer) == 0 {
			if !flushStreamStatisticsBuffer(ctx, &buffer, &messageIDs, client, cfg) {
				if !waitForStatisticsRetry(ctx) {
					return
				}
				continue
			}
		}

		if len(buffer) >= statisticsBatchThreshold {
			if !flushStreamStatisticsBuffer(ctx, &buffer, &messageIDs, client, cfg) {
				if !waitForStatisticsRetry(ctx) {
					return
				}
				continue
			}
			resetTimer(flushTimer)
		}

		if lastClaim.IsZero() || time.Since(lastClaim) >= statisticsStreamClaimEvery {
			claimed, nextStart, err := claimStaleProxyStatisticStreamMessages(ctx, client, cfg, consumerName, claimStart)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Error("Failed to claim stale proxy statistics stream messages", "error", err, "consumer", consumerName)
				if repairProxyStatisticGroup(ctx, client, cfg, err) {
					streamID, claimStart, lastClaim = "0", statisticsStreamClaimStart, time.Time{}
				}
				if !waitForStatisticsRetry(ctx) {
					return
				}
				continue
			}
			lastClaim = time.Now()
			claimStart = nextStart
			if appendStreamMessagesToBuffer(ctx, claimed, &buffer, &messageIDs, client, cfg) > 0 {
				continue
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-flushTimer.C:
			if flushStreamStatisticsBuffer(ctx, &buffer, &messageIDs, client, cfg) {
				flushTimer.Reset(statisticsFlushInterval)
			} else {
				if !waitForStatisticsRetry(ctx) {
					return
				}
				flushTimer.Reset(statisticsFlushInterval)
			}
		default:
		}

		streams, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    cfg.groupName,
			Consumer: consumerName,
			Streams:  []string{cfg.streamKey, streamID},
			Count:    int64(statisticsBatchThreshold),
			Block:    statisticsStreamReadBlock,
			NoAck:    false,
		}).Result()
		if errors.Is(err, redis.Nil) {
			if streamID != ">" {
				streamID = ">"
			}
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Error("Failed reading proxy statistics stream", "error", err, "consumer", consumerName)
			if repairProxyStatisticGroup(ctx, client, cfg, err) {
				streamID, claimStart, lastClaim = "0", statisticsStreamClaimStart, time.Time{}
			}
			if !waitForStatisticsRetry(ctx) {
				return
			}
			continue
		}

		readMessages := 0
		for _, stream := range streams {
			readMessages += len(stream.Messages)
			appendStreamMessagesToBuffer(ctx, stream.Messages, &buffer, &messageIDs, client, cfg)
			if streamID != ">" && len(stream.Messages) > 0 {
				streamID = stream.Messages[len(stream.Messages)-1].ID
			}
		}
		if streamID != ">" && readMessages == 0 {
			streamID = ">"
			// Finish a partial recovery page before waiting for new traffic.
			if !flushStreamStatisticsBuffer(ctx, &buffer, &messageIDs, client, cfg) {
				if !waitForStatisticsRetry(ctx) {
					return
				}
			}
		}
	}
}

func waitForStatisticsRetry(ctx context.Context) bool {
	timer := time.NewTimer(statisticsStreamErrorDelay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func claimStaleProxyStatisticStreamMessages(ctx context.Context, client *redis.Client, cfg proxyStatisticStreamConfig, consumerName, start string) ([]redis.XMessage, string, error) {
	if strings.TrimSpace(start) == "" {
		start = statisticsStreamClaimStart
	}

	claimCtx, cancel := context.WithTimeout(ctx, statisticsStreamReadBlock)
	defer cancel()

	messages, nextStart, err := client.XAutoClaim(claimCtx, &redis.XAutoClaimArgs{
		Stream:   cfg.streamKey,
		Group:    cfg.groupName,
		Consumer: consumerName,
		MinIdle:  statisticsStreamClaimMinIdle,
		Start:    start,
		Count:    statisticsStreamClaimBatch,
	}).Result()
	if err != nil {
		return nil, start, err
	}
	if strings.TrimSpace(nextStart) == "" {
		nextStart = statisticsStreamClaimStart
	}
	return messages, nextStart, nil
}

// appendStreamMessagesToBuffer returns the number of decoded results appended,
// deduplicating IDs already buffered. Malformed payloads are logged and discarded
// with acknowledgement/deletion so a poison entry cannot stall ingestion forever.
func appendStreamMessagesToBuffer(ctx context.Context, messages []redis.XMessage, buffer *[]domain.ProxyStatistic, messageIDs *[]string, client *redis.Client, cfg proxyStatisticStreamConfig) int {
	if len(messages) == 0 {
		return 0
	}

	appended := 0
	seen := make(map[string]struct{}, len(*messageIDs)+len(messages))
	for _, id := range *messageIDs {
		seen[id] = struct{}{}
	}
	for _, msg := range messages {
		if _, duplicate := seen[msg.ID]; duplicate {
			continue
		}
		seen[msg.ID] = struct{}{}
		stat, err := decodeProxyStatisticStreamMessage(msg)
		if err != nil {
			log.Warn("Dropping malformed proxy statistic stream message", "id", msg.ID, "error", err)
			if _, err := ackAndDeleteStreamMessageIDs(ctx, []string{msg.ID}, client, cfg); err != nil {
				*messageIDs = append(*messageIDs, msg.ID)
			}
			continue
		}

		stat.EventStream = cfg.streamKey
		stat.EventID = msg.ID
		// XAUTOCLAIM and pending recovery may overlap within the same buffer.
		*buffer = append(*buffer, stat)
		*messageIDs = append(*messageIDs, msg.ID)
		appended++
	}

	return appended
}

func decodeProxyStatisticStreamMessage(msg redis.XMessage) (domain.ProxyStatistic, error) {
	raw, ok := msg.Values["stat"]
	if !ok {
		return domain.ProxyStatistic{}, fmt.Errorf("missing stat payload")
	}

	payload, ok := raw.(string)
	if !ok {
		payload = fmt.Sprint(raw)
	}
	if strings.TrimSpace(payload) == "" {
		return domain.ProxyStatistic{}, fmt.Errorf("empty stat payload")
	}

	var stat domain.ProxyStatistic
	if err := json.Unmarshal([]byte(payload), &stat); err != nil {
		return domain.ProxyStatistic{}, err
	}
	return stat, nil
}

// flushStreamStatisticsBuffer persists decoded results before acknowledging IDs.
// Deleted routes and malformed entries need no insert. False retains unfinished
// work for retry. After a commit, the cleared buffer avoids another insert here
// while retained IDs retry acknowledgement. A later consumer can replay those
// IDs safely through the transactional event ledger.
func flushStreamStatisticsBuffer(ctx context.Context, buffer *[]domain.ProxyStatistic, messageIDs *[]string, client *redis.Client, cfg proxyStatisticStreamConfig) bool {
	if len(*buffer) > 0 {
		if _, flushed := flushProxyStatisticsContext(ctx, buffer); !flushed {
			return false
		}
	}

	ackedCount, err := ackAndDeleteStreamMessageIDs(ctx, *messageIDs, client, cfg)
	if ackedCount > 0 {
		*messageIDs = (*messageIDs)[ackedCount:]
	}
	if err != nil {
		repairProxyStatisticGroup(ctx, client, cfg, err)
		log.Error("Failed to ack proxy statistic stream messages", "error", err, "remaining", len(*messageIDs))
		return false
	}
	return true
}

// ackAndDeleteStreamMessageIDs deletes and acknowledges each batch in one Lua
// call. The count is the successfully handled prefix of messageIDs, including
// IDs already gone from Redis, so callers retain only the unfinished suffix.
func ackAndDeleteStreamMessageIDs(ctx context.Context, messageIDs []string, client *redis.Client, cfg proxyStatisticStreamConfig) (int, error) {
	if len(messageIDs) == 0 {
		return 0, nil
	}

	ackedCount := 0
	for start := 0; start < len(messageIDs); start += statisticsStreamAckBatchCap {
		end := start + statisticsStreamAckBatchCap
		if end > len(messageIDs) {
			end = len(messageIDs)
		}
		ids := messageIDs[start:end]
		if len(ids) == 0 {
			continue
		}
		args := make([]any, 0, len(ids)+1)
		args = append(args, cfg.groupName)
		for _, id := range ids {
			args = append(args, id)
		}
		if _, err := ackDeleteStatisticScript.Run(ctx, client, []string{cfg.streamKey}, args...).Result(); err != nil {
			return ackedCount, err
		}
		ackedCount += len(ids)
	}

	return ackedCount, nil
}

func flushStatisticsBuffer(buffer *[]domain.ProxyStatistic) bool {
	_, flushed := flushProxyStatistics(buffer)
	return flushed
}

func resolveStatisticsIngestWorkers() int {
	workers := support.GetEnvInt(envProxyStatisticsIngestWorkers, defaultStatisticsIngestWorkers)
	if workers <= 0 {
		return 1
	}
	if workers > maxStatisticsIngestWorkers {
		return maxStatisticsIngestWorkers
	}
	return workers
}

// A nonpositive setting means no timed wait in memory fallback. The stream block
// loop interprets the resulting zero deadline as unlimited admission retries.
func resolveStatisticsProducerMaxBlock() time.Duration {
	ms := support.GetEnvInt(envProxyStatisticsProducerBlockMS, int(defaultStatisticsProducerMaxBlock/time.Millisecond))
	if ms <= 0 {
		return 0
	}
	return time.Duration(ms) * time.Millisecond
}

func flushProxyStatistics(buffer *[]domain.ProxyStatistic) ([]uint64, bool) {
	return flushProxyStatisticsContext(context.Background(), buffer)
}

// flushProxyStatisticsContext returns prepared route IDs and whether the batch
// was handled. Failure leaves the buffer intact for retry. Success clears it,
// including when deleted routes or previously committed stream events leave
// nothing new to insert; true does not mean every submitted result became a row.
func flushProxyStatisticsContext(ctx context.Context, buffer *[]domain.ProxyStatistic) ([]uint64, bool) {
	if len(*buffer) == 0 {
		return nil, true
	}

	toInsert := *buffer

	start := time.Now()
	dbCtx, cancel := context.WithTimeout(ctx, statisticsInsertTimeout)
	defer cancel()

	preparedStats, proxyIDs, err := prepareProxyStatistics(dbCtx, toInsert)
	if err != nil {
		log.Error("Failed to prepare proxy statistics", "error", err)
		return nil, false
	}
	if len(preparedStats) == 0 {
		*buffer = nil
		return nil, true
	}

	batchSize := database.CalculateProxyStatisticBatchSize(len(preparedStats))
	if err := database.InsertProxyStatistics(dbCtx, preparedStats, batchSize); err != nil {
		log.Error("Failed to insert proxy statistics", "error", err, "count", len(preparedStats))
		return nil, false
	}

	*buffer = nil
	log.Debug("Inserted proxy statistics", "seconds", time.Since(start).Seconds(), "count", len(preparedStats))
	return proxyIDs, true
}

func drainProxyStatisticQueue(buffer *[]domain.ProxyStatistic) {
	for {
		select {
		case stat := <-proxyStatisticQueue:
			*buffer = append(*buffer, stat)
		default:
			return
		}
	}
}

func resetTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(statisticsFlushInterval)
}

func collectProxyIDs(stats []domain.ProxyStatistic) []uint64 {
	if len(stats) == 0 {
		return nil
	}

	seen := make(map[uint64]struct{}, len(stats))
	for _, stat := range stats {
		if stat.ProxyID == 0 {
			continue
		}
		seen[stat.ProxyID] = struct{}{}
	}

	if len(seen) == 0 {
		return nil
	}

	ids := make([]uint64, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}

	return ids
}

// prepareProxyStatistics filters routes deleted between checking and ingestion.
// Skipping their results lets the rest of the batch persist instead of retrying
// forever against a missing foreign key. Lookup failure preserves the batch.
func prepareProxyStatistics(ctx context.Context, stats []domain.ProxyStatistic) ([]domain.ProxyStatistic, []uint64, error) {
	if len(stats) == 0 {
		return nil, nil, nil
	}

	proxyIDs := collectProxyIDs(stats)
	if len(proxyIDs) == 0 {
		return stats, nil, nil
	}

	existing, err := database.GetExistingProxyIDSet(ctx, proxyIDs)
	if err != nil {
		return nil, nil, err
	}

	if len(existing) == 0 {
		return nil, nil, nil
	}

	if len(existing) == len(proxyIDs) {
		return stats, proxyIDs, nil
	}

	filtered := make([]domain.ProxyStatistic, 0, len(stats))
	for _, stat := range stats {
		if _, ok := existing[stat.ProxyID]; ok {
			filtered = append(filtered, stat)
		}
	}

	if len(filtered) == 0 {
		return nil, nil, nil
	}

	validIDs := make([]uint64, 0, len(existing))
	for id := range existing {
		validIDs = append(validIDs, id)
	}

	dropped := len(stats) - len(filtered)
	if dropped > 0 {
		log.Info("Skipped proxy statistics for removed proxies", "dropped", dropped)
	}

	return filtered, validIDs, nil
}
