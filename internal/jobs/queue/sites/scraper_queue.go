package sitequeue

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"strconv"
	"time"

	"magpie/internal/checkpace"
	"magpie/internal/config"
	"magpie/internal/domain"
	"magpie/internal/instanceid"
	queueutil "magpie/internal/jobs/queue"
	"magpie/internal/support"

	"github.com/charmbracelet/log"
	"github.com/redis/go-redis/v9"
)

const (
	scrapesiteKeyPrefix = "scrapesite:"

	legacyScrapesiteQueueKey   = "scrapesite_queue"
	scrapesiteQueueHeadKey     = "scrapesite_queue_heads"
	scrapesiteQueueShardPrefix = "scrapesite_queue:"
	defaultScrapeQueueShards   = 8
	maxScrapeQueueShards       = 64
	minDequeueSleep            = 10 * time.Millisecond
	idleQueueSleep             = 250 * time.Millisecond
	maxDequeueSleep            = 2 * time.Second
	processingLease            = 5 * time.Minute
	requeueAllMinimumWindow    = 15 * time.Minute
)

//go:embed pop.lua
var luaScrapePopScript string

type RedisScrapeSiteQueue struct {
	client         *redis.Client
	ctx            context.Context
	popScript      *redis.Script
	queueShardKeys []string
	popQueueKeys   []string
	leaseDuration  time.Duration
	// pace supplies the allowance snapshot that spreads RequeueAll. A nil
	// Pacer has no snapshot, so RequeueAll uses checkpace.FallbackInterval,
	// as before the first allowance load.
	pace *checkpace.Pacer
}

type scrapePopResult struct {
	Found        bool
	SiteJSON     string
	ScoreMs      int64
	NextReadyMs  int64
	LeaseScoreMS int64
	ScheduleJSON string
	QueueKey     string
	ClaimID      int64
}

var PublicScrapeSiteQueue RedisScrapeSiteQueue

func init() {
	PublicScrapeSiteQueue = *NewRedisScrapeSiteQueue(nil, nil)
}

// NewRedisScrapeSiteQueue returns a queue on client that spreads RequeueAll
// with pace's allowance snapshot. pace may be nil for tools that pace nothing.
func NewRedisScrapeSiteQueue(client *redis.Client, pace *checkpace.Pacer) *RedisScrapeSiteQueue {
	shards := support.GetEnvInt("SCRAPE_QUEUE_SHARDS", defaultScrapeQueueShards)
	if shards <= 0 {
		shards = defaultScrapeQueueShards
	}
	if shards > maxScrapeQueueShards {
		shards = maxScrapeQueueShards
	}

	shardKeys := buildScrapeQueueShardKeys(shards)
	popKeys := buildScrapePopQueueKeys(shardKeys)

	queue := &RedisScrapeSiteQueue{
		client:         client,
		ctx:            context.Background(),
		popScript:      redis.NewScript(luaScrapePopScript),
		queueShardKeys: shardKeys,
		popQueueKeys:   popKeys,
		pace:           pace,
	}
	if client != nil {
		if err := queue.refreshQueueHeads(); err != nil {
			log.Warn("scrape queue head refresh failed", "error", err)
		}
	}
	return queue
}

func buildScrapeQueueShardKeys(shards int) []string {
	keys := make([]string, shards)
	for i := 0; i < shards; i++ {
		keys[i] = fmt.Sprintf("%s%d", scrapesiteQueueShardPrefix, i)
	}
	return keys
}

func buildScrapePopQueueKeys(shardKeys []string) []string {
	keys := make([]string, 0, len(shardKeys)+1)
	keys = append(keys, legacyScrapesiteQueueKey)
	keys = append(keys, shardKeys...)
	return keys
}

func (rssq *RedisScrapeSiteQueue) clientOrErr() (*redis.Client, error) {
	if rssq == nil {
		return nil, errors.New("redis scrape queue is nil")
	}
	if rssq.client != nil {
		return rssq.client, nil
	}

	client, err := support.GetRedisClient()
	if err != nil {
		return nil, fmt.Errorf("redis scrape queue unavailable: %w", err)
	}
	return client, nil
}

func (rssq *RedisScrapeSiteQueue) baseContext() context.Context {
	if rssq == nil || rssq.ctx == nil {
		return context.Background()
	}
	return rssq.ctx
}

func (rssq *RedisScrapeSiteQueue) popKeys() []string {
	if len(rssq.popQueueKeys) == 0 {
		return []string{legacyScrapesiteQueueKey}
	}
	return rssq.popQueueKeys
}

func (rssq *RedisScrapeSiteQueue) queueKeyForMember(member string) string {
	if rssq == nil || len(rssq.queueShardKeys) == 0 {
		return legacyScrapesiteQueueKey
	}
	idx := scrapeQueueShardIndex(member, len(rssq.queueShardKeys))
	return rssq.queueShardKeys[idx]
}

func scrapeQueueShardIndex(member string, shards int) int {
	if shards <= 1 {
		return 0
	}

	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(member))
	return int(hasher.Sum32() % uint32(shards))
}

func (rssq *RedisScrapeSiteQueue) refreshQueueHeads() error {
	client, err := rssq.clientOrErr()
	if err != nil {
		return err
	}
	return queueutil.RefreshHeads(rssq.baseContext(), client, scrapesiteQueueHeadKey, rssq.popKeys())
}

func (rssq *RedisScrapeSiteQueue) AddToQueue(sites []domain.ScrapeSite) error {
	filtered := make([]domain.ScrapeSite, 0, len(sites))
	for _, site := range sites {
		if site.URL == "" {
			continue
		}
		if config.IsWebsiteBlocked(site.URL) {
			log.Info("Skipping blocked scrape site when queuing", "url", site.URL)
			continue
		}
		filtered = append(filtered, site)
	}

	if len(filtered) == 0 {
		return nil
	}

	client, err := rssq.clientOrErr()
	if err != nil {
		return err
	}
	ctx := rssq.baseContext()

	pipe := client.Pipeline()
	now := time.Now()
	batchSize := 50

	for i, site := range filtered {
		nextCheck := now
		siteKey := scrapesiteKeyPrefix + site.URL
		queueKey := rssq.queueKeyForMember(site.URL)

		proxyJSON, err := json.Marshal(site)
		if err != nil {
			return fmt.Errorf("failed to marshal site: %w", err)
		}

		pipe.Set(ctx, siteKey, proxyJSON, 0)
		pipe.ZAddArgs(ctx, queueKey, redis.ZAddArgs{
			NX: true,
			Members: []redis.Z{{
				Score:  float64(nextCheck.UnixMilli()),
				Member: site.URL,
			}},
		})
		if queueKey != legacyScrapesiteQueueKey {
			pipe.ZRem(ctx, legacyScrapesiteQueueKey, site.URL)
		}
		pipe.ZAddArgs(ctx, scrapesiteQueueHeadKey, redis.ZAddArgs{
			LT: true,
			Members: []redis.Z{{
				Score:  float64(nextCheck.UnixMilli()),
				Member: queueKey,
			}},
		})

		// Execute in batches to prevent oversized pipelines
		if i%batchSize == 0 && i > 0 {
			if _, err := pipe.Exec(ctx); err != nil {
				return fmt.Errorf("batch pipeline failed: %w", err)
			}
			pipe = client.Pipeline()
		}
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("final pipeline exec failed: %w", err)
	}

	return nil
}

func (rssq *RedisScrapeSiteQueue) RemoveFromQueue(sites []domain.ScrapeSite) error {
	if rssq == nil {
		return errors.New("redis scrape queue is nil")
	}
	if len(sites) == 0 {
		return nil
	}

	client, err := rssq.clientOrErr()
	if err != nil {
		return err
	}
	ctx := rssq.baseContext()

	const batchSize = 250
	pipe := client.Pipeline()
	opCount := 0

	flush := func() error {
		if opCount == 0 {
			return nil
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return fmt.Errorf("remove pipeline exec failed: %w", err)
		}
		pipe = client.Pipeline()
		opCount = 0
		return nil
	}

	for _, site := range sites {
		if site.URL == "" {
			continue
		}

		key := scrapesiteKeyPrefix + site.URL
		queueKey := rssq.queueKeyForMember(site.URL)

		pipe.Del(ctx, key)
		pipe.Del(ctx, scrapeScheduleKeyPrefix+site.URL)
		pipe.Del(ctx, scrapeLeaseKeyPrefix+site.URL)
		opCount += 3
		pipe.ZRem(ctx, queueKey, site.URL)
		opCount++
		if queueKey != legacyScrapesiteQueueKey {
			pipe.ZRem(ctx, legacyScrapesiteQueueKey, site.URL)
		}
		opCount++

		if opCount >= batchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}

	return flush()
}

func (rssq *RedisScrapeSiteQueue) GetNextScrapeSite() (ScrapeSiteJob, error) {
	return rssq.GetNextScrapeSiteContext(rssq.baseContext())
}

func (rssq *RedisScrapeSiteQueue) GetNextScrapeSiteContext(ctx context.Context) (ScrapeSiteJob, error) {
	if ctx == nil {
		ctx = rssq.baseContext()
	}
	client, err := rssq.clientOrErr()
	if err != nil {
		return ScrapeSiteJob{}, err
	}
	popScript := rssq.popScript
	if popScript == nil {
		popScript = redis.NewScript(luaScrapePopScript)
	}

	for {
		select {
		case <-ctx.Done():
			return ScrapeSiteJob{}, ctx.Err()
		default:
		}

		currentTimeMs := time.Now().UnixMilli()
		result, err := popScript.Run(
			ctx,
			client,
			[]string{scrapesiteQueueHeadKey},
			currentTimeMs,
			rssq.leasePeriod().Milliseconds(),
			scrapesiteKeyPrefix,
			scrapeScheduleKeyPrefix,
			scrapeLeaseKeyPrefix,
			scrapeClaimSequenceKey,
		).Result()

		if err != nil {
			return ScrapeSiteJob{}, fmt.Errorf("lua script failed: %w", err)
		}

		popResult, err := parseScrapePopResult(result)
		if err != nil {
			return ScrapeSiteJob{}, fmt.Errorf("invalid lua response: %w", err)
		}
		if !popResult.Found {
			waitDuration := dequeueWaitDuration(popResult.NextReadyMs, currentTimeMs)
			if err := waitForNextScrapePoll(ctx, waitDuration); err != nil {
				return ScrapeSiteJob{}, err
			}
			continue
		}

		var site domain.ScrapeSite
		if err := json.Unmarshal([]byte(popResult.SiteJSON), &site); err != nil {
			return ScrapeSiteJob{}, fmt.Errorf("failed to unmarshal scrapesite: %w", err)
		}
		var schedule ScrapeSchedule
		if err := json.Unmarshal([]byte(popResult.ScheduleJSON), &schedule); err != nil {
			return ScrapeSiteJob{}, fmt.Errorf("failed to unmarshal scrape schedule: %w", err)
		}
		return ScrapeSiteJob{Site: site, Interval: checkpace.FallbackInterval, Schedule: schedule,
			queueKey: popResult.QueueKey, leaseScoreMS: popResult.LeaseScoreMS, claimID: popResult.ClaimID}, nil
	}
}

func parseScrapePopResult(result interface{}) (scrapePopResult, error) {
	resSlice, ok := result.([]interface{})
	if !ok {
		return scrapePopResult{}, fmt.Errorf("unexpected lua result type %T", result)
	}
	if len(resSlice) != 8 {
		return scrapePopResult{}, fmt.Errorf("unexpected lua result length %d", len(resSlice))
	}

	foundFlag, err := coerceLuaInt64(resSlice[0])
	if err != nil {
		return scrapePopResult{}, fmt.Errorf("invalid found flag: %w", err)
	}

	if foundFlag == 0 {
		nextReadyMs, err := coerceLuaInt64(resSlice[3])
		if err != nil {
			return scrapePopResult{}, fmt.Errorf("invalid next-ready score: %w", err)
		}
		return scrapePopResult{
			Found:       false,
			NextReadyMs: nextReadyMs,
		}, nil
	}

	siteJSON, err := coerceLuaString(resSlice[2])
	if err != nil {
		return scrapePopResult{}, fmt.Errorf("invalid site payload: %w", err)
	}

	score, err := coerceLuaInt64(resSlice[3])
	if err != nil {
		return scrapePopResult{}, fmt.Errorf("invalid score: %w", err)
	}
	lease, err := coerceLuaInt64(resSlice[4])
	if err != nil {
		return scrapePopResult{}, fmt.Errorf("invalid lease: %w", err)
	}
	schedule, err := coerceLuaString(resSlice[5])
	if err != nil {
		return scrapePopResult{}, fmt.Errorf("invalid schedule: %w", err)
	}
	queueKey, err := coerceLuaString(resSlice[6])
	if err != nil {
		return scrapePopResult{}, fmt.Errorf("invalid queue key: %w", err)
	}
	claimID, err := coerceLuaInt64(resSlice[7])
	if err != nil {
		return scrapePopResult{}, fmt.Errorf("invalid claim ID: %w", err)
	}

	return scrapePopResult{
		Found:        true,
		SiteJSON:     siteJSON,
		ScoreMs:      score,
		NextReadyMs:  -1,
		LeaseScoreMS: lease,
		ScheduleJSON: schedule,
		QueueKey:     queueKey,
		ClaimID:      claimID,
	}, nil
}

func dequeueWaitDuration(nextReadyMs int64, currentMs int64) time.Duration {
	if nextReadyMs <= 0 {
		return idleQueueSleep
	}

	waitMs := nextReadyMs - currentMs
	if waitMs <= 0 {
		return minDequeueSleep
	}

	wait := time.Duration(waitMs) * time.Millisecond
	if wait < minDequeueSleep {
		return minDequeueSleep
	}
	if wait > maxDequeueSleep {
		return maxDequeueSleep
	}
	return wait
}

func waitForNextScrapePoll(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		duration = minDequeueSleep
	}

	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func coerceLuaString(value interface{}) (string, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	default:
		return "", fmt.Errorf("expected string/[]byte, got %T", value)
	}
}

func coerceLuaInt64(value interface{}) (int64, error) {
	switch v := value.(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case uint64:
		if v > math.MaxInt64 {
			return 0, fmt.Errorf("uint64 value out of range: %d", v)
		}
		return int64(v), nil
	case float64:
		if math.Trunc(v) != v {
			return 0, fmt.Errorf("non-integer float64 value %f", v)
		}
		return int64(v), nil
	case string:
		parsed, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, err
		}
		return parsed, nil
	case []byte:
		parsed, err := strconv.ParseInt(string(v), 10, 64)
		if err != nil {
			return 0, err
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("expected numeric lua response, got %T", value)
	}
}

func (rssq *RedisScrapeSiteQueue) GetScrapeSiteCount() (int64, error) {
	client, err := rssq.clientOrErr()
	if err != nil {
		return 0, err
	}
	ctx := rssq.baseContext()

	var total int64
	for _, key := range rssq.popKeys() {
		count, err := client.ZCard(ctx, key).Result()
		if err != nil {
			return 0, err
		}
		total += count
	}
	return total, nil
}

func (rssq *RedisScrapeSiteQueue) GetActiveInstances() (int, error) {
	client, err := rssq.clientOrErr()
	if err != nil {
		return 0, err
	}
	return instanceid.CountActiveInstances(rssq.baseContext(), client)
}

func (rssq *RedisScrapeSiteQueue) RequeueAll() (int64, error) {
	if rssq == nil {
		return 0, errors.New("redis scrape queue is nil")
	}

	client, err := rssq.clientOrErr()
	if err != nil {
		return 0, err
	}
	ctx := rssq.baseContext()
	interval := requeueAllWindow(rssq.pace.Current())

	const batchSize int64 = 500

	var total int64
	now := time.Now()

	for _, key := range rssq.popKeys() {
		count, err := client.ZCard(ctx, key).Result()
		if err != nil {
			return total, fmt.Errorf("requeue all: count queue %s: %w", key, err)
		}
		if count == 0 {
			continue
		}

		total += count
		members, err := client.ZRange(ctx, key, 0, count-1).Result()
		if err != nil {
			return total, fmt.Errorf("requeue all: list queue %s: %w", key, err)
		}

		for start := 0; start < len(members); start += int(batchSize) {
			end := start + int(batchSize)
			if end > len(members) {
				end = len(members)
			}

			pipe := client.Pipeline()
			for index, member := range members[start:end] {
				position := int64(start + index)
				offset := (interval * time.Duration(position)) / time.Duration(count)
				nextCheck := now.Add(offset)
				pipe.Eval(ctx, luaScrapeForceScript, rssq.forceKeys(key, member), member, nextCheck.UnixMilli(), "")
			}

			if _, err := pipe.Exec(ctx); err != nil {
				return total, fmt.Errorf("requeue all: update queue %s: %w", key, err)
			}
		}
	}

	if err := rssq.refreshQueueHeads(); err != nil {
		return total, fmt.Errorf("requeue all: refresh queue heads: %w", err)
	}

	return total, nil
}

// requeueAllWindow spreads sources over the fastest workspace scrape cadence,
// bounded to avoid a burst of fetches.
func requeueAllWindow(pace *checkpace.Snapshot) time.Duration {
	if pace == nil || pace.FastestScrapeInterval <= 0 {
		return checkpace.FallbackInterval
	}
	return min(max(pace.FastestScrapeInterval, requeueAllMinimumWindow), checkpace.MaxRouteInterval)
}

func (rssq *RedisScrapeSiteQueue) Close() error {
	return support.CloseRedisClient()
}
