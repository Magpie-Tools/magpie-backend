package sitequeue

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"magpie/internal/config"
	"magpie/internal/domain"
	"magpie/internal/jobs/runtime"
	"magpie/internal/support"

	"github.com/charmbracelet/log"
	"github.com/redis/go-redis/v9"
)

const (
	scrapesiteKeyPrefix = "scrapesite:"
	scrapesiteQueueKey  = "scrapesite_queue"
	emptyQueueSleep     = 1 * time.Second
)

//go:embed pop.lua
var luaScrapePopScript string

type RedisScrapeSiteQueue struct {
	client    *redis.Client
	ctx       context.Context
	popScript *redis.Script
}

var PublicScrapeSiteQueue RedisScrapeSiteQueue

func init() {
	client, err := support.GetRedisClient()
	if err != nil {
		log.Fatal("Could not connect to redis for scrape site queue", "error", err)
	}
	PublicScrapeSiteQueue = *NewRedisScrapeSiteQueue(client)

	go func() {
		updates := config.ScrapeIntervalUpdates()
		for interval := range updates {
			if err := PublicScrapeSiteQueue.Reschedule(interval); err != nil {
				log.Error("Failed to reschedule scrape queue after interval update", "error", err)
			}
		}
	}()
}

func NewRedisScrapeSiteQueue(client *redis.Client) *RedisScrapeSiteQueue {
	return &RedisScrapeSiteQueue{
		client:    client,
		ctx:       context.Background(),
		popScript: redis.NewScript(luaScrapePopScript),
	}
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

	pipe := rssq.client.Pipeline()
	interval := config.GetTimeBetweenScrapes()
	now := time.Now()
	sitesLenDuration := time.Duration(len(filtered))
	batchSize := 50

	for i, site := range filtered {
		offset := (interval * time.Duration(i)) / sitesLenDuration
		nextCheck := now.Add(offset)
		proxyKey := scrapesiteKeyPrefix + site.URL

		proxyJSON, err := json.Marshal(site)
		if err != nil {
			return fmt.Errorf("failed to marshal site: %w", err)
		}

		pipe.Set(rssq.ctx, proxyKey, proxyJSON, 0)
		pipe.ZAdd(rssq.ctx, scrapesiteQueueKey, redis.Z{
			Score:  float64(nextCheck.Unix()),
			Member: site.URL,
		})

		// Execute in batches to prevent oversized pipelines
		if i%batchSize == 0 && i > 0 {
			if _, err := pipe.Exec(rssq.ctx); err != nil {
				return fmt.Errorf("batch pipeline failed: %w", err)
			}
			pipe = rssq.client.Pipeline()
		}
	}

	if _, err := pipe.Exec(rssq.ctx); err != nil {
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

	const batchSize = 250
	pipe := rssq.client.Pipeline()
	opCount := 0

	flush := func() error {
		if opCount == 0 {
			return nil
		}
		if _, err := pipe.Exec(rssq.ctx); err != nil {
			return fmt.Errorf("remove pipeline exec failed: %w", err)
		}
		pipe = rssq.client.Pipeline()
		opCount = 0
		return nil
	}

	for _, site := range sites {
		if site.URL == "" {
			continue
		}

		key := scrapesiteKeyPrefix + site.URL

		pipe.Del(rssq.ctx, key)
		opCount++
		pipe.ZRem(rssq.ctx, scrapesiteQueueKey, site.URL)
		opCount++

		if opCount >= batchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}

	return flush()
}

func (rssq *RedisScrapeSiteQueue) GetNextScrapeSite() (domain.ScrapeSite, time.Time, error) {
	return rssq.GetNextScrapeSiteContext(rssq.ctx)
}

func (rssq *RedisScrapeSiteQueue) GetNextScrapeSiteContext(ctx context.Context) (domain.ScrapeSite, time.Time, error) {
	if ctx == nil {
		ctx = rssq.ctx
	}

	for {
		select {
		case <-ctx.Done():
			return domain.ScrapeSite{}, time.Time{}, ctx.Err()
		default:
		}

		currentTime := time.Now().Unix()
		result, err := rssq.popScript.Run(ctx, rssq.client, []string{scrapesiteQueueKey, scrapesiteKeyPrefix}, currentTime).Result()

		if errors.Is(err, redis.Nil) {
			select {
			case <-ctx.Done():
				return domain.ScrapeSite{}, time.Time{}, ctx.Err()
			case <-time.After(emptyQueueSleep):
			}
			continue
		} else if err != nil {
			return domain.ScrapeSite{}, time.Time{}, fmt.Errorf("lua script failed: %w", err)
		}

		resSlice := result.([]interface{})
		siteJSON := resSlice[1].(string)
		score := resSlice[2].(int64)

		var site domain.ScrapeSite
		if err := json.Unmarshal([]byte(siteJSON), &site); err != nil {
			return domain.ScrapeSite{}, time.Time{}, fmt.Errorf("failed to unmarshal scrapesite: %w", err)
		}

		return site, time.Unix(score, 0), nil
	}
}

func (rssq *RedisScrapeSiteQueue) RequeueScrapeSite(site domain.ScrapeSite, lastCheckTime time.Time) error {
	interval := config.GetTimeBetweenScrapes()
	base := lastCheckTime
	if now := time.Now(); now.After(base) {
		base = now
	}
	nextCheck := base.Add(interval)
	proxyKey := scrapesiteKeyPrefix + site.URL

	proxyJSON, err := json.Marshal(site)
	if err != nil {
		return fmt.Errorf("failed to marshal proxy: %w", err)
	}

	pipe := rssq.client.Pipeline()
	pipe.Set(rssq.ctx, proxyKey, proxyJSON, 0)
	pipe.ZAdd(rssq.ctx, scrapesiteQueueKey, redis.Z{
		Score:  float64(nextCheck.Unix()),
		Member: site.URL,
	})

	_, err = pipe.Exec(rssq.ctx)
	return err
}

func (rssq *RedisScrapeSiteQueue) GetScrapeSiteCount() (int64, error) {
	return rssq.client.ZCard(rssq.ctx, scrapesiteQueueKey).Result()
}

func (rssq *RedisScrapeSiteQueue) GetActiveInstances() (int, error) {
	return runtime.CountActiveInstances(rssq.ctx, rssq.client)
}

func (rssq *RedisScrapeSiteQueue) Close() error {
	return support.CloseRedisClient()
}

func (rssq *RedisScrapeSiteQueue) Reschedule(interval time.Duration) error {
	if rssq == nil {
		return errors.New("redis scrape queue is nil")
	}

	if interval <= 0 {
		interval = time.Second
	}

	total, err := rssq.client.ZCard(rssq.ctx, scrapesiteQueueKey).Result()
	if err != nil {
		return fmt.Errorf("reschedule: failed to count queue entries: %w", err)
	}

	if total == 0 {
		return nil
	}

	now := time.Now()
	totalDuration := time.Duration(total)
	const fetchBatch int64 = 500
	const updateBatch = 250

	pipe := rssq.client.Pipeline()
	opCount := 0

	flush := func() error {
		if opCount == 0 {
			return nil
		}
		if _, err := pipe.Exec(rssq.ctx); err != nil {
			return fmt.Errorf("reschedule: pipeline exec failed: %w", err)
		}
		pipe = rssq.client.Pipeline()
		opCount = 0
		return nil
	}

	for start := int64(0); start < total; start += fetchBatch {
		end := start + fetchBatch - 1
		if end >= total {
			end = total - 1
		}

		members, err := rssq.client.ZRange(rssq.ctx, scrapesiteQueueKey, start, end).Result()
		if err != nil {
			return fmt.Errorf("reschedule: failed to fetch members: %w", err)
		}

		for idx, member := range members {
			globalIndex := start + int64(idx)
			offset := (interval * time.Duration(globalIndex)) / totalDuration
			nextCheck := now.Add(offset).Unix()

			pipe.ZAdd(rssq.ctx, scrapesiteQueueKey, redis.Z{
				Score:  float64(nextCheck),
				Member: member,
			})
			opCount++

			if opCount != 0 && opCount%updateBatch == 0 {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}

	if err := flush(); err != nil {
		return err
	}

	log.Debug("scrape queue rescheduled", "entries", total, "interval", interval)
	return nil
}
