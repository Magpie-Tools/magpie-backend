package sitequeue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"magpie/internal/config"
	"magpie/internal/domain"
	"magpie/internal/support"

	"github.com/charmbracelet/log"
	"github.com/redis/go-redis/v9"
)

const (
	scrapesiteKeyPrefix = "scrapesite:"
	scrapesiteQueueKey  = "scrapesite_queue"
	emptyQueueSleep     = 1 * time.Second
)

var luaScrapePopScript = `
local popped = redis.call('ZPOPMIN', KEYS[1], 1)
if #popped == 0 then
  return nil                                  -- queue empty
end

local member = popped[1]                      -- site url
local score  = tonumber(popped[2])            -- next‑due timestamp
local now    = tonumber(ARGV[1])

-- If the next‑due time is still in the future, push it back and exit
if score > now then
  redis.call('ZADD', KEYS[1], score, member)  -- restore exactly as it was
  return nil
end

-- Fetch the cached site definition, then delete the key
local site_key  = KEYS[2] .. member
local site_data = redis.call('GET', site_key)
redis.call('DEL', site_key)

return { member, site_data, score }
`

type RedisScrapeSiteQueue struct {
	client    *redis.Client
	ctx       context.Context
	popScript *redis.Script
}

var PublicScrapeSiteQueue RedisScrapeSiteQueue

func init() {
	sssq, err := NewRedisScrapeSiteQueue(support.GetEnv("redisUrl", "redis://localhost:8946"))
	if err != nil {
		log.Fatal("Could not connect to redis for scrape site queue", "error", err)
	}
	PublicScrapeSiteQueue = *sssq
}

func NewRedisScrapeSiteQueue(redisURL string) (*RedisScrapeSiteQueue, error) {
	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Redis URL: %w", err)
	}

	client := redis.NewClient(opt)
	ctx := context.Background()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return &RedisScrapeSiteQueue{
		client:    client,
		ctx:       ctx,
		popScript: redis.NewScript(luaScrapePopScript),
	}, nil
}

func (rssq *RedisScrapeSiteQueue) AddToQueue(sites []domain.ScrapeSite) error {
	pipe := rssq.client.Pipeline()
	interval := config.GetTimeBetweenScrapes()
	now := time.Now()
	sitesLenDuration := time.Duration(len(sites))
	batchSize := 50

	for i, site := range sites {
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

func (rssq *RedisScrapeSiteQueue) GetNextScrapeSite() (domain.ScrapeSite, time.Time, error) {
	for {
		currentTime := time.Now().Unix()
		result, err := rssq.popScript.Run(rssq.ctx, rssq.client, []string{scrapesiteQueueKey, scrapesiteKeyPrefix}, currentTime).Result()

		if errors.Is(err, redis.Nil) {
			time.Sleep(emptyQueueSleep)
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
	nextCheck := lastCheckTime.Add(config.GetTimeBetweenScrapes())
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
	keys, err := rssq.client.Keys(rssq.ctx, "magpie:instance:*").Result()
	if err != nil {
		return 0, err
	}
	return len(keys), nil
}

func (rssq *RedisScrapeSiteQueue) Close() error {
	return rssq.client.Close()
}
