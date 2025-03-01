package redis_queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/charmbracelet/log"
	"github.com/redis/go-redis/v9"
	"magpie/helper"
	"magpie/models"
	"magpie/settings"
)

const (
	scrapesiteKeyPrefix = "scrapesite:"
	scrapesiteQueueKey  = "scrapesite_queue"
	emptyQueueSleep     = 1 * time.Second
)

var luaScrapePopScript = `
local result = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
if #result == 0 then return nil end

local member = result[1]
local score = tonumber(result[2])
local current_time = tonumber(ARGV[1])

if score > current_time then return nil end

local site_key = KEYS[2] .. member
local site_data = redis.call('GET', site_key)

if redis.call('ZREM', KEYS[1], member) == 0 then return nil end
redis.call('DEL', site_key)

return {member, site_data, score}
`

type RedisScrapeSiteQueue struct {
	client    *redis.Client
	ctx       context.Context
	popScript *redis.Script
}

var PublicScrapeSiteQueue RedisScrapeSiteQueue

func init() {
	sssq, err := NewRedisScrapeSiteQueue(helper.GetEnv("redisUrl", "redis://host.docker.internal:6379"))
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

func (rssq *RedisScrapeSiteQueue) AddToQueue(sites []models.ScrapeSite) error {
	pipe := rssq.client.Pipeline()
	interval := settings.GetTimeBetweenChecks()
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

func (rssq *RedisScrapeSiteQueue) GetNextScrapeSite() (models.ScrapeSite, time.Time, error) {
	for {
		currentTime := time.Now().Unix()
		result, err := rssq.popScript.Run(rssq.ctx, rssq.client, []string{scrapesiteQueueKey, scrapesiteKeyPrefix}, currentTime).Result()

		if errors.Is(err, redis.Nil) {
			time.Sleep(emptyQueueSleep)
			continue
		} else if err != nil {
			return models.ScrapeSite{}, time.Time{}, fmt.Errorf("lua script failed: %w", err)
		}

		resSlice := result.([]interface{})
		siteJSON := resSlice[1].(string)
		score := resSlice[2].(int64)

		var site models.ScrapeSite
		if err := json.Unmarshal([]byte(siteJSON), &site); err != nil {
			return models.ScrapeSite{}, time.Time{}, fmt.Errorf("failed to unmarshal scrapesite: %w", err)
		}

		return site, time.Unix(score, 0), nil
	}
}

func (rssq *RedisScrapeSiteQueue) RequeueScrapeSite(site models.ScrapeSite, lastCheckTime time.Time) error {
	nextCheck := lastCheckTime.Add(settings.GetTimeBetweenScrapes())
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
