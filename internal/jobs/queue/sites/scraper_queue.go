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
}

func NewRedisScrapeSiteQueue(client *redis.Client) *RedisScrapeSiteQueue {
	return &RedisScrapeSiteQueue{
		client:    client,
		ctx:       context.Background(),
		popScript: redis.NewScript(luaScrapePopScript),
	}
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
	return runtime.CountActiveInstances(rssq.ctx, rssq.client)
}

func (rssq *RedisScrapeSiteQueue) Close() error {
	return support.CloseRedisClient()
}
