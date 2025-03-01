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
	proxyKeyPrefix  = "proxy:"
	queueKey        = "proxy_queue"
	emptyQueueSleep = 1 * time.Second
)

var luaPopScript = `
local result = redis_queue.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
if #result == 0 then return nil end

local member = result[1]
local score = tonumber(result[2])
local current_time = tonumber(ARGV[1])

if score > current_time then return nil end

local proxy_key = KEYS[2] .. member
local proxy_data = redis_queue.call('GET', proxy_key)

if redis_queue.call('ZREM', KEYS[1], member) == 0 then return nil end
redis_queue.call('DEL', proxy_key)

return {member, proxy_data, score}
`

type RedisProxyQueue struct {
	client    *redis.Client
	ctx       context.Context
	popScript *redis.Script
}

var PublicProxyQueue RedisProxyQueue

func init() {
	ppq, err := NewRedisProxyQueue(helper.GetEnv("redisUrl", "redis://localhost:6379"))
	if err != nil {
		log.Fatal("Could not connect to redis for proxy queue", "error", err)
	}
	PublicProxyQueue = *ppq

	go startInstanceHeartbeat()
}

func NewRedisProxyQueue(redisURL string) (*RedisProxyQueue, error) {
	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Redis URL: %w", err)
	}

	client := redis.NewClient(opt)
	ctx := context.Background()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return &RedisProxyQueue{
		client:    client,
		ctx:       ctx,
		popScript: redis.NewScript(luaPopScript),
	}, nil
}

func (rpq *RedisProxyQueue) AddToQueue(proxies []models.Proxy) error {
	pipe := rpq.client.Pipeline()
	interval := settings.GetTimeBetweenChecks()
	now := time.Now()
	proxyLenDuration := time.Duration(len(proxies))
	batchSize := 500 // Adjust based on your Redis server capabilities

	for i, proxy := range proxies {
		offset := (interval * time.Duration(i)) / proxyLenDuration
		nextCheck := now.Add(offset)
		hashKey := string(proxy.Hash)
		proxyKey := proxyKeyPrefix + hashKey

		proxyJSON, err := json.Marshal(proxy)
		if err != nil {
			return fmt.Errorf("failed to marshal proxy: %w", err)
		}

		pipe.Set(rpq.ctx, proxyKey, proxyJSON, 0)
		pipe.ZAdd(rpq.ctx, queueKey, redis.Z{
			Score:  float64(nextCheck.Unix()),
			Member: hashKey,
		})

		// Execute in batches to prevent oversized pipelines
		if i%batchSize == 0 && i > 0 {
			if _, err := pipe.Exec(rpq.ctx); err != nil {
				return fmt.Errorf("batch pipeline failed: %w", err)
			}
			pipe = rpq.client.Pipeline()
		}
	}

	if _, err := pipe.Exec(rpq.ctx); err != nil {
		return fmt.Errorf("final pipeline exec failed: %w", err)
	}

	return nil
}

func (rpq *RedisProxyQueue) GetNextProxy() (models.Proxy, time.Time, error) {
	for {
		currentTime := time.Now().Unix()
		result, err := rpq.popScript.Run(rpq.ctx, rpq.client, []string{queueKey, proxyKeyPrefix}, currentTime).Result()

		if errors.Is(err, redis.Nil) {
			time.Sleep(emptyQueueSleep)
			continue
		} else if err != nil {
			return models.Proxy{}, time.Time{}, fmt.Errorf("lua script failed: %w", err)
		}

		resSlice := result.([]interface{})
		proxyJSON := resSlice[1].(string)
		score := resSlice[2].(int64)

		var proxy models.Proxy
		if err := json.Unmarshal([]byte(proxyJSON), &proxy); err != nil {
			return models.Proxy{}, time.Time{}, fmt.Errorf("failed to unmarshal proxy: %w", err)
		}

		return proxy, time.Unix(score, 0), nil
	}
}

func (rpq *RedisProxyQueue) RequeueProxy(proxy models.Proxy, lastCheckTime time.Time) error {
	nextCheck := lastCheckTime.Add(settings.GetTimeBetweenChecks())
	hashKey := string(proxy.Hash)
	proxyKey := proxyKeyPrefix + hashKey

	proxyJSON, err := json.Marshal(proxy)
	if err != nil {
		return fmt.Errorf("failed to marshal proxy: %w", err)
	}

	pipe := rpq.client.Pipeline()
	pipe.Set(rpq.ctx, proxyKey, proxyJSON, 0)
	pipe.ZAdd(rpq.ctx, queueKey, redis.Z{
		Score:  float64(nextCheck.Unix()),
		Member: hashKey,
	})

	_, err = pipe.Exec(rpq.ctx)
	return err
}

func (rpq *RedisProxyQueue) GetProxyCount() (int64, error) {
	return rpq.client.ZCard(rpq.ctx, queueKey).Result()
}

func (rpq *RedisProxyQueue) GetActiveInstances() (int, error) {
	keys, err := rpq.client.Keys(rpq.ctx, "magpie:instance:*").Result()
	if err != nil {
		return 0, err
	}
	return len(keys), nil
}

func (rpq *RedisProxyQueue) Close() error {
	return rpq.client.Close()
}
