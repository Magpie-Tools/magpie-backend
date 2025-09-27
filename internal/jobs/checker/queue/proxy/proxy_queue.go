package proxyqueue

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/charmbracelet/log"
	"github.com/redis/go-redis/v9"
	"magpie/internal/config"
	"magpie/internal/domain"
	"magpie/internal/support"
)

const (
	proxyKeyPrefix  = "proxy:"
	queueKey        = "proxy_queue"
	emptyQueueSleep = 1 * time.Second
)

//go:embed pop.lua
var luaPopScript string

type RedisProxyQueue struct {
	client    *redis.Client
	ctx       context.Context
	popScript *redis.Script
}

var PublicProxyQueue RedisProxyQueue

func init() {
	client, err := support.GetRedisClient()
	if err != nil {
		log.Fatal("Could not connect to redis for proxy queue", "error", err)
	}
	PublicProxyQueue = *NewRedisProxyQueue(client)

	go startInstanceHeartbeat()
}

func NewRedisProxyQueue(client *redis.Client) *RedisProxyQueue {
	return &RedisProxyQueue{
		client:    client,
		ctx:       context.Background(),
		popScript: redis.NewScript(luaPopScript),
	}
}

func (rpq *RedisProxyQueue) AddToQueue(proxies []domain.Proxy) error {
	pipe := rpq.client.Pipeline()
	interval := config.GetTimeBetweenChecks()
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

func (rpq *RedisProxyQueue) GetNextProxy() (domain.Proxy, time.Time, error) {
	for {
		currentTime := time.Now().Unix()
		result, err := rpq.popScript.Run(rpq.ctx, rpq.client, []string{queueKey, proxyKeyPrefix}, currentTime).Result()

		if errors.Is(err, redis.Nil) {
			time.Sleep(emptyQueueSleep)
			continue
		} else if err != nil {
			return domain.Proxy{}, time.Time{}, fmt.Errorf("lua script failed: %w", err)
		}

		resSlice := result.([]interface{})
		proxyJSON := resSlice[1].(string)
		score := resSlice[2].(int64)

		var proxy domain.Proxy
		if err := json.Unmarshal([]byte(proxyJSON), &proxy); err != nil {
			return domain.Proxy{}, time.Time{}, fmt.Errorf("failed to unmarshal proxy: %w", err)
		}

		return proxy, time.Unix(score, 0), nil
	}
}

func (rpq *RedisProxyQueue) RequeueProxy(proxy domain.Proxy, lastCheckTime time.Time) error {
	nextCheck := lastCheckTime.Add(config.GetTimeBetweenChecks())
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
	return support.CloseRedisClient()
}
