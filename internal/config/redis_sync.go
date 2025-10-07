package config

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/charmbracelet/log"
	"github.com/redis/go-redis/v9"
)

const (
	redisConfigKey     = "magpie:config:settings"
	redisConfigChannel = "magpie:config:updates"
	redisOpTimeout     = 5 * time.Second
)

type redisSyncState struct {
	mu     sync.RWMutex
	client *redis.Client
	ctx    context.Context
	cancel context.CancelFunc
}

var globalRedisSync redisSyncState

func EnableRedisSynchronization(ctx context.Context, client *redis.Client) {
	if client == nil {
		log.Warn("Config synchronization disabled: redis client is nil")
		return
	}

	if ctx == nil {
		ctx = context.Background()
	}

	syncCtx, cancel := context.WithCancel(ctx)

	globalRedisSync.mu.Lock()
	if globalRedisSync.client != nil {
		globalRedisSync.mu.Unlock()
		cancel()
		return
	}

	globalRedisSync.client = client
	globalRedisSync.ctx = syncCtx
	globalRedisSync.cancel = cancel
	globalRedisSync.mu.Unlock()

	loaded, err := loadConfigFromRedis(syncCtx, client)
	if err != nil {
		log.Error("Config sync: failed to load configuration from redis", "error", err)
	}

	if !loaded {
		payload, err := json.Marshal(GetConfig())
		if err != nil {
			log.Error("Config sync: failed to serialize configuration for redis", "error", err)
		} else if err := broadcastConfigUpdate(payload); err != nil {
			log.Error("Config sync: failed to publish configuration to redis", "error", err)
		}
	}

	go subscribeToConfigUpdates(syncCtx, client)
}

func loadConfigFromRedis(ctx context.Context, client *redis.Client) (bool, error) {
	opCtx, cancel := context.WithTimeout(ctx, redisOpTimeout)
	defer cancel()

	payload, err := client.Get(opCtx, redisConfigKey).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return false, nil
		}
		return false, err
	}

	var cfg Config
	if err := json.Unmarshal([]byte(payload), &cfg); err != nil {
		return true, err
	}

	if err := applyConfigUpdate(cfg, configUpdateOptions{persistToFile: true, source: "redis"}); err != nil {
		return true, err
	}

	return true, nil
}

func subscribeToConfigUpdates(ctx context.Context, client *redis.Client) {
	pubsub := client.Subscribe(ctx, redisConfigChannel)
	defer pubsub.Close()

	for {
		msg, err := pubsub.ReceiveMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, redis.ErrClosed) || ctx.Err() != nil {
				return
			}
			log.Error("Config sync: subscription error", "error", err)
			time.Sleep(time.Second)
			continue
		}

		var cfg Config
		if err := json.Unmarshal([]byte(msg.Payload), &cfg); err != nil {
			log.Error("Config sync: invalid payload", "error", err)
			continue
		}

		if err := applyConfigUpdate(cfg, configUpdateOptions{persistToFile: true, source: "redis"}); err != nil {
			log.Error("Config sync: failed to apply remote update", "error", err)
		}
	}
}

func broadcastConfigUpdate(payload []byte) error {
	if len(payload) == 0 {
		return nil
	}

	globalRedisSync.mu.RLock()
	client := globalRedisSync.client
	baseCtx := globalRedisSync.ctx
	globalRedisSync.mu.RUnlock()

	if client == nil {
		return nil
	}

	ctx := baseCtx
	if ctx == nil || ctx.Err() != nil {
		ctx = context.Background()
	}

	opCtx, cancel := context.WithTimeout(ctx, redisOpTimeout)
	defer cancel()

	if err := client.Set(opCtx, redisConfigKey, payload, 0).Err(); err != nil {
		return err
	}

	if err := client.Publish(opCtx, redisConfigChannel, payload).Err(); err != nil {
		return err
	}

	return nil
}
