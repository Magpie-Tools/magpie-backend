package support

import (
	"context"
	"fmt"
	"sync"

	"github.com/redis/go-redis/v9"
)

var (
	redisMu     sync.Mutex
	redisClient *redis.Client
)

func GetRedisClient() (*redis.Client, error) {
	redisMu.Lock()
	defer redisMu.Unlock()

	if redisClient != nil {
		return redisClient, nil
	}

	redisURL := GetEnv("redisUrl", "redis://localhost:8946")

	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Redis URL %q: %w", redisURL, err)
	}

	client := redis.NewClient(opt)
	if err := client.Ping(context.Background()).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	redisClient = client
	return redisClient, nil
}

func CloseRedisClient() error {
	redisMu.Lock()
	defer redisMu.Unlock()

	if redisClient == nil {
		return nil
	}

	err := redisClient.Close()
	redisClient = nil
	return err
}
