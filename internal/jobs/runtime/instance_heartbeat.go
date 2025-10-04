package runtime

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/charmbracelet/log"
	"github.com/redis/go-redis/v9"
)

const (
	InstanceHeartbeatKeyPrefix = "magpie:instance:"
	DefaultHeartbeatInterval   = 15 * time.Second
	DefaultHeartbeatTTL        = 30 * time.Second
)

var instanceID = generateInstanceID()

func generateInstanceID() string {
	hostname, _ := os.Hostname()
	return fmt.Sprintf("%s-%d-%d", hostname, os.Getpid(), time.Now().UnixNano())
}

func StartInstanceHeartbeat(ctx context.Context, client *redis.Client, keyPrefix string, interval, ttl time.Duration) {
	if ctx == nil {
		ctx = context.Background()
	}
	heartbeatKey := keyPrefix + instanceID

	sendHeartbeat := func() {
		if err := client.SetEx(ctx, heartbeatKey, "alive", ttl).Err(); err != nil {
			log.Error("Failed to update instance heartbeat", "key", heartbeatKey, "error", err)
		}
	}

	sendHeartbeat()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sendHeartbeat()
		}
	}
}

func LaunchInstanceHeartbeat(parent context.Context, client *redis.Client) context.CancelFunc {
	ctx, cancel := context.WithCancel(parent)
	go StartInstanceHeartbeat(ctx, client, InstanceHeartbeatKeyPrefix, DefaultHeartbeatInterval, DefaultHeartbeatTTL)
	return cancel
}

func CountActiveInstances(ctx context.Context, client *redis.Client) (int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	keys, err := client.Keys(ctx, InstanceHeartbeatKeyPrefix+"*").Result()
	if err != nil {
		return 0, err
	}
	return len(keys), nil
}
