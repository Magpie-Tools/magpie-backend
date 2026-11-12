package runtime

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/charmbracelet/log"
	"github.com/redis/go-redis/v9"

	"magpie/internal/instanceid"
	"magpie/internal/support"
)

const (
	DefaultHeartbeatInterval = 15 * time.Second
	DefaultHeartbeatTTL      = 30 * time.Second
	heartbeatRetryInterval   = 5 * time.Second
)

type ActiveInstance struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Region    string `json:"region"`
	PortStart int    `json:"port_start"`
	PortEnd   int    `json:"port_end"`
}

func currentInstancePayload() ActiveInstance {
	instanceID := currentInstanceID()
	start, end := support.GetRotatingProxyPortRange()
	return ActiveInstance{
		ID:        instanceID,
		Name:      support.GetInstanceName(),
		Region:    support.GetInstanceRegion(),
		PortStart: start,
		PortEnd:   end,
	}
}

func CurrentInstance() ActiveInstance {
	return currentInstancePayload()
}

func StartInstanceHeartbeat(ctx context.Context, client *redis.Client, keyPrefix string, interval, ttl time.Duration) {
	if ctx == nil {
		ctx = context.Background()
	}
	if client == nil {
		log.Warn("Instance heartbeat disabled: redis client is nil")
		<-ctx.Done()
		return
	}
	instanceID := currentInstanceID()
	heartbeatKey := heartbeatKeyForInstance(instanceID, keyPrefix)
	heartbeatValue, _ := json.Marshal(currentInstancePayload())

	sendHeartbeat := func() {
		expiresAt := time.Now().Add(ttl).UnixMilli()
		pipe := client.Pipeline()
		pipe.SetEx(ctx, heartbeatKey, heartbeatValue, ttl)
		pipe.ZAdd(ctx, instanceid.InstanceHeartbeatIndexKey, redis.Z{
			Score:  float64(expiresAt),
			Member: instanceID,
		})
		if _, err := pipe.Exec(ctx); err != nil {
			log.Error("Failed to update instance heartbeat", "key", heartbeatKey, "error", err)
		}
	}

	sendHeartbeat()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			pipe := client.Pipeline()
			pipe.Del(cleanupCtx, heartbeatKey)
			pipe.ZRem(cleanupCtx, instanceid.InstanceHeartbeatIndexKey, instanceID)
			if _, err := pipe.Exec(cleanupCtx); err != nil {
				log.Warn("Failed to cleanup instance heartbeat", "instance_id", instanceID, "error", err)
			}
			cancel()
			return
		case <-ticker.C:
			sendHeartbeat()
		}
	}
}

func LaunchInstanceHeartbeat(parent context.Context, client *redis.Client) context.CancelFunc {
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	go func() {
		defer close(done)
		activeClient := client
		loggedUnavailable := false

		for {
			if activeClient == nil {
				retryClient, err := support.GetRedisClient()
				if err != nil {
					if !loggedUnavailable {
						log.Warn("Instance heartbeat postponed until redis is available", "error", err)
						loggedUnavailable = true
					}

					select {
					case <-ctx.Done():
						return
					case <-time.After(heartbeatRetryInterval):
						continue
					}
				}

				activeClient = retryClient
				if loggedUnavailable {
					log.Info("Instance heartbeat connected to redis")
				}
			}

			StartInstanceHeartbeat(ctx, activeClient, instanceid.InstanceHeartbeatKeyPrefix, DefaultHeartbeatInterval, DefaultHeartbeatTTL)
			return
		}
	}()
	return func() { cancel(); <-done }
}

func currentInstanceID() string {
	return strings.TrimSpace(support.GetInstanceID())
}

func ListActiveInstances(ctx context.Context, client *redis.Client) ([]ActiveInstance, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := instanceid.PruneExpiredInstanceIndex(ctx, client); err != nil {
		return nil, err
	}

	instanceIDs, err := client.ZRange(ctx, instanceid.InstanceHeartbeatIndexKey, 0, -1).Result()
	if err != nil {
		return nil, err
	}
	if len(instanceIDs) == 0 {
		if _, rebuildErr := instanceid.RebuildInstanceIndexFromScan(ctx, client); rebuildErr != nil {
			return nil, rebuildErr
		}
		instanceIDs, err = client.ZRange(ctx, instanceid.InstanceHeartbeatIndexKey, 0, -1).Result()
		if err != nil {
			return nil, err
		}
	}
	if len(instanceIDs) == 0 {
		return []ActiveInstance{}, nil
	}

	keys := make([]string, 0, len(instanceIDs))
	for _, id := range instanceIDs {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		keys = append(keys, heartbeatKeyForInstance(id, instanceid.InstanceHeartbeatKeyPrefix))
	}
	if len(keys) == 0 {
		return []ActiveInstance{}, nil
	}

	values, err := client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}

	result := make([]ActiveInstance, 0, len(keys))
	staleIDs := make([]string, 0)
	for idx, key := range keys {
		instanceID := strings.TrimPrefix(key, instanceid.InstanceHeartbeatKeyPrefix)
		instance := ActiveInstance{ID: instanceID}
		if instance.ID == "" {
			continue
		}

		if idx < len(values) {
			if raw, ok := values[idx].(string); ok && strings.TrimSpace(raw) != "" {
				var payload ActiveInstance
				if err := json.Unmarshal([]byte(raw), &payload); err == nil {
					if strings.TrimSpace(payload.ID) != "" {
						instance.ID = strings.TrimSpace(payload.ID)
					}
					instance.Name = strings.TrimSpace(payload.Name)
					instance.Region = strings.TrimSpace(payload.Region)
					instance.PortStart = payload.PortStart
					instance.PortEnd = payload.PortEnd
				}
			} else {
				staleIDs = append(staleIDs, instance.ID)
			}
		}

		if instance.Name == "" {
			instance.Name = instance.ID
		}
		if instance.Region == "" {
			instance.Region = "Unknown"
		}
		if instance.PortStart <= 0 || instance.PortEnd <= 0 || instance.PortEnd < instance.PortStart {
			start, end := support.GetRotatingProxyPortRange()
			instance.PortStart = start
			instance.PortEnd = end
		}

		result = append(result, instance)
	}
	if len(staleIDs) > 0 {
		staleMembers := make([]interface{}, 0, len(staleIDs))
		for _, id := range staleIDs {
			staleMembers = append(staleMembers, id)
		}
		_ = client.ZRem(ctx, instanceid.InstanceHeartbeatIndexKey, staleMembers...).Err()
	}

	return result, nil
}

func heartbeatKeyForInstance(id string, keyPrefix string) string {
	return keyPrefix + id
}
