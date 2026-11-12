package instanceid

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	InstanceHeartbeatKeyPrefix = "magpie:instance:"
	InstanceHeartbeatIndexKey  = "magpie:instances:active"
	heartbeatScanBatchSize     = 200
)

// CountActiveInstances counts the backend instances whose heartbeat has not
// expired. It prunes expired index entries first and rebuilds the index from
// the heartbeat keys when it is empty. The checker and the scraper call it
// when they size their workers, not per check.
func CountActiveInstances(ctx context.Context, client *redis.Client) (int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := PruneExpiredInstanceIndex(ctx, client); err != nil {
		return 0, err
	}

	count, err := client.ZCard(ctx, InstanceHeartbeatIndexKey).Result()
	if err != nil {
		return 0, err
	}
	if count == 0 {
		count, err = RebuildInstanceIndexFromScan(ctx, client)
		if err != nil {
			return 0, err
		}
	}

	return int(count), nil
}

func PruneExpiredInstanceIndex(ctx context.Context, client *redis.Client) error {
	now := time.Now().UnixMilli()
	return client.ZRemRangeByScore(ctx, InstanceHeartbeatIndexKey, "-inf", strconv.FormatInt(now, 10)).Err()
}

func RebuildInstanceIndexFromScan(ctx context.Context, client *redis.Client) (int64, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	var (
		cursor uint64
		total  int64
	)

	for {
		keys, nextCursor, err := client.Scan(ctx, cursor, InstanceHeartbeatKeyPrefix+"*", heartbeatScanBatchSize).Result()
		if err != nil {
			return 0, err
		}

		if len(keys) > 0 {
			ttlCmds := make([]*redis.DurationCmd, len(keys))
			pipe := client.Pipeline()
			for i, key := range keys {
				ttlCmds[i] = pipe.TTL(ctx, key)
			}
			if _, err := pipe.Exec(ctx); err != nil {
				return 0, err
			}

			addPipe := client.Pipeline()
			batchAdds := int64(0)
			for i, key := range keys {
				id := strings.TrimPrefix(key, InstanceHeartbeatKeyPrefix)
				if id == "" {
					continue
				}
				ttl := ttlCmds[i].Val()
				if ttl <= 0 {
					continue
				}
				expiresAt := time.Now().Add(ttl).UnixMilli()
				addPipe.ZAdd(ctx, InstanceHeartbeatIndexKey, redis.Z{
					Score:  float64(expiresAt),
					Member: id,
				})
				batchAdds++
			}
			if batchAdds > 0 {
				if _, err := addPipe.Exec(ctx); err != nil {
					return 0, err
				}
				total += batchAdds
			}
		}

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	if err := PruneExpiredInstanceIndex(ctx, client); err != nil {
		return 0, err
	}
	return client.ZCard(ctx, InstanceHeartbeatIndexKey).Result()
}
