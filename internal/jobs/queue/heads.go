package queue

import (
	"context"

	"github.com/redis/go-redis/v9"
)

var refreshHeads = redis.NewScript(`
for i = 2, #KEYS do
  local head = redis.call('ZRANGE', KEYS[i], 0, 0, 'WITHSCORES')
  if #head == 0 then
    redis.call('ZREM', KEYS[1], KEYS[i])
  else
    redis.call('ZADD', KEYS[1], head[2], KEYS[i])
  end
end
return 1
`)

// RefreshHeads reads and repairs shard heads in one Redis operation. Other
// instances can enqueue before or after it, without having their heads erased
// by a rebuild based on an earlier snapshot.
func RefreshHeads(ctx context.Context, client *redis.Client, headKey string, shardKeys []string) error {
	keys := append([]string{headKey}, shardKeys...)
	return refreshHeads.Run(ctx, client, keys).Err()
}
