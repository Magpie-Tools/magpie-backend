package queue

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestHeadRepairForProxyAndScrapeQueues(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	for _, prefix := range []string{"proxy_queue", "scrapesite_queue"} {
		t.Run(prefix, func(t *testing.T) {
			head, filled, empty, other := prefix+"_heads", prefix+":0", prefix+":1", prefix+":2"
			if err := client.ZAdd(ctx, head, redis.Z{Score: 1, Member: empty}, redis.Z{Score: 20, Member: other}).Err(); err != nil {
				t.Fatal(err)
			}
			if err := client.ZAdd(ctx, filled, redis.Z{Score: 10, Member: "ready"}).Err(); err != nil {
				t.Fatal(err)
			}
			if err := RefreshHeads(ctx, client, head, []string{filled, empty}); err != nil {
				t.Fatal(err)
			}
			if score, err := client.ZScore(ctx, head, filled).Result(); err != nil || score != 10 {
				t.Fatalf("filled shard head missing: %v %v", score, err)
			}
			if err := client.ZScore(ctx, head, empty).Err(); err != redis.Nil {
				t.Fatalf("empty shard retained: %v", err)
			}
			if err := client.ZScore(ctx, head, other).Err(); err != nil {
				t.Fatalf("another instance's shard head erased: %v", err)
			}
		})
	}
}
