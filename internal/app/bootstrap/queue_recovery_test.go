package bootstrap

import (
	"context"
	"errors"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"magpie/internal/support"
)

func TestQueueStateLossRebuildsWithoutRestart(t *testing.T) {
	old := startupQueueBootstrapCompleted.Load()
	t.Cleanup(func() { startupQueueBootstrapCompleted.Store(old) })
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	ctx := context.Background()
	if err := client.Set(ctx, runtimeQueueBootstrapMarker, "ready", 0).Err(); err != nil {
		t.Fatal(err)
	}
	restored := 0
	restore := func(ctx context.Context) error {
		if StartupQueueBootstrapCompleted() {
			t.Fatal("readiness concealed missing queues")
		}
		restored++
		return client.Set(ctx, runtimeQueueBootstrapMarker, "ready", 0).Err()
	}
	if err := recoverQueueState(ctx, client, restore); err != nil || restored != 0 || !StartupQueueBootstrapCompleted() {
		t.Fatalf("existing queue state: restored=%d err=%v", restored, err)
	}
	if err := client.Del(ctx, runtimeQueueBootstrapMarker).Err(); err != nil {
		t.Fatal(err)
	}
	if err := recoverQueueState(ctx, client, restore); err != nil || restored != 1 || !StartupQueueBootstrapCompleted() {
		t.Fatalf("missing queue state: restored=%d err=%v", restored, err)
	}
	if err := recoverQueueState(ctx, client, restore); err != nil || !StartupQueueBootstrapCompleted() || restored != 1 {
		t.Fatal("queue recovery did not become ready")
	}
	client.Del(ctx, runtimeQueueBootstrapMarker)
	if err := recoverQueueState(ctx, client, func(context.Context) error { return errors.New("database unavailable") }); err == nil || StartupQueueBootstrapCompleted() {
		t.Fatal("failed rebuild marked queues ready")
	}
	if err := recoverQueueState(ctx, client, func(context.Context) error { return nil }); err == nil || StartupQueueBootstrapCompleted() {
		t.Fatal("missing completion marker marked queues ready")
	}
	if err := recoverQueueState(ctx, client, func(context.Context) error { return support.ErrLeaderLockNotAcquired }); !errors.Is(err, support.ErrLeaderLockNotAcquired) || StartupQueueBootstrapCompleted() {
		t.Fatal("follower marked queues ready before leader finished")
	}
	if err := recoverQueueState(ctx, client, func(ctx context.Context) error {
		if err := client.Set(ctx, runtimeQueueBootstrapMarker, "ready", 0).Err(); err != nil {
			return err
		}
		return support.ErrLeaderLockNotAcquired
	}); err != nil || !StartupQueueBootstrapCompleted() {
		t.Fatal("follower did not observe leader completion")
	}
	client.Close()
	if err := recoverQueueState(ctx, client, restore); err == nil || StartupQueueBootstrapCompleted() {
		t.Fatal("failed Redis observation left stale readiness")
	}
}
