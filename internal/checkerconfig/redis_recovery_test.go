package checkerconfig

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"magpie/internal/domain"
)

func TestCheckerSynchronizationCanAttachAfterDegradedStartup(t *testing.T) {
	state.Lock()
	oldWorkspaces, oldLoader, oldClient, oldReady := state.workspaces, state.loader, state.client, state.ready
	state.workspaces = make(map[uint]*Workspace)
	state.client = nil
	state.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		deadline := time.Now().Add(time.Second)
		for {
			state.RLock()
			stopped := state.client == nil
			state.RUnlock()
			if stopped || time.Now().After(deadline) {
				break
			}
			time.Sleep(time.Millisecond)
		}
		state.Lock()
		state.workspaces, state.loader, state.client, state.ready = oldWorkspaces, oldLoader, oldClient, oldReady
		state.Unlock()
	})
	var loads atomic.Uint64
	loader := func(context.Context, uint) (*Workspace, error) {
		generation := loads.Add(1)
		return &Workspace{Value: domain.Workspace{ID: 1}, Generation: generation}, nil
	}
	if err := Initialize(ctx, nil, loader, func(context.Context) ([]uint, error) { return []uint{1}, nil }); err != nil {
		t.Fatal(err)
	}
	if _, ready := Lookup(1); !ready {
		t.Fatal("degraded snapshot unavailable")
	}
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	if err := EnableRedisSynchronization(ctx, client); err != nil {
		t.Fatal(err)
	}
	if err := EnableRedisSynchronization(ctx, client); err != nil {
		t.Fatal(err)
	}
	if err := client.Publish(ctx, syncChannel, "another-instance 1").Err(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		snapshot, _ := Lookup(1)
		if snapshot.Generation == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("late Redis notification not applied")
		}
		time.Sleep(time.Millisecond)
	}
	if loads.Load() != 2 {
		t.Fatalf("duplicate subscription: loads=%d", loads.Load())
	}
}
