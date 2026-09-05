package scraper

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBrowserManagerSharesStartupAndRejectsStaleInvalidation(t *testing.T) {
	var launches, closes atomic.Int32
	releaseLaunch := make(chan struct{})
	manager := newBrowserManager(4, func(context.Context) (*browserInstance, error) {
		launches.Add(1)
		<-releaseLaunch
		return &browserInstance{stop: func() { closes.Add(1) }}, nil
	})
	var wg sync.WaitGroup
	results := make(chan *browserInstance, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b, err := manager.get(context.Background())
			if err != nil {
				t.Error(err)
				return
			}
			results <- b
		}()
	}
	close(releaseLaunch)
	wg.Wait()
	close(results)
	var first *browserInstance
	for b := range results {
		if first == nil {
			first = b
		}
		if b != first {
			t.Fatal("callers received different browser generations")
		}
	}
	if launches.Load() != 1 {
		t.Fatalf("launched %d browsers", launches.Load())
	}
	manager.invalidate(first)
	replacement, err := manager.get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	manager.invalidate(first)
	if manager.current != replacement || closes.Load() != 1 {
		t.Fatal("stale failure invalidated recovered browser")
	}
	manager.invalidate(replacement)
}

func TestBrowserManagerBusyAndLaunchBackoff(t *testing.T) {
	manager := newBrowserManager(1, func(context.Context) (*browserInstance, error) { return &browserInstance{stop: func() {}}, nil })
	_, release, err := manager.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := manager.acquire(context.Background()); !errors.Is(err, errBrowserBusy) {
		t.Fatalf("expected busy, got %v", err)
	}
	release()
	manager.invalidate(manager.current)
	var launches atomic.Int32
	manager = newBrowserManager(1, func(context.Context) (*browserInstance, error) {
		launches.Add(1)
		return nil, errors.New("missing Chromium")
	})
	for i := 0; i < 3; i++ {
		if _, err := manager.get(context.Background()); !errors.Is(err, errBrowserUnavailable) {
			t.Fatal(err)
		}
	}
	if launches.Load() != 1 {
		t.Fatalf("launch failures retried without backoff: %d", launches.Load())
	}
}

func TestBrowserManagerWaitHonorsCallerCancellation(t *testing.T) {
	finish := make(chan struct{})
	manager := newBrowserManager(1, func(context.Context) (*browserInstance, error) { <-finish; return nil, errors.New("stopped") })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, _, err := manager.acquire(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline, got %v", err)
	}
	close(finish)
	manager.mu.Lock()
	ready := manager.starting
	manager.mu.Unlock()
	if ready != nil {
		<-ready
	}
	if len(manager.slots) != 0 {
		t.Fatal("canceled request leaked browser capacity")
	}
}
