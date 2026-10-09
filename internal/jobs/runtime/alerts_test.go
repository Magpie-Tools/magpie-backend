package runtime

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"magpie/internal/domain"
)

func waitAlertWorkerEvent[T any](t *testing.T, ctx context.Context, events <-chan T) T {
	t.Helper()
	// Bound shutdown checks too, even when the test's work context was canceled.
	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	select {
	case event := <-events:
		return event
	case <-waitCtx.Done():
		t.Fatal("alert worker stalled")
		var zero T
		return zero
	}
}

func TestAlertDeliveryDrainsBacklogWhileOneSendStalls(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	const concurrency, total = 3, 1000
	started, sent := make(chan uint64, total), make(chan uint64, total)
	fastRelease, slowRelease, stopped := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var active, peak atomic.Int32
	next := uint64(1)
	claim := func(_ context.Context, limit int, _ time.Time) ([]domain.AlertDelivery, error) {
		messages := []domain.AlertDelivery{}
		for next <= total && len(messages) < limit {
			messages = append(messages, domain.AlertDelivery{ID: next})
			next++
		}
		return messages, nil
	}
	deliver := func(ctx context.Context, message domain.AlertDelivery) {
		current := active.Add(1)
		defer active.Add(-1)
		for previous := peak.Load(); current > previous; previous = peak.Load() {
			if peak.CompareAndSwap(previous, current) {
				break
			}
		}
		started <- message.ID
		release := fastRelease
		if message.ID == 1 {
			release = slowRelease
		}
		select {
		case <-ctx.Done():
			return
		case <-release:
			sent <- message.ID
		}
	}
	go func() {
		defer close(stopped)
		runAlertDeliveryLoop(ctx, concurrency, make(chan time.Time), claim, deliver)
	}()
	defer func() { cancel(); waitAlertWorkerEvent(t, context.Background(), stopped) }()
	for i := 0; i < concurrency; i++ {
		waitAlertWorkerEvent(t, ctx, started)
	}
	close(fastRelease)
	seen := map[uint64]bool{}
	for i := 0; i < total-1; i++ {
		id := waitAlertWorkerEvent(t, ctx, sent)
		if id == 1 || seen[id] {
			t.Fatal("stalled message finished early or a message was sent twice")
		}
		seen[id] = true
	}
	if peak.Load() != concurrency {
		t.Fatalf("worker concurrency = %d, want %d", peak.Load(), concurrency)
	}
	close(slowRelease)
	if id := waitAlertWorkerEvent(t, ctx, sent); id != 1 {
		t.Fatalf("stalled message = %d", id)
	}
}

func TestAlertDeliveryWaitsForPollWhenEmptyOrClaimFails(t *testing.T) {
	for _, failed := range []bool{false, true} {
		name := "empty"
		if failed {
			name = "claim error"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			calls, poll, stopped := make(chan int, 10), make(chan time.Time, 1), make(chan struct{})
			attempt := 0
			claim := func(context.Context, int, time.Time) ([]domain.AlertDelivery, error) {
				attempt++
				calls <- attempt
				if failed {
					return nil, errors.New("test database unavailable")
				}
				return nil, nil
			}
			go func() {
				defer close(stopped)
				runAlertDeliveryLoop(ctx, 2, poll, claim, func(context.Context, domain.AlertDelivery) { t.Error("nothing was claimed") })
			}()
			waitAlertWorkerEvent(t, ctx, calls)
			select {
			case <-calls:
				t.Fatal("empty or failed claim caused a busy poll loop")
			case <-time.After(20 * time.Millisecond):
			}
			poll <- time.Now()
			if call := waitAlertWorkerEvent(t, ctx, calls); call != 2 {
				t.Fatalf("poll resumed at claim %d", call)
			}
			cancel()
			waitAlertWorkerEvent(t, context.Background(), stopped)
		})
	}
}

func TestAlertDeliveryCompletionUnblocksOrderedMessageWithoutPoll(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	release, empty, sent, stopped := make(chan struct{}), make(chan struct{}), make(chan uint64, 2), make(chan struct{})
	var firstSent atomic.Bool
	claimedFirst, claimedSecond := false, false
	claim := func(context.Context, int, time.Time) ([]domain.AlertDelivery, error) {
		if !claimedFirst {
			claimedFirst = true
			return []domain.AlertDelivery{{ID: 1}}, nil
		}
		if firstSent.Load() && !claimedSecond {
			claimedSecond = true
			return []domain.AlertDelivery{{ID: 2}}, nil
		}
		if !firstSent.Load() {
			close(empty)
		}
		return nil, nil
	}
	deliver := func(ctx context.Context, message domain.AlertDelivery) {
		if message.ID == 1 {
			select {
			case <-ctx.Done():
				return
			case <-release:
			}
			firstSent.Store(true)
		}
		sent <- message.ID
	}
	go func() { defer close(stopped); runAlertDeliveryLoop(ctx, 2, make(chan time.Time), claim, deliver) }()
	defer func() { cancel(); waitAlertWorkerEvent(t, context.Background(), stopped) }()
	waitAlertWorkerEvent(t, ctx, empty)
	close(release)
	for _, want := range []uint64{1, 2} {
		if id := waitAlertWorkerEvent(t, ctx, sent); id != want {
			t.Fatalf("delivery order %d, want %d", id, want)
		}
	}
}
