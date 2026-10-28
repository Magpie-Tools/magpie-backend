package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestDashboardPrewarmDeadlineAllowsStartup(t *testing.T) {
	parent := context.Background()
	var refreshContext context.Context
	const budget = 20 * time.Millisecond
	started := time.Now()
	err := prewarmDashboardCachesWithin(parent, budget, func(ctx context.Context) error {
		refreshContext = ctx
		deadline, ok := ctx.Deadline()
		if !ok || deadline.After(started.Add(budget+time.Second)) {
			t.Fatal("refresh did not inherit the total prewarm budget")
		}
		<-ctx.Done()
		return fmt.Errorf("database refresh: %w", ctx.Err())
	})
	if err != nil || parent.Err() != nil || !errors.Is(refreshContext.Err(), context.DeadlineExceeded) {
		t.Fatalf("optional deadline err=%v parent=%v refresh=%v", err, parent.Err(), refreshContext.Err())
	}
}

func TestDashboardPrewarmParentCancellationAbortsStartup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- prewarmDashboardCachesWithin(ctx, time.Hour, func(refreshCtx context.Context) error {
			close(started)
			<-refreshCtx.Done()
			return fmt.Errorf("database refresh: %w", refreshCtx.Err())
		})
	}()
	select {
	case <-started:
	case err := <-done:
		t.Fatalf("prewarm returned before starting the refresh: %v", err)
	case <-time.After(time.Second):
		cancel()
		<-done
		t.Fatal("prewarm did not start the refresh")
	}
	cancel()
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("parent cancellation was treated as an optional failure: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("prewarm did not stop after parent cancellation")
	}
}

func TestDashboardPrewarmJoinsCancellationCleanup(t *testing.T) {
	interrupted := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- prewarmDashboardCachesWithin(context.Background(), time.Millisecond, func(ctx context.Context) error {
			<-ctx.Done()
			close(interrupted)
			<-release
			close(finished)
			return ctx.Err()
		})
	}()
	select {
	case <-interrupted:
	case <-time.After(time.Second):
		close(release)
		<-done
		t.Fatal("refresh did not receive its deadline")
	}
	returnedEarly := false
	select {
	case <-done:
		returnedEarly = true
	default:
	}
	close(release)
	if !returnedEarly {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	<-finished
	if returnedEarly {
		t.Fatal("startup continued while the canceled refresh still had work running")
	}
}

func TestDashboardPrewarmPreservesCancellationEvenWhenRefreshReturnsNil(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := prewarmDashboardCachesWithin(ctx, time.Hour, func(context.Context) error {
		cancel()
		return nil
	})
	if err != context.Canceled {
		t.Fatalf("refresh success hid parent cancellation: %v", err)
	}
}

func TestDashboardPrewarmStrictRefreshReportsItsDeadline(t *testing.T) {
	err := refreshDashboardCachesWithin(context.Background(), time.Millisecond, func(ctx context.Context) error {
		<-ctx.Done()
		return nil
	})
	if err != context.DeadlineExceeded {
		t.Fatalf("expired fixture refresh was reported as successful: %v", err)
	}
}

func TestDashboardPrewarmRefreshFailureIsOptionalOnlyForNormalStartup(t *testing.T) {
	failure := errors.New("database unavailable")
	refresh := func(context.Context) error { return failure }
	if err := prewarmDashboardCachesWithin(context.Background(), time.Hour, refresh); err != nil {
		t.Fatalf("normal optional prewarm aborted startup: %v", err)
	}
	if err := refreshDashboardCachesWithin(context.Background(), time.Hour, refresh); err != failure {
		t.Fatalf("strict fixture prewarm hid database failure: %v", err)
	}
}

func TestDashboardPrewarmSuccessCancelsItsChildContext(t *testing.T) {
	var refreshContext context.Context
	err := prewarmDashboardCachesWithin(context.Background(), time.Hour, func(ctx context.Context) error {
		refreshContext = ctx
		return nil
	})
	if err != nil || refreshContext.Err() != context.Canceled {
		t.Fatalf("successful prewarm retained its child context: result=%v context=%v", err, refreshContext.Err())
	}
}

func TestSetupAlreadyCanceledDoesNotReadConfiguration(t *testing.T) {
	// A configuration error would prove setup ran before honoring cancellation.
	t.Setenv("DEVELOPMENT_FIXTURES", "invalid")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	running, err := Setup(ctx)
	if err != context.Canceled {
		t.Fatalf("canceled setup performed initialization: %v", err)
	}
	shutdownCtx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err := running.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("canceled setup left a background routine running: %v", err)
	}
}

func TestDashboardPrewarmAlreadyCanceledDoesNotRefresh(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	err := prewarmDashboardCachesWithin(ctx, time.Hour, func(context.Context) error {
		called = true
		return nil
	})
	if called || err != context.Canceled {
		t.Fatalf("canceled startup refreshed dashboard: called=%v err=%v", called, err)
	}
}
