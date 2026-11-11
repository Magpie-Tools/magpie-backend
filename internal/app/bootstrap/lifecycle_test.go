package bootstrap

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"magpie/internal/checkpace"
	"magpie/internal/jobs/checker"
)

func TestShutdownWaitsForLastProducerBeforeFinalConsumerFlush(t *testing.T) {
	parent, signal := context.WithCancel(context.Background())
	running := newRuntime(parent)
	producerStopped := make(chan struct{})
	releaseProducer := make(chan struct{})
	lastSample := make(chan int, 1)
	flushed := make(chan int, 1)
	running.producers.start(func(ctx context.Context) {
		<-ctx.Done()
		close(producerStopped)
		<-releaseProducer
		lastSample <- 123
	})
	running.consumers.start(func(ctx context.Context) {
		<-ctx.Done()
		select {
		case sample := <-lastSample:
			flushed <- sample
		default:
			flushed <- 0
		}
	})
	signal()
	if running.consumers.ctx.Err() != nil {
		t.Fatal("signal canceled consumers before producers stopped")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- running.Shutdown(ctx) }()
	<-producerStopped
	if running.consumers.ctx.Err() != nil {
		t.Fatal("consumer stopped while producer still records traffic")
	}
	close(releaseProducer)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := <-flushed; got != 123 {
		t.Fatalf("final flush lost producer sample: %d", got)
	}
}

func TestShutdownDrainsReadModelsAfterLastConsumerWrite(t *testing.T) {
	parent, signal := context.WithCancel(context.Background())
	running := newRuntime(parent)
	consumerDraining := make(chan struct{})
	releaseConsumer := make(chan struct{})
	dirty := make(chan int, 1)
	refreshed := make(chan int, 1)
	running.consumers.start(func(ctx context.Context) {
		<-ctx.Done()
		close(consumerDraining)
		<-releaseConsumer
		dirty <- 123
	})
	running.readModels.start(func(ctx context.Context) {
		<-ctx.Done()
		select {
		case value := <-dirty:
			refreshed <- value
		default:
			refreshed <- 0
		}
	})
	signal()
	if running.readModels.ctx.Err() != nil {
		t.Fatal("signal stopped read models before the final consumer write")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- running.Shutdown(ctx) }()
	<-consumerDraining
	stoppedEarly := running.readModels.ctx.Err() != nil
	close(releaseConsumer)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := <-refreshed; stoppedEarly || got != 123 {
		t.Fatalf("read models stopped before consumers: stopped_early=%v final_value=%d", stoppedEarly, got)
	}
}

func TestShutdownReportsDeadlineWhenReadModelsCannotStop(t *testing.T) {
	running := newRuntime(context.Background())
	release := make(chan struct{})
	running.readModels.start(func(context.Context) { <-release })
	defer func() { close(release); running.readModels.wg.Wait() }()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := running.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown err=%v", err)
	}
}

func TestShutdownReportsDeadlineWhenProducerCannotStop(t *testing.T) {
	running := newRuntime(context.Background())
	release := make(chan struct{})
	running.producers.start(func(context.Context) { <-release })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := running.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown err=%v", err)
	}
	close(release)
	running.producers.wg.Wait()
}

// Checker workers charge requests until the producers have stopped. The usage
// meter's last flush must come after that, or those requests are never written.
func TestShutdownWritesCheckUsageChargedUntilProducersStopped(t *testing.T) {
	const workspaceID = 9_000_001 // the meter lives for the process
	var written atomic.Uint64
	checkpace.Configure(checkpace.Store{FlushUsage: func(_ context.Context, batch checkpace.UsageBatch) error {
		for _, usage := range batch.Usage {
			if usage.WorkspaceID == workspaceID {
				written.Add(usage.Attempts)
			}
		}
		return nil
	}})
	t.Cleanup(func() { checkpace.Configure(checkpace.Store{}) })

	running := newRuntime(context.Background())
	running.startCheckUsageMeter()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// Shutdown's first stage has finished: the last request has just been charged.
	if err := running.producers.stop(ctx); err != nil {
		t.Fatal(err)
	}
	if !checkpace.Current().ChargeRequest(workspaceID, true, time.Now()) {
		t.Fatal("the request was refused")
	}
	if err := running.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if got := written.Load(); got != 1 {
		t.Fatalf("shutdown wrote %d of the 1 request charged before the producers stopped", got)
	}
}

// Checker workers are producers. Shutdown stops them first, then the checker
// batchers they submit to, then the consumers, whose check usage meter makes
// the final flush, then the read models.
func TestShutdownStopsCheckerBatchersBetweenWorkersAndFinalFlush(t *testing.T) {
	running := newRuntime(context.Background())
	stopped := make(chan string, 4)
	record := func(name string) func(context.Context) {
		return func(ctx context.Context) {
			<-ctx.Done()
			stopped <- name
		}
	}
	running.producers.start(record("checker workers"))
	running.batchers.start(record("checker batchers"))
	running.consumers.start(record("usage meter final flush"))
	running.readModels.start(record("read models"))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := running.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	close(stopped)
	var order []string
	for name := range stopped {
		order = append(order, name)
	}
	want := []string{"checker workers", "checker batchers", "usage meter final flush", "read models"}
	if len(order) != len(want) {
		t.Fatalf("stop order %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("stop order %v, want %v", order, want)
		}
	}
}

// The real checker runtime: the dispatcher's workers have returned before the
// batchers stop, and the batchers process what they received before the
// consumers' final flush begins.
func TestCheckerRuntimeStopsAfterItsDispatcher(t *testing.T) {
	running := newRuntime(context.Background())
	checkers := checker.NewRuntime()
	batchersDone := make(chan struct{})
	running.batchers.start(func(ctx context.Context) {
		checkers.RunBatchers(ctx)
		close(batchersDone)
	})
	dispatcherDone := make(chan struct{})
	running.producers.start(func(ctx context.Context) {
		<-ctx.Done()
		close(dispatcherDone)
	})
	flushSawBatchersStopped := make(chan bool, 1)
	running.consumers.start(func(ctx context.Context) {
		<-ctx.Done()
		select {
		case <-batchersDone:
			flushSawBatchersStopped <- true
		default:
			flushSawBatchersStopped <- false
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := running.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-dispatcherDone:
	default:
		t.Fatal("dispatcher still running after shutdown")
	}
	if !<-flushSawBatchersStopped {
		t.Fatal("the final flush began before the checker batchers stopped")
	}
}
