package bootstrap

import (
	"context"
	"errors"
	"testing"
	"time"
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
