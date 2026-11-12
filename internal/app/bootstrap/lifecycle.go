package bootstrap

import (
	"context"
	"fmt"
	"sync"

	"magpie/internal/checkpace"
)

type routineGroup struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newRoutineGroup(parent context.Context) *routineGroup {
	ctx, cancel := context.WithCancel(context.WithoutCancel(parent))
	return &routineGroup{ctx: ctx, cancel: cancel}
}

func (g *routineGroup) start(run func(context.Context)) {
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		run(g.ctx)
	}()
}

func (g *routineGroup) stop(ctx context.Context) error {
	g.cancel()
	done := make(chan struct{})
	go func() { g.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Runtime stops producers, then the checker batchers, then result consumers,
// then their read-model refreshes. Checker workers are producers, so the
// batchers they submit to stop after the last worker has returned and before
// the check usage meter, a consumer, makes its final flush.
// The API is drained before Shutdown; shared clients are closed after it returns.
type Runtime struct {
	producers  *routineGroup
	batchers   *routineGroup
	consumers  *routineGroup
	readModels *routineGroup
	// pace is the process's Pacer, constructed by app. Setup configures it,
	// runs it among the consumers and hands it to the checker and scraper.
	pace *checkpace.Pacer
}

func newRuntime(parent context.Context) *Runtime {
	return &Runtime{
		producers:  newRoutineGroup(parent),
		batchers:   newRoutineGroup(parent),
		consumers:  newRoutineGroup(parent),
		readModels: newRoutineGroup(parent),
	}
}

func (r *Runtime) Shutdown(ctx context.Context) error {
	producerErr := r.producers.stop(ctx)
	batcherErr := r.batchers.stop(ctx)
	consumerErr := r.consumers.stop(ctx)
	readModelErr := r.readModels.stop(ctx)
	if producerErr != nil {
		return fmt.Errorf("stop background producers: %w", producerErr)
	}
	if batcherErr != nil {
		return fmt.Errorf("stop checker batchers: %w", batcherErr)
	}
	if consumerErr != nil {
		return fmt.Errorf("flush background consumers: %w", consumerErr)
	}
	if readModelErr != nil {
		return fmt.Errorf("drain read models: %w", readModelErr)
	}
	return nil
}
