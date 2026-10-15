package bootstrap

import (
	"context"
	"fmt"
	"sync"
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

// Runtime keeps result consumers alive until every managed producer has stopped.
// The API is drained before Shutdown; shared clients are closed after it returns.
type Runtime struct {
	producers *routineGroup
	consumers *routineGroup
}

func newRuntime(parent context.Context) *Runtime {
	return &Runtime{producers: newRoutineGroup(parent), consumers: newRoutineGroup(parent)}
}

func (r *Runtime) Shutdown(ctx context.Context) error {
	producerErr := r.producers.stop(ctx)
	consumerErr := r.consumers.stop(ctx)
	if producerErr != nil {
		return fmt.Errorf("stop background producers: %w", producerErr)
	}
	if consumerErr != nil {
		return fmt.Errorf("flush background consumers: %w", consumerErr)
	}
	return nil
}
