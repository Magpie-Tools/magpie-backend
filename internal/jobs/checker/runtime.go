package checker

import (
	"context"
	"sync"
)

// Runtime owns the checker's shared batchers: the failure tracker and the
// ownership verifier. app/bootstrap constructs one per process. Workers
// started by its ThreadDispatcher submit to its batchers, so concurrent
// cycles share one batch of statements.
//
// Constructing a Runtime starts nothing. RunBatchers runs both batch loops
// until its context ends; stop it after the dispatcher has joined its workers
// and before the check usage meter's final flush.
type Runtime struct {
	failures  *failureTracker
	ownership *ownershipVerifier
}

// NewRuntime constructs the batchers without starting them.
func NewRuntime() *Runtime {
	return &Runtime{failures: newFailureTracker(), ownership: newOwnershipVerifier()}
}

// RunBatchers runs one failure-tracking loop and one ownership loop until ctx
// ends. Each loop processes the requests it has already received, then
// RunBatchers returns.
func (r *Runtime) RunBatchers(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	var loops sync.WaitGroup
	loops.Go(func() { r.failures.run(ctx) })
	loops.Go(func() { r.ownership.run(ctx) })
	loops.Wait()
}
