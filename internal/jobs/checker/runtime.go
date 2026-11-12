package checker

import (
	"context"
	"sync"

	"magpie/internal/checkpace"
)

// Runtime owns the checker's shared batchers, the failure tracker and the
// ownership verifier, and holds the process's Pacer. app/bootstrap constructs
// one per process. Workers started by its ThreadDispatcher submit to its
// batchers, so concurrent cycles share one batch of statements, and read the
// allowance snapshot from and charge requests to its Pacer.
//
// Constructing a Runtime starts nothing. RunBatchers runs both batch loops
// until its context ends; stop it after the dispatcher has joined its workers
// and before the Pacer's final flush.
type Runtime struct {
	failures  *failureTracker
	ownership *ownershipVerifier
	pace      *checkpace.Pacer
}

// NewRuntime constructs the batchers without starting them. pace is the
// process's Pacer, which the caller runs; it must not be nil, because a nil
// Pacer would admit every request without metering it.
func NewRuntime(pace *checkpace.Pacer) *Runtime {
	if pace == nil {
		panic("checker: NewRuntime needs the process's Pacer")
	}
	return &Runtime{failures: newFailureTracker(), ownership: newOwnershipVerifier(), pace: pace}
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
