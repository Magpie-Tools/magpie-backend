package checkpace

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Check usage is metered here, in memory, by the checker that made the
// requests. It does not depend on the statistics pipeline, which may drop or
// delay results under load, so every outbound attempt is charged. Workers add
// to atomic counters; the refresh routine flushes the deltas to the usage
// meter every FlushInterval and before each snapshot load.

// FlushInterval is how often metered check usage is written to the database.
// A crash loses at most this much usage on the crashed instance.
const FlushInterval = 10 * time.Second

type checkCounter struct {
	attempts       atomic.Uint64 // outbound requests, retries included
	protocolChecks atomic.Uint64 // protocol checks; estimates attempts per check
}

// CheckUsage is metered usage that has not been written yet.
type CheckUsage struct {
	WorkspaceID    uint
	Attempts       uint64
	ProtocolChecks uint64
}

// Flusher adds metered usage to the period that contains at. It must add
// either all rows or none.
type Flusher func(ctx context.Context, at time.Time, usage []CheckUsage) error

var (
	counters sync.Map // uint -> *checkCounter, for the life of the process
	// flushed is how much of each counter is in the database. It is guarded by
	// refreshMu, which also serializes snapshot loads with flushes.
	flushed = map[uint]CheckUsage{}
)

// ChargeChecks charges a workspace for one protocol check that made attempts
// outbound requests, retries included. It is two atomic additions and, after
// a workspace's first check on this instance, a lock-free map read: no Redis,
// database, JSON or cryptographic work.
func ChargeChecks(workspaceID uint, attempts uint64) {
	if workspaceID == 0 || attempts == 0 {
		return
	}
	counter := counterFor(workspaceID)
	counter.attempts.Add(attempts)
	counter.protocolChecks.Add(1)
}

func counterFor(workspaceID uint) *checkCounter {
	if value, ok := counters.Load(workspaceID); ok {
		return value.(*checkCounter)
	}
	value, _ := counters.LoadOrStore(workspaceID, &checkCounter{})
	return value.(*checkCounter)
}

// flushLocked writes every counter's unflushed delta. On failure nothing is
// marked flushed, so the next flush retries the same usage. The caller holds
// refreshMu.
func flushLocked(ctx context.Context, flusher Flusher) error {
	if flusher == nil {
		return nil
	}
	pending := make([]CheckUsage, 0)
	counters.Range(func(key, value any) bool {
		workspaceID := key.(uint)
		counter := value.(*checkCounter)
		done := flushed[workspaceID]
		delta := CheckUsage{
			WorkspaceID:    workspaceID,
			Attempts:       counter.attempts.Load() - done.Attempts,
			ProtocolChecks: counter.protocolChecks.Load() - done.ProtocolChecks,
		}
		if delta.Attempts > 0 || delta.ProtocolChecks > 0 {
			pending = append(pending, delta)
		}
		return true
	})
	if len(pending) == 0 {
		return nil
	}
	if err := flusher(ctx, time.Now(), pending); err != nil {
		return err
	}
	for _, delta := range pending {
		done := flushed[delta.WorkspaceID]
		done.Attempts += delta.Attempts
		done.ProtocolChecks += delta.ProtocolChecks
		flushed[delta.WorkspaceID] = done
	}
	return nil
}

// unflushedSinceLoad is what this instance charged a workspace after the
// snapshot's usage was read. The snapshot read happened right after a flush,
// so the database then held exactly the flushed mark.
func (p *Pace) unflushedSinceLoad() uint64 {
	if p.counter == nil {
		return 0
	}
	return p.counter.attempts.Load() - p.flushedMark
}

// checksExhausted reports whether the allowance is used up, counting checks
// this instance made since the snapshot was loaded. Other instances' checks
// are seen at the next refresh, so a workspace checked by several instances
// can overshoot by at most about one RefreshInterval of their usage.
func (p *Pace) checksExhausted() bool {
	if p.ChecksExhausted {
		return true
	}
	allowance := p.Allowance
	if allowance.UnlimitedChecks || p.counter == nil {
		return false
	}
	return allowance.UsedChecks+p.unflushedSinceLoad() >= allowance.MonthlyChecks
}

// resetMeterForTest clears every counter and flushed mark.
func resetMeterForTest() {
	refreshMu.Lock()
	defer refreshMu.Unlock()
	counters.Range(func(key, _ any) bool {
		counters.Delete(key)
		return true
	})
	flushed = map[uint]CheckUsage{}
}

// CurrentUsedChecks is the snapshot's used checks plus this instance's checks
// since the snapshot was loaded.
func (p Pace) CurrentUsedChecks() uint64 {
	return p.Allowance.UsedChecks + p.unflushedSinceLoad()
}

// CurrentChecksExhausted is ChecksExhausted including this instance's checks
// since the snapshot was loaded.
func (p Pace) CurrentChecksExhausted() bool {
	return p.checksExhausted()
}
