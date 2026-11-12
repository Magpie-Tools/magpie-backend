package checkpace

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/charmbracelet/log"
)

// Pacer owns one process's pacing state: the published snapshot, the refresh
// requests, the store, the check usage meter and the health of its writes.
// app constructs one with NewPacer and passes it to everything that paces
// work; app/bootstrap configures its store and runs it among the consumers.
//
// Constructing a Pacer starts nothing. Run refreshes and flushes until its
// context ends, then flushes once more; stop it after the checker workers that
// charge requests to it.
//
// A nil *Pacer has no snapshot, never pauses and meters nothing. Only tools
// and tests that pace nothing use one; the checker requires a Pacer.
type Pacer struct {
	current   atomic.Pointer[Snapshot]
	refreshCh chan struct{}
	store     atomic.Pointer[Store]

	// refreshMu serializes flushes with snapshot loads, which keeps flushed
	// marks consistent with the usage a snapshot read.
	refreshMu sync.Mutex
	// reloadPending records that a load was skipped because a flush failed.
	// Guarded by refreshMu.
	reloadPending bool

	meters sync.Map // uint -> *workspaceMeter, for the life of the process

	// instanceID names this process's batches. It never repeats, so batch
	// numbers can restart at one after a restart.
	instanceID string

	// Guarded by refreshMu.
	batchSequence uint64
	pending       *pendingBatch
	lastWritten   time.Time // last successful write

	latestMonth atomic.Pointer[monthWindow]

	// failingSince is when the current run of failed writes began, in Unix
	// nanoseconds, or 0 while writes succeed.
	failingSince atomic.Int64
	pauseAfter   atomic.Int64 // nanoseconds; 0 never pauses

	healthMu  sync.Mutex
	lastError string // guarded by healthMu
	pausedLog bool   // guarded by healthMu; the pause was logged
}

// NewPacer returns a Pacer without a snapshot or a store. It starts nothing.
func NewPacer() *Pacer {
	p := &Pacer{refreshCh: make(chan struct{}, 1), instanceID: newInstanceID()}
	p.pauseAfter.Store(int64(DefaultPauseAfter))
	return p
}

// Current returns the latest published snapshot, or nil before the first one.
func (p *Pacer) Current() *Snapshot {
	if p == nil {
		return nil
	}
	return p.current.Load()
}

// Publish replaces the current snapshot. Tests may publish nil to restore the
// startup state.
func (p *Pacer) Publish(snapshot *Snapshot) { p.current.Store(snapshot) }

// RequestRefresh asks the refresh routine to recompute soon. It never blocks,
// so the checker may call it when it meets a workspace the snapshot lacks.
func (p *Pacer) RequestRefresh() {
	if p == nil {
		return
	}
	select {
	case p.refreshCh <- struct{}{}:
	default:
	}
}

// Configure sets the store used by Refresh, Flush and Run.
func (p *Pacer) Configure(store Store) { p.store.Store(&store) }

// Refresh flushes metered check usage, loads allowances and publishes a new
// snapshot. A failed load keeps the previous snapshot. API handlers call it
// after changing an allowance or interval, so the response already reflects
// the change on this instance.
//
// A failed flush also keeps the previous snapshot. The batch may have
// committed without an acknowledgement, and a load would then count it both
// in the database usage and in this instance's unflushed counters. The kept
// snapshot still counts every local request through its counters, and the
// next flush that commits the batch reloads.
func (p *Pacer) Refresh(ctx context.Context) error {
	store := p.store.Load()
	if store == nil || store.Load == nil {
		return nil
	}
	p.refreshMu.Lock()
	defer p.refreshMu.Unlock()
	if err := p.flushLocked(ctx, store.FlushUsage); err != nil {
		p.reloadPending = true
		return fmt.Errorf("flush workspace check usage; keeping the current snapshot: %w", err)
	}
	return p.loadLocked(ctx, store)
}

// loadLocked loads allowances and publishes a snapshot. The caller holds
// refreshMu and has just flushed successfully.
func (p *Pacer) loadLocked(ctx context.Context, store *Store) error {
	now := time.Now()
	allowances, err := store.Load(ctx, now)
	if err != nil {
		return err
	}
	snapshot := Compute(now, allowances)
	p.attachMeter(snapshot)
	p.Publish(snapshot)
	p.reloadPending = false
	return nil
}

// Flush writes metered check usage without reloading allowances, unless an
// earlier load was skipped because a flush failed.
func (p *Pacer) Flush(ctx context.Context) error {
	store := p.store.Load()
	if store == nil {
		return nil
	}
	p.refreshMu.Lock()
	defer p.refreshMu.Unlock()
	if err := p.flushLocked(ctx, store.FlushUsage); err != nil {
		return err
	}
	if p.reloadPending && store.Load != nil {
		return p.loadLocked(ctx, store)
	}
	return nil
}

// Run refreshes immediately, then on RefreshInterval and on RequestRefresh,
// and flushes metered usage every FlushInterval. Explicit requests are
// coalesced to at most one load per five seconds. Usage is flushed once more
// when ctx ends, so ctx must end only after the checker has stopped charging.
func (p *Pacer) Run(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	refresh := func() {
		loadCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		if err := p.Refresh(loadCtx); err != nil && ctx.Err() == nil {
			log.Warn("refresh workspace check allowances", "error", err)
		}
	}
	refresh()
	ticker := time.NewTicker(RefreshInterval)
	defer ticker.Stop()
	flushTicker := time.NewTicker(FlushInterval)
	defer flushTicker.Stop()
	var last time.Time
	for {
		select {
		case <-ctx.Done():
			flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if err := p.Flush(flushCtx); err != nil {
				log.Warn("final flush of workspace check usage", "error", err)
			}
			cancel()
			return
		case <-ticker.C:
			refresh()
			last = time.Now()
		case <-flushTicker.C:
			flushCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			if err := p.Flush(flushCtx); err != nil && ctx.Err() == nil {
				log.Warn("flush workspace check usage", "error", err)
			}
			cancel()
			p.logPauseOnce()
		case <-p.refreshCh:
			if time.Since(last) < 5*time.Second {
				continue
			}
			refresh()
			last = time.Now()
		}
	}
}
