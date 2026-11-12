package checkpace

import (
	"errors"
	"time"

	"github.com/charmbracelet/log"
)

// Check usage that cannot be written stays in memory and is retried
// indefinitely. While it is unwritten, this instance enforces allowances from
// a snapshot that no longer changes, and a crash would lose the usage. After
// PauseAfter without a successful write, the instance stops starting checks
// until a write succeeds. Counting continues, nothing is discarded, and
// checking resumes by itself.

// DefaultPauseAfter is how long check usage may stay unwritten before this
// instance pauses checking.
const DefaultPauseAfter = 15 * time.Minute

// unhealthyAfter is how long writes must fail before the instance reports
// itself degraded, which keeps a single slow write from alerting.
const unhealthyAfter = time.Minute

// ErrBatchOutcomeUnknown is returned by a Flusher for a resent batch whose
// replay mark no longer exists, so the database cannot tell whether the batch
// was applied. The batch is dropped rather than risk charging it twice.
var ErrBatchOutcomeUnknown = errors.New("check usage batch outcome unknown: replay mark missing")

// SetPauseAfter sets how long check usage may stay unwritten before checking
// pauses. Zero or less never pauses.
func (p *Pacer) SetPauseAfter(limit time.Duration) {
	p.pauseAfter.Store(int64(max(limit, 0)))
}

// checksPaused is one atomic load while writes succeed. A nil Pacer never
// pauses.
func (p *Pacer) checksPaused() bool {
	if p == nil {
		return false
	}
	since := p.failingSince.Load()
	if since == 0 {
		return false
	}
	limit := p.pauseAfter.Load()
	return limit > 0 && time.Now().UnixNano()-since >= limit
}

func (p *Pacer) recordFlushFailure(err error, now time.Time) {
	p.failingSince.CompareAndSwap(0, now.UnixNano())
	p.healthMu.Lock()
	p.lastError = err.Error()
	p.healthMu.Unlock()
}

func (p *Pacer) recordFlushSuccess() {
	since := p.failingSince.Swap(0)
	p.healthMu.Lock()
	p.lastError, p.pausedLog = "", false
	p.healthMu.Unlock()
	if since != 0 {
		log.Info("workspace check usage writes recovered; checking resumes", "failing_for", time.Since(time.Unix(0, since)).Round(time.Second))
	}
}

// logPauseOnce reports the moment checking pauses. Run calls it on every
// flush tick.
func (p *Pacer) logPauseOnce() {
	if !p.checksPaused() {
		return
	}
	p.healthMu.Lock()
	defer p.healthMu.Unlock()
	if p.pausedLog {
		return
	}
	p.pausedLog = true
	log.Error("workspace check usage has not been written; checking is paused until a write succeeds",
		"failing_since", time.Unix(0, p.failingSince.Load()).UTC(), "pause_after", time.Duration(p.pauseAfter.Load()), "error", p.lastError)
}

// PersistenceHealth describes this instance's check usage writes.
type PersistenceHealth struct {
	// Healthy is false once writes have failed for about a minute.
	Healthy      bool
	FailingSince time.Time // zero while writes succeed
	Paused       bool      // checking is paused until a write succeeds
	LastError    string
}

// Health reports whether check usage is being written.
func (p *Pacer) Health() PersistenceHealth {
	since := p.failingSince.Load()
	if since == 0 {
		return PersistenceHealth{Healthy: true}
	}
	p.healthMu.Lock()
	defer p.healthMu.Unlock()
	failing := time.Unix(0, since)
	return PersistenceHealth{
		Healthy:      time.Since(failing) < unhealthyAfter,
		FailingSince: failing.UTC(),
		Paused:       p.checksPaused(),
		LastError:    p.lastError,
	}
}
