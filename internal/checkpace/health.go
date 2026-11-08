package checkpace

import (
	"errors"
	"sync"
	"sync/atomic"
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

var (
	// failingSince is when the current run of failed writes began, in Unix
	// nanoseconds, or 0 while writes succeed.
	failingSince atomic.Int64
	pauseAfter   atomic.Int64 // nanoseconds; 0 never pauses

	healthMu  sync.Mutex
	lastError string // guarded by healthMu
	pausedLog bool   // guarded by healthMu; the pause was logged
)

func init() { pauseAfter.Store(int64(DefaultPauseAfter)) }

// SetPauseAfter sets how long check usage may stay unwritten before checking
// pauses. Zero or less never pauses.
func SetPauseAfter(limit time.Duration) {
	pauseAfter.Store(int64(max(limit, 0)))
}

// checksPaused is one atomic load while writes succeed.
func checksPaused() bool {
	since := failingSince.Load()
	if since == 0 {
		return false
	}
	limit := pauseAfter.Load()
	return limit > 0 && time.Now().UnixNano()-since >= limit
}

func recordFlushFailure(err error, now time.Time) {
	failingSince.CompareAndSwap(0, now.UnixNano())
	healthMu.Lock()
	lastError = err.Error()
	healthMu.Unlock()
}

func recordFlushSuccess() {
	since := failingSince.Swap(0)
	healthMu.Lock()
	lastError, pausedLog = "", false
	healthMu.Unlock()
	if since != 0 {
		log.Info("workspace check usage writes recovered; checking resumes", "failing_for", time.Since(time.Unix(0, since)).Round(time.Second))
	}
}

// logPauseOnce reports the moment checking pauses. Run calls it on every
// flush tick.
func logPauseOnce() {
	if !checksPaused() {
		return
	}
	healthMu.Lock()
	defer healthMu.Unlock()
	if pausedLog {
		return
	}
	pausedLog = true
	log.Error("workspace check usage has not been written; checking is paused until a write succeeds",
		"failing_since", time.Unix(0, failingSince.Load()).UTC(), "pause_after", time.Duration(pauseAfter.Load()), "error", lastError)
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
func Health() PersistenceHealth {
	since := failingSince.Load()
	if since == 0 {
		return PersistenceHealth{Healthy: true}
	}
	healthMu.Lock()
	defer healthMu.Unlock()
	failing := time.Unix(0, since)
	return PersistenceHealth{
		Healthy:      time.Since(failing) < unhealthyAfter,
		FailingSince: failing.UTC(),
		Paused:       checksPaused(),
		LastError:    lastError,
	}
}

func resetHealthForTest() {
	failingSince.Store(0)
	pauseAfter.Store(int64(DefaultPauseAfter))
	healthMu.Lock()
	lastError, pausedLog = "", false
	healthMu.Unlock()
}
