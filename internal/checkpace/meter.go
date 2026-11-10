package checkpace

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/charmbracelet/log"
)

// Check usage is metered here, in memory, by the checker that sends the
// requests. It does not depend on the statistics pipeline, which may drop or
// delay results under load. The checker charges each request just before it is
// sent (Snapshot.ChargeRequest). The refresh routine flushes the counters to
// the usage meter every FlushInterval and before each snapshot load.
//
// The limit is soft. A charge checks the allowance first, so this instance
// stops sending a workspace's requests as soon as its own counters reach the
// allowance. Requests that were admitted concurrently may still finish, which
// bounds the local overshoot to one request per worker serving the workspace.
// Other instances' usage is seen at the next refresh.
//
// Usage is counted in the month it was made, whenever it is flushed, and every
// flush is a numbered batch. A batch whose result is unknown is retried
// unchanged, and the database applies each batch number at most once, so a
// lost acknowledgement cannot charge the same requests twice.

// FlushInterval is how often metered check usage is written to the database.
// A crash loses the usage charged since the last successful write: normally
// at most this long, longer while writes fail.
const FlushInterval = 10 * time.Second

// monthCounter is one workspace's usage in one usage period on this instance.
type monthCounter struct {
	month int32 // monthIndex of the period's start

	attempts       atomic.Uint64 // outbound requests, retries included
	protocolChecks atomic.Uint64 // protocol checks; estimates requests per check

	// committed is what the database holds, guarded by refreshMu.
	committedAttempts       uint64
	committedProtocolChecks uint64
}

func (c *monthCounter) unflushed() (uint64, uint64) {
	return c.attempts.Load() - c.committedAttempts, c.protocolChecks.Load() - c.committedProtocolChecks
}

// workspaceMeter holds a workspace's counters by month. Charges for the
// current month read one atomic pointer; another month takes the mutex.
type workspaceMeter struct {
	current atomic.Pointer[monthCounter]
	mu      sync.Mutex
	months  []*monthCounter // guarded by mu
}

func (m *workspaceMeter) counter(month int32) *monthCounter {
	if counter := m.current.Load(); counter != nil && counter.month == month {
		return counter
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var found *monthCounter
	for _, counter := range m.months {
		if counter.month == month {
			found = counter
			break
		}
	}
	if found == nil {
		found = &monthCounter{month: month}
		m.months = append(m.months, found)
	}
	if current := m.current.Load(); current == nil || month > current.month {
		m.current.Store(found)
	}
	return found
}

// attemptsIn reads a workspace's requests in month on this instance without
// creating a counter.
func attemptsIn(workspaceID uint, month int32) uint64 {
	value, ok := meters.Load(workspaceID)
	if !ok {
		return 0
	}
	meter := value.(*workspaceMeter)
	if counter := meter.current.Load(); counter != nil && counter.month == month {
		return counter.attempts.Load()
	}
	meter.mu.Lock()
	defer meter.mu.Unlock()
	for _, counter := range meter.months {
		if counter.month == month {
			return counter.attempts.Load()
		}
	}
	return 0
}

// CheckUsage is metered usage of one workspace in one usage period.
type CheckUsage struct {
	WorkspaceID    uint
	PeriodStart    time.Time
	Attempts       uint64
	ProtocolChecks uint64
}

// UsageBatch is one flush. Instance identifies this process and Sequence
// increases with each new batch; a retried batch keeps both and sets Resend.
type UsageBatch struct {
	Instance string
	Sequence uint64
	Usage    []CheckUsage
	// Resend marks a batch sent before without a confirmed outcome.
	Resend bool
}

// Flusher adds a batch to the usage meter. It must apply all rows or none, and
// must ignore a batch whose Sequence it already applied for the Instance. When
// it cannot tell for a resent batch, it returns ErrBatchOutcomeUnknown.
type Flusher func(ctx context.Context, batch UsageBatch) error

type pendingBatch struct {
	batch    UsageBatch
	counters []*monthCounter // parallel to batch.Usage
}

var (
	meters sync.Map // uint -> *workspaceMeter, for the life of the process

	// instanceID names this process's batches. It never repeats, so batch
	// numbers can restart at one after a restart.
	instanceID = newInstanceID()

	// Guarded by refreshMu, which also serializes snapshot loads with flushes.
	batchSequence uint64
	pending       *pendingBatch
	lastWritten   time.Time // last successful write
)

// markKeepAlive is how often an idle process writes an empty batch. It is far
// below the database's replay mark retention.
const markKeepAlive = 24 * time.Hour

// monthPruneDelay is how long after its period ends a fully flushed counter is
// kept, so a charge that was already under way still finds it.
const monthPruneDelay = time.Hour

func newInstanceID() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		panic("checkpace: read random instance id: " + err.Error())
	}
	return hex.EncodeToString(raw[:])
}

func meterFor(workspaceID uint) *workspaceMeter {
	if value, ok := meters.Load(workspaceID); ok {
		return value.(*workspaceMeter)
	}
	value, _ := meters.LoadOrStore(workspaceID, &workspaceMeter{})
	return value.(*workspaceMeter)
}

// monthIndex numbers usage periods: months since January of year 0, UTC.
func monthIndex(at time.Time) int32 {
	utc := at.UTC()
	return int32(utc.Year())*12 + int32(utc.Month()) - 1
}

func monthStart(month int32) time.Time {
	return time.Date(int(month/12), time.Month(month%12+1), 1, 0, 0, 0, 0, time.UTC)
}

type monthWindow struct {
	start, end int64 // Unix nanoseconds
	month      int32
}

var latestMonth atomic.Pointer[monthWindow]

// monthOf is monthIndex with the latest month cached, which keeps calendar
// arithmetic off the request path.
func monthOf(at time.Time) int32 {
	nanos := at.UnixNano()
	window := latestMonth.Load()
	if window != nil && nanos >= window.start && nanos < window.end {
		return window.month
	}
	month := monthIndex(at)
	// Only the present month is cached, so a timestamp from another
	// instance's clock or a far-off schedule cannot displace it.
	if (window == nil || month > window.month) && month <= monthIndex(time.Now()) {
		start := monthStart(month)
		latestMonth.Store(&monthWindow{start: start.UnixNano(), end: start.AddDate(0, 1, 0).UnixNano(), month: month})
	}
	return month
}

// ChargeRequest is called by the checker just before it sends one request
// that serves workspaceID. firstOfCheck marks a protocol check's first
// request. It returns false, charging nothing, when the workspace's check
// allowance is used up on this instance or checking is paused because usage
// could not be written; the request must then not be sent for the workspace.
// The cost is a map read, a few atomic loads and one or two atomic additions:
// no Redis, database, JSON or cryptographic work.
func (s *Snapshot) ChargeRequest(workspaceID uint, firstOfCheck bool, at time.Time) bool {
	if workspaceID == 0 {
		return true
	}
	if checksPaused() {
		return false
	}
	month := monthOf(at)
	var counter *monthCounter
	if pace := s.lookup(workspaceID); pace != nil && pace.counter != nil {
		if pace.exhaustedIn(month) {
			return false
		}
		if pace.counter.month == month {
			counter = pace.counter
		}
	}
	if counter == nil {
		// No snapshot yet, a workspace the snapshot lacks, or another
		// month than the snapshot's. The next refresh follows within
		// RefreshInterval unless usage cannot be written.
		counter = meterFor(workspaceID).counter(month)
	}
	counter.attempts.Add(1)
	if firstOfCheck {
		counter.protocolChecks.Add(1)
	}
	return true
}

// flushLocked writes unflushed usage as one batch. A batch that failed is
// retried unchanged before any new usage is batched, because its rows may
// already be committed. The caller holds refreshMu.
func flushLocked(ctx context.Context, flusher Flusher) error {
	if flusher == nil {
		return nil
	}
	retrying := pending != nil
	if !retrying {
		now := time.Now()
		pending = collectBatch(now)
		if pending == nil {
			if now.Sub(lastWritten) < markKeepAlive {
				return nil
			}
			// Nothing to write for a day: an empty batch refreshes this
			// process's replay mark so it is never pruned while alive.
			batchSequence++
			pending = &pendingBatch{batch: UsageBatch{Instance: instanceID, Sequence: batchSequence}}
		}
	}
	err := flusher(ctx, pending.batch)
	switch {
	case errors.Is(err, ErrBatchOutcomeUnknown):
		// The batch was resent so long after its first attempt that its
		// replay mark was pruned. It may or may not be in the database;
		// dropping it can lose usage but never charges it twice. Treating it
		// as committed keeps local accounting consistent with the next load.
		log.Error("dropping a check usage batch whose outcome is unknown", "sequence", pending.batch.Sequence,
			"workspaces", len(pending.batch.Usage), "error", err)
	case err != nil:
		pending.batch.Resend = true
		recordFlushFailure(err, time.Now())
		return err
	}
	recordFlushSuccess()
	lastWritten = time.Now()
	for index, counter := range pending.counters {
		counter.committedAttempts += pending.batch.Usage[index].Attempts
		counter.committedProtocolChecks += pending.batch.Usage[index].ProtocolChecks
	}
	pending = nil
	if retrying {
		// Usage made while the batch was failing goes out now, not a flush
		// interval later.
		return flushLocked(ctx, flusher)
	}
	return nil
}

// collectBatch numbers a batch of every counter's unflushed usage and prunes
// counters of past months that are fully flushed. It returns nil when nothing
// is unflushed. The caller holds refreshMu.
func collectBatch(now time.Time) *pendingBatch {
	var batch *pendingBatch
	meters.Range(func(key, value any) bool {
		workspaceID, meter := key.(uint), value.(*workspaceMeter)
		meter.mu.Lock()
		defer meter.mu.Unlock()
		current := meter.current.Load()
		kept := meter.months[:0]
		for _, counter := range meter.months {
			attempts, protocolChecks := counter.unflushed()
			if attempts == 0 && protocolChecks == 0 {
				if counter != current && now.After(monthStart(counter.month).AddDate(0, 1, 0).Add(monthPruneDelay)) {
					continue
				}
				kept = append(kept, counter)
				continue
			}
			kept = append(kept, counter)
			if batch == nil {
				batch = &pendingBatch{}
			}
			batch.batch.Usage = append(batch.batch.Usage, CheckUsage{
				WorkspaceID: workspaceID, PeriodStart: monthStart(counter.month),
				Attempts: attempts, ProtocolChecks: protocolChecks,
			})
			batch.counters = append(batch.counters, counter)
		}
		clear(meter.months[len(kept):])
		meter.months = kept
		return true
	})
	if batch == nil {
		return nil
	}
	batchSequence++
	batch.batch.Instance, batch.batch.Sequence = instanceID, batchSequence
	return batch
}

// attachMeter connects a computed snapshot to this instance's counters for
// the snapshot's period. The caller holds refreshMu, right after a flush, so
// the database then held exactly the committed usage.
func attachMeter(snapshot *Snapshot) {
	month := monthIndex(snapshot.PeriodStart)
	for workspaceID, pace := range snapshot.paces {
		pace.counter = meterFor(workspaceID).counter(month)
		pace.flushedMark = pace.counter.committedAttempts
	}
}

// exhaustedIn reports whether the workspace may not be charged for a request
// in month. For the snapshot's month it is checksExhausted. When the snapshot
// is from an earlier month, which happens for up to RefreshInterval after
// midnight or for longer while usage cannot be written, the last known monthly
// allowance applies to this instance's own usage in the new month, so a soft
// limit still bounds spending. Charges dated before the snapshot's month are
// not limited.
func (p *Pace) exhaustedIn(month int32) bool {
	switch {
	case p.counter == nil || month == p.counter.month:
		return p.checksExhausted()
	case month < p.counter.month || p.Allowance.UnlimitedChecks:
		return false
	default:
		return attemptsIn(p.Allowance.WorkspaceID, month) >= p.Allowance.MonthlyChecks
	}
}

// unflushedSinceLoad is what this instance charged a workspace after the
// snapshot's usage was read.
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

// resetMeterForTest clears every counter and any pending batch.
func resetMeterForTest() {
	refreshMu.Lock()
	defer refreshMu.Unlock()
	meters.Range(func(key, _ any) bool {
		meters.Delete(key)
		return true
	})
	pending = nil
	batchSequence = 0
	lastWritten = time.Now()
	reloadPending = false
	resetHealthForTest()
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
