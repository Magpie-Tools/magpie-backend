package checkpace

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"sync/atomic"
	"time"
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
// A crash loses at most this much usage on the crashed instance.
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

// CheckUsage is metered usage of one workspace in one usage period.
type CheckUsage struct {
	WorkspaceID    uint
	PeriodStart    time.Time
	Attempts       uint64
	ProtocolChecks uint64
}

// UsageBatch is one flush. Instance identifies this process and Sequence
// increases with each new batch; a retried batch keeps both.
type UsageBatch struct {
	Instance string
	Sequence uint64
	Usage    []CheckUsage
}

// Flusher adds a batch to the usage meter. It must apply all rows or none, and
// must ignore a batch whose Sequence it already applied for the Instance.
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
)

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
	if window == nil || month > window.month {
		start := monthStart(month)
		latestMonth.Store(&monthWindow{start: start.UnixNano(), end: start.AddDate(0, 1, 0).UnixNano(), month: month})
	}
	return month
}

// ChargeRequest is called by the checker just before it sends one request
// that serves workspaceID. firstOfCheck marks a protocol check's first
// request. It returns false, charging nothing, when the workspace's check
// allowance is used up on this instance; the request must then not be sent
// for the workspace. The cost is a map read, an atomic load and one or two
// atomic additions: no Redis, database, JSON or cryptographic work.
func (s *Snapshot) ChargeRequest(workspaceID uint, firstOfCheck bool, at time.Time) bool {
	if workspaceID == 0 {
		return true
	}
	month := monthOf(at)
	var counter *monthCounter
	if pace := s.lookup(workspaceID); pace != nil && pace.counter != nil && pace.counter.month == month {
		if pace.checksExhausted() {
			return false
		}
		counter = pace.counter
	} else {
		// No snapshot yet, a workspace the snapshot lacks, or a new month
		// the snapshot has not reached: charge without a limit until the next
		// refresh, which follows within RefreshInterval.
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
		pending = collectBatch(time.Now())
		if pending == nil {
			return nil
		}
	}
	if err := flusher(ctx, pending.batch); err != nil {
		return err
	}
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
