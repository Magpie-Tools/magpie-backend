package checkpace

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type usageKey struct {
	workspaceID uint
	period      time.Time
}

// fakeStore keeps usage the way the database would: each batch adds its rows
// once per instance and sequence, and loads report the current month.
type fakeStore struct {
	mu      sync.Mutex
	monthly uint64
	flushed map[usageKey]CheckUsage
	applied map[string]uint64
	batches []UsageBatch
	loads   int
	// failFlush fails before committing; loseAck commits and then fails, as
	// when the acknowledgement of a commit is lost.
	failFlush bool
	loseAck   bool
	// unknown answers like a database whose replay mark was pruned.
	unknown bool
	// p is the Pacer under test, configured with this store.
	p *Pacer
}

func (f *fakeStore) load(_ context.Context, now time.Time) ([]Allowance, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.loads++
	start, _ := UsagePeriod(now)
	return []Allowance{{WorkspaceID: 7, MonthlyChecks: f.monthly, UsedChecks: f.flushed[usageKey{7, start}].Attempts, ChecksPerCycle: 1}}, nil
}

func (f *fakeStore) flush(_ context.Context, batch UsageBatch) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.batches = append(f.batches, batch)
	if f.unknown {
		return ErrBatchOutcomeUnknown
	}
	if f.failFlush {
		return errors.New("database unavailable")
	}
	if batch.Sequence > f.applied[batch.Instance] {
		f.applied[batch.Instance] = batch.Sequence
		for _, delta := range batch.Usage {
			key := usageKey{delta.WorkspaceID, delta.PeriodStart}
			total := f.flushed[key]
			total.Attempts += delta.Attempts
			total.ProtocolChecks += delta.ProtocolChecks
			f.flushed[key] = total
		}
	}
	if f.loseAck {
		return errors.New("connection reset after commit")
	}
	return nil
}

func (f *fakeStore) total(workspaceID uint, at time.Time) CheckUsage {
	f.mu.Lock()
	defer f.mu.Unlock()
	start, _ := UsagePeriod(at)
	return f.flushed[usageKey{workspaceID, start}]
}

func useFakeStore(t testing.TB, monthly uint64) *fakeStore {
	t.Helper()
	store := &fakeStore{monthly: monthly, flushed: map[usageKey]CheckUsage{}, applied: map[string]uint64{}}
	// Each test owns its Pacer. A recent last write keeps an idle flush from
	// sending the daily keepalive batch.
	store.p = NewPacer()
	store.p.lastWritten = time.Now()
	store.p.Configure(Store{Load: store.load, FlushUsage: store.flush})
	return store
}

// check charges one protocol check of requests requests, as the checker does,
// and returns how many were admitted.
func check(p *Pacer, workspaceID uint, requests int) int {
	for sent := range requests {
		if !p.ChargeRequest(p.Current(), workspaceID, sent == 0, time.Now()) {
			return sent
		}
	}
	return requests
}

func TestRequestsStopAtTheAllowanceBeforeTheyAreFlushed(t *testing.T) {
	store := useFakeStore(t, 10)
	if err := store.p.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	point, holds := ownPoint(store.p, store.p.Current(), 7, 1, time.Now())
	if !holds || !dueAt(store.p, store.p.Current(), 7, 1, point, time.Now()) {
		t.Fatal("a workspace with allowance left is not due at its cadence point")
	}
	// Three protocol checks, two with two retries: seven requests.
	if check(store.p, 7, 1)+check(store.p, 7, 3)+check(store.p, 7, 3) != 7 {
		t.Fatal("requests within the allowance were refused")
	}
	pace, _ := store.p.Current().Pace(7)
	if pace.CurrentUsedChecks() != 7 || pace.CurrentChecksExhausted() {
		t.Fatalf("used = %d, exhausted = %v; want 7, false", pace.CurrentUsedChecks(), pace.CurrentChecksExhausted())
	}
	// Nothing has been flushed, yet the fourth check stops at the
	// allowance: its last retry is never sent.
	if sent := check(store.p, 7, 4); sent != 3 {
		t.Fatalf("sent %d requests with 3 checks left", sent)
	}
	pace, _ = store.p.Current().Pace(7)
	if pace.CurrentUsedChecks() != 10 || !pace.CurrentChecksExhausted() || dueAt(store.p, store.p.Current(), 7, 1, point, time.Now()) {
		t.Fatal("a workspace at its allowance on this instance is still due")
	}
	if _, holds := ownPoint(store.p, store.p.Current(), 7, 1, time.Now()); holds {
		t.Fatal("a workspace at its allowance on this instance still holds its routes in the queue")
	}
	if next := store.p.FirstRouteCheck(store.p.Current(), 1, nil, time.Now()); !next.After(time.Now()) {
		t.Fatal("a route must always be due again later")
	}
}

func TestFlushAndReloadNeverCountUsageTwice(t *testing.T) {
	store := useFakeStore(t, 1_000)
	check(store.p, 7, 2)
	if err := store.p.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := store.total(7, time.Now()); got.Attempts != 2 || got.ProtocolChecks != 1 {
		t.Fatalf("flushed = %+v", got)
	}
	check(store.p, 7, 1)
	// Flushed between loads: the snapshot does not see it in the database
	// yet, and the counter covers it exactly once.
	if err := store.p.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	check(store.p, 7, 4)
	pace, _ := store.p.Current().Pace(7)
	if got := pace.CurrentUsedChecks(); got != 7 {
		t.Fatalf("used between loads = %d, want 7", got)
	}
	if err := store.p.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	pace, _ = store.p.Current().Pace(7)
	if got, flushed := pace.CurrentUsedChecks(), store.total(7, time.Now()); got != 7 || flushed.Attempts != 7 || flushed.ProtocolChecks != 3 {
		t.Fatalf("after reload used = %d, flushed = %+v", got, flushed)
	}
}

func TestFailedFlushIsRetriedWithoutLosingUsage(t *testing.T) {
	store := useFakeStore(t, 1_000)
	if err := store.p.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	store.failFlush = true
	check(store.p, 7, 5)
	if err := store.p.Flush(context.Background()); err == nil {
		t.Fatal("expected the flush to fail")
	}
	// The refresh keeps the snapshot, which still counts this instance's
	// checks through the counters.
	if err := store.p.Refresh(context.Background()); err == nil {
		t.Fatal("a refresh after a failed flush reported success")
	}
	if pace, _ := store.p.Current().Pace(7); pace.CurrentUsedChecks() != 5 {
		t.Fatalf("used after a failed flush = %d, want 5", pace.CurrentUsedChecks())
	}
	store.failFlush = false
	if err := store.p.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := store.total(7, time.Now()).Attempts; got != 5 {
		t.Fatalf("retried flush wrote %d checks, want 5", got)
	}
	if pace, _ := store.p.Current().Pace(7); pace.Allowance.UsedChecks != 5 || pace.CurrentUsedChecks() != 5 {
		t.Fatalf("after the retry the snapshot read %d and counts %d; want a reload with 5", pace.Allowance.UsedChecks, pace.CurrentUsedChecks())
	}
}

func TestLostAcknowledgementDoesNotChargeTwice(t *testing.T) {
	store := useFakeStore(t, 5)
	if err := store.p.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	check(store.p, 7, 3)
	store.loseAck = true
	// The batch commits, but the checker cannot know. Neither the flush nor
	// a refresh may count those three checks twice.
	if err := store.p.Flush(context.Background()); err == nil {
		t.Fatal("expected the flush to report a failure")
	}
	if err := store.p.Refresh(context.Background()); err == nil {
		t.Fatal("expected the refresh to report the failed flush")
	}
	pace, _ := store.p.Current().Pace(7)
	if pace.CurrentUsedChecks() != 3 || pace.CurrentChecksExhausted() {
		t.Fatalf("used = %d, exhausted = %v after a lost acknowledgement; want 3, false", pace.CurrentUsedChecks(), pace.CurrentChecksExhausted())
	}
	// More usage arrives while the outcome is unknown. The retry resends the
	// first batch unchanged, then flushes the new usage as its own batch,
	// then reloads.
	check(store.p, 7, 1)
	store.loseAck = false
	if err := store.p.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := store.total(7, time.Now()).Attempts; got != 4 {
		t.Fatalf("four requests were charged as %d checks", got)
	}
	if len(store.batches) != 4 || store.batches[0].Sequence != store.batches[1].Sequence ||
		store.batches[1].Sequence != store.batches[2].Sequence || store.batches[2].Usage[0] != store.batches[0].Usage[0] ||
		store.batches[3].Sequence <= store.batches[2].Sequence {
		t.Fatalf("batches = %+v; want the first resent unchanged, then a new one", store.batches)
	}
	pace, _ = store.p.Current().Pace(7)
	if pace.Allowance.UsedChecks != 4 || pace.CurrentUsedChecks() != 4 || pace.CurrentChecksExhausted() {
		t.Fatalf("after the retry the snapshot read %d and counts %d; want a reload with 4", pace.Allowance.UsedChecks, pace.CurrentUsedChecks())
	}
}

func TestUsageStaysInTheMonthItWasMade(t *testing.T) {
	store := useFakeStore(t, 1_000)
	if err := store.p.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	start, _ := UsagePeriod(time.Now())
	lastMonth := start.Add(-time.Second)
	// Requests just before midnight, flushed after it, and a failed flush
	// carrying them further into the new month.
	store.p.ChargeRequest(store.p.Current(), 7, true, lastMonth)
	store.p.ChargeRequest(store.p.Current(), 7, false, lastMonth)
	check(store.p, 7, 1)
	store.failFlush = true
	_ = store.p.Flush(context.Background())
	store.failFlush = false
	if err := store.p.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if previous, current := store.total(7, lastMonth), store.total(7, time.Now()); previous.Attempts != 2 || previous.ProtocolChecks != 1 || current.Attempts != 1 {
		t.Fatalf("previous month = %+v, current month = %+v; want 2 and 1 checks", previous, current)
	}
	// Last month's charges never count against this month's allowance.
	if pace, _ := store.p.Current().Pace(7); pace.CurrentUsedChecks() != 1 {
		t.Fatalf("this month's used checks = %d, want 1", pace.CurrentUsedChecks())
	}
	// Once flushed and long over, last month's counter is dropped.
	store.p.refreshMu.Lock()
	store.p.collectBatch(start.Add(monthPruneDelay + time.Second))
	store.p.refreshMu.Unlock()
	value, _ := store.p.meters.Load(uint(7))
	meter := value.(*workspaceMeter)
	meter.mu.Lock()
	months := len(meter.months)
	meter.mu.Unlock()
	if months != 1 {
		t.Fatalf("%d monthly counters kept, want only the current month", months)
	}
}

func TestChecksPauseWhileUsageStaysUnwrittenAndResume(t *testing.T) {
	store := useFakeStore(t, 1_000)
	if err := store.p.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	store.p.SetPauseAfter(time.Minute)
	point, _ := ownPoint(store.p, store.p.Current(), 7, 1, time.Now())
	store.failFlush = true
	check(store.p, 7, 2)
	if err := store.p.Flush(context.Background()); err == nil {
		t.Fatal("expected the flush to fail")
	}
	if health := store.p.Health(); !health.Healthy || health.Paused || health.FailingSince.IsZero() {
		t.Fatalf("health right after one failure = %+v; want healthy, failing, not paused", health)
	}
	if check(store.p, 7, 1) != 1 || !dueAt(store.p, store.p.Current(), 7, 1, point, time.Now()) {
		t.Fatal("checking paused before the limit")
	}
	// Writes have now failed for longer than the limit.
	store.p.failingSince.Store(time.Now().Add(-2 * time.Minute).UnixNano())
	if health := store.p.Health(); health.Healthy || !health.Paused || health.LastError == "" {
		t.Fatalf("health after the limit = %+v; want unhealthy and paused", health)
	}
	if check(store.p, 7, 1) != 0 || dueAt(store.p, store.p.Current(), 7, 1, point, time.Now()) {
		t.Fatal("checking continued after usage stayed unwritten past the limit")
	}
	// Nothing was discarded, and the first successful write resumes checking.
	store.failFlush = false
	if err := store.p.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := store.total(7, time.Now()).Attempts; got != 3 {
		t.Fatalf("wrote %d checks after recovery, want 3", got)
	}
	if health := store.p.Health(); !health.Healthy || health.Paused {
		t.Fatalf("health after recovery = %+v", health)
	}
	if check(store.p, 7, 1) != 1 {
		t.Fatal("checking did not resume after recovery")
	}

	// A limit of zero never pauses.
	store.p.SetPauseAfter(0)
	store.p.failingSince.Store(time.Now().Add(-24 * time.Hour).UnixNano())
	if check(store.p, 7, 1) != 1 {
		t.Fatal("checking paused with pausing disabled")
	}
}

func TestStaleSnapshotKeepsLastKnownAllowanceInANewMonth(t *testing.T) {
	store := useFakeStore(t, 3)
	if err := store.p.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	snapshot := store.p.Current()
	// Writes fail across midnight, so the snapshot stays on last month.
	nextMonth := snapshot.PeriodEnd.Add(time.Hour)
	// The last known cadence still applies while the allowance lasts.
	point, holds := ownPoint(store.p, snapshot, 7, 1, nextMonth)
	if !holds || !dueAt(store.p, snapshot, 7, 1, point, point) {
		t.Fatal("a workspace with allowance left in the new month is not due at its cadence point")
	}
	admitted := 0
	for range 5 {
		if store.p.ChargeRequest(snapshot, 7, true, nextMonth) {
			admitted++
		}
	}
	if admitted != 3 {
		t.Fatalf("admitted %d requests in the new month with a last known allowance of 3", admitted)
	}
	if dueAt(store.p, snapshot, 7, 1, point, point) {
		t.Fatal("a workspace at its last known allowance in the new month is still due")
	}
	// The new month's usage is its own: last month's counter is untouched.
	if pace, _ := snapshot.Pace(7); pace.CurrentUsedChecks() != 0 {
		t.Fatalf("new-month requests counted against the old month: %d", pace.CurrentUsedChecks())
	}
}

func TestIdleProcessRefreshesItsReplayMark(t *testing.T) {
	store := useFakeStore(t, 1_000)
	if err := store.p.Flush(context.Background()); err != nil || len(store.batches) != 0 {
		t.Fatalf("an idle flush wrote %d batches: %v", len(store.batches), err)
	}
	store.p.refreshMu.Lock()
	store.p.lastWritten = time.Now().Add(-markKeepAlive - time.Minute)
	store.p.refreshMu.Unlock()
	if err := store.p.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(store.batches) != 1 || len(store.batches[0].Usage) != 0 || store.batches[0].Sequence != 1 {
		t.Fatalf("batches = %+v; want one empty keepalive batch", store.batches)
	}
}

func TestBatchWithUnknownOutcomeIsDroppedNotChargedTwice(t *testing.T) {
	store := useFakeStore(t, 1_000)
	check(store.p, 7, 2)
	store.failFlush = true
	_ = store.p.Flush(context.Background())
	// Much later, the database no longer has the replay mark.
	store.failFlush, store.unknown = false, true
	if err := store.p.Flush(context.Background()); err != nil {
		t.Fatalf("an unknown outcome should resolve the batch: %v", err)
	}
	if !store.batches[1].Resend || !store.p.Health().Healthy {
		t.Fatalf("resend flag = %v, health = %+v", store.batches[1].Resend, store.p.Health())
	}
	store.unknown = false
	check(store.p, 7, 1)
	if err := store.p.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	last := store.batches[len(store.batches)-1]
	if last.Resend || len(last.Usage) != 1 || last.Usage[0].Attempts != 1 {
		t.Fatalf("next batch = %+v; want only the new request", last)
	}
}

func TestUnlimitedWorkspaceIgnoresMeteredUsage(t *testing.T) {
	p := NewPacer()
	snapshot := Compute(testNow, []Allowance{{WorkspaceID: 3, UnlimitedChecks: true, ChecksPerCycle: 1, RequestedCheckInterval: time.Minute}})
	p.attachMeter(snapshot)
	pace := snapshot.paces[3]
	for range 1_000 {
		if !p.ChargeRequest(snapshot, 3, false, testNow) {
			t.Fatal("an unlimited workspace was refused a request")
		}
	}
	if pace.CurrentChecksExhausted() {
		t.Fatal("an unlimited workspace was exhausted by metered usage")
	}
}

func BenchmarkChargeRequest(b *testing.B) {
	store := useFakeStore(b, 1<<62)
	if err := store.p.Refresh(context.Background()); err != nil {
		b.Fatal(err)
	}
	snapshot, now := store.p.Current(), time.Now()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.p.ChargeRequest(snapshot, 7, i == 0, now)
	}
}
