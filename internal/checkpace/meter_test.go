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
	// failFlush fails before committing; loseAck commits and then fails, as
	// when the acknowledgement of a commit is lost.
	failFlush bool
	loseAck   bool
	// unknown answers like a database whose replay mark was pruned.
	unknown bool
}

func (f *fakeStore) load(_ context.Context, now time.Time) ([]Allowance, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
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
	resetMeterForTest()
	store := &fakeStore{monthly: monthly, flushed: map[usageKey]CheckUsage{}, applied: map[string]uint64{}}
	previousStore, previousSnapshot := storeValue.Load(), Current()
	Configure(Store{Load: store.load, FlushUsage: store.flush})
	t.Cleanup(func() {
		storeValue.Store(previousStore)
		Publish(previousSnapshot)
		resetMeterForTest()
	})
	return store
}

// check charges one protocol check of requests requests, as the checker does,
// and returns how many were admitted.
func check(workspaceID uint, requests int) int {
	for sent := range requests {
		if !Current().ChargeRequest(workspaceID, sent == 0, time.Now()) {
			return sent
		}
	}
	return requests
}

func TestRequestsStopAtTheAllowanceBeforeTheyAreFlushed(t *testing.T) {
	useFakeStore(t, 10)
	if err := Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Three protocol checks, two with two retries: seven requests.
	if check(7, 1)+check(7, 3)+check(7, 3) != 7 {
		t.Fatal("requests within the allowance were refused")
	}
	pace, _ := Current().Pace(7)
	if pace.CurrentUsedChecks() != 7 || pace.CurrentChecksExhausted() {
		t.Fatalf("used = %d, exhausted = %v; want 7, false", pace.CurrentUsedChecks(), pace.CurrentChecksExhausted())
	}
	// Nothing has been flushed, yet the fourth check stops at the
	// allowance: its last retry is never sent.
	if sent := check(7, 4); sent != 3 {
		t.Fatalf("sent %d requests with 3 checks left", sent)
	}
	pace, _ = Current().Pace(7)
	if pace.CurrentUsedChecks() != 10 || !pace.CurrentChecksExhausted() || Current().CheckDue(7, 1, time.Now(), time.Minute) {
		t.Fatal("a workspace at its allowance on this instance is still due")
	}
	if Current().RouteCheckInterval(nil) <= 0 {
		t.Fatal("route interval must stay positive")
	}
}

func TestFlushAndReloadNeverCountUsageTwice(t *testing.T) {
	store := useFakeStore(t, 1_000)
	check(7, 2)
	if err := Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := store.total(7, time.Now()); got.Attempts != 2 || got.ProtocolChecks != 1 {
		t.Fatalf("flushed = %+v", got)
	}
	check(7, 1)
	// Flushed between loads: the snapshot does not see it in the database
	// yet, and the counter covers it exactly once.
	if err := Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	check(7, 4)
	pace, _ := Current().Pace(7)
	if got := pace.CurrentUsedChecks(); got != 7 {
		t.Fatalf("used between loads = %d, want 7", got)
	}
	if err := Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	pace, _ = Current().Pace(7)
	if got, flushed := pace.CurrentUsedChecks(), store.total(7, time.Now()); got != 7 || flushed.Attempts != 7 || flushed.ProtocolChecks != 3 {
		t.Fatalf("after reload used = %d, flushed = %+v", got, flushed)
	}
}

func TestFailedFlushIsRetriedWithoutLosingUsage(t *testing.T) {
	store := useFakeStore(t, 1_000)
	if err := Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	store.failFlush = true
	check(7, 5)
	if err := Flush(context.Background()); err == nil {
		t.Fatal("expected the flush to fail")
	}
	// The refresh keeps the snapshot, which still counts this instance's
	// checks through the counters.
	if err := Refresh(context.Background()); err == nil {
		t.Fatal("a refresh after a failed flush reported success")
	}
	if pace, _ := Current().Pace(7); pace.CurrentUsedChecks() != 5 {
		t.Fatalf("used after a failed flush = %d, want 5", pace.CurrentUsedChecks())
	}
	store.failFlush = false
	if err := Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := store.total(7, time.Now()).Attempts; got != 5 {
		t.Fatalf("retried flush wrote %d checks, want 5", got)
	}
	if pace, _ := Current().Pace(7); pace.Allowance.UsedChecks != 5 || pace.CurrentUsedChecks() != 5 {
		t.Fatalf("after the retry the snapshot read %d and counts %d; want a reload with 5", pace.Allowance.UsedChecks, pace.CurrentUsedChecks())
	}
}

func TestLostAcknowledgementDoesNotChargeTwice(t *testing.T) {
	store := useFakeStore(t, 5)
	if err := Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	check(7, 3)
	store.loseAck = true
	// The batch commits, but the checker cannot know. Neither the flush nor
	// a refresh may count those three checks twice.
	if err := Flush(context.Background()); err == nil {
		t.Fatal("expected the flush to report a failure")
	}
	if err := Refresh(context.Background()); err == nil {
		t.Fatal("expected the refresh to report the failed flush")
	}
	pace, _ := Current().Pace(7)
	if pace.CurrentUsedChecks() != 3 || pace.CurrentChecksExhausted() {
		t.Fatalf("used = %d, exhausted = %v after a lost acknowledgement; want 3, false", pace.CurrentUsedChecks(), pace.CurrentChecksExhausted())
	}
	// More usage arrives while the outcome is unknown. The retry resends the
	// first batch unchanged, then flushes the new usage as its own batch,
	// then reloads.
	check(7, 1)
	store.loseAck = false
	if err := Flush(context.Background()); err != nil {
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
	pace, _ = Current().Pace(7)
	if pace.Allowance.UsedChecks != 4 || pace.CurrentUsedChecks() != 4 || pace.CurrentChecksExhausted() {
		t.Fatalf("after the retry the snapshot read %d and counts %d; want a reload with 4", pace.Allowance.UsedChecks, pace.CurrentUsedChecks())
	}
}

func TestUsageStaysInTheMonthItWasMade(t *testing.T) {
	store := useFakeStore(t, 1_000)
	if err := Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	start, _ := UsagePeriod(time.Now())
	lastMonth := start.Add(-time.Second)
	// Requests just before midnight, flushed after it, and a failed flush
	// carrying them further into the new month.
	Current().ChargeRequest(7, true, lastMonth)
	Current().ChargeRequest(7, false, lastMonth)
	check(7, 1)
	store.failFlush = true
	_ = Flush(context.Background())
	store.failFlush = false
	if err := Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if previous, current := store.total(7, lastMonth), store.total(7, time.Now()); previous.Attempts != 2 || previous.ProtocolChecks != 1 || current.Attempts != 1 {
		t.Fatalf("previous month = %+v, current month = %+v; want 2 and 1 checks", previous, current)
	}
	// Last month's charges never count against this month's allowance.
	if pace, _ := Current().Pace(7); pace.CurrentUsedChecks() != 1 {
		t.Fatalf("this month's used checks = %d, want 1", pace.CurrentUsedChecks())
	}
	// Once flushed and long over, last month's counter is dropped.
	refreshMu.Lock()
	collectBatch(start.Add(monthPruneDelay + time.Second))
	refreshMu.Unlock()
	value, _ := meters.Load(uint(7))
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
	if err := Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	SetPauseAfter(time.Minute)
	store.failFlush = true
	check(7, 2)
	if err := Flush(context.Background()); err == nil {
		t.Fatal("expected the flush to fail")
	}
	if health := Health(); !health.Healthy || health.Paused || health.FailingSince.IsZero() {
		t.Fatalf("health right after one failure = %+v; want healthy, failing, not paused", health)
	}
	if check(7, 1) != 1 {
		t.Fatal("checking paused before the limit")
	}
	// Writes have now failed for longer than the limit.
	failingSince.Store(time.Now().Add(-2 * time.Minute).UnixNano())
	if health := Health(); health.Healthy || !health.Paused || health.LastError == "" {
		t.Fatalf("health after the limit = %+v; want unhealthy and paused", health)
	}
	if check(7, 1) != 0 || Current().CheckDue(7, 1, time.Now(), time.Minute) {
		t.Fatal("checking continued after usage stayed unwritten past the limit")
	}
	// Nothing was discarded, and the first successful write resumes checking.
	store.failFlush = false
	if err := Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := store.total(7, time.Now()).Attempts; got != 3 {
		t.Fatalf("wrote %d checks after recovery, want 3", got)
	}
	if health := Health(); !health.Healthy || health.Paused {
		t.Fatalf("health after recovery = %+v", health)
	}
	if check(7, 1) != 1 {
		t.Fatal("checking did not resume after recovery")
	}

	// A limit of zero never pauses.
	SetPauseAfter(0)
	failingSince.Store(time.Now().Add(-24 * time.Hour).UnixNano())
	if check(7, 1) != 1 {
		t.Fatal("checking paused with pausing disabled")
	}
}

func TestStaleSnapshotKeepsLastKnownAllowanceInANewMonth(t *testing.T) {
	useFakeStore(t, 3)
	if err := Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	snapshot := Current()
	// Writes fail across midnight, so the snapshot stays on last month.
	nextMonth := snapshot.PeriodEnd.Add(time.Hour)
	admitted := 0
	for range 5 {
		if snapshot.ChargeRequest(7, true, nextMonth) {
			admitted++
		}
	}
	if admitted != 3 {
		t.Fatalf("admitted %d requests in the new month with a last known allowance of 3", admitted)
	}
	if snapshot.CheckDue(7, 1, nextMonth, time.Minute) {
		t.Fatal("a workspace at its last known allowance in the new month is still due")
	}
	// The new month's usage is its own: last month's counter is untouched.
	if pace, _ := snapshot.Pace(7); pace.CurrentUsedChecks() != 0 {
		t.Fatalf("new-month requests counted against the old month: %d", pace.CurrentUsedChecks())
	}
}

func TestIdleProcessRefreshesItsReplayMark(t *testing.T) {
	store := useFakeStore(t, 1_000)
	if err := Flush(context.Background()); err != nil || len(store.batches) != 0 {
		t.Fatalf("an idle flush wrote %d batches: %v", len(store.batches), err)
	}
	refreshMu.Lock()
	lastWritten = time.Now().Add(-markKeepAlive - time.Minute)
	refreshMu.Unlock()
	if err := Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(store.batches) != 1 || len(store.batches[0].Usage) != 0 || store.batches[0].Sequence != 1 {
		t.Fatalf("batches = %+v; want one empty keepalive batch", store.batches)
	}
}

func TestBatchWithUnknownOutcomeIsDroppedNotChargedTwice(t *testing.T) {
	store := useFakeStore(t, 1_000)
	check(7, 2)
	store.failFlush = true
	_ = Flush(context.Background())
	// Much later, the database no longer has the replay mark.
	store.failFlush, store.unknown = false, true
	if err := Flush(context.Background()); err != nil {
		t.Fatalf("an unknown outcome should resolve the batch: %v", err)
	}
	if !store.batches[1].Resend || !Health().Healthy {
		t.Fatalf("resend flag = %v, health = %+v", store.batches[1].Resend, Health())
	}
	store.unknown = false
	check(7, 1)
	if err := Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	last := store.batches[len(store.batches)-1]
	if last.Resend || len(last.Usage) != 1 || last.Usage[0].Attempts != 1 {
		t.Fatalf("next batch = %+v; want only the new request", last)
	}
}

func TestUnlimitedWorkspaceIgnoresMeteredUsage(t *testing.T) {
	resetMeterForTest()
	t.Cleanup(resetMeterForTest)
	snapshot := Compute(testNow, []Allowance{{WorkspaceID: 3, UnlimitedChecks: true, ChecksPerCycle: 1, RequestedCheckInterval: time.Minute}})
	attachMeter(snapshot)
	pace := snapshot.paces[3]
	for range 1_000 {
		if !snapshot.ChargeRequest(3, false, testNow) {
			t.Fatal("an unlimited workspace was refused a request")
		}
	}
	if pace.CurrentChecksExhausted() {
		t.Fatal("an unlimited workspace was exhausted by metered usage")
	}
}

func BenchmarkChargeRequest(b *testing.B) {
	store := useFakeStore(b, 1<<62)
	_ = store
	if err := Refresh(context.Background()); err != nil {
		b.Fatal(err)
	}
	snapshot, now := Current(), time.Now()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		snapshot.ChargeRequest(7, i == 0, now)
	}
}
