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
	store.failFlush = true
	check(7, 5)
	if err := Flush(context.Background()); err == nil {
		t.Fatal("expected the flush to fail")
	}
	// The load still works, and this instance still counts its checks.
	if err := Refresh(context.Background()); err != nil {
		t.Fatal(err)
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
}

func TestLostAcknowledgementDoesNotChargeTwice(t *testing.T) {
	store := useFakeStore(t, 1_000)
	check(7, 3)
	store.loseAck = true
	if err := Flush(context.Background()); err == nil {
		t.Fatal("expected the flush to report a failure")
	}
	// More usage arrives while the outcome is unknown. The retry resends the
	// first batch unchanged, then flushes the new usage as its own batch.
	check(7, 2)
	store.loseAck = false
	if err := Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := store.total(7, time.Now()).Attempts; got != 5 {
		t.Fatalf("five requests were charged as %d checks", got)
	}
	if len(store.batches) != 3 || store.batches[0].Sequence != store.batches[1].Sequence ||
		store.batches[1].Usage[0] != store.batches[0].Usage[0] || store.batches[2].Sequence <= store.batches[1].Sequence {
		t.Fatalf("batches = %+v; want the first resent unchanged, then a new one", store.batches)
	}
	if err := Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if pace, _ := Current().Pace(7); pace.CurrentUsedChecks() != 5 {
		t.Fatalf("used after the retry = %d, want 5", pace.CurrentUsedChecks())
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
