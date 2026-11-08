package checkpace

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// fakeStore keeps usage the way the database would: flushed deltas add up,
// and loads report what was flushed.
type fakeStore struct {
	mu        sync.Mutex
	monthly   uint64
	flushed   map[uint]CheckUsage
	failFlush bool
}

func (f *fakeStore) load(_ context.Context, _ time.Time) ([]Allowance, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return []Allowance{{WorkspaceID: 7, MonthlyChecks: f.monthly, UsedChecks: f.flushed[7].Attempts, ChecksPerCycle: 1}}, nil
}

func (f *fakeStore) flush(_ context.Context, _ time.Time, usage []CheckUsage) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failFlush {
		return errors.New("database unavailable")
	}
	for _, delta := range usage {
		total := f.flushed[delta.WorkspaceID]
		total.Attempts += delta.Attempts
		total.ProtocolChecks += delta.ProtocolChecks
		f.flushed[delta.WorkspaceID] = total
	}
	return nil
}

func useFakeStore(t *testing.T, monthly uint64) *fakeStore {
	t.Helper()
	resetMeterForTest()
	store := &fakeStore{monthly: monthly, flushed: map[uint]CheckUsage{}}
	previousStore, previousSnapshot := storeValue.Load(), Current()
	Configure(Store{Load: store.load, FlushUsage: store.flush})
	t.Cleanup(func() {
		storeValue.Store(previousStore)
		Publish(previousSnapshot)
		resetMeterForTest()
	})
	return store
}

func TestMeteredChecksExhaustAllowanceBeforeTheyAreFlushed(t *testing.T) {
	useFakeStore(t, 10)
	if err := Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Three protocol checks, one with two retries: seven requests.
	ChargeChecks(7, 1)
	ChargeChecks(7, 3)
	ChargeChecks(7, 3)
	pace, _ := Current().Pace(7)
	if pace.CurrentUsedChecks() != 7 || pace.CurrentChecksExhausted() {
		t.Fatalf("used = %d, exhausted = %v; want 7, false", pace.CurrentUsedChecks(), pace.CurrentChecksExhausted())
	}
	ChargeChecks(7, 3)
	// Nothing has been flushed, yet the workspace stops at once on this
	// instance: enforcement does not wait for persistence.
	pace, _ = Current().Pace(7)
	if !pace.CurrentChecksExhausted() || Current().CheckDue(7, 1, time.Now(), time.Minute) {
		t.Fatal("a workspace past its allowance on this instance is still due")
	}
	if Current().RouteCheckInterval(nil) <= 0 {
		t.Fatal("route interval must stay positive")
	}
}

func TestFlushAndReloadNeverCountUsageTwice(t *testing.T) {
	store := useFakeStore(t, 1_000)
	ChargeChecks(7, 2)
	if err := Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if store.flushed[7] != (CheckUsage{Attempts: 2, ProtocolChecks: 1}) {
		t.Fatalf("flushed = %+v", store.flushed[7])
	}
	ChargeChecks(7, 1)
	// Flushed between loads: the snapshot does not see it in the database
	// yet, and the counter covers it exactly once.
	if err := Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	ChargeChecks(7, 4)
	pace, _ := Current().Pace(7)
	if got := pace.CurrentUsedChecks(); got != 7 {
		t.Fatalf("used between loads = %d, want 7", got)
	}
	if err := Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	pace, _ = Current().Pace(7)
	if got := pace.CurrentUsedChecks(); got != 7 || store.flushed[7].Attempts != 7 || store.flushed[7].ProtocolChecks != 3 {
		t.Fatalf("after reload used = %d, flushed = %+v", got, store.flushed[7])
	}
}

func TestFailedFlushIsRetriedWithoutLosingUsage(t *testing.T) {
	store := useFakeStore(t, 1_000)
	store.failFlush = true
	ChargeChecks(7, 5)
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
	if store.flushed[7].Attempts != 5 {
		t.Fatalf("retried flush wrote %+v", store.flushed[7])
	}
}

func TestUnlimitedWorkspaceIgnoresMeteredUsage(t *testing.T) {
	resetMeterForTest()
	t.Cleanup(resetMeterForTest)
	snapshot := Compute(testNow, []Allowance{{WorkspaceID: 3, UnlimitedChecks: true, ChecksPerCycle: 1, RequestedCheckInterval: time.Minute}})
	pace := snapshot.paces[3]
	pace.counter = counterFor(3)
	ChargeChecks(3, 1_000_000)
	if pace.CurrentChecksExhausted() {
		t.Fatal("an unlimited workspace was exhausted by metered usage")
	}
}

func BenchmarkChargeChecks(b *testing.B) {
	resetMeterForTest()
	b.Cleanup(resetMeterForTest)
	ChargeChecks(1, 1)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ChargeChecks(1, 1)
	}
}
