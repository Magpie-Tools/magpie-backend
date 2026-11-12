package checkpace

import (
	"context"
	"runtime"
	"strings"
	"testing"
	"time"

	"magpie/internal/domain"
)

// pacerGoroutines counts goroutines running one of a Pacer's methods.
func pacerGoroutines() int {
	buffer := make([]byte, 1<<20)
	for {
		written := runtime.Stack(buffer, true)
		if written < len(buffer) {
			buffer = buffer[:written]
			break
		}
		buffer = make([]byte, 2*len(buffer))
	}
	count := 0
	for _, stack := range strings.Split(string(buffer), "\n\n") {
		if strings.Contains(stack, "checkpace.(*Pacer).") {
			count++
		}
	}
	return count
}

// Constructing a Pacer starts nothing; Run is its only routine and returns
// after one more flush when its context ends.
func TestNewPacerStartsNothingAndRunFlushesOnceMoreAtTheEnd(t *testing.T) {
	store := useFakeStore(t, 1_000)
	if got := pacerGoroutines(); got != 0 {
		t.Fatalf("a new Pacer runs %d goroutines", got)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		store.p.Run(ctx)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for store.p.Current() == nil {
		if time.Now().After(deadline) {
			t.Fatal("Run did not publish its first snapshot")
		}
		time.Sleep(time.Millisecond)
	}
	if admitted := check(store.p, 7, 3); admitted != 3 {
		t.Fatalf("admitted %d of 3 requests", admitted)
	}
	cancel()
	<-done
	if got := store.total(7, time.Now()); got.Attempts != 3 || got.ProtocolChecks != 1 {
		t.Fatalf("after Run returned the store holds %+v, want 3 attempts and 1 protocol check", got)
	}
	if got := pacerGoroutines(); got != 0 {
		t.Fatalf("%d Pacer goroutines outlived Run", got)
	}
}

// Two Pacers, as two tests or two processes would hold, share no counters,
// snapshot or write health.
func TestPacersShareNoState(t *testing.T) {
	first, second := useFakeStore(t, 1_000), useFakeStore(t, 1_000)
	ctx := context.Background()
	for _, store := range []*fakeStore{first, second} {
		if err := store.p.Refresh(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if first.p.instanceID == second.p.instanceID {
		t.Fatal("two Pacers write batches under one instance id")
	}
	if first.p.Current() == second.p.Current() {
		t.Fatal("two Pacers publish one snapshot")
	}
	check(first.p, 7, 2)
	first.failFlush = true
	if err := first.p.Flush(ctx); err == nil {
		t.Fatal("the first Pacer's flush did not fail")
	}
	if err := second.p.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if got := second.total(7, time.Now()); got.Attempts != 0 {
		t.Fatalf("the second Pacer wrote %d attempts charged to the first", got.Attempts)
	}
	if pace, _ := second.p.Current().Pace(7); pace.CurrentUsedChecks() != 0 {
		t.Fatalf("the second Pacer's snapshot counts %d checks charged to the first", pace.CurrentUsedChecks())
	}
	if first.p.failingSince.Load() == 0 || second.p.failingSince.Load() != 0 || !second.p.Health().Healthy {
		t.Fatal("one Pacer's failed write changed the other's health")
	}
	first.failFlush = false
	if err := first.p.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if got := first.total(7, time.Now()); got.Attempts != 2 {
		t.Fatalf("the first Pacer wrote %d of its 2 attempts", got.Attempts)
	}
}

// Before the first load a Pacer has no snapshot. Every consumer then behaves
// as it did before allowances existed: routes use the fallback interval and
// every owner is due, while requests are still metered. A nil Pacer, held by
// tools that pace nothing, behaves the same and meters nothing.
func TestNilSnapshotUsesTheFallbackAndKeepsMetering(t *testing.T) {
	owners := []domain.Workspace{{ID: 1}, {ID: 2}}
	now := time.Now()
	store := useFakeStore(t, 1_000)
	for name, p := range map[string]*Pacer{"pacer before its first load": store.p, "nil pacer": nil} {
		if p.Current() != nil {
			t.Fatalf("%s has a snapshot", name)
		}
		snapshot := p.Current()
		if got := p.RouteCheckInterval(snapshot, owners); got != FallbackInterval {
			t.Fatalf("%s: route interval %s, want the fallback", name, got)
		}
		if got := p.SourceScrapeInterval(snapshot, []uint{1, 2}); got != FallbackInterval {
			t.Fatalf("%s: scrape interval %s, want the fallback", name, got)
		}
		if !p.CheckDue(snapshot, 1, 9, now, FallbackInterval) || !snapshot.ScrapeDue(1, 9, now, FallbackInterval) {
			t.Fatalf("%s: an owner was not due", name)
		}
		if !p.ChargeRequest(snapshot, 1, true, now) {
			t.Fatalf("%s refused a request", name)
		}
		p.RequestRefresh()
	}
	if err := store.p.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := store.total(1, now); got.Attempts != 1 || got.ProtocolChecks != 1 {
		t.Fatalf("a request charged before the first load was written as %+v", got)
	}
	select {
	case <-store.p.refreshCh:
	default:
		t.Fatal("a refresh request before the first load was dropped")
	}
}
