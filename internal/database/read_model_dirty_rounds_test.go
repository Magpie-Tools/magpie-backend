package database

import (
	"reflect"
	"testing"
	"time"
)

func isolateReadModelDirtyRound(t *testing.T) {
	t.Helper()
	readModelDirtyMu.Lock()
	dirty, round, batch := readModelDirtyProxyIDs, readModelDirtyRound, readModelPendingBatch
	readModelDirtyProxyIDs = make(map[uint64]struct{})
	readModelDirtyRound, readModelPendingBatch = nil, nil
	readModelDirtyMu.Unlock()
	t.Cleanup(func() {
		readModelDirtyMu.Lock()
		readModelDirtyProxyIDs, readModelDirtyRound, readModelPendingBatch = dirty, round, batch
		readModelDirtyMu.Unlock()
	})
}

func TestReadModelDirtyRoundKeepsNewInvalidationsAndBoundedProgress(t *testing.T) {
	isolateReadModelDirtyRound(t)
	QueueReadModelRefreshForProxyIDs([]uint64{8, 3, 6, 1, 6, 0})
	if got := popDirtyReadModelProxyIDs(2); !reflect.DeepEqual(got, []uint64{1, 3}) {
		t.Fatal("first bounded prefix did not group nearby routes", got)
	}
	if !readModelProxyWorkPending() {
		t.Fatal("closed-round suffix falsely drained when the dirty map was empty")
	}
	QueueReadModelRefreshForProxyIDs([]uint64{2, 3, 6, 9, 0})
	// ID 6 is still unseen in the round, so its new invalidation is covered
	// by that future projection. ID 3 has been popped and must run again.
	for _, want := range [][]uint64{{6, 8}, {2, 3}, {9}} {
		if got := popDirtyReadModelProxyIDs(2); !reflect.DeepEqual(got, want) {
			t.Fatal("new invalidations displaced or escaped the retained round", got, want)
		}
	}
	if readModelProxyWorkPending() || len(popDirtyReadModelProxyIDs(2)) != 0 {
		t.Fatal("complete rounds retained work")
	}
}

func TestReadModelDirtyRoundRetainsOwnerDiscoveryRetry(t *testing.T) {
	isolateReadModelDirtyRound(t)
	QueueReadModelRefreshForProxyIDs([]uint64{4, 3, 2, 1})
	failed := popDirtyReadModelProxyIDs(2)
	requeueDirtyReadModelProxyIDs(failed)
	if got := popDirtyReadModelProxyIDs(2); !reflect.DeepEqual(got, []uint64{3, 4}) {
		t.Fatal("owner retry displaced unseen routes", got)
	}
	if got := popDirtyReadModelProxyIDs(2); !reflect.DeepEqual(got, failed) {
		t.Fatal("owner-discovery failure lost its physical IDs", got, failed)
	}
	if readModelProxyWorkPending() {
		t.Fatal("owner retry did not finish")
	}
}

func TestReadModelDirtyRoundConcurrentInvalidation(t *testing.T) {
	isolateReadModelDirtyRound(t)
	ids := make([]uint64, 10000)
	for i := range ids {
		ids[i] = uint64(i + 1)
	}
	QueueReadModelRefreshForProxyIDs(ids)
	counts := make(map[uint64]int)
	for _, id := range popDirtyReadModelProxyIDs(100) {
		counts[id]++
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := uint64(1); i <= 100; i++ {
			QueueReadModelRefreshForProxyIDs([]uint64{i, 10000 + i})
		}
	}()
	// Pop only the rest of the closed round while the writer adds new work.
	for n := 100; n < len(ids); n += 100 {
		for _, id := range popDirtyReadModelProxyIDs(100) {
			counts[id]++
		}
	}
	<-done
	for readModelProxyWorkPending() {
		for _, id := range popDirtyReadModelProxyIDs(100) {
			counts[id]++
		}
	}
	for id := uint64(1); id <= 10100; id++ {
		want := 1
		if id <= 100 {
			want = 2
		}
		if counts[id] != want {
			t.Fatal("concurrent invalidation lost or duplicated route work", id, counts[id], want)
		}
	}
}

func TestReadModelDirtyRoundMillionRoutes(t *testing.T) {
	isolateReadModelDirtyRound(t)
	const routes = 1000000
	ids := make([]uint64, routes)
	for i := range ids {
		ids[i] = uint64(routes - i)
	}
	start := time.Now()
	QueueReadModelRefreshForProxyIDs(ids)
	var next uint64 = 1
	first := popDirtyReadModelProxyIDs(5000)
	for _, id := range first {
		if id != next {
			t.Fatal("first prefix scattered a route", id, next)
		}
		next++
	}
	// Repeated notifications for every unseen route must coalesce without
	// another round of SQL work. The input is in descending physical order.
	QueueReadModelRefreshForProxyIDs(ids[:routes-5000])
	for readModelProxyWorkPending() {
		batch := popDirtyReadModelProxyIDs(5000)
		if len(batch) == 0 || len(batch) > 5000 {
			t.Fatal("million-route round lost its bounded progress", len(batch))
		}
		for _, id := range batch {
			if id != next {
				t.Fatal("round skipped, repeated or scattered a route", id, next)
			}
			next++
		}
	}
	if next != routes+1 {
		t.Fatal("round lost physical routes", next)
	}
	t.Logf("million physical invalidations, 995000 coalesced notifications and 200 ordered pops: %s", time.Since(start))
}
