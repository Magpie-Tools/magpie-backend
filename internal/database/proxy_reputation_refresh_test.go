package database

import (
	"context"
	"sync"
	"testing"
	"time"

	"magpie/internal/domain"
)

func TestReputationRefreshSurvivesNewEventsAndExpiredClaims(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	ctx := context.Background()
	if err := queueProxyReputationRefresh(db, []uint64{11, 12}); err != nil {
		t.Fatal(err)
	}
	jobs, err := ClaimProxyReputationRefreshes(ctx, 2, time.Minute)
	if err != nil || len(jobs) != 2 {
		t.Fatalf("claim: %v %v", jobs, err)
	}
	if err := queueProxyReputationRefresh(db, []uint64{jobs[0].ProxyID}); err != nil {
		t.Fatal(err)
	}
	if err := CompleteProxyReputationRefreshes(ctx, jobs, true); err != nil {
		t.Fatal(err)
	}
	remaining, err := ClaimProxyReputationRefreshes(ctx, 2, time.Minute)
	if err != nil || len(remaining) != 1 || remaining[0].Version != 2 {
		t.Fatalf("new event was lost: %v %v", remaining, err)
	}
	if err := db.Model(&domain.ProxyReputationRefresh{}).Where("proxy_id = ?", remaining[0].ProxyID).
		Update("lease_until", time.Now().UTC().Add(-time.Second)).Error; err != nil {
		t.Fatal(err)
	}
	claimedAgain, err := ClaimProxyReputationRefreshes(ctx, 2, time.Minute)
	if err != nil || len(claimedAgain) != 1 {
		t.Fatalf("crashed worker's request was not recovered: %v %v", claimedAgain, err)
	}
	if err := CompleteProxyReputationRefreshes(ctx, remaining, true); err != nil {
		t.Fatal(err)
	}
	count, oldest, err := ProxyReputationRefreshBacklog(ctx)
	if err != nil || count != 1 || oldest.IsZero() {
		t.Fatalf("stale completion erased a newer claim: count=%d oldest=%v err=%v", count, oldest, err)
	}
	if err := CompleteProxyReputationRefreshes(ctx, claimedAgain, true); err != nil {
		t.Fatal(err)
	}
	count, _, err = ProxyReputationRefreshBacklog(ctx)
	if err != nil || count != 0 {
		t.Fatalf("completed job remains: count=%d error=%v", count, err)
	}
}

func TestReputationRefreshClaimsDisjointWorkPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	ids := make([]uint64, 100)
	for i := range ids {
		ids[i] = uint64(i + 1)
	}
	if err := queueProxyReputationRefresh(db, ids); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	results := make(chan []domain.ProxyReputationRefresh, 4)
	errors := make(chan error, 4)
	for i := 0; i < 4; i++ {
		wg.Go(func() {
			jobs, err := ClaimProxyReputationRefreshes(context.Background(), 25, time.Minute)
			results <- jobs
			errors <- err
		})
	}
	wg.Wait()
	close(results)
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	seen := make(map[uint64]bool)
	for jobs := range results {
		for _, job := range jobs {
			if seen[job.ProxyID] {
				t.Fatalf("concurrent workers claimed route %d", job.ProxyID)
			}
			seen[job.ProxyID] = true
		}
	}
	if len(seen) != len(ids) {
		t.Fatalf("claimed %d routes, want %d", len(seen), len(ids))
	}
}
