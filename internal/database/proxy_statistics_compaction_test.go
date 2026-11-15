package database

import (
	"context"
	"testing"
	"time"

	"gorm.io/gorm"
	"magpie/internal/domain"
	"magpie/internal/platform/pg"
	testdb "magpie/internal/testkit/db"
)

func TestStatisticLedgerRetirementPreservesReplayProtection(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		name := "sqlite"
		if postgres {
			name = "postgres"
		}
		t.Run(name, func(t *testing.T) {
			var db *gorm.DB
			if postgres {
				db = setupProxyIngestionPostgresTest(t)
			} else {
				db = testdb.OpenSQLite(t)
				old := pg.DB
				pg.DB = db
				t.Cleanup(func() { pg.DB = old })
				if err := db.AutoMigrate(&domain.ProxyStatisticEvent{}, &domain.ProxyStatisticStream{}); err != nil {
					t.Fatal(err)
				}
			}
			stats := []domain.ProxyStatistic{{EventStream: "checks", EventID: "9-1"}, {EventStream: "checks", EventID: "10-0"}, {EventStream: "checks", EventID: "10-2"}}
			if accepted, err := acceptProxyStatisticEvents(db, stats); err != nil || len(accepted) != 3 {
				t.Fatalf("accept: %d %v", len(accepted), err)
			}
			deleted, err := RetireProxyStatisticEvents(context.Background(), "checks", "10-0", 100)
			if err != nil || deleted != 1 {
				t.Fatalf("retire: %d %v", deleted, err)
			}
			stats = append(stats, domain.ProxyStatistic{EventStream: "checks", EventID: "10-1"})
			if accepted, err := acceptProxyStatisticEvents(db, stats); err != nil || len(accepted) != 1 || accepted[0].EventID != "10-1" {
				t.Fatalf("replay: %+v %v", accepted, err)
			}
			if _, err := RetireProxyStatisticEvents(context.Background(), "checks", "9-0", 100); err != nil {
				t.Fatal(err)
			}
			var floor domain.ProxyStatisticStream
			if err := db.First(&floor, "stream = ?", "checks").Error; err != nil || floor.ReplayBefore != ProxyStatisticEventSortID("10-0") {
				t.Fatalf("floor moved back: %+v %v", floor, err)
			}
			// Upgrade a legacy ledger row without losing its replay fence.
			if err := db.Create(&domain.ProxyStatisticEvent{Stream: "checks", EventID: "8-1"}).Error; err != nil {
				t.Fatal(err)
			}
			if _, err := RetireProxyStatisticEvents(context.Background(), "checks", "10-3", 2); err != nil {
				t.Fatal(err)
			}
			if accepted, err := acceptProxyStatisticEvents(db, []domain.ProxyStatistic{{EventStream: "checks", EventID: "8-1"}}); err != nil || len(accepted) != 0 {
				t.Fatalf("legacy replay: %+v %v", accepted, err)
			}
		})
	}
}

func TestStatisticLedgerRetirementWaitsForIngestTransaction(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stat := domain.ProxyStatistic{EventStream: "checks", EventID: "100-0"}
	if _, err := acceptProxyStatisticEvents(db, []domain.ProxyStatistic{stat}); err != nil {
		t.Fatal(err)
	}
	tx := db.WithContext(ctx).Begin()
	defer tx.Rollback()
	if _, err := acceptProxyStatisticEvents(tx, []domain.ProxyStatistic{stat}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := RetireProxyStatisticEvents(ctx, "checks", "101-0", 100); done <- err }()
	select {
	case err := <-done:
		t.Fatalf("retirement bypassed active transaction: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	if err := tx.Commit().Error; err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("retirement did not resume")
	}
	if accepted, err := acceptProxyStatisticEvents(db, []domain.ProxyStatistic{stat}); err != nil || len(accepted) != 0 {
		t.Fatalf("retired replay accepted: %v %v", accepted, err)
	}
}

func TestIngestWaitingForRetirementReadsNewFloor(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	if err := db.Create(&domain.ProxyStatisticStream{Stream: "checks"}).Error; err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tx := db.WithContext(ctx).Begin()
	defer tx.Rollback()
	if err := tx.Model(&domain.ProxyStatisticStream{}).Where("stream = ?", "checks").Update("replay_before", ProxyStatisticEventSortID("101-0")).Error; err != nil {
		t.Fatal(err)
	}
	type result struct {
		count int
		err   error
	}
	done := make(chan result, 1)
	go func() {
		accepted, err := acceptProxyStatisticEvents(db.WithContext(ctx), []domain.ProxyStatistic{{EventStream: "checks", EventID: "100-0"}})
		done <- result{len(accepted), err}
	}()
	select {
	case result := <-done:
		t.Fatalf("ingest bypassed retirement lock: %+v", result)
	case <-time.After(50 * time.Millisecond):
	}
	if err := tx.Commit().Error; err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-done:
		if result.err != nil || result.count != 0 {
			t.Fatalf("stale replay floor: %+v", result)
		}
	case <-ctx.Done():
		t.Fatal("ingest did not resume")
	}
}
