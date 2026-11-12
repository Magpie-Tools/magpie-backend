package database

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"magpie/internal/domain"
	"magpie/internal/platform/schema"

	"github.com/jackc/pgx/v5/pgconn"
	"gorm.io/gorm"
)

// The core calls the read model's projection functions inside its own
// transactions (statistics ingestion, route lifecycle, checker settings and
// source changes). These tests hold what that boundary must keep: the writes
// belong to the caller's transaction, concurrent callers projecting the same
// routes in different orders do not deadlock, and the asynchronous projection
// waits for an open caller instead of overwriting it.

// boundaryFixture is one workspace with a compiled HTTP key, routes it
// manages, and one scrape source holding all of them.
func boundaryFixture(t *testing.T, routes int) (*gorm.DB, domain.Workspace, []uint64, domain.ScrapeSite) {
	t.Helper()
	db := setupProxyIngestionPostgresTest(t)
	if err := schema.EnsureReadModelSchema(db); err != nil {
		t.Fatal(err)
	}
	drainReadModel(t)
	for i, name := range domain.CheckerProtocols {
		if err := db.Create(&domain.Protocol{ID: i + 1, Name: name}).Error; err != nil {
			t.Fatal(err)
		}
	}
	workspace := domain.Workspace{Name: "Read-model boundary", CheckerGeneration: 1, CheckerHTTPKey: "boundary-http"}
	if err := db.Create(&workspace).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`INSERT INTO proxies (id,host,port,country,estimated_type)
SELECT n,'192.0.2.' || n%250,8080,'','' FROM generate_series(1,?::bigint) n`, routes).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`INSERT INTO user_proxies (workspace_id,proxy_id,state) SELECT ?,n,'active' FROM generate_series(1,?::bigint) n`, workspace.ID, routes).Error; err != nil {
		t.Fatal(err)
	}
	site := domain.ScrapeSite{URL: "http://boundary.invalid/source"}
	if err := db.Create(&site).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&domain.WorkspaceScrapeSite{WorkspaceID: workspace.ID, ScrapeSiteID: site.ID, FetchMode: domain.ScrapeFetchHTTP}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`INSERT INTO proxy_scrape_site (proxy_id,scrape_site_id) SELECT n,? FROM generate_series(1,?::bigint) n`, site.ID, routes).Error; err != nil {
		t.Fatal(err)
	}
	ids := make([]uint64, routes)
	for i := range ids {
		ids[i] = uint64(i + 1)
	}
	return db, workspace, ids, site
}

// setBoundaryEvidence replaces the latest HTTP evidence of routes on handle,
// in key order, as ingestion does before it projects them.
func setBoundaryEvidence(t *testing.T, handle *gorm.DB, workspace domain.Workspace, ids []uint64, alive bool, at time.Time) {
	t.Helper()
	if err := handle.Exec(`INSERT INTO proxy_latest_statistics (workspace_id,config_key,proxy_id,protocol_id,alive,statistic_id,checked_at)
SELECT ?,?,id,1,?,id,? FROM unnest(?::bigint[]) id ORDER BY id
ON CONFLICT (workspace_id,config_key,proxy_id,protocol_id) DO UPDATE SET alive=EXCLUDED.alive, checked_at=EXCLUDED.checked_at`,
		workspace.ID, workspace.CheckerHTTPKey, alive, at, pgArray(ids)).Error; err != nil {
		t.Fatal(err)
	}
}

func pgArray(ids []uint64) string {
	out := "{"
	for i, id := range ids {
		if i > 0 {
			out += ","
		}
		out += fmt.Sprint(id)
	}
	return out + "}"
}

func countFilterRows(t *testing.T, handle *gorm.DB, workspaceID uint, alive bool) int64 {
	t.Helper()
	var rows int64
	if err := handle.Model(&domain.WorkspaceProxyFilterIndex{}).Where("workspace_id = ? AND alive = ?", workspaceID, alive).Count(&rows).Error; err != nil {
		t.Fatal(err)
	}
	return rows
}

func countSourceStats(t *testing.T, handle *gorm.DB, workspaceID uint) int64 {
	t.Helper()
	var rows int64
	if err := handle.Model(&domain.WorkspaceScrapeSourceStat{}).Where("workspace_id = ?", workspaceID).Count(&rows).Error; err != nil {
		t.Fatal(err)
	}
	return rows
}

func TestRefreshWorkspaceProxyFilterIndexesForWorkspaceProxyIDsRollsBackWithItsCallerPostgres(t *testing.T) {
	db, workspace, ids, _ := boundaryFixture(t, 3)
	count := func(handle *gorm.DB) int64 {
		var rows int64
		if err := handle.Model(&domain.WorkspaceProxyFilterIndex{}).Where("workspace_id = ?", workspace.ID).Count(&rows).Error; err != nil {
			t.Fatal(err)
		}
		return rows
	}
	rollBackCaller(t, db, func(tx *gorm.DB) error {
		return refreshWorkspaceProxyFilterIndexesForWorkspaceProxyIDs(tx, workspace.ID, ids[:2])
	}, count, count, 2)
}

func TestRefreshWorkspaceScrapeSourceStatsForWorkspaceProxyIDsRollsBackWithItsCallerPostgres(t *testing.T) {
	db, workspace, ids, _ := boundaryFixture(t, 3)
	count := func(handle *gorm.DB) int64 { return countSourceStats(t, handle, workspace.ID) }
	rollBackCaller(t, db, func(tx *gorm.DB) error {
		return refreshWorkspaceScrapeSourceStatsForWorkspaceProxyIDs(tx, workspace.ID, ids[:1])
	}, count, count, 1)
}

func TestRefreshWorkspaceScrapeSourceStatsForSitesRollsBackWithItsCallerPostgres(t *testing.T) {
	db, workspace, _, site := boundaryFixture(t, 3)
	count := func(handle *gorm.DB) int64 { return countSourceStats(t, handle, workspace.ID) }
	rollBackCaller(t, db, func(tx *gorm.DB) error {
		return refreshWorkspaceScrapeSourceStatsForSites(tx, []uint64{site.ID})
	}, count, count, 1)
}

// Two callers project the same routes, listed in opposite orders, inside
// transactions that overlap, as a checker-settings change and an ingestion
// batch can. The projection locks each route's row; the callers serialize on
// those locks and neither is chosen as a deadlock victim.
func TestOverlappingCallerProjectionsDoNotDeadlockPostgres(t *testing.T) {
	const routes, rounds = 4000, 6
	db, workspace, ids, _ := boundaryFixture(t, routes)
	if err := refreshWorkspaceProxyFilterIndexesForWorkspaceProxyIDs(db, workspace.ID, ids); err != nil {
		t.Fatal(err)
	}
	descending := slices.Clone(ids)
	slices.Reverse(descending)
	for round := 0; round < rounds; round++ {
		setBoundaryEvidence(t, db, workspace, ids, round%2 == 0, time.Now().UTC().Truncate(time.Microsecond))
		start := make(chan struct{})
		errs := make(chan error, 2)
		var callers sync.WaitGroup
		for _, order := range [][]uint64{ids, descending} {
			callers.Go(func() {
				<-start
				errs <- db.Transaction(func(tx *gorm.DB) error {
					if err := refreshWorkspaceProxyFilterIndexesForWorkspaceProxyIDs(tx, workspace.ID, order); err != nil {
						return err
					}
					// Hold the projected rows a moment, as a caller does while
					// it finishes the rest of its transaction.
					time.Sleep(20 * time.Millisecond)
					return nil
				})
			})
		}
		close(start)
		callers.Wait()
		close(errs)
		for err := range errs {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "40P01" {
				t.Fatalf("round %d: overlapping projections deadlocked: %v", round, err)
			}
			if err != nil {
				t.Fatalf("round %d: %v", round, err)
			}
		}
	}
	if got := countFilterRows(t, db, workspace.ID, true) + countFilterRows(t, db, workspace.ID, false); got != routes {
		t.Fatalf("%d projected rows after the overlapping rounds, want %d", got, routes)
	}
}

// startDrain runs the read model's shutdown drain in the background and
// returns a channel that is closed when it finishes.
func startDrain() <-chan struct{} {
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		StartReadModelRefreshRoutine(ctx)
	}()
	return drained
}

// openBoundaryCaller begins a caller's transaction that writes new evidence
// for route and projects it, and leaves it open.
func openBoundaryCaller(t *testing.T, db *gorm.DB, workspace domain.Workspace, route uint64, at time.Time) *gorm.DB {
	t.Helper()
	caller := db.Begin()
	if caller.Error != nil {
		t.Fatal(caller.Error)
	}
	t.Cleanup(func() { caller.Rollback() })
	setBoundaryEvidence(t, caller, workspace, []uint64{route}, true, at)
	if err := refreshWorkspaceProxyFilterIndexesForWorkspaceProxyIDs(caller, workspace.ID, []uint64{route}); err != nil {
		t.Fatal(err)
	}
	return caller
}

func boundaryRow(t *testing.T, db *gorm.DB, workspaceID uint, route uint64) domain.WorkspaceProxyFilterIndex {
	t.Helper()
	var row domain.WorkspaceProxyFilterIndex
	if err := db.First(&row, "workspace_id = ? AND proxy_id = ?", workspaceID, route).Error; err != nil {
		t.Fatal(err)
	}
	return row
}

// When the committed projection already matches committed evidence, the
// asynchronous projection writes nothing for the route, so a caller that has
// projected the route in an open transaction neither blocks the drain nor has
// its row overwritten. The caller's commit publishes its row.
func TestAsyncProjectionLeavesAnOpenCallersRowAlonePostgres(t *testing.T) {
	db, workspace, ids, _ := boundaryFixture(t, 2)
	if err := refreshWorkspaceProxyFilterIndexesForWorkspaceProxyIDs(db, workspace.ID, ids); err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC().Truncate(time.Microsecond)
	caller := openBoundaryCaller(t, db, workspace, ids[0], at)
	QueueReadModelRefreshForProxyIDs(ids[:1])
	select {
	case <-startDrain():
	case <-time.After(10 * time.Second):
		t.Fatal("the drain waited for a route it had nothing to write for")
	}
	if row := boundaryRow(t, db, workspace.ID, ids[0]); row.Alive || !row.LatestCheck.IsZero() {
		t.Fatalf("committed row changed under the open caller: %+v", row)
	}
	if err := caller.Commit().Error; err != nil {
		t.Fatal(err)
	}
	if row := boundaryRow(t, db, workspace.ID, ids[0]); !row.Alive || !row.LatestCheck.Equal(at) {
		t.Fatalf("after the caller committed the route shows %+v", row)
	}
}

// When committed evidence has moved past the committed projection, the
// asynchronous projection must write the route. If an open caller holds the
// route's row, the projection waits for it, bounded by its statement timeout
// and retried by the drain, instead of deadlocking or giving up; the drain
// finishes once the caller commits. The caller's own notification after its
// commit, as ingestion sends, then projects the caller's evidence.
func TestAsyncProjectionWaitsForAnOpenCallerItMustOverwritePostgres(t *testing.T) {
	db, workspace, ids, _ := boundaryFixture(t, 2)
	t.Setenv("READ_MODEL_REFRESH_TIMEOUT_SECONDS", "1")
	if err := refreshWorkspaceProxyFilterIndexesForWorkspaceProxyIDs(db, workspace.ID, ids); err != nil {
		t.Fatal(err)
	}
	// Committed evidence the projection has not seen yet.
	earlier := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	setBoundaryEvidence(t, db, workspace, ids[:1], false, earlier)
	QueueReadModelRefreshForProxyIDs(ids[:1])

	at := time.Now().UTC().Truncate(time.Microsecond)
	caller := openBoundaryCaller(t, db, workspace, ids[0], at)
	drained := startDrain()
	select {
	case <-drained:
		t.Fatal("the drain finished while a caller held the row it had to write")
	case <-time.After(2500 * time.Millisecond):
	}
	if err := caller.Commit().Error; err != nil {
		t.Fatal(err)
	}
	select {
	case <-drained:
	case <-time.After(10 * time.Second):
		t.Fatal("the drain did not finish after the caller committed")
	}
	QueueReadModelRefreshForProxyIDs(ids[:1])
	select {
	case <-startDrain():
	case <-time.After(10 * time.Second):
		t.Fatal("the caller's notification was not drained")
	}
	if row := boundaryRow(t, db, workspace.ID, ids[0]); !row.Alive || !row.LatestCheck.Equal(at) {
		t.Fatalf("after the caller's notification the route shows %+v", row)
	}
}
