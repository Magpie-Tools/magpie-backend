package database

import (
	"reflect"
	"testing"
	"time"

	"gorm.io/gorm"
	"magpie/internal/domain"
)

func TestAlertBatchedWorkspaceCountsPreserveEligibility(t *testing.T) {
	t.Run("SQLite", func(t *testing.T) { testAlertWorkspaceCounts(t, setupRotatingProxyTestDB(t)) })
	t.Run("Postgres", func(t *testing.T) { testAlertWorkspaceCounts(t, setupProxyIngestionPostgresTest(t)) })
}

func testAlertWorkspaceCounts(t *testing.T, db *gorm.DB) {
	t.Helper()
	alertMust(t, db.Create(&[]domain.Protocol{{ID: 1, Name: "http"}, {ID: 2, Name: "https"}}).Error)
	workspaces := []domain.Workspace{
		{Name: "A", CheckerGeneration: 1, CheckerHTTPKey: "a-http", CheckerHTTPSKey: "a-https"},
		{Name: "B", CheckerGeneration: 1, CheckerHTTPKey: "b-http", CheckerHTTPSKey: "b-https"},
		{Name: "Legacy"},
		{Name: "Empty", CheckerGeneration: 1},
		{Name: "Paused", CheckerGeneration: 1, CheckerHTTPKey: "paused-http"},
		{Name: "Dirty", CheckerGeneration: 1, CheckerDirty: true, CheckerHTTPKey: "dirty-http"},
		{Name: "Stale", CheckerGeneration: 1, CheckerHTTPKey: "stale-http"},
		{Name: "Unchecked", CheckerGeneration: 1, CheckerHTTPKey: "unchecked-http"},
	}
	alertMust(t, db.Create(&workspaces).Error)
	proxies := []domain.Proxy{{IP: "192.0.2.1", Port: 8080}, {IP: "192.0.2.2", Port: 8080}, {IP: "192.0.2.3", Port: 8080}, {IP: "192.0.2.4", Port: 8080}, {IP: "192.0.2.5", Port: 8080}}
	alertMust(t, db.Create(&proxies).Error)
	for _, wi := range []int{0, 1, 2} {
		for _, pi := range []int{0, 1, 2} {
			alertMust(t, db.Create(&domain.ManagedProxy{WorkspaceID: workspaces[wi].ID, ProxyID: proxies[pi].ID, State: domain.ManagedProxyStateActive}).Error)
		}
	}
	for _, wi := range []int{4, 5, 6, 7} {
		state := domain.ManagedProxyStateActive
		if wi == 4 {
			state = domain.ManagedProxyStatePaused
		}
		alertMust(t, db.Create(&domain.ManagedProxy{WorkspaceID: workspaces[wi].ID, ProxyID: proxies[0].ID, State: state}).Error)
	}
	alertMust(t, db.Create(&domain.ManagedProxy{WorkspaceID: workspaces[0].ID, ProxyID: proxies[4].ID, State: domain.ManagedProxyStatePaused}).Error)
	alertMust(t, db.Create(&domain.ProxyCheckerPlan{WorkspaceID: workspaces[0].ID, ProxyID: proxies[1].ID, ProtocolID: 1, ConfigKey: "a-override", Transport: "tcp"}).Error)
	now := time.Now().UTC().Truncate(time.Microsecond)
	add := func(wi, pi, protocol int, key string, alive bool, offset time.Duration, transport string) {
		t.Helper()
		workspaceID := uint(0)
		if wi >= 0 {
			workspaceID = workspaces[wi].ID
		}
		alertMust(t, db.Create(&domain.ProxyLatestStatistic{WorkspaceID: workspaceID, ConfigKey: key, ProxyID: proxies[pi].ID, ProtocolID: protocol, Alive: alive, CheckedAt: now.Add(offset), TransportProtocol: transport}).Error)
	}
	add(0, 0, 1, "a-http", false, 0, "tcp")
	add(0, 0, 2, "a-https", true, -16*time.Minute, "tcp")
	add(0, 1, 1, "a-http", true, 0, "tcp") // Default superseded by a sparse override.
	add(0, 1, 1, "a-override", false, 0, "tcp")
	add(0, 1, 1, "obsolete", true, 0, "tcp")
	add(0, 2, 1, "a-http", true, 0, "udp") // Workspace eligibility includes UDP.
	add(0, 3, 1, "a-http", true, 0, "tcp") // No managed ownership.
	add(0, 4, 1, "a-http", true, 0, "tcp") // Paused ownership.
	add(1, 0, 1, "b-http", false, 0, "tcp")
	add(1, 0, 2, "b-https", false, time.Hour, "tcp") // Future evidence is not a fresh sample.
	add(1, 1, 1, "a-override", true, 0, "tcp")       // Another workspace's key cannot qualify.
	add(1, 2, 1, "b-http", false, 0, "tcp")
	add(-1, 0, 1, "", false, 0, "")
	add(-1, 1, 1, "", true, 0, "")
	add(-1, 2, 1, "", false, -16*time.Minute, "")
	add(4, 0, 1, "paused-http", true, 0, "tcp")
	add(5, 0, 1, "dirty-http", true, 0, "tcp")
	add(6, 0, 1, "stale-http", true, -16*time.Minute, "tcp")
	// Legacy eligibility comes from the legacy overall projection, rather than
	// from the protocol verdict or another workspace's attributed evidence.
	alertMust(t, db.Create(&[]domain.ProxyOverallStatus{{ProxyID: proxies[0].ID, OverallAlive: true}, {ProxyID: proxies[1].ID, OverallAlive: false}, {ProxyID: proxies[2].ID, OverallAlive: false}}).Error)
	ids := make([]uint, len(workspaces))
	for i, workspace := range workspaces {
		ids[i] = workspace.ID
	}
	batched, err := loadAlertWorkspaceRouteObservations(db, ids, now)
	alertMust(t, err)
	want := []AlertObservation{
		{Value: alertFloat(2), Samples: 3},
		{Value: alertFloat(0), Samples: 2},
		{Value: alertFloat(1), Samples: 2},
		{Value: alertFloat(0)},
		{Value: alertFloat(0)},
		{UnknownReason: "No current check evidence in the last 15 minutes"},
		{UnknownReason: "No current check evidence in the last 15 minutes"},
		{UnknownReason: "No current check evidence in the last 15 minutes"},
	}
	for i, workspace := range workspaces {
		individual := observeUsableAlertRoutes(db, workspace, nil, now)
		if !reflect.DeepEqual(batched[workspace.ID], want[i]) || !reflect.DeepEqual(individual, want[i]) {
			t.Fatalf("workspace %s: batch=%#v individual=%#v want=%#v", workspace.Name, batched[workspace.ID], individual, want[i])
		}
	}
}
