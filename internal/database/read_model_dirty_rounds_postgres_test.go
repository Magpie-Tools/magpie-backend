package database

import (
	"context"
	"testing"
	"time"

	"magpie/internal/domain"
)

func TestReadModelDirtyRoundUnseenRouteReadsNewCommittedEvidencePostgres(t *testing.T) {
	db, _ := setupSourceStatsShutdownTest(t)
	var workspace domain.Workspace
	if err := db.First(&workspace).Error; err != nil {
		t.Fatal(err)
	}
	var judge domain.Judge
	if err := db.First(&judge).Error; err != nil {
		t.Fatal(err)
	}
	snapshot, err := LoadCheckerWorkspace(context.Background(), workspace.ID)
	if err != nil {
		t.Fatal(err)
	}
	var added []domain.Proxy
	for _, host := range []string{"198.51.100.78", "198.51.100.79"} {
		proxy := domain.Proxy{IP: host, Port: 3128}
		if err := db.Create(&proxy).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Create(&domain.ManagedProxy{WorkspaceID: workspace.ID, ProxyID: proxy.ID, State: domain.ManagedProxyStateActive}).Error; err != nil {
			t.Fatal(err)
		}
		added = append(added, proxy)
		QueueReadModelRefreshForProxyIDs([]uint64{proxy.ID})
	}
	t.Setenv("READ_MODEL_REFRESH_BATCH_SIZE", "1")
	t.Setenv("READ_MODEL_REFRESH_MAX_BATCHES", "1")
	var expectedPairs int64
	if err := db.Model(&domain.ManagedProxy{}).Count(&expectedPairs).Error; err != nil {
		t.Fatal(err)
	}
	attempts := captureReadModelSubprefixAttempts(t, db)
	flushReadModelDirtyProxyIDs(context.Background(), false)
	// Both added routes remain unseen in the closed round. A new result for
	// the final route must be read by its future statement, without a duplicate
	// round or a captured stale evidence snapshot.
	checkedAt := time.Now().UTC().Truncate(time.Microsecond)
	statistic := domain.ProxyStatistic{
		ProxyID: added[1].ID, JudgeID: judge.ID, ProtocolID: 1, Alive: true,
		CreatedAt:     checkedAt,
		CheckEvidence: []domain.WorkspaceCheckEvidence{{WorkspaceID: workspace.ID, ConfigKey: snapshot.Default.Keys[0], Alive: true}},
	}
	if err := InsertProxyStatistics(context.Background(), []domain.ProxyStatistic{statistic}, 1); err != nil {
		t.Fatal(err)
	}
	readModelDirtyMu.Lock()
	_, duplicated := readModelDirtyProxyIDs[added[1].ID]
	readModelDirtyMu.Unlock()
	if duplicated {
		t.Fatal("an unseen route was queued for unnecessary repeated SQL")
	}
	finishReadModelSubprefixDrain(t)
	if widths := attempts.snapshot(); len(widths) != int(expectedPairs) {
		t.Fatal("round lost a route or repeated a coalesced invalidation", widths)
	}
	var row domain.WorkspaceProxyFilterIndex
	if err := db.First(&row, "workspace_id=? AND proxy_id=?", workspace.ID, added[1].ID).Error; err != nil {
		t.Fatal(err)
	}
	if !row.Alive || !row.AliveHTTP || !row.LatestCheck.Equal(checkedAt) {
		t.Fatal("unseen route did not read the newly committed evidence", row)
	}
}
