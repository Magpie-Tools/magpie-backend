package database

import (
	"context"
	"testing"
	"time"

	"magpie/internal/domain"
)

// A bounded statement should share physical metadata across owners, while the
// retained suffix still updates every owner with its own current evidence.
func TestReadModelSharedRouteLocalityPreservesAllOwnersPostgres(t *testing.T) {
	db, firstID, owners := seedReadModelFanoutTest(t)
	second := domain.Proxy{IP: "198.51.100.77", Port: 3128}
	if err := db.Create(&second).Error; err != nil {
		t.Fatal(err)
	}
	checkedAt := time.Now().UTC().Truncate(time.Microsecond)
	for _, owner := range owners {
		var workspace domain.Workspace
		if err := db.First(&workspace, owner).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Create(&domain.ManagedProxy{WorkspaceID: owner, ProxyID: second.ID, State: domain.ManagedProxyStateActive}).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Create(&domain.ProxyLatestStatistic{WorkspaceID: owner, ConfigKey: workspace.CheckerHTTPKey,
			ProxyID: second.ID, ProtocolID: 1, Alive: true, CheckedAt: checkedAt, StatisticID: 20}).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Where("proxy_id IN ?", []uint64{firstID, second.ID}).Delete(&domain.WorkspaceProxyFilterIndex{}).Error; err != nil {
		t.Fatal(err)
	}
	t.Setenv("READ_MODEL_REFRESH_BATCH_SIZE", "8")
	t.Setenv("READ_MODEL_REFRESH_MAX_BATCHES", "1")
	QueueReadModelRefreshForProxyIDs([]uint64{second.ID})
	flushReadModelDirtyProxyIDs(context.Background(), false)
	var first []domain.WorkspaceProxyFilterIndex
	if err := db.Order("workspace_id").Find(&first).Error; err != nil {
		t.Fatal(err)
	}
	if len(first) != len(owners) {
		t.Fatal("first statement exceeded or failed its owner-pair limit", len(first))
	}
	for i, row := range first {
		if row.ProxyID != firstID || row.WorkspaceID != owners[i] {
			t.Fatal("statement repeated physical metadata instead of grouping shared owners", row)
		}
	}
	if !readModelProxyWorkPending() {
		t.Fatal("bounded statement lost the other route's suffix")
	}
	finishReadModelSubprefixDrain(t)
	var all []domain.WorkspaceProxyFilterIndex
	if err := db.Order("proxy_id,workspace_id").Find(&all).Error; err != nil {
		t.Fatal(err)
	}
	if len(all) != 2*len(owners) {
		t.Fatal("drain lost or duplicated an owner", len(all))
	}
	for i, row := range all {
		wantID := firstID
		if i >= len(owners) {
			wantID = second.ID
		}
		if row.ProxyID != wantID || row.WorkspaceID != owners[i%len(owners)] {
			t.Fatal("drain changed ownership", row)
		}
		if row.ProxyID == second.ID && (!row.Alive || !row.AliveHTTP || !row.LatestCheck.Equal(checkedAt)) {
			t.Fatal("owner did not receive its current evidence", row)
		}
	}
}
