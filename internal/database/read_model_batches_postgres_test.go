package database

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"magpie/internal/domain"

	"gorm.io/gorm"
)

func seedReadModelFanoutTest(t *testing.T) (*gorm.DB, uint64, []uint) {
	t.Helper()
	db, _ := setupSourceStatsShutdownTest(t)
	var proxy domain.Proxy
	if err := db.First(&proxy).Error; err != nil {
		t.Fatal(err)
	}
	var original domain.Workspace
	if err := db.First(&original).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Where("workspace_id <> ? AND proxy_id = ?", original.ID, proxy.ID).Delete(&domain.ManagedProxy{}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Where("workspace_id <> ? AND proxy_id = ?", original.ID, proxy.ID).Delete(&domain.WorkspaceProxyFilterIndex{}).Error; err != nil {
		t.Fatal(err)
	}
	owners := []uint{original.ID}
	for i := 0; i < 7; i++ {
		owner := domain.Workspace{Name: "Shared projection", CheckerGeneration: 1, CheckerHTTPKey: "shared-http"}
		if err := db.Create(&owner).Error; err != nil {
			t.Fatal(err)
		}
		owners = append(owners, owner.ID)
		if err := db.Create(&domain.ManagedProxy{WorkspaceID: owner.ID, ProxyID: proxy.ID, State: domain.ManagedProxyStateActive}).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Create(&domain.ProxyLatestStatistic{WorkspaceID: owner.ID, ConfigKey: "shared-http", ProxyID: proxy.ID, ProtocolID: 1, Alive: true, CheckedAt: time.Now().UTC(), StatisticID: 10}).Error; err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("READ_MODEL_REFRESH_BATCH_SIZE", "2")
	t.Setenv("READ_MODEL_REFRESH_MAX_BATCHES", "1")
	return db, proxy.ID, owners
}

func TestReadModelPairPrefixesPreserveProgressAndNewInvalidationsPostgres(t *testing.T) {
	db, proxyID, owners := seedReadModelFanoutTest(t)
	flushReadModelDirtyProxyIDs(context.Background(), false)
	var count int64
	if err := db.Model(&domain.WorkspaceProxyFilterIndex{}).Where("proxy_id=?", proxyID).Count(&count).Error; err != nil || count != 2 {
		t.Fatal("one physical route did not respect the two-pair budget", count, err)
	}
	readModelDirtyMu.Lock()
	batch := readModelPendingBatch
	if batch == nil || batch.next != 2 || len(batch.pairs) != 8 {
		readModelDirtyMu.Unlock()
		t.Fatal("expanded suffix was not retained", batch)
	}
	readModelDirtyMu.Unlock()
	var first domain.WorkspaceProxyFilterIndex
	if err := db.First(&first, "workspace_id=? AND proxy_id=?", owners[0], proxyID).Error; err != nil {
		t.Fatal(err)
	}
	fail := true
	if err := db.Callback().Raw().Before("gorm:raw").Register("test:fail_pair_suffix", func(tx *gorm.DB) {
		if fail && strings.Contains(tx.Statement.SQL.String(), "INSERT INTO user_proxy_filter_indexes") {
			tx.AddError(errors.New("injected suffix failure"))
		}
	}); err != nil {
		t.Fatal(err)
	}
	flushReadModelDirtyProxyIDs(context.Background(), false)
	if !readModelProxyWorkPending() {
		t.Fatal("failed pair suffix falsely drained")
	}
	// An owner removed while a suffix is pending must not be resurrected by
	// the captured pair. A newly associated owner arrives via a new invalidation.
	if err := db.Where("workspace_id=? AND proxy_id=?", owners[7], proxyID).Delete(&domain.ManagedProxy{}).Error; err != nil {
		t.Fatal(err)
	}
	newOwner := domain.Workspace{Name: "New owner", CheckerGeneration: 1, CheckerHTTPKey: "new-http"}
	if err := db.Create(&newOwner).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&domain.ManagedProxy{WorkspaceID: newOwner.ID, ProxyID: proxyID, State: domain.ManagedProxyStatePaused, PauseReason: domain.ManagedProxyPauseReasonManual}).Error; err != nil {
		t.Fatal(err)
	}
	QueueReadModelRefreshForProxyIDs([]uint64{proxyID})
	fail = false
	for i := 0; i < 3; i++ {
		flushReadModelDirtyProxyIDs(context.Background(), false)
	}
	var unchanged domain.WorkspaceProxyFilterIndex
	if err := db.First(&unchanged, "workspace_id=? AND proxy_id=?", owners[0], proxyID).Error; err != nil || !first.UpdatedAt.Equal(unchanged.UpdatedAt) {
		t.Fatal("retry rewrote a committed prefix", first.UpdatedAt, unchanged.UpdatedAt, err)
	}
	if !readModelProxyWorkPending() {
		t.Fatal("invalidation received during a suffix was lost")
	}
	drainReadModelRefreshes(context.Background())
	if readModelProxyWorkPending() {
		t.Fatal("complete pair/source drain retained work")
	}
	if err := db.Model(&domain.WorkspaceProxyFilterIndex{}).Where("workspace_id=? AND proxy_id=?", owners[7], proxyID).Count(&count).Error; err != nil || count != 0 {
		t.Fatal("removed ownership was recreated", count, err)
	}
	var added domain.WorkspaceProxyFilterIndex
	if err := db.First(&added, "workspace_id=? AND proxy_id=?", newOwner.ID, proxyID).Error; err != nil || added.State != domain.ManagedProxyStatePaused || added.PauseReason != domain.ManagedProxyPauseReasonManual {
		t.Fatal("new owner or lifecycle state was lost", added, err)
	}
}

func TestReadModelPairSourceDiscoveryFailureRetainsCompletedPrefixPostgres(t *testing.T) {
	db, proxyID, _ := seedReadModelFanoutTest(t)
	t.Setenv("READ_MODEL_REFRESH_BATCH_SIZE", "20")
	if err := db.Exec("ALTER TABLE proxy_scrape_site RENAME TO hidden_proxy_scrape_site").Error; err != nil {
		t.Fatal(err)
	}
	flushReadModelDirtyProxyIDs(context.Background(), false)
	readModelDirtyMu.Lock()
	batch := readModelPendingBatch
	complete := batch != nil && batch.next == len(batch.pairs)
	readModelDirtyMu.Unlock()
	if !complete || !readModelProxyWorkPending() {
		t.Fatal("source failure lost completion or falsely drained", batch)
	}
	if err := db.Exec("ALTER TABLE hidden_proxy_scrape_site RENAME TO proxy_scrape_site").Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Callback().Raw().Before("gorm:raw").Register("test:completed_projection_must_not_repeat", func(tx *gorm.DB) {
		if strings.Contains(tx.Statement.SQL.String(), "INSERT INTO user_proxy_filter_indexes") {
			tx.AddError(errors.New("completed projection repeated"))
		}
	}); err != nil {
		t.Fatal(err)
	}
	flushReadModelDirtyProxyIDs(context.Background(), false)
	if readModelProxyWorkPending() {
		t.Fatal("source-only retry did not finish", proxyID)
	}
}

func TestReadModelExplicitRebuildCanceledPartialRetryPostgres(t *testing.T) {
	db, proxyID, owners := seedReadModelFanoutTest(t)
	// Simulate loss of dirty memory after a failed process stop. Existing stale
	// projections are nonempty, so the migration backfill would not repair them.
	readModelDirtyMu.Lock()
	readModelDirtyProxyIDs = make(map[uint64]struct{})
	readModelDirtyRound = nil
	readModelPendingBatch = nil
	readModelDirtyMu.Unlock()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := RefreshReadModelIndexes(canceled); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled repair reported completion", err)
	}
	calls := 0
	if err := db.Callback().Raw().Before("gorm:raw").Register("test:partial_explicit_repair", func(tx *gorm.DB) {
		if strings.Contains(tx.Statement.SQL.String(), "INSERT INTO user_proxy_filter_indexes") {
			calls++
			if calls == 2 {
				tx.AddError(errors.New("injected partial repair failure"))
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := RefreshReadModelIndexes(context.Background()); err == nil {
		t.Fatal("partial repair reported completion")
	}
	if err := db.Callback().Raw().Remove("test:partial_explicit_repair"); err != nil {
		t.Fatal(err)
	}
	if err := RefreshReadModelIndexes(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, owner := range owners {
		var row domain.WorkspaceProxyFilterIndex
		if err := db.First(&row, "workspace_id=? AND proxy_id=?", owner, proxyID).Error; err != nil || !row.Alive || row.LatestCheck.IsZero() {
			t.Fatal("repair did not restore durable current evidence", owner, row, err)
		}
	}
	// A metadata change and a nullable aggregate must still replace a row.
	if err := db.Model(&domain.Proxy{}).Where("id=?", proxyID).Update("country", "DE").Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&domain.WorkspaceProxyFilterIndex{}).Where("proxy_id=?", proxyID).Update("health_https", 100).Error; err != nil {
		t.Fatal(err)
	}
	if err := RefreshReadModelIndexes(context.Background()); err != nil {
		t.Fatal(err)
	}
	var row domain.WorkspaceProxyFilterIndex
	if err := db.First(&row, "workspace_id=? AND proxy_id=?", owners[0], proxyID).Error; err != nil || row.Country != "DE" || row.HealthHTTPS != nil {
		t.Fatal("no-op guard hid a changed metadata/nullable value", row, err)
	}
}

func TestReadModelConcurrentFlushesDoNotRepeatPrefixPostgres(t *testing.T) {
	db, _, _ := seedReadModelFanoutTest(t)
	queries := 0
	if err := db.Callback().Raw().Before("gorm:raw").Register("test:count_serialized_prefixes", func(tx *gorm.DB) {
		if strings.Contains(tx.Statement.SQL.String(), "INSERT INTO user_proxy_filter_indexes") {
			queries++
		}
	}); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); flushReadModelDirtyProxyIDs(context.Background(), false) }()
	}
	wg.Wait()
	if queries != 4 || readModelProxyWorkPending() {
		t.Fatal("concurrent flushes repeated/lost a pair prefix", queries)
	}
}

func TestReadModelExplicitRebuildRejectsMissingProjectionPostgres(t *testing.T) {
	db, _, _ := seedReadModelFanoutTest(t)
	if err := db.Exec("DROP TABLE user_proxy_filter_indexes").Error; err != nil {
		t.Fatal(err)
	}
	if err := RefreshReadModelIndexes(context.Background()); err == nil || !strings.Contains(err.Error(), "required repair table") {
		t.Fatal("missing projection was reported repaired", err)
	}
}

func TestReadModelProjectionPairBudget(t *testing.T) {
	for _, tc := range []struct {
		setting string
		want    int
	}{{"0", 5000}, {"17", 17}, {"70000", 5000}} {
		t.Run(tc.setting, func(t *testing.T) {
			t.Setenv("READ_MODEL_REFRESH_BATCH_SIZE", tc.setting)
			if got := readModelProjectionPairBudget(); got != tc.want {
				t.Fatal("unexpected pair/parameter budget", got, tc.want)
			}
		})
	}
}

func TestReadModelBackgroundOperationCountsPostgres(t *testing.T) {
	db, proxyID, _ := seedReadModelFanoutTest(t)
	queries := &checkerScaleQueryLogger{Interface: db.Logger}
	DB = db.Session(&gorm.Session{Logger: queries})
	QueueReadModelRefreshForProxyIDs([]uint64{proxyID})
	if len(queries.queries) != 0 {
		t.Fatal("queue notification added a direct database operation", queries.queries)
	}
	for readModelProxyWorkPending() {
		flushReadModelDirtyProxyIDs(context.Background(), false)
	}
	counts := map[string]int{"total": len(queries.queries)}
	for _, sql := range queries.queries {
		switch {
		case strings.Contains(sql, "INSERT INTO user_proxy_filter_indexes"):
			counts["projection_upserts"]++
		case strings.Contains(sql, "information_schema.tables"):
			counts["table_checks"]++
		case strings.Contains(sql, "proxy_scrape_site pss"):
			counts["source_pair_discovery"]++
		case strings.Contains(sql, "FROM \"user_proxies\""):
			counts["owner_pair_discovery"]++
		default:
			counts["other"]++
			t.Logf("other asynchronous statement: %s", sql)
		}
	}
	if counts["projection_upserts"] != 4 || counts["owner_pair_discovery"] != 1 {
		t.Fatal("one route/eight owners did not produce four bounded pair prefixes", counts)
	}
	data, _ := json.MarshalIndent(map[string]any{"physical_routes": 1, "owners": 8, "pair_budget": 2, "committed_pair_prefixes": 4, "direct_queue_notification_database_queries": 0, "asynchronous_statements": counts}, "", "  ")
	writeReadModelEvidence(t, "background-operation-counts.json", data)
	t.Logf("background operation counts: %s", data)
}

func TestReadModelExplicitRebuildRemovesCommittedOrphansPostgres(t *testing.T) {
	db, proxyID, owners := seedReadModelFanoutTest(t)
	if err := RefreshReadModelIndexes(context.Background()); err != nil {
		t.Fatal(err)
	}
	var source domain.WorkspaceScrapeSite
	if err := db.Where("workspace_id=?", owners[0]).Order("scrape_site_id").First(&source).Error; err != nil {
		t.Fatal(err)
	}
	// Reproduce a crash after authoritative removal but before the separate
	// derived-row delete. The orphan was already projected and publicly visible.
	if err := db.Where("workspace_id=? AND proxy_id=?", owners[0], proxyID).Delete(&domain.ManagedProxy{}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Where("workspace_id=? AND scrape_site_id=?", source.WorkspaceID, source.ScrapeSiteID).Delete(&domain.WorkspaceScrapeSite{}).Error; err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		"SELECT count(*) FROM user_proxy_filter_indexes WHERE workspace_id=? AND proxy_id=?",
		"SELECT count(*) FROM user_scrape_source_stats WHERE workspace_id=? AND scrape_site_id=?",
	} {
		var count int64
		id := proxyID
		if strings.Contains(query, "scrape") {
			id = source.ScrapeSiteID
		}
		if err := db.Raw(query, owners[0], id).Scan(&count).Error; err != nil || count != 1 {
			t.Fatal("fixture did not retain a committed orphan", count, err)
		}
	}
	fingerprint := func() string {
		var value string
		if err := db.Raw(`SELECT jsonb_build_object(
'proxies',(SELECT jsonb_agg(to_jsonb(p) ORDER BY id) FROM proxies p),
'ownership',(SELECT jsonb_agg(to_jsonb(up) ORDER BY workspace_id,proxy_id) FROM user_proxies up),
'sources',(SELECT jsonb_agg(to_jsonb(uss) ORDER BY workspace_id,scrape_site_id) FROM user_scrape_site uss),
'latest',(SELECT jsonb_agg(to_jsonb(pls) ORDER BY workspace_id,proxy_id,config_key,protocol_id) FROM proxy_latest_statistics pls),
'workspaces',(SELECT jsonb_agg(to_jsonb(w) ORDER BY id) FROM workspaces w))::text`).Scan(&value).Error; err != nil {
			t.Fatal(err)
		}
		return value
	}
	before := fingerprint()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := db.Callback().Raw().After("gorm:raw").Register("test:cancel_after_orphan_prefix", func(tx *gorm.DB) {
		if strings.Contains(tx.Statement.SQL.String(), "DELETE FROM user_proxy_filter_indexes") {
			cancel()
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := RefreshReadModelIndexes(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled orphan repair reported complete", err)
	}
	if err := db.Callback().Raw().Remove("test:cancel_after_orphan_prefix"); err != nil {
		t.Fatal(err)
	}
	if err := RefreshReadModelIndexes(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		"SELECT count(*) FROM user_proxy_filter_indexes f WHERE NOT EXISTS (SELECT 1 FROM user_proxies up WHERE up.workspace_id=f.workspace_id AND up.proxy_id=f.proxy_id)",
		"SELECT count(*) FROM user_scrape_source_stats s WHERE NOT EXISTS (SELECT 1 FROM user_scrape_site uss WHERE uss.workspace_id=s.workspace_id AND uss.scrape_site_id=s.scrape_site_id)",
	} {
		var count int64
		if err := db.Raw(query).Scan(&count).Error; err != nil || count != 0 {
			t.Fatal("repair left a derived orphan", count, err)
		}
	}
	var count int64
	if err := db.Model(&domain.WorkspaceProxyFilterIndex{}).Where("proxy_id=?", proxyID).Count(&count).Error; err != nil || count != 7 {
		t.Fatal("repair removed valid owner projections", count, err)
	}
	if before != fingerprint() {
		t.Fatal("derived repair changed durable inventory, credentials, checker keys or evidence")
	}
}
