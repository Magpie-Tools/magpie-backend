package database

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"magpie/internal/api/dto"
	"magpie/internal/domain"
)

type bulkDeleteQueryLog struct {
	logger.Interface
	counts   map[string]int
	duration map[string]time.Duration
}

func (l *bulkDeleteQueryLog) Trace(_ context.Context, begin time.Time, fc func() (string, int64), _ error) {
	sql, _ := fc()
	category := "other"
	switch {
	case strings.Contains(sql, "INSERT INTO user_scrape_source_stats"):
		category = "source health"
	case strings.Contains(sql, "WITH affected AS MATERIALIZED"):
		category = "ownership delete"
	case strings.Contains(sql, `DELETE FROM "user_proxy_filter_indexes"`):
		category = "filter delete"
	case strings.HasPrefix(sql, "SELECT DISTINCT proxy_id"):
		category = "orphan IDs"
	case strings.HasPrefix(sql, `SELECT * FROM "proxies"`):
		category = "orphan routes"
	}
	l.counts[category]++
	l.duration[category] += time.Since(begin)
}

func seedBulkDeletePostgres(t *testing.T, routes int) (*gorm.DB, domain.Workspace, domain.Workspace) {
	t.Helper()
	db := setupProxyIngestionPostgresTest(t)
	a, b, _, _ := seedCheckerEvidenceTest(t, db)
	a.CheckerConfig.Rules = make([]dto.TagCheckerRule, 4)
	for i := range a.CheckerConfig.Rules {
		tag := seedCheckerTag(t, db, a.ID, fmt.Sprintf("Checker %d", i))
		timeout := uint16(2000)
		a.CheckerConfig.Rules[i] = dto.TagCheckerRule{TagID: tag.ID, Mode: "replace", Protocols: []string{"http"}, Timeout: &timeout}
	}
	if err := db.Model(&a).Updates(map[string]any{"checker_config": a.CheckerConfig, "checker_generation": 1, "checker_http_key": "bulk-default"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&b).Updates(map[string]any{"checker_generation": 1, "checker_http_key": "other-default"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := ensureCheckerSettingsSchema(db); err != nil {
		t.Fatal(err)
	}
	if err := ensureReadModelSchema(db); err != nil {
		t.Fatal(err)
	}
	for _, statement := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO proxies (id,host,port,country,estimated_type,hash)
SELECT n,'192.0.2.2',8080,'','',decode(md5(n::text),'hex') FROM generate_series(2,?) n`, []any{routes}},
		{`INSERT INTO user_proxies (workspace_id,proxy_id,state)
SELECT ?,n,'active' FROM generate_series(2,?) n`, []any{a.ID, routes}},
		// Foreign-key plans must see the populated ownership table.
		{`ANALYZE user_proxies`, nil},
		{`INSERT INTO proxy_checker_plans (workspace_id,proxy_id,protocol_id,config_key,transport)
SELECT workspace_id,proxy_id,1,'bulk-override','tcp' FROM user_proxies WHERE workspace_id=? AND proxy_id%4<>0`, []any{a.ID}},
		{`INSERT INTO scrape_sites (id,url)
SELECT n,'https://source-'||n||'.example.test/proxies' FROM generate_series(1,41) n`, nil},
		{`INSERT INTO user_scrape_site (workspace_id,scrape_site_id,fetch_mode)
SELECT ?,n,'http' FROM generate_series(1,41) n`, []any{a.ID}},
		{`INSERT INTO user_scrape_site (workspace_id,scrape_site_id,fetch_mode) VALUES (?,1,'http')`, []any{b.ID}},
		{`INSERT INTO proxy_scrape_site (proxy_id,scrape_site_id)
SELECT n,1 FROM generate_series(1,?) n UNION ALL
SELECT n,2+((n/2)%40) FROM generate_series(1,?) n WHERE n%2=0`, []any{routes, routes}},
		{`INSERT INTO proxy_latest_statistics (workspace_id,config_key,proxy_id,protocol_id,alive,statistic_id,checked_at)
SELECT workspace_id,CASE WHEN workspace_id=? THEN CASE WHEN proxy_id%4<>0 THEN 'bulk-override' ELSE 'bulk-default' END ELSE 'other-default' END,
proxy_id,1,proxy_id%2=0,proxy_id,CURRENT_TIMESTAMP FROM user_proxies`, []any{a.ID}},
		{`INSERT INTO user_proxy_filter_indexes (workspace_id,proxy_id,host,port,state,alive)
SELECT workspace_id,proxy_id,'192.0.2.2',8080,'active',proxy_id%2=0 FROM user_proxies`, nil},
	} {
		if err := db.Exec(statement.sql, statement.args...).Error; err != nil {
			t.Fatal(err)
		}
	}
	for _, rule := range a.CheckerConfig.Rules {
		if err := db.Exec(`INSERT INTO proxy_tag_assignments (workspace_id,proxy_id,proxy_tag_id)
SELECT workspace_id,proxy_id,? FROM user_proxies WHERE workspace_id=? AND proxy_id%4<>0 AND (proxy_id/4)%4=?`, rule.TagID, a.ID, (rule.TagID-1)%4).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Exec("ANALYZE").Error; err != nil {
		t.Fatal(err)
	}
	if err := refreshAllUserScrapeSourceStats(db); err != nil {
		t.Fatal(err)
	}
	if err := refreshWorkspaceUsageActiveRoutes(db, a.ID); err != nil {
		t.Fatal(err)
	}
	return db, a, b
}

// Increase MAGPIE_TEST_BULK_DELETE_ROUTES to 70000 to cover the reported scale
// and the extended-protocol limit, using an isolated schema and real cascades.
func TestBulkProxyDeletionPostgres(t *testing.T) {
	routes := 2*deleteChunkSize + 1
	if value := os.Getenv("MAGPIE_TEST_BULK_DELETE_ROUTES"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < routes {
			t.Fatalf("invalid bulk deletion size %q", value)
		}
		routes = parsed
	}
	db, a, b := seedBulkDeletePostgres(t, routes)
	queries := &bulkDeleteQueryLog{Interface: db.Logger, counts: make(map[string]int), duration: make(map[string]time.Duration)}
	DB = db.Session(&gorm.Session{Logger: queries})
	started := time.Now()
	deleted, orphans, err := DeleteProxiesWithSettings(a.ID, dto.DeleteSettings{Scope: "all"})
	t.Logf("routes=%d sources=41 elapsed=%s statements=%v SQL_time=%v", routes, time.Since(started), queries.counts, queries.duration)
	if err != nil || deleted != int64(routes) || len(orphans) != routes-1 {
		t.Fatalf("deleted=%d orphans=%d want=%d error=%v", deleted, len(orphans), routes, err)
	}
	if queries.counts["source health"] != 1 {
		t.Fatalf("recalculated the same sources %d times", queries.counts["source health"])
	}
	seen := make(map[uint64]bool, len(orphans))
	for _, orphan := range orphans {
		if orphan.ID == 1 || seen[orphan.ID] || len(orphan.Hash) == 0 {
			t.Fatalf("invalid orphan ID=%d", orphan.ID)
		}
		seen[orphan.ID] = true
	}
	assertBulkDeleteState(t, db, a.ID, b.ID, 0, int64(routes-routes/4))
}

func TestBulkProxyDeletionRefreshesCommittedBatchesOnErrorPostgres(t *testing.T) {
	db, a, b := seedBulkDeletePostgres(t, deleteChunkSize+1)
	if err := db.Exec(fmt.Sprintf(`CREATE FUNCTION reject_delete_batch() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN IF OLD.workspace_id=%d AND OLD.proxy_id>%d THEN RAISE EXCEPTION 'rejected test batch'; END IF; RETURN OLD; END $$;
CREATE TRIGGER reject_delete_batch BEFORE DELETE ON user_proxies FOR EACH ROW EXECUTE FUNCTION reject_delete_batch()`, a.ID, deleteChunkSize)).Error; err != nil {
		t.Fatal(err)
	}
	ids := make([]int, deleteChunkSize+1)
	for i := range ids {
		ids[i] = i + 1
	}
	deleted, _, err := DeleteProxyRelation(a.ID, ids)
	if err == nil || !strings.Contains(err.Error(), "rejected test batch") || deleted != deleteChunkSize {
		t.Fatalf("deleted=%d error=%v", deleted, err)
	}
	assertBulkDeleteState(t, db, a.ID, b.ID, 1, int64(deleteChunkSize-deleteChunkSize/4))
}

func assertBulkDeleteState(t *testing.T, db *gorm.DB, workspaceID, otherWorkspaceID uint, remaining, tombstones int64) {
	t.Helper()
	for _, model := range []any{&domain.ManagedProxy{}, &domain.WorkspaceProxyFilterIndex{}} {
		var count int64
		if err := db.Model(model).Where("workspace_id=?", workspaceID).Count(&count).Error; err != nil || count != remaining {
			t.Fatalf("%T remaining=%d want=%d error=%v", model, count, remaining, err)
		}
		if err := db.Model(model).Where("workspace_id=?", otherWorkspaceID).Count(&count).Error; err != nil || count != 1 {
			t.Fatalf("changed other workspace %T count=%d error=%v", model, count, err)
		}
	}
	for _, model := range []any{&domain.ProxyTagAssignment{}, &domain.ProxyCheckerPlan{}} {
		var count int64
		if err := db.Model(model).Where("workspace_id=?", workspaceID).Count(&count).Error; err != nil || count != remaining {
			t.Fatalf("%T remaining=%d want=%d error=%v", model, count, remaining, err)
		}
	}
	var deletedChanges int64
	if err := db.Model(&domain.CheckerProxyChange{}).Where("workspace_id=? AND deleted", workspaceID).Count(&deletedChanges).Error; err != nil || deletedChanges != tombstones {
		t.Fatalf("deleted checker changes=%d want=%d error=%v", deletedChanges, tombstones, err)
	}
	var stat domain.WorkspaceScrapeSourceStat
	if err := db.First(&stat, "workspace_id=? AND scrape_site_id=1", workspaceID).Error; err != nil || int64(stat.ProxyCount) != remaining {
		t.Fatalf("source count=%d want=%d error=%v", stat.ProxyCount, remaining, err)
	}
	if stat.AliveCount != 0 || int64(stat.DeadCount) != remaining || stat.UnknownCount != 0 {
		t.Fatalf("source health alive=%d dead=%d unknown=%d", stat.AliveCount, stat.DeadCount, stat.UnknownCount)
	}
	stat = domain.WorkspaceScrapeSourceStat{}
	if err := db.First(&stat, "workspace_id=? AND scrape_site_id=1", otherWorkspaceID).Error; err != nil || stat.ProxyCount != 1 {
		t.Fatalf("other workspace source count=%d error=%v", stat.ProxyCount, err)
	}
	if stat.AliveCount != 0 || stat.DeadCount != 1 || stat.UnknownCount != 0 {
		t.Fatalf("other workspace source health alive=%d dead=%d unknown=%d", stat.AliveCount, stat.DeadCount, stat.UnknownCount)
	}
	var usage domain.WorkspaceUsagePeriod
	if err := db.First(&usage, "workspace_id=?", workspaceID).Error; err != nil || int64(usage.ActiveRoutes) != remaining {
		t.Fatalf("usage active=%d want=%d error=%v", usage.ActiveRoutes, remaining, err)
	}
}
