package database

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"magpie/internal/domain"

	"gorm.io/gorm"
)

func seedEvidencePrunePointers(t *testing.T, db *gorm.DB, count int) domain.Workspace {
	t.Helper()
	workspace := domain.Workspace{Name: "Prune owner", CheckerGeneration: 1, CheckerHTTPKey: "current"}
	if err := db.Create(&workspace).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&domain.Protocol{ID: 1, Name: "http"}).Error; err != nil {
		t.Fatal(err)
	}
	judge := domain.Judge{FullString: "https://prune.invalid/"}
	if err := db.Create(&judge).Error; err != nil {
		t.Fatal(err)
	}
	for id := 1; id <= count; id++ {
		proxy := domain.Proxy{ID: uint64(id), IP: fmt.Sprintf("192.0.2.%d", id), Port: 8080}
		for _, row := range []any{&proxy,
			&domain.ManagedProxy{WorkspaceID: workspace.ID, ProxyID: proxy.ID},
			&domain.ProxyStatistic{ID: proxy.ID, ProxyID: proxy.ID, ProtocolID: 1, JudgeID: judge.ID, CreatedAt: time.Now().AddDate(0, 0, -60)},
			&domain.ProxyLatestStatistic{WorkspaceID: workspace.ID, ConfigKey: "current", ProxyID: proxy.ID, ProtocolID: 1, Alive: true, StatisticID: proxy.ID, CheckedAt: time.Now()},
		} {
			if err := db.Create(row).Error; err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := ensureProxyStatisticsRetentionSchema(db); err != nil {
		t.Fatal(err)
	}
	return workspace
}

func TestCheckerEvidencePrunePagePortableFallback(t *testing.T) {
	db := setupRotatingProxyTestDB(t)
	seedEvidencePrunePointers(t, db, 3)
	if err := db.Exec(`UPDATE proxy_latest_statistics SET config_key='obsolete' WHERE proxy_id=2`).Error; err != nil {
		t.Fatal(err)
	}
	page, err := PruneObsoleteCheckerEvidencePage(context.Background(), nil, 3)
	if err != nil || page.Deleted != 1 || page.Next != nil {
		t.Fatalf("portable page=%+v error=%v", page, err)
	}
}

func TestCheckerEvidencePrunePagesPreserveKeysAndOwnersPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	w := seedEvidencePrunePointers(t, db, 12)
	dirty := domain.Workspace{Name: "Pending owner", CheckerGeneration: 1, CheckerDirty: true, CheckerHTTPKey: "pending"}
	legacy := domain.Workspace{Name: "Legacy owner"}
	for _, row := range []*domain.Workspace{&dirty, &legacy} {
		if err := db.Create(row).Error; err != nil {
			t.Fatal(err)
		}
	}
	for _, statement := range []struct {
		sql  string
		args []any
	}{
		{`UPDATE proxy_latest_statistics SET config_key='obsolete' WHERE proxy_id=2`, nil},
		{`UPDATE proxy_latest_statistics SET config_key='override' WHERE proxy_id=3`, nil},
		{`INSERT INTO proxy_checker_plans (workspace_id,proxy_id,protocol_id,config_key,transport) VALUES (?,3,1,'override','quic'),(?,8,1,'','tcp')`, []any{w.ID, w.ID}},
		{`UPDATE proxy_latest_statistics SET workspace_id=?,config_key='obsolete' WHERE proxy_id=4`, []any{dirty.ID}},
		{`UPDATE proxy_latest_statistics SET config_key='' WHERE proxy_id=5`, nil},
		{`DELETE FROM user_proxies WHERE workspace_id=? AND proxy_id=6`, []any{w.ID}},
		{`UPDATE user_proxies SET state='paused' WHERE proxy_id=7`, nil},
		{`INSERT INTO user_proxies (workspace_id,proxy_id,state) VALUES (?,9,'archived')`, []any{legacy.ID}},
		{`UPDATE proxy_latest_statistics SET workspace_id=0,config_key='' WHERE proxy_id IN (9,10)`, nil},
		{`UPDATE proxy_latest_statistics SET workspace_id=999 WHERE proxy_id=11`, nil},
	} {
		if err := db.Exec(statement.sql, statement.args...).Error; err != nil {
			t.Fatal(err)
		}
	}
	var cursor *CheckerEvidencePruneCursor
	var scanned, deleted int64
	for pages := 0; ; pages++ {
		if pages > 12 {
			t.Fatal("prune scan did not finish")
		}
		page, err := PruneObsoleteCheckerEvidencePage(context.Background(), cursor, 2)
		if err != nil || page.Scanned > 2 {
			t.Fatalf("page=%+v error=%v", page, err)
		}
		scanned += page.Scanned
		deleted += page.Deleted
		cursor = page.Next
		if cursor == nil {
			break
		}
	}
	if scanned != 12 || deleted != 6 {
		t.Fatalf("scanned=%d deleted=%d want12/6", scanned, deleted)
	}
	var ids []uint64
	if err := db.Model(&domain.ProxyLatestStatistic{}).Order("proxy_id").Pluck("proxy_id", &ids).Error; err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(ids) != "[1 3 4 7 9 12]" {
		t.Fatalf("retained current/dirty/legacy pointers=%v", ids)
	}
}

func TestCheckerEvidencePruneCursorFinishesAndRevisitsPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	w := seedEvidencePrunePointers(t, db, 12)
	first, err := PruneObsoleteCheckerEvidencePage(context.Background(), nil, 3)
	if err != nil || first.Scanned != 3 || first.Deleted != 0 || first.Next == nil {
		t.Fatalf("first page=%+v error=%v", first, err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := PruneObsoleteCheckerEvidencePage(canceled, first.Next, 3); err == nil {
		t.Fatal("canceled page succeeded")
	}
	if err := db.Model(&w).Update("checker_http_key", "next").Error; err != nil {
		t.Fatal(err)
	}
	// This append sorts beyond the captured upper key. It must not postpone
	// completion or the next visit to invalidations behind the current cursor.
	if err := db.Exec(`INSERT INTO proxy_latest_statistics (workspace_id,config_key,proxy_id,protocol_id,statistic_id,checked_at,alive) VALUES (999,'obsolete',12,1,12,CURRENT_TIMESTAMP,TRUE)`).Error; err != nil {
		t.Fatal(err)
	}
	cursor := first.Next
	var scanned int64 = first.Scanned
	for pages := 0; cursor != nil; pages++ {
		if pages > 4 {
			t.Fatal("finite prune cycle followed appended keys")
		}
		page, err := PruneObsoleteCheckerEvidencePage(context.Background(), cursor, 3)
		if err != nil {
			t.Fatal(err)
		}
		scanned += page.Scanned
		cursor = page.Next
	}
	if scanned != 12 {
		t.Fatalf("scan escaped captured upper key: %d", scanned)
	}
	var remaining int64
	if err := db.Model(&domain.ProxyLatestStatistic{}).Count(&remaining).Error; err != nil || remaining != 4 {
		t.Fatalf("remaining=%d want first3+append error=%v", remaining, err)
	}
	for {
		page, err := PruneObsoleteCheckerEvidencePage(context.Background(), cursor, 3)
		if err != nil {
			t.Fatal(err)
		}
		cursor = page.Next
		if cursor == nil {
			break
		}
	}
	if err := db.Model(&domain.ProxyLatestStatistic{}).Count(&remaining).Error; err != nil || remaining != 0 {
		t.Fatalf("next cycle starved old/new invalidations: remaining=%d error=%v", remaining, err)
	}
}

func TestCheckerEvidencePruneWithConcurrentHistoryRetentionPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	seedEvidencePrunePointers(t, db, 48)
	if err := db.Exec(`UPDATE proxy_latest_statistics SET config_key='obsolete' WHERE proxy_id%2=0`).Error; err != nil {
		t.Fatal(err)
	}
	errors := make(chan error, 5)
	var workers sync.WaitGroup
	for i := 0; i < 4; i++ {
		workers.Go(func() {
			_, err := DeleteOldProxyStatistics(context.Background(), time.Now().AddDate(0, 0, -30), 10)
			errors <- err
		})
	}
	workers.Go(func() {
		var cursor *CheckerEvidencePruneCursor
		for {
			page, err := PruneObsoleteCheckerEvidencePage(context.Background(), cursor, 5)
			if err != nil {
				errors <- err
				return
			}
			cursor = page.Next
			if cursor == nil {
				errors <- nil
				return
			}
		}
	})
	workers.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := DeleteOldProxyStatistics(context.Background(), time.Now().AddDate(0, 0, -30), 100); err != nil {
		t.Fatal(err)
	}
	var pointers, history, dangling int64
	for query, target := range map[string]*int64{
		`SELECT COUNT(*) FROM proxy_latest_statistics`: &pointers,
		`SELECT COUNT(*) FROM proxy_statistics`:        &history,
		`SELECT COUNT(*) FROM proxy_latest_statistics l LEFT JOIN proxy_statistics s ON s.id=l.statistic_id WHERE s.id IS NULL`: &dangling,
	} {
		if err := db.Raw(query).Scan(target).Error; err != nil {
			t.Fatal(err)
		}
	}
	if pointers != 24 || history != 24 || dangling != 0 {
		t.Fatalf("retention lost current pins: pointers=%d history=%d dangling=%d", pointers, history, dangling)
	}
}
