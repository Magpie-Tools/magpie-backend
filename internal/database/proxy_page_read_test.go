package database

import (
	"context"
	"strings"
	"testing"
	"time"

	"magpie/internal/api/dto"
	"magpie/internal/domain"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type proxyPageReadLogger struct {
	logger.Interface
	queries []string
}

func (l *proxyPageReadLogger) Trace(_ context.Context, _ time.Time, sql func() (string, int64), _ error) {
	query, _ := sql()
	l.queries = append(l.queries, query)
}

func TestProxyPageDefaultOrderKeepsLifecycleCountsAndPendingMasks(t *testing.T) {
	db := setupProxyTagTestDB(t)
	if err := db.AutoMigrate(&domain.ProxyReputation{}); err != nil {
		t.Fatal(err)
	}
	workspace := domain.Workspace{Name: "Page owner", CheckerGeneration: 1}
	other := domain.Workspace{Name: "Other owner", CheckerGeneration: 1}
	if err := db.Create(&[]domain.Workspace{workspace, other}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Order("id").First(&workspace).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Order("id DESC").First(&other).Error; err != nil {
		t.Fatal(err)
	}
	checked := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	health := float32(80)
	for i, state := range []string{"active", "paused", "archived"} {
		id := uint64(i + 1)
		proxy := domain.Proxy{ID: id, IP: "192.0.2.1", Port: uint16(8000 + i)}
		if err := db.Create(&proxy).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Create(&domain.ProxyReputation{ProxyID: id, Kind: domain.ProxyReputationKindOverall, Label: "unknown"}).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Create(&domain.ManagedProxy{WorkspaceID: workspace.ID, ProxyID: id, State: state}).Error; err != nil {
			t.Fatal(err)
		}
		row := domain.WorkspaceProxyFilterIndex{WorkspaceID: workspace.ID, ProxyID: id, State: state,
			Host: proxy.IP, Port: proxy.Port, Alive: i < 2, LatestCheck: checked,
			AnonymityLevel: "elite", ResponseTime: 37, HealthOverall: &health}
		if i == 2 {
			row.LatestCheck = checked.Add(time.Hour)
		}
		if err := db.Create(&row).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Create(&domain.ManagedProxy{WorkspaceID: other.ID, ProxyID: 1}).Error; err != nil {
		t.Fatal(err)
	}
	for page, wantID := range []int{1, 2, 3} {
		rows, count := GetProxyInfoPageWithFiltersAndOptions(workspace.ID, page+1, 1, "", dto.ProxyListFilters{}, ProxyPageQueryOptions{})
		if count != 3 || len(rows) != 1 || rows[0].Id != wantID {
			t.Fatalf("page %d count=%d rows=%+v want ID=%d", page+1, count, rows, wantID)
		}
	}
	if err := db.Model(&workspace).Update("checker_dirty", true).Error; err != nil {
		t.Fatal(err)
	}
	rows, total := GetProxyInfoPageWithCount(workspace.ID, 1)
	if total != 3 || len(rows) != 3 {
		t.Fatalf("pending page rows=%d total=%d", len(rows), total)
	}
	for i, row := range rows {
		if row.Id != i+1 || row.HealthKnown || row.Alive || row.Health != nil || row.ResponseTime != 0 || row.AnonymityLevel != "N/A" || !row.LatestCheck.IsZero() {
			t.Fatalf("pending mask or deterministic order changed: %+v", row)
		}
	}
	filtered, total := GetProxyInfoPageWithFiltersAndOptions(workspace.ID, 1, 40, "", dto.ProxyListFilters{Status: "alive"}, ProxyPageQueryOptions{})
	if len(filtered) != 0 || total != 0 {
		t.Fatalf("pending health filter returned rows=%v total=%d", filtered, total)
	}
	queryLog := &proxyPageReadLogger{Interface: logger.Default}
	DB = db.Session(&gorm.Session{Logger: queryLog})
	if count := GetWorkspaceProxyCount(workspace.ID); count != 3 {
		t.Fatal("ownership count changed", count)
	}
	if len(queryLog.queries) != 1 || strings.Contains(strings.ToLower(queryLog.queries[0]), "join") || !strings.Contains(strings.ReplaceAll(queryLog.queries[0], "`", `"`), `FROM "user_proxies"`) {
		t.Fatal("count no longer reads ownership directly", queryLog.queries)
	}
}
