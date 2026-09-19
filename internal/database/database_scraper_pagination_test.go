package database

import (
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"magpie/internal/api/dto"
	"magpie/internal/domain"
)

func TestNormalizeScrapeSitePage(t *testing.T) {
	tests := []struct {
		name         string
		page         int
		pageSize     int
		wantPage     int
		wantPageSize int
	}{
		{name: "keeps requested values", page: 3, pageSize: 100, wantPage: 3, wantPageSize: 100},
		{name: "uses default size when omitted", page: 1, pageSize: 0, wantPage: 1, wantPageSize: 40},
		{name: "uses default size above maximum", page: 1, pageSize: 101, wantPage: 1, wantPageSize: 40},
		{name: "normalizes invalid page", page: 0, pageSize: 20, wantPage: 1, wantPageSize: 20},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			page, pageSize := normalizeScrapeSitePage(tt.page, tt.pageSize)
			if page != tt.wantPage || pageSize != tt.wantPageSize {
				t.Fatalf("normalizeScrapeSitePage(%d, %d) = (%d, %d), want (%d, %d)", tt.page, tt.pageSize, page, pageSize, tt.wantPage, tt.wantPageSize)
			}
		})
	}
}

func TestScrapeSourceSortingAcrossPages(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "sources.db")), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	original := DB
	DB = db
	t.Cleanup(func() { DB = original; sqlDB, _ := db.DB(); sqlDB.Close() })
	if err := db.AutoMigrate(&domain.WorkspaceScrapeSourceStat{}, &domain.WorkspaceScrapeSite{}); err != nil {
		t.Fatal(err)
	}
	added := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	sources := []domain.WorkspaceScrapeSourceStat{
		{WorkspaceID: 1, ScrapeSiteID: 1, URL: "https://z.example", ProxyCount: 10, AliveCount: 2, AddedAt: added, ProtocolKey: "https"},
		{WorkspaceID: 1, ScrapeSiteID: 2, URL: "https://A.example", ProxyCount: 4, AliveCount: 2, AddedAt: added, ProtocolKey: "https"},
		{WorkspaceID: 1, ScrapeSiteID: 3, URL: "https://b.example", ProxyCount: 0, AliveCount: 0, AddedAt: added, ProtocolKey: "https"},
		{WorkspaceID: 1, ScrapeSiteID: 4, URL: "http://c.example", ProxyCount: 2, AliveCount: 0, AddedAt: added, ProtocolKey: "http"},
		{WorkspaceID: 2, ScrapeSiteID: 5, URL: "https://other.example", ProxyCount: 100, AliveCount: 100, AddedAt: added, ProtocolKey: "https"},
	}
	for _, source := range sources {
		if err := db.Create(&source).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Create(&domain.WorkspaceScrapeSite{WorkspaceID: source.WorkspaceID, ScrapeSiteID: source.ScrapeSiteID}).Error; err != nil {
			t.Fatal(err)
		}
	}
	tests := []struct {
		name, field, order string
		filters            dto.ScrapeSourceListFilters
		want               []uint64
	}{
		{name: "default stable order", want: []uint64{1, 2, 3, 4}},
		{name: "URL ascending", field: "url", order: "asc", want: []uint64{4, 2, 3, 1}},
		{name: "URL descending", field: "url", order: "desc", want: []uint64{1, 3, 2, 4}},
		{name: "count ascending", field: "proxy_count", order: "asc", want: []uint64{3, 4, 2, 1}},
		{name: "count descending", field: "proxy_count", order: "desc", want: []uint64{1, 2, 4, 3}},
		{name: "alive ascending ties", field: "alive_count", order: "asc", want: []uint64{3, 4, 1, 2}},
		{name: "alive descending ties", field: "alive_count", order: "desc", want: []uint64{1, 2, 3, 4}},
		{name: "health ascending", field: "health", order: "asc", want: []uint64{3, 4, 1, 2}},
		{name: "health descending", field: "health", order: "desc", want: []uint64{2, 1, 4, 3}},
		{name: "filtered before pagination", field: "health", order: "desc", filters: dto.ScrapeSourceListFilters{Protocols: []string{"https"}, ProxyCountOperator: ">", ProxyCount: 1}, want: []uint64{2, 1}},
		{name: "invalid field", field: "url; DROP TABLE user_scrape_source_stats", order: "asc", want: []uint64{1, 2, 3, 4}},
		{name: "invalid direction", field: "url", order: "desc; SELECT 1", want: []uint64{1, 2, 3, 4}},
		{name: "missing direction", field: "url", want: []uint64{1, 2, 3, 4}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var ids []uint64
			for page := 1; page <= 3; page++ {
				rows := GetScrapeSiteInfoPageWithOptions(1, page, 2, "", tt.filters, ScrapeSourcePageQueryOptions{SortField: tt.field, SortOrder: tt.order})
				for _, row := range rows {
					ids = append(ids, row.Id)
				}
			}
			if !reflect.DeepEqual(ids, tt.want) {
				t.Fatalf("got IDs %v, want %v", ids, tt.want)
			}
		})
	}
}
