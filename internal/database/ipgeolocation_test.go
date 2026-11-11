package database

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"magpie/internal/config"
	"magpie/internal/domain"
	"magpie/internal/ipgeolocation"
	"magpie/internal/platform/pg"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func setupIPGeolocationTest(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = sqlDB.Close() })
	if err := db.AutoMigrate(&domain.IPGeolocationLookup{}, &domain.IPGeolocationState{}); err != nil {
		t.Fatal(err)
	}
	previous := pg.DB
	pg.DB = db
	t.Cleanup(func() { pg.DB = previous })
	if _, err := GetIPGeolocationState(context.Background()); err != nil {
		t.Fatal(err)
	}
	return db
}

func setIPGeolocationTestConfig(t *testing.T, cfg config.IPGeolocationPluginConfig) {
	t.Helper()
	t.Chdir(t.TempDir())
	original := config.GetConfig()
	t.Cleanup(func() {
		if err := config.SetConfig(original); err != nil {
			t.Error(err)
		}
	})
	settings := original
	settings.Plugins.IPGeolocation = cfg
	if err := config.SetConfig(settings); err != nil {
		t.Fatal(err)
	}
}

func TestIPGeolocationCacheReadsAreDeduplicatedAndBounded(t *testing.T) {
	for _, dialect := range []string{"sqlite", "postgres"} {
		t.Run(dialect, func(t *testing.T) {
			var db *gorm.DB
			if dialect == "postgres" {
				db = setupProxyIngestionPostgresTest(t)
			} else {
				db = setupIPGeolocationTest(t)
			}
			cfg := config.IPGeolocationPluginConfig{Enabled: true, APIKey: "test-key", DailyCreditLimit: 300, CacheHours: 24}
			setIPGeolocationTestConfig(t, cfg)
			for _, shared := range []bool{true, false} {
				t.Run(fmt.Sprintf("shared=%t", shared), func(t *testing.T) {
					proxies := make([]domain.Proxy, 65536)
					for i := range proxies {
						ip := "8.8.8.8"
						if !shared {
							ip = fmt.Sprintf("11.%d.%d.%d", i>>16, (i>>8)&255, i&255)
						}
						if err := proxies[i].SetIP(ip); err != nil {
							t.Fatal(err)
						}
					}
					now := time.Now().UTC()
					expires := now.Add(24 * time.Hour)
					cached := []domain.IPGeolocationLookup{{IP: proxies[0].GetIPAddress(), Country: "Provider", CheckedAt: &now, ExpiresAt: &expires, NextLookupAt: expires}}
					if !shared {
						for _, index := range []int{len(proxies) / 2, len(proxies) - 1} {
							cached = append(cached, domain.IPGeolocationLookup{IP: proxies[index].GetIPAddress(), Country: "Provider", CheckedAt: &now, ExpiresAt: &expires, NextLookupAt: expires})
						}
					}
					if err := db.Create(&cached).Error; err != nil {
						t.Fatal(err)
					}
					queries := 0
					if err := db.Callback().Query().After("gorm:query").Register("test:ipgeolocation_cache_batch", func(tx *gorm.DB) {
						queries++
						if len(tx.Statement.Vars) > ipGeolocationBatchSize+1 {
							t.Errorf("cache query used %d parameters", len(tx.Statement.Vars))
						}
					}); err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = db.Callback().Query().Remove("test:ipgeolocation_cache_batch") })
					rows, err := LoadIPGeolocationOverrides(context.Background(), proxies)
					if err != nil || len(rows) != len(cached) {
						t.Fatalf("cache read returned %d rows, want %d: %v", len(rows), len(cached), err)
					}
					for _, row := range cached {
						if rows[row.IP].Country != row.Country {
							t.Fatalf("missing cached metadata for %s", row.IP)
						}
					}
					wantQueries := 1
					if !shared {
						wantQueries = (len(proxies) + ipGeolocationBatchSize - 1) / ipGeolocationBatchSize
					}
					if queries != wantQueries {
						t.Fatalf("ran %d cache queries, want %d", queries, wantQueries)
					}
				})
			}
		})
	}
}

func TestIPGeolocationSettingsChangesReuseFreshResults(t *testing.T) {
	t.Run("sqlite", func(t *testing.T) { testIPGeolocationSettingsChanges(t, setupIPGeolocationTest(t)) })
	t.Run("postgres", func(t *testing.T) { testIPGeolocationSettingsChanges(t, setupProxyIngestionPostgresTest(t)) })
}

func testIPGeolocationSettingsChanges(t *testing.T, db *gorm.DB) {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Microsecond)
	for _, test := range []struct {
		name             string
		oldHours         uint32
		newHours         uint32
		age              time.Duration
		oldSecurity      bool
		newSecurity      bool
		securityIncluded bool
		wantLookup       bool
	}{
		{name: "longer cache", oldHours: 24, newHours: 168, age: time.Hour},
		{name: "longer cache reuses previously expired result", oldHours: 24, newHours: 168, age: 48 * time.Hour},
		{name: "shorter cache still fresh", oldHours: 168, newHours: 24, age: time.Hour},
		{name: "shorter cache expired", oldHours: 168, newHours: 24, age: 48 * time.Hour, wantLookup: true},
		{name: "disable security", oldHours: 24, newHours: 24, age: time.Hour, oldSecurity: true, securityIncluded: true},
		{name: "enable security already cached", oldHours: 24, newHours: 24, age: time.Hour, newSecurity: true, securityIncluded: true},
		{name: "enable missing security", oldHours: 24, newHours: 24, age: time.Hour, newSecurity: true, wantLookup: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := db.Where("ip = ?", "8.8.8.8").Delete(&domain.IPGeolocationLookup{}).Error; err != nil {
				t.Fatal(err)
			}
			old := config.IPGeolocationPluginConfig{DailyCreditLimit: 300, CacheHours: test.oldHours, IncludeSecurity: test.oldSecurity}
			cfg := old
			cfg.CacheHours, cfg.IncludeSecurity = test.newHours, test.newSecurity
			checked := now.Add(-test.age)
			expires := checked.Add(time.Duration(old.CacheHours) * time.Hour)
			cached := domain.IPGeolocationLookup{IP: "8.8.8.8", Country: "Provider", EstimatedType: "ISP", Payload: "cached payload",
				CheckedAt: &checked, ExpiresAt: &expires, NextLookupAt: expires, Policy: IPGeolocationPolicy(old),
				SecurityIncluded: test.securityIncluded, ApplyPending: true, ApplyCursor: 17}
			if err := db.Create(&cached).Error; err != nil {
				t.Fatal(err)
			}
			if err := enqueueIPGeolocationRows(db, []domain.Proxy{{IP: cached.IP}}, cfg, now); err != nil {
				t.Fatal(err)
			}
			var row domain.IPGeolocationLookup
			if err := db.First(&row, "ip = ?", cached.IP).Error; err != nil {
				t.Fatal(err)
			}
			wantExpires := checked.Add(time.Duration(cfg.CacheHours) * time.Hour)
			wantNext := wantExpires
			if test.wantLookup {
				wantNext = now
			}
			if !row.NextLookupAt.Equal(wantNext) || row.ExpiresAt == nil || !row.ExpiresAt.Equal(wantExpires) || row.Policy != IPGeolocationPolicy(cfg) {
				t.Fatalf("settings change scheduled %v, expires %v; want %v, %v", row.NextLookupAt, row.ExpiresAt, wantNext, wantExpires)
			}
			if row.Country != cached.Country || row.EstimatedType != cached.EstimatedType || row.Payload != cached.Payload || row.SecurityIncluded != cached.SecurityIncluded || !row.ApplyPending || row.ApplyCursor != cached.ApplyCursor {
				t.Fatal("rescheduling changed cached data or projection progress", row)
			}
		})
	}
}

func TestIPGeolocationReservationRechecksFreshness(t *testing.T) {
	db := setupIPGeolocationTest(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	checked := now.Add(-48 * time.Hour)
	expires := checked.Add(24 * time.Hour)
	cfg := config.IPGeolocationPluginConfig{DailyCreditLimit: 300, CacheHours: 168}
	row := domain.IPGeolocationLookup{IP: "8.8.8.8", CheckedAt: &checked, ExpiresAt: &expires, NextLookupAt: expires}
	if err := db.Create(&row).Error; err != nil {
		t.Fatal(err)
	}
	reserved, wait, err := ReserveIPGeolocationCredits(context.Background(), row.IP, now, cfg)
	if err != nil || reserved || wait != 120*time.Hour {
		t.Fatalf("fresh result reserved=%t wait=%v: %v", reserved, wait, err)
	}
	state, err := GetIPGeolocationState(context.Background())
	if err != nil || state.UsedCredits != 0 || state.NextRequestAt != nil {
		t.Fatal("fresh result consumed credits or an API slot", state, err)
	}
}

func TestIPGeolocationDueQueueCleanupPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	cfg := config.IPGeolocationPluginConfig{DailyCreditLimit: 300, CacheHours: 168}
	workspace := domain.Workspace{Name: "IP queue cleanup"}
	if err := db.Create(&workspace).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`INSERT INTO proxies (host,ip_address,port,country,estimated_type,hash)
SELECT host(address),address,3128,'N/A','N/A',decode(md5(n::text),'hex') FROM
(SELECT n,CASE WHEN n=1001 THEN '9.9.9.9'::inet WHEN n=1002 THEN '1.1.1.1'::inet ELSE '11.0.0.0'::inet+n END address FROM generate_series(1,1002)n)s`).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`INSERT INTO user_proxies (workspace_id,proxy_id,state)
SELECT ?,id,CASE WHEN host IN ('9.9.9.9','1.1.1.1') THEN 'active' ELSE 'paused' END FROM proxies`, workspace.ID).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`INSERT INTO ipgeolocation_lookups (ip,next_lookup_at)
SELECT host,? FROM proxies`, now.Add(-time.Hour)).Error; err != nil {
		t.Fatal(err)
	}
	checked := now.Add(-48 * time.Hour)
	if err := db.Model(&domain.IPGeolocationLookup{}).Where("ip = '1.1.1.1'").Update("checked_at", checked).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&domain.IPGeolocationLookup{IP: "8.8.8.8", NextLookupAt: now.Add(-2 * time.Hour)}).Error; err != nil {
		t.Fatal(err)
	}
	row, err := NextIPGeolocationLookup(ctx, cfg, now)
	if err != nil || row == nil || row.IP != "9.9.9.9" {
		t.Fatal("active lookup was delayed by local cleanup", row, err)
	}
	var count int64
	if err := db.Model(&domain.IPGeolocationLookup{}).Where("next_lookup_at = ?", now.Add(time.Hour)).Count(&count).Error; err != nil || count != 1000 {
		t.Fatal("paused IPs were not deferred", count, err)
	}
	if err := db.Model(&domain.IPGeolocationLookup{}).Where("ip = '8.8.8.8'").Count(&count).Error; err != nil || count != 0 {
		t.Fatal("orphan IP was not deleted", count, err)
	}
	var fresh domain.IPGeolocationLookup
	if err := db.First(&fresh, "ip = '1.1.1.1'").Error; err != nil || !fresh.NextLookupAt.Equal(checked.Add(168*time.Hour)) {
		t.Fatal("fresh IP was not rescheduled", fresh, err)
	}
}

func TestIPGeolocationQueueDeduplicatesAndPreservesFreshData(t *testing.T) {
	db := setupIPGeolocationTest(t)
	ctx := context.Background()
	now := time.Now().UTC()
	cfg := config.IPGeolocationPluginConfig{DailyCreditLimit: 300, CacheHours: 168}
	var proxies []domain.Proxy
	for _, ip := range []string{"8.8.8.8", "8.8.8.8", "::ffff:8.8.8.8", "2001:4860:4860::8888", "192.168.1.1", "192.0.2.1"} {
		p := domain.Proxy{}
		if err := p.SetIP(ip); err != nil {
			t.Fatal(err)
		}
		proxies = append(proxies, p)
	}
	if err := enqueueIPGeolocationRows(db, proxies, cfg, now); err != nil {
		t.Fatal(err)
	}
	var count int64
	db.Model(&domain.IPGeolocationLookup{}).Count(&count)
	if count != 2 {
		t.Fatalf("created %d cache rows, want 2 public IPs", count)
	}
	future := now.Add(7 * 24 * time.Hour)
	if err := db.WithContext(ctx).Model(&domain.IPGeolocationLookup{}).Where("ip = ?", "8.8.8.8").Update("next_lookup_at", future).Error; err != nil {
		t.Fatal(err)
	}
	if err := enqueueIPGeolocationRows(db, proxies, cfg, now); err != nil {
		t.Fatal(err)
	}
	var row domain.IPGeolocationLookup
	db.First(&row, "ip = ?", "8.8.8.8")
	if !row.NextLookupAt.Equal(future) {
		t.Fatal("reimport reset cached IP deadline")
	}
	cfg.IncludeSecurity = true
	if err := enqueueIPGeolocationRows(db, proxies, cfg, now); err != nil {
		t.Fatal(err)
	}
	db.First(&row, "ip = ?", "8.8.8.8")
	if !row.NextLookupAt.Equal(now) {
		t.Fatal("security mode change did not requeue cached IP")
	}
}

func TestIPGeolocationProjectionUsesOrderedIPIndexPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	if err := db.Exec(`INSERT INTO proxies (host,ip_address,port,country,estimated_type,hash)
SELECT host(address),address,3128,'N/A','N/A',decode(md5(n::text),'hex') FROM
(SELECT n,CASE WHEN n%10=0 THEN '8.8.8.8'::inet ELSE '10.0.0.0'::inet+n END address FROM generate_series(1,30000)n)s`).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("ANALYZE proxies").Error; err != nil {
		t.Fatal(err)
	}
	var plan string
	if err := db.Raw(`EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) SELECT id FROM proxies WHERE ip_address='8.8.8.8' AND id>5000 ORDER BY id LIMIT 256`).Row().Scan(&plan); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(plan, `"Index Name": "idx_proxy_ip_id"`) {
		t.Fatal("shared-IP projection lost its ordered index", plan)
	}
	t.Log("shared-IP projection uses the composite IP/id index in a 30000-route inventory")
}

func TestIPGeolocationBudgetSurvivesRestartsAndRetries(t *testing.T) {
	db := setupIPGeolocationTest(t)
	ctx := context.Background()
	now := time.Now().UTC()
	cfg := config.IPGeolocationPluginConfig{DailyCreditLimit: 3, CacheHours: 24, IncludeSecurity: true}
	if err := db.Create(&domain.IPGeolocationLookup{IP: "8.8.8.8", NextLookupAt: now}).Error; err != nil {
		t.Fatal(err)
	}
	reserved, _, err := ReserveIPGeolocationCredits(ctx, "8.8.8.8", now, cfg)
	if err != nil || !reserved {
		t.Fatalf("reserve: %v %v", reserved, err)
	}
	if reserved, _, err := ReserveIPGeolocationCredits(ctx, "8.8.8.8", now.Add(time.Minute), cfg); err != nil || reserved {
		t.Fatal("overspent daily limit")
	}
	if err := CompleteIPGeolocationLookup(ctx, "8.8.8.8", cfg, ipgeolocation.Response{}, errors.New("request failed"), now); err != nil {
		t.Fatal(err)
	}
	state, err := GetIPGeolocationState(ctx)
	if err != nil || state.UsedCredits != 3 {
		t.Fatal("uncertain request lost its credit reservation", state, err)
	}
	nextDay := now.Add(24 * time.Hour)
	if reserved, _, err := ReserveIPGeolocationCredits(ctx, "8.8.8.8", nextDay, cfg); err != nil || !reserved {
		t.Fatal("daily budget did not reset")
	}
	if err := CompleteIPGeolocationLookup(ctx, "8.8.8.8", cfg, ipgeolocation.Response{Status: 429, RetryAfter: 48 * time.Hour}, errors.New("quota exhausted"), nextDay); err != nil {
		t.Fatal(err)
	}
	state, _ = GetIPGeolocationState(ctx)
	if state.UsedCredits != 0 || state.NextRequestAt == nil || state.NextRequestAt.Before(nextDay.Add(48*time.Hour)) {
		t.Fatal("explicit failure was charged or long provider backoff lost", state)
	}
	if reserved, _, err := ReserveIPGeolocationCredits(ctx, "8.8.8.8", nextDay.Add(time.Minute), cfg); err != nil || reserved {
		t.Fatal("ignored persisted backoff")
	}

}

func TestIPGeolocationConcurrentReservationsPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	ctx := context.Background()
	now := time.Now().UTC()
	if _, err := GetIPGeolocationState(ctx); err != nil {
		t.Fatal(err)
	}
	cfg := config.IPGeolocationPluginConfig{DailyCreditLimit: 3, CacheHours: 24, IncludeSecurity: true}
	if err := db.Create(&domain.IPGeolocationLookup{IP: "8.8.8.8", NextLookupAt: now}).Error; err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	outcomes := make(chan bool, 12)
	for range 12 {
		wg.Go(func() {
			reserved, _, err := ReserveIPGeolocationCredits(ctx, "8.8.8.8", now, cfg)
			if err != nil {
				t.Error(err)
			}
			outcomes <- reserved
		})
	}
	wg.Wait()
	close(outcomes)
	count := 0
	for reserved := range outcomes {
		if reserved {
			count++
		}
	}
	if count != 1 {
		t.Fatal("concurrent callers exceeded one reservation", count)
	}
	var state domain.IPGeolocationState
	if err := db.First(&state, 1).Error; err != nil {
		t.Fatal(err)
	}
	if state.UsedCredits != 3 {
		t.Fatal(fmt.Sprintf("credits=%d, want 3", state.UsedCredits))
	}
}
