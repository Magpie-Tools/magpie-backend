package database

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"magpie/internal/config"
	"magpie/internal/domain"
	"magpie/internal/ipgeolocation"

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
	previous := DB
	DB = db
	t.Cleanup(func() { DB = previous })
	if _, err := GetIPGeolocationState(context.Background()); err != nil {
		t.Fatal(err)
	}
	return db
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
	if err := DeferIPGeolocationLookup(ctx, "8.8.8.8", future); err != nil {
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

func TestIPGeolocationWaitingForProviderPreservesExistingLabels(t *testing.T) {
	db := setupIPGeolocationTest(t)
	t.Chdir(t.TempDir())
	original := config.GetConfig()
	t.Cleanup(func() {
		if err := config.SetConfig(original); err != nil {
			t.Error(err)
		}
	})
	settings := original
	settings.Plugins.GeoLite.Enabled = false
	settings.Plugins.IPGeolocation = config.IPGeolocationPluginConfig{Enabled: true, APIKey: "test-key", DailyCreditLimit: 300, CacheHours: 24}
	if err := config.SetConfig(settings); err != nil {
		t.Fatal(err)
	}
	proxy := domain.Proxy{Country: "Existing country", EstimatedType: "ISP"}
	if err := proxy.SetIP("8.8.8.8"); err != nil {
		t.Fatal(err)
	}
	proxies := []domain.Proxy{proxy}
	if candidates := EnrichProxiesWithCountryAndType(&proxies); len(candidates) != 0 {
		t.Fatal("queued unavailable local DNS", candidates)
	}
	if proxies[0].Country != proxy.Country || proxies[0].EstimatedType != proxy.EstimatedType {
		t.Fatal("waiting for provider erased existing metadata", proxies)
	}
	now := time.Now().UTC()
	expires := now.Add(24 * time.Hour)
	if err := db.Create(&domain.IPGeolocationLookup{IP: "8.8.8.8", Country: "Provider country", CheckedAt: &now, ExpiresAt: &expires, NextLookupAt: expires}).Error; err != nil {
		t.Fatal(err)
	}
	EnrichProxiesWithCountryAndType(&proxies)
	if proxies[0].Country != "Provider country" || proxies[0].EstimatedType != "ISP" {
		t.Fatal("partial provider data erased existing type", proxies)
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

func TestIPGeolocationProjectionIsBoundedAndAuthoritativePostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	ctx := context.Background()
	now := time.Now().UTC()
	t.Chdir(t.TempDir())
	original := config.GetConfig()
	t.Cleanup(func() {
		if err := config.SetConfig(original); err != nil {
			t.Error(err)
		}
	})
	cfg := config.IPGeolocationPluginConfig{Enabled: true, APIKey: "test-key", DailyCreditLimit: 300, CacheHours: 168}
	settings := original
	settings.Plugins.IPGeolocation = cfg
	if err := config.SetConfig(settings); err != nil {
		t.Fatal(err)
	}
	if _, err := GetIPGeolocationState(ctx); err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`INSERT INTO proxies (host,ip_address,port,country,estimated_type,hash) SELECT '8.8.8.8','8.8.8.8',n,'Local','Residential',decode(md5(n::text),'hex') FROM generate_series(1,600)n`).Error; err != nil {
		t.Fatal(err)
	}
	workspace := domain.Workspace{Name: "IP intelligence pool"}
	if err := db.Create(&workspace).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`INSERT INTO user_proxies (workspace_id,proxy_id,state) SELECT ?,id,'active' FROM proxies`, workspace.ID).Error; err != nil {
		t.Fatal(err)
	}
	if count, err := DiscoverIPGeolocationIPs(ctx, cfg, now); err != nil || count != 256 {
		t.Fatal("discovery page not bounded", count, err)
	}
	var count int64
	db.Model(&domain.IPGeolocationLookup{}).Count(&count)
	if count != 1 {
		t.Fatal("duplicate IP lookups", count)
	}
	if reserved, _, err := ReserveIPGeolocationCredits(ctx, "8.8.8.8", now, cfg); err != nil || !reserved {
		t.Fatal(reserved, err)
	}
	var result ipgeolocation.Result
	if err := json.Unmarshal([]byte(`{"ip":"8.8.8.8","location":{"country_code2":"US","country_name":"United States"},"company":{"type":"HOSTING"}}`), &result); err != nil {
		t.Fatal(err)
	}
	if err := CompleteIPGeolocationLookup(ctx, "8.8.8.8", cfg, ipgeolocation.Response{Result: result, Status: 200, Credits: 1}, nil, now); err != nil {
		t.Fatal(err)
	}
	for i, want := range []int64{256, 512, 600} {
		worked, err := ApplyIPGeolocationBatch(ctx, cfg)
		if err != nil || !worked {
			t.Fatal(worked, err)
		}
		db.Model(&domain.Proxy{}).Where("estimated_type = ?", "Datacenter").Count(&count)
		if count != want {
			t.Fatalf("batch %d updated %d routes, want %d", i, count, want)
		}
	}
	if worked, err := ApplyIPGeolocationBatch(ctx, cfg); err != nil || worked {
		t.Fatal("reapplied settled projection", worked, err)
	}
	var proxies []domain.Proxy
	if err := db.Select("id", "host", "ip_address", "country", "estimated_type").Limit(2).Find(&proxies).Error; err != nil {
		t.Fatal(err)
	}
	for i := range proxies {
		proxies[i].Country = "Local"
		proxies[i].EstimatedType = "Residential"
	}
	if err := persistProxyMetadata(proxies); err != nil {
		t.Fatal(err)
	}
	db.Model(&domain.Proxy{}).Where("estimated_type = ?", "Datacenter").Count(&count)
	if count != 600 {
		t.Fatal("stale import overwrote provider data")
	}
	if err := applyProxyGeoUpdates(ctx, []proxyGeoUpdate{{ID: proxies[0].ID, Country: "Local", EstimatedType: "Residential"}}); err != nil {
		t.Fatal(err)
	}
	db.Model(&domain.Proxy{}).Where("estimated_type = ?", "Datacenter").Count(&count)
	if count != 600 {
		t.Fatal("stale geo refresh overwrote provider data")
	}
	// Existing local enrichment also uses the cached provider when GeoLite is off.
	candidates := EnrichProxiesWithCountryAndType(&proxies)
	if len(candidates) != 0 || proxies[0].EstimatedType != "Datacenter" || proxies[0].Country != "United States" {
		t.Fatal("cached override not applied", proxies, candidates)
	}
	updates, err := buildProxyGeoUpdates(ctx, proxies)
	if err != nil || len(updates) != 0 {
		t.Fatal("refresh disagrees with provider cache", updates, err)
	}
	queries := &checkerScaleQueryLogger{Interface: db.Logger}
	DB = db.Session(&gorm.Session{Logger: queries})
	settings.Plugins.IPGeolocation.Enabled = false
	if err := config.SetConfig(settings); err != nil {
		t.Fatal(err)
	}
	if rows, err := loadIPGeolocationOverrides(ctx, proxies); err != nil || len(rows) != 0 {
		t.Fatal(rows, err)
	}
	if len(queries.queries) != 0 {
		t.Fatal("disabled plugin added a query", queries.queries)
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
