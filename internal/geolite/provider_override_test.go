package geolite

import (
	"context"
	"encoding/json"
	"gorm.io/gorm"
	"testing"
	"time"

	"magpie/internal/config"
	"magpie/internal/database"
	"magpie/internal/domain"
	"magpie/internal/ipgeolocation"
	"magpie/internal/platform/pg"
)

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

func TestIPGeolocationProjectionIsBoundedAndAuthoritativePostgres(t *testing.T) {
	db := setupGeoLitePostgres(t)
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
	if _, err := database.GetIPGeolocationState(ctx); err != nil {
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
	if count, err := database.DiscoverIPGeolocationIPs(ctx, cfg, now); err != nil || count != 256 {
		t.Fatal("discovery page not bounded", count, err)
	}
	var count int64
	db.Model(&domain.IPGeolocationLookup{}).Count(&count)
	if count != 1 {
		t.Fatal("duplicate IP lookups", count)
	}
	if reserved, _, err := database.ReserveIPGeolocationCredits(ctx, "8.8.8.8", now, cfg); err != nil || !reserved {
		t.Fatal(reserved, err)
	}
	var result ipgeolocation.Result
	if err := json.Unmarshal([]byte(`{"ip":"8.8.8.8","location":{"country_code2":"US","country_name":"United States"},"company":{"type":"HOSTING"}}`), &result); err != nil {
		t.Fatal(err)
	}
	if err := database.CompleteIPGeolocationLookup(ctx, "8.8.8.8", cfg, ipgeolocation.Response{Result: result, Status: 200, Credits: 1}, nil, now); err != nil {
		t.Fatal(err)
	}
	for i, want := range []int64{256, 512, 600} {
		worked, err := database.ApplyIPGeolocationBatch(ctx, cfg)
		if err != nil || !worked {
			t.Fatal(worked, err)
		}
		db.Model(&domain.Proxy{}).Where("estimated_type = ?", "Datacenter").Count(&count)
		if count != want {
			t.Fatalf("batch %d updated %d routes, want %d", i, count, want)
		}
	}
	if worked, err := database.ApplyIPGeolocationBatch(ctx, cfg); err != nil || worked {
		t.Fatal("reapplied settled projection", worked, err)
	}
	// A longer cache duration also applies before discovery updates the stored
	// expiration and schedule for a result fetched under the shorter policy.
	checked := now.Add(-48 * time.Hour)
	expired := checked.Add(24 * time.Hour)
	if err := db.Model(&domain.IPGeolocationLookup{}).Where("ip = ?", "8.8.8.8").Updates(map[string]any{
		"checked_at": checked, "expires_at": expired,
	}).Error; err != nil {
		t.Fatal(err)
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
	if err := applyProxyGeoUpdates(ctx, []database.ProxyMetadataUpdate{{ID: proxies[0].ID, Country: "Local", EstimatedType: "Residential"}}); err != nil {
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
	pg.DB = db.Session(&gorm.Session{Logger: queries})
	settings.Plugins.IPGeolocation.Enabled = false
	if err := config.SetConfig(settings); err != nil {
		t.Fatal(err)
	}
	if rows, err := database.LoadIPGeolocationOverrides(ctx, proxies); err != nil || len(rows) != 0 {
		t.Fatal(rows, err)
	}
	if len(queries.queries) != 0 {
		t.Fatal("disabled plugin added a query", queries.queries)
	}
}
