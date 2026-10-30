package runtime

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"magpie/internal/config"
	"magpie/internal/database"
	"magpie/internal/domain"
	"magpie/internal/ipgeolocation"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type ipGeoTransport func(*http.Request) (*http.Response, error)

func (f ipGeoTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func setIPGeoRuntimeConfig(t *testing.T, cfg config.IPGeolocationPluginConfig) {
	t.Helper()
	withTempRuntimeWorkingDir(t)
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

func TestIPGeolocationDisabledDoesNotTouchDatabase(t *testing.T) {
	setIPGeoRuntimeConfig(t, config.IPGeolocationPluginConfig{})
	original := database.DB
	database.DB = nil
	t.Cleanup(func() { database.DB = original })
	client := ipgeolocation.Client{HTTPClient: &http.Client{Transport: ipGeoTransport(func(*http.Request) (*http.Response, error) {
		t.Fatal("disabled plugin made an API request")
		return nil, nil
	})}}
	if wait, err := runIPGeolocationOnce(context.Background(), client, time.Now().UTC()); err != nil || wait != time.Minute {
		t.Fatal(wait, err)
	}
}

func setupIPGeoRuntimeDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, _ := db.DB()
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = sqlDB.Close() })
	if err := db.AutoMigrate(&domain.IPGeolocationLookup{}, &domain.IPGeolocationState{}); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		`CREATE TABLE proxies (id INTEGER PRIMARY KEY,host TEXT,ip_address TEXT)`,
		`CREATE INDEX proxies_ip ON proxies(ip_address)`,
		`CREATE TABLE user_proxies (proxy_id INTEGER,workspace_id INTEGER,state TEXT)`,
		`CREATE INDEX user_proxies_proxy_state ON user_proxies(proxy_id,state)`,
	} {
		if err := db.Exec(query).Error; err != nil {
			t.Fatal(err)
		}
	}
	original := database.DB
	database.DB = db
	t.Cleanup(func() { database.DB = original })
	return db
}

func TestIPGeolocationWorkerDiscoversAndCachesWithoutRepeatRequests(t *testing.T) {
	setIPGeoRuntimeConfig(t, config.IPGeolocationPluginConfig{Enabled: true, APIKey: "never-log-key", DailyCreditLimit: 300, CacheHours: 24})
	db := setupIPGeoRuntimeDB(t)
	for _, query := range []string{
		`INSERT INTO proxies VALUES(1,'8.8.8.8','8.8.8.8'),(2,'8.8.8.8','8.8.8.8'),(3,'provider.example',NULL)`,
		`INSERT INTO user_proxies VALUES(1,1,'active'),(2,1,'active')`,
	} {
		if err := db.Exec(query).Error; err != nil {
			t.Fatal(err)
		}
	}
	requests := 0
	client := ipgeolocation.Client{HTTPClient: &http.Client{Transport: ipGeoTransport(func(*http.Request) (*http.Response, error) {
		requests++
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"ip":"8.8.8.8","location":{"country_code2":"US"},"company":{"type":"HOSTING"}}`))}, nil
	})}}
	ctx := context.Background()
	if _, err := runIPGeolocationOnce(ctx, client, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if requests != 1 {
		t.Fatal("shared IP was not fetched exactly once", requests)
	}
	var cached domain.IPGeolocationLookup
	if err := db.First(&cached, "ip = ?", "8.8.8.8").Error; err != nil {
		t.Fatal(err)
	}
	if cached.EstimatedType != "Datacenter" || cached.CheckedAt == nil || !cached.ApplyPending {
		t.Fatal("provider result was not cached", cached)
	}
	state, err := database.GetIPGeolocationState(ctx)
	if err != nil || state.UsedCredits != 1 || state.LastError != "" {
		t.Fatal(state, err)
	}
	// Projection is exercised against PostgreSQL by database package tests.
	if err := db.Model(&cached).Update("apply_pending", false).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := runIPGeolocationOnce(ctx, client, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if requests != 1 {
		t.Fatal("pacing was ignored")
	}
	if err := db.Model(&domain.IPGeolocationState{}).Where("id = 1").Update("next_request_at", nil).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := runIPGeolocationOnce(ctx, client, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if requests != 1 {
		t.Fatal("fresh cache was fetched again")
	}
}

func TestIPGeolocationWorkerDrainsPausedAndOrphanedIPsBeforeRequest(t *testing.T) {
	cfg := config.IPGeolocationPluginConfig{Enabled: true, APIKey: "test-key", DailyCreditLimit: 300, CacheHours: 24}
	setIPGeoRuntimeConfig(t, cfg)
	db := setupIPGeoRuntimeDB(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	discoverAt := now.Add(time.Minute)
	if err := db.Create(&domain.IPGeolocationState{ID: 1, Policy: database.IPGeolocationPolicy(cfg), NextDiscoveryAt: &discoverAt}).Error; err != nil {
		t.Fatal(err)
	}
	const pausedCount = 10000
	var lookups []domain.IPGeolocationLookup
	type queuedProxy struct {
		ID        uint64
		Host      string
		IPAddress string
	}
	type queuedOwner struct {
		ProxyID     uint64
		WorkspaceID uint
		State       string
	}
	var proxies []queuedProxy
	var managed []queuedOwner
	for i := range pausedCount {
		ip := fmt.Sprintf("11.%d.%d.%d", i>>16, (i>>8)&255, i&255)
		proxies = append(proxies, queuedProxy{uint64(i + 1), ip, ip})
		managed = append(managed, queuedOwner{uint64(i + 1), 1, "paused"})
		lookups = append(lookups, domain.IPGeolocationLookup{IP: ip, NextLookupAt: now.Add(-time.Hour), Policy: database.IPGeolocationPolicy(cfg)})
	}
	for i := range 300 {
		lookups = append(lookups, domain.IPGeolocationLookup{IP: fmt.Sprintf("12.0.%d.%d", i>>8, i&255), NextLookupAt: now.Add(-2 * time.Hour)})
	}
	lookups = append(lookups, domain.IPGeolocationLookup{IP: "9.9.9.9", NextLookupAt: now, Policy: database.IPGeolocationPolicy(cfg)})
	if err := db.Table("proxies").CreateInBatches(proxies, 256).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Table("user_proxies").CreateInBatches(managed, 256).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.CreateInBatches(lookups, 256).Error; err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		`INSERT INTO proxies VALUES(10001,'9.9.9.9','9.9.9.9'),(10002,'9.9.9.9','9.9.9.9')`,
		`INSERT INTO user_proxies VALUES(10001,1,'paused'),(10002,1,'active')`,
	} {
		if err := db.Exec(query).Error; err != nil {
			t.Fatal(err)
		}
	}
	queries := 0
	if err := db.Callback().Query().After("gorm:query").Register("test:ipgeolocation_queue_queries", func(*gorm.DB) { queries++ }); err != nil {
		t.Fatal(err)
	}
	requests := 0
	client := ipgeolocation.Client{HTTPClient: &http.Client{Transport: ipGeoTransport(func(req *http.Request) (*http.Response, error) {
		requests++
		if ip := req.URL.Query().Get("ip"); ip != "9.9.9.9" {
			t.Fatalf("requested inactive IP %s", ip)
		}
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"ip":"9.9.9.9","location":{"country_code2":"US"}}`))}, nil
	})}}
	started := time.Now()
	if _, err := runIPGeolocationOnce(context.Background(), client, now); err != nil {
		t.Fatal(err)
	}
	if requests != 1 || queries > 50 {
		t.Fatalf("one worker pass made %d requests and %d database reads", requests, queries)
	}
	var count int64
	if err := db.Model(&domain.IPGeolocationLookup{}).Where("next_lookup_at = ?", now.Add(time.Hour)).Count(&count).Error; err != nil || count != pausedCount {
		t.Fatal("paused IPs were not deferred together", count, err)
	}
	if err := db.Model(&domain.IPGeolocationLookup{}).Where("ip LIKE '12.%'").Count(&count).Error; err != nil || count != 0 {
		t.Fatal("orphan IPs were not removed", count, err)
	}
	state, err := database.GetIPGeolocationState(context.Background())
	if err != nil || state.UsedCredits != 1 {
		t.Fatal("cleanup spent provider credits", state, err)
	}
	t.Logf("active request after 10000 paused and 300 orphan IPs completed in %s with %d database reads", time.Since(started), queries)
}
