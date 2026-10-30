package runtime

import (
	"context"
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

func TestIPGeolocationWorkerDiscoversAndCachesWithoutRepeatRequests(t *testing.T) {
	setIPGeoRuntimeConfig(t, config.IPGeolocationPluginConfig{Enabled: true, APIKey: "never-log-key", DailyCreditLimit: 300, CacheHours: 24})
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
		`CREATE TABLE user_proxies (proxy_id INTEGER,workspace_id INTEGER,state TEXT)`,
		`INSERT INTO proxies VALUES(1,'8.8.8.8','8.8.8.8'),(2,'8.8.8.8','8.8.8.8'),(3,'provider.example',NULL)`,
		`INSERT INTO user_proxies VALUES(1,1,'active'),(2,1,'active')`,
	} {
		if err := db.Exec(query).Error; err != nil {
			t.Fatal(err)
		}
	}
	original := database.DB
	database.DB = db
	t.Cleanup(func() { database.DB = original })
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
