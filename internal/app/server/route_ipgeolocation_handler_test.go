package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"magpie/internal/auth"
	"magpie/internal/config"
	"magpie/internal/database"
	"magpie/internal/domain"
)

func TestIPGeolocationStatusRequiresAdminAndDoesNotExposeCredentials(t *testing.T) {
	setupUserRegistrationTestDB(t)
	withTempServerWorkingDir(t)
	t.Setenv("JWT_SECRET", "ipgeo-status-test-secret")
	t.Setenv("AUTH_REVOCATION_FAIL_OPEN", "true")
	if err := database.DB.AutoMigrate(&domain.IPGeolocationState{}); err != nil {
		t.Fatal(err)
	}
	if err := database.DB.Create(&domain.IPGeolocationState{ID: 1, BudgetDay: time.Now().UTC().Add(-24 * time.Hour).Format("2006-01-02"), UsedCredits: 999}).Error; err != nil {
		t.Fatal(err)
	}
	original := config.GetConfig()
	t.Cleanup(func() {
		if err := config.SetConfig(original); err != nil {
			t.Error(err)
		}
	})
	cfg := original
	cfg.Plugins.IPGeolocation = config.IPGeolocationPluginConfig{APIKey: "never-include-in-status", DailyCreditLimit: 300, CacheHours: 168}
	if err := config.SetConfig(cfg); err != nil {
		t.Fatal(err)
	}
	for _, role := range []string{"", "user", "admin"} {
		req := httptest.NewRequest(http.MethodGet, "/api/plugins/ipgeolocation/status", nil)
		if role != "" {
			token, err := auth.GenerateJWT(1, role)
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Authorization", "Bearer "+token)
		}
		rec := httptest.NewRecorder()
		auth.IsAdmin(http.HandlerFunc(getIPGeolocationStatus)).ServeHTTP(rec, req)
		want := map[string]int{"": 401, "user": 403, "admin": 200}[role]
		if rec.Code != want {
			t.Fatalf("role %q: %d, want %d: %s", role, rec.Code, want, rec.Body.String())
		}
		if strings.Contains(rec.Body.String(), cfg.Plugins.IPGeolocation.APIKey) {
			t.Fatal("status exposed the key")
		}
		if role == "admin" {
			var body struct {
				Used  int `json:"used_credits"`
				Limit int `json:"daily_credit_limit"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if body.Used != 0 || body.Limit != 300 {
				t.Fatal("stale budget was displayed", body)
			}
		}
	}
}

func TestIPGeolocationSettingsRejectInvalidPolicyBeforePersistence(t *testing.T) {
	original := config.GetConfig()
	for _, cfg := range []config.IPGeolocationPluginConfig{
		{Enabled: true, DailyCreditLimit: 0, CacheHours: 24},
		{Enabled: true, DailyCreditLimit: 1, CacheHours: 0},
		{Enabled: true, DailyCreditLimit: 100001, CacheHours: 24},
		{Enabled: true, DailyCreditLimit: 300, CacheHours: 721},
		{Enabled: true, DailyCreditLimit: 1, CacheHours: 24, IncludeSecurity: true},
	} {
		settings := original
		settings.Plugins.IPGeolocation = cfg
		body, err := json.Marshal(settings)
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, "/api/saveSettings", bytes.NewReader(body))
		rec := httptest.NewRecorder()
		saveSettings(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatal("invalid policy accepted", rec.Code, rec.Body.String())
		}
		if config.GetConfig().Plugins.IPGeolocation != original.Plugins.IPGeolocation {
			t.Fatal("invalid policy changed the configuration")
		}
	}
}
