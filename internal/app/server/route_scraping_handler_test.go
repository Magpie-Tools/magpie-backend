package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"magpie/internal/api/dto"
	"magpie/internal/auth"
	"magpie/internal/database"
	"magpie/internal/domain"
)

func TestRequeueScrapeSource_ReturnsQueuedSource(t *testing.T) {
	t.Setenv("JWT_SECRET", "unit-test-server-route-secret")
	originalDetail, originalForce := getScrapeSourceDetailForUser, forceScrapeSite
	t.Cleanup(func() { getScrapeSourceDetailForUser, forceScrapeSite = originalDetail, originalForce })
	getScrapeSourceDetailForUser = func(workspaceID uint, sourceID uint64) (*dto.ScrapeSiteDetail, error) {
		if workspaceID != 7 || sourceID != 42 {
			t.Fatalf("workspace=%d source=%d", workspaceID, sourceID)
		}
		return &dto.ScrapeSiteDetail{Id: 42, Url: "https://example.com/list"}, nil
	}
	var forced domain.ScrapeSite
	forceScrapeSite = func(site domain.ScrapeSite) error { forced = site; return nil }
	req := newAdminRequest(t, http.MethodPost, "/global/scrapeSources/42/requeue", 7)
	req.SetPathValue("id", "42")
	rec := httptest.NewRecorder()
	requeueScrapeSource(rec, req)
	if rec.Code != http.StatusOK || forced.ID != 42 || forced.URL != "https://example.com/list" {
		t.Fatalf("status=%d forced=%+v", rec.Code, forced)
	}
	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload["message"] != "Scrape source queued successfully" || payload["source_id"] != float64(42) {
		t.Fatalf("response=%v", payload)
	}
}

func TestRequeueScrapeSource_ReturnsNotFoundWhenSourceMissing(t *testing.T) {
	t.Setenv("JWT_SECRET", "unit-test-server-route-secret")
	original := getScrapeSourceDetailForUser
	t.Cleanup(func() { getScrapeSourceDetailForUser = original })
	getScrapeSourceDetailForUser = func(uint, uint64) (*dto.ScrapeSiteDetail, error) { return nil, nil }
	req := newAdminRequest(t, http.MethodPost, "/global/scrapeSources/404/requeue", 7)
	req.SetPathValue("id", "404")
	rec := httptest.NewRecorder()
	requeueScrapeSource(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status=%d", rec.Code)
	}
}

func TestRequeueScrapeSource_ReturnsServiceUnavailableOnQueueFailure(t *testing.T) {
	t.Setenv("JWT_SECRET", "unit-test-server-route-secret")
	originalDetail, originalForce := getScrapeSourceDetailForUser, forceScrapeSite
	t.Cleanup(func() { getScrapeSourceDetailForUser, forceScrapeSite = originalDetail, originalForce })
	getScrapeSourceDetailForUser = func(uint, uint64) (*dto.ScrapeSiteDetail, error) {
		return &dto.ScrapeSiteDetail{Id: 42, Url: "https://example.com/list"}, nil
	}
	forceScrapeSite = func(domain.ScrapeSite) error { return errors.New("Redis unavailable") }
	req := newAdminRequest(t, http.MethodPost, "/global/scrapeSources/42/requeue", 7)
	req.SetPathValue("id", "42")
	rec := httptest.NewRecorder()
	requeueScrapeSource(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d", rec.Code)
	}
}

func TestRequeueScrapeSource_ReturnsBadRequestOnInvalidID(t *testing.T) {
	t.Setenv("JWT_SECRET", "unit-test-server-route-secret")

	req := newAdminRequest(t, http.MethodPost, "/global/scrapeSources/not-a-number/requeue", 7)
	req.SetPathValue("id", "not-a-number")
	rec := httptest.NewRecorder()

	requeueScrapeSource(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status code = %d, want %d", rec.Code, http.StatusBadRequest)
	}

	var payload map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal error response: %v", err)
	}
	if payload["error"] != "Invalid scrape source id" {
		t.Fatalf("error = %q, want invalid id message", payload["error"])
	}
}

func newAdminRequest(t *testing.T, method, path string, userID uint) *http.Request {
	t.Helper()

	token, err := auth.GenerateJWT(userID, "admin")
	if err != nil {
		t.Fatalf("GenerateJWT failed: %v", err)
	}

	req := httptest.NewRequest(method, path, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	return req.WithContext(context.WithValue(req.Context(), workspaceContextKey{}, database.WorkspaceAccess{
		WorkspaceID: userID,
		UserID:      userID,
		Role:        domain.WorkspaceRoleOwner,
	}))
}

func TestUpdateScrapeSourceSettingsValidatesModeAndWorkspace(t *testing.T) {
	t.Setenv("JWT_SECRET", "unit-test-server-route-secret")
	original := updateScrapeSourceSubscriptionSettings
	t.Cleanup(func() { updateScrapeSourceSubscriptionSettings = original })
	for _, tc := range []struct {
		body  string
		found bool
		want  int
	}{
		{`{"fetch_mode":"http"}`, true, http.StatusOK},
		{`{"fetch_mode":"browser"}`, true, http.StatusOK},
		{`{"auto_tag_ids":[1,2]}`, true, http.StatusOK},
		{`{"auto_tag_ids":[]}`, true, http.StatusOK},
		{`{"auto_tag_ids":[0]}`, true, http.StatusBadRequest},
		{`{"auto_tag_ids":[-1]}`, true, http.StatusBadRequest},
		{`{"auto_tag_ids":"1"}`, true, http.StatusBadRequest},
		{`{"fetch_mode":"auto"}`, true, http.StatusBadRequest},
		{`{}`, true, http.StatusBadRequest},
		{`{"fetch_mode":"http"}`, false, http.StatusNotFound},
	} {
		called := false
		updateScrapeSourceSubscriptionSettings = func(ctx context.Context, workspaceID uint, siteID uint64, settings dto.ScrapeSourceSettings) (bool, error) {
			called = true
			if workspaceID != 7 || siteID != 42 {
				t.Fatalf("wrong update scope: %d/%d", workspaceID, siteID)
			}
			return tc.found, nil
		}
		req := newAdminRequest(t, http.MethodPatch, "/api/scrapingSources/42", 7)
		req.Body = io.NopCloser(strings.NewReader(tc.body))
		req.SetPathValue("id", "42")
		rec := httptest.NewRecorder()
		updateScrapeSourceSettings(rec, req)
		if rec.Code != tc.want {
			t.Fatalf("body=%s code=%d want=%d response=%s", tc.body, rec.Code, tc.want, rec.Body.String())
		}
		if tc.want == http.StatusBadRequest && called {
			t.Fatal("invalid settings reached database")
		}
	}
}

func TestUpdateScrapeSourceSettingsRejectsForeignTags(t *testing.T) {
	t.Setenv("JWT_SECRET", "unit-test-server-route-secret")
	original := updateScrapeSourceSubscriptionSettings
	t.Cleanup(func() { updateScrapeSourceSubscriptionSettings = original })
	updateScrapeSourceSubscriptionSettings = func(context.Context, uint, uint64, dto.ScrapeSourceSettings) (bool, error) {
		return false, database.ErrProxyTagNotFound
	}
	req := newAdminRequest(t, http.MethodPatch, "/api/scrapingSources/42", 7)
	req.Body = io.NopCloser(strings.NewReader(`{"auto_tag_ids":[9]}`))
	req.SetPathValue("id", "42")
	rec := httptest.NewRecorder()
	updateScrapeSourceSettings(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestScrapeSourceDetailExposesScheduleAndToleratesRedisOutage(t *testing.T) {
	t.Setenv("JWT_SECRET", "unit-test-server-route-secret")
	originalDetail, originalSchedule := getScrapeSourceDetailForUser, getScrapeSchedule
	t.Cleanup(func() { getScrapeSourceDetailForUser, getScrapeSchedule = originalDetail, originalSchedule })
	getScrapeSourceDetailForUser = func(uint, uint64) (*dto.ScrapeSiteDetail, error) {
		return &dto.ScrapeSiteDetail{Id: 42, Url: "https://example.com/list", FetchMode: "http"}, nil
	}
	next := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	for _, unavailable := range []bool{false, true} {
		getScrapeSchedule = func(ctx context.Context, url, mode string) (time.Time, string, error) {
			if url != "https://example.com/list" || mode != "http" {
				t.Fatalf("url=%s mode=%s", url, mode)
			}
			if unavailable {
				return time.Time{}, "", errors.New("Redis unavailable")
			}
			return next, "Provider requested a delay with Retry-After", nil
		}
		req := newAdminRequest(t, http.MethodGet, "/scrapingSources/42", 7)
		req.SetPathValue("id", "42")
		rec := httptest.NewRecorder()
		getScrapeSourceDetail(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
		var detail dto.ScrapeSiteDetail
		if err := json.Unmarshal(rec.Body.Bytes(), &detail); err != nil {
			t.Fatal(err)
		}
		if unavailable {
			if detail.NextScrapeAt != nil {
				t.Fatal("invented a schedule during Redis outage")
			}
		} else if detail.NextScrapeAt == nil || !detail.NextScrapeAt.Equal(next) || detail.ScrapeScheduleReason != "Provider requested a delay with Retry-After" {
			t.Fatalf("detail=%+v", detail)
		}
	}
}
