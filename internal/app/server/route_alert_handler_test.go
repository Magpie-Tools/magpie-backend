package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"magpie/internal/database"
	"magpie/internal/domain"
)

func TestAlertAPIRolesIsolationAndWriteOnlyDestinations(t *testing.T) {
	setupUserRegistrationTestDB(t)
	t.Setenv("JWT_SECRET", "alert-api-test-secret")
	t.Setenv("AUTH_REVOCATION_FAIL_OPEN", "true")
	t.Setenv("PROXY_ENCRYPTION_KEY", "alert-destination-test-key")
	if err := database.DB.AutoMigrate(&domain.AlertRule{}, &domain.AlertDestination{}, &domain.AlertIncident{}, &domain.AlertDelivery{}, &domain.RotatingProxy{}); err != nil {
		t.Fatal(err)
	}
	owner, workspace := createWorkspaceMiddlewareTestUser(t, "alert-owner@example.test")
	mux := http.NewServeMux()
	registerAlertRoutes(mux)
	request := func(userID uint, method, path, body string) *httptest.ResponseRecorder {
		t.Helper()
		base := workspaceMiddlewareTestRequest(t, userID, strconv.FormatUint(uint64(workspace.ID), 10))
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		r.Header = base.Header.Clone()
		r.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, r)
		return response
	}
	for _, role := range []string{domain.WorkspaceRoleViewer, domain.WorkspaceRoleOperator, domain.WorkspaceRoleAdmin} {
		user, _ := createWorkspaceMiddlewareTestUser(t, "alert-"+role+"@example.test")
		if _, err := database.AddWorkspaceMember(workspace.ID, user.Email, role, false); err != nil {
			t.Fatal(err)
		}
		response := request(user.ID, http.MethodGet, "/alerts", "")
		if response.Code != http.StatusOK {
			t.Fatalf("%s read: %d %s", role, response.Code, response.Body.String())
		}
		response = request(user.ID, http.MethodGet, "/alerts/rotators", "")
		if response.Code != http.StatusOK || strings.TrimSpace(response.Body.String()) != "[]" {
			t.Fatalf("%s metadata read: %d %s", role, response.Code, response.Body.String())
		}
		response = request(user.ID, http.MethodPost, "/alerts/rules", `{"name":"Minimum routes","metric":"usable_routes","threshold":50,"enabled":true}`)
		want := http.StatusCreated
		if role == domain.WorkspaceRoleViewer {
			want = http.StatusForbidden
		}
		if response.Code != want {
			t.Fatalf("%s rule write: %d %s", role, response.Code, response.Body.String())
		}
		response = request(user.ID, http.MethodPost, "/alerts/destinations", `{"name":"Ops","kind":"email","target":"secret-team@example.com","enabled":true}`)
		want = http.StatusForbidden
		if role == domain.WorkspaceRoleAdmin {
			want = http.StatusCreated
		}
		if response.Code != want {
			t.Fatalf("%s destination write: %d %s", role, response.Code, response.Body.String())
		}
		if strings.Contains(response.Body.String(), "secret-team") || strings.Contains(response.Body.String(), "encrypted") {
			t.Fatal("API returned a destination secret")
		}
	}
	response := request(owner.ID, http.MethodPost, "/alerts/rules", `{"name":"Missing threshold","metric":"success_rate","enabled":true}`)
	if response.Code != http.StatusBadRequest {
		t.Fatal("omitted threshold accepted")
	}
	outsider, _ := createWorkspaceMiddlewareTestUser(t, "alert-outsider@example.test")
	response = request(outsider.ID, http.MethodGet, "/alerts", "")
	if response.Code != http.StatusForbidden {
		t.Fatal("outsider can read alerts")
	}
	response = request(outsider.ID, http.MethodGet, "/alerts/rotators", "")
	if response.Code != http.StatusForbidden {
		t.Fatal("outsider can read rotator metadata")
	}
	response = request(owner.ID, http.MethodGet, "/alerts?before=-1", "")
	if response.Code != http.StatusBadRequest {
		t.Fatal("invalid pagination cursor accepted")
	}
	response = request(owner.ID, http.MethodGet, "/alerts", "")
	var body struct {
		Destinations []map[string]any `json:"destinations"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Destinations) != 1 || strings.Contains(response.Body.String(), "secret-team") {
		t.Fatal("read endpoint leaked destination or returned wrong data")
	}
}
