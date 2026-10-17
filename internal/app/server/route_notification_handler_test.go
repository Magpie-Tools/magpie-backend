package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"magpie/internal/auth"
	"magpie/internal/database"
	"magpie/internal/domain"
)

func TestNotificationReadAPIIsolationPersistenceAndAccess(t *testing.T) {
	setupUserRegistrationTestDB(t)
	t.Setenv("JWT_SECRET", "notification-api-test-secret")
	t.Setenv("AUTH_REVOCATION_FAIL_OPEN", "true")
	if err := database.DB.AutoMigrate(&domain.NotificationRead{}, &domain.AlertIncident{}); err != nil {
		t.Fatal(err)
	}
	owner, workspace := createWorkspaceMiddlewareTestUser(t, "notification-owner@example.test")
	reader, _ := createWorkspaceMiddlewareTestUser(t, "notification-reader@example.test")
	outsider, _ := createWorkspaceMiddlewareTestUser(t, "notification-outsider@example.test")
	if _, err := database.AddWorkspaceMember(workspace.ID, reader.Email, domain.WorkspaceRoleViewer, false); err != nil {
		t.Fatal(err)
	}
	invitation, err := database.CreateWorkspaceInvitation(workspace.ID, owner.ID, outsider.Email, domain.WorkspaceRoleViewer, false, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	incident := domain.AlertIncident{WorkspaceID: workspace.ID, RuleID: 1, RuleRevision: 1, RuleName: "Minimum routes", ScopeName: "Production", Metric: domain.AlertMetricUsableRoutes, OpenedAt: time.Now()}
	if err := database.DB.Create(&incident).Error; err != nil {
		t.Fatal(err)
	}
	alertID := "alert-" + strconv.FormatUint(incident.ID, 10)
	invitationID := "invitation-" + strconv.FormatUint(uint64(invitation.ID), 10)
	newSession := func(userID uint) string {
		t.Helper()
		token, err := auth.GenerateJWT(userID, "user")
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	readerBrowserOne, readerBrowserTwo := newSession(reader.ID), newSession(reader.ID)
	outsiderSession := newSession(outsider.ID)
	mux := http.NewServeMux()
	registerNotificationRoutes(mux)
	request := func(token, method, path string, want int) *httptest.ResponseRecorder {
		t.Helper()
		r := httptest.NewRequest(method, path, nil)
		if token != "" {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		// Account read state must not depend on a browser's workspace selection.
		r.Header.Set("X-Workspace-ID", "999999")
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, r)
		if response.Code != want {
			t.Fatalf("%s %s = %d: %s, want %d", method, path, response.Code, response.Body.String(), want)
		}
		return response
	}
	statePath := "/notifications/read-state?ids=" + url.QueryEscape(strings.Join([]string{alertID, invitationID, "release-42"}, ","))
	readIDs := func(token string) []string {
		t.Helper()
		response := request(token, http.MethodGet, statePath, http.StatusOK)
		if response.Header().Get("Cache-Control") != "no-store" {
			t.Fatal("read state must not be cached between accounts")
		}
		var body struct {
			ReadIDs []string `json:"read_ids"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		return body.ReadIDs
	}
	request("", http.MethodGet, statePath, http.StatusUnauthorized)
	request("", http.MethodPut, "/notifications/release-42/read", http.StatusUnauthorized)
	if got := readIDs(readerBrowserOne); len(got) != 0 {
		t.Fatalf("new account already has receipts: %v", got)
	}
	request(readerBrowserOne, http.MethodPut, "/notifications/"+alertID+"/read", http.StatusNoContent)
	request(readerBrowserOne, http.MethodPut, "/notifications/release-42/read", http.StatusNoContent)
	// Another browser and a new server router see the same stored receipts.
	mux = http.NewServeMux()
	registerNotificationRoutes(mux)
	if got := readIDs(readerBrowserTwo); !reflect.DeepEqual(got, []string{alertID, "release-42"}) {
		t.Fatalf("second session read IDs = %v", got)
	}
	if got := readIDs(outsiderSession); len(got) != 0 {
		t.Fatalf("another account inherited read state: %v", got)
	}
	request(outsiderSession, http.MethodPut, "/notifications/"+alertID+"/read", http.StatusNotFound)
	request(readerBrowserOne, http.MethodPut, "/notifications/"+invitationID+"/read", http.StatusNotFound)
	request(outsiderSession, http.MethodPut, "/notifications/"+invitationID+"/read", http.StatusNoContent)
	request(readerBrowserTwo, http.MethodPut, "/notifications/"+alertID+"/read", http.StatusNoContent)
	var count int64
	if err := database.DB.Model(&domain.NotificationRead{}).Where("user_id = ? AND notification_id = ?", reader.ID, alertID).Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("duplicate receipts: %d, %v", count, err)
	}
	// Reading an invitation neither accepts nor deletes it.
	if err := database.DB.Model(&domain.WorkspaceInvitation{}).Where("id = ?", invitation.ID).Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("read changed invitation: %d, %v", count, err)
	}
	for _, invalid := range []string{"unknown-1", "alert-0", "alert-01", "release--1", "release-9223372036854775808"} {
		request(readerBrowserOne, http.MethodPut, "/notifications/"+invalid+"/read", http.StatusBadRequest)
		request(readerBrowserOne, http.MethodGet, "/notifications/read-state?ids="+url.QueryEscape(invalid), http.StatusBadRequest)
	}
	tooMany := strings.TrimSuffix(strings.Repeat("release-1,", 51), ",")
	request(readerBrowserOne, http.MethodGet, "/notifications/read-state?ids="+url.QueryEscape(tooMany), http.StatusBadRequest)
	request(readerBrowserOne, http.MethodPut, "/notifications/alert-999999/read", http.StatusNotFound)
	request(readerBrowserOne, http.MethodGet, "/notifications/read-state", http.StatusOK)
	if err := database.DB.Migrator().DropTable(&domain.NotificationRead{}); err != nil {
		t.Fatal(err)
	}
	request(readerBrowserOne, http.MethodGet, statePath, http.StatusInternalServerError)
	request(readerBrowserOne, http.MethodPut, "/notifications/release-42/read", http.StatusInternalServerError)
}
