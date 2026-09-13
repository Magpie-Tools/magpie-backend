package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGetUserProfile(t *testing.T) {
	setupUserRegistrationTestDB(t)
	t.Setenv("JWT_SECRET", "profile-test-secret")
	t.Setenv("AUTH_REVOCATION_FAIL_OPEN", "true")
	user, _ := createWorkspaceMiddlewareTestUser(t, "profile@example.test")
	createWorkspaceMiddlewareTestUser(t, "other@example.test")

	t.Run("returns only the authenticated account email and role", func(t *testing.T) {
		request := workspaceMiddlewareTestRequest(t, user.ID, "")
		recorder := httptest.NewRecorder()
		getUserProfile(recorder, request)
		if recorder.Code != http.StatusOK {
			t.Fatalf("status = %d: %s", recorder.Code, recorder.Body.String())
		}
		var profile map[string]string
		if err := json.Unmarshal(recorder.Body.Bytes(), &profile); err != nil {
			t.Fatal(err)
		}
		if len(profile) != 2 || profile["email"] != user.Email || profile["role"] != user.Role {
			t.Fatalf("unexpected profile: %v", profile)
		}
	})
	t.Run("rejects unauthenticated requests", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		getUserProfile(recorder, httptest.NewRequest(http.MethodGet, "/api/user/profile", nil))
		if recorder.Code != http.StatusUnauthorized {
			t.Fatalf("status = %d", recorder.Code)
		}
	})
	t.Run("handles missing accounts", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		getUserProfile(recorder, workspaceMiddlewareTestRequest(t, user.ID+1000, ""))
		if recorder.Code != http.StatusNotFound {
			t.Fatalf("status = %d", recorder.Code)
		}
	})
}
