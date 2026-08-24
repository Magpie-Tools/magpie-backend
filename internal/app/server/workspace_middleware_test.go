package server

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"magpie/internal/auth"
	"magpie/internal/database"
	"magpie/internal/domain"
)

func TestResolveRequestWorkspaceAccessUsesDefaultAndExplicitMembership(t *testing.T) {
	setupUserRegistrationTestDB(t)
	t.Setenv("JWT_SECRET", "workspace-middleware-test-secret")
	t.Setenv("AUTH_REVOCATION_FAIL_OPEN", "true")

	user, personal := createWorkspaceMiddlewareTestUser(t, "member@example.test")
	shared, err := database.CreateWorkspace(user.ID, "Shared workspace")
	if err != nil {
		t.Fatalf("create shared workspace: %v", err)
	}
	outsider, outsiderWorkspace := createWorkspaceMiddlewareTestUser(t, "outsider@example.test")

	request := workspaceMiddlewareTestRequest(t, user.ID, "")
	access, err := resolveRequestWorkspaceAccess(request)
	if err != nil {
		t.Fatalf("resolve default workspace: %v", err)
	}
	if access.WorkspaceID != personal.ID || !access.IsDefault || !access.IsOwner() {
		t.Fatalf("default access = %#v, want personal owner membership", access)
	}

	request = workspaceMiddlewareTestRequest(t, user.ID, strconv.FormatUint(uint64(shared.ID), 10))
	access, err = resolveRequestWorkspaceAccess(request)
	if err != nil {
		t.Fatalf("resolve explicit workspace: %v", err)
	}
	if access.WorkspaceID != shared.ID || !access.IsOwner() {
		t.Fatalf("explicit access = %#v, want shared owner membership", access)
	}

	request = workspaceMiddlewareTestRequest(t, user.ID, strconv.FormatUint(uint64(outsiderWorkspace.ID), 10))
	if _, err = resolveRequestWorkspaceAccess(request); !errors.Is(err, database.ErrWorkspaceNotFound) {
		t.Fatalf("outsider workspace error = %v, want membership failure", err)
	}

	request = workspaceMiddlewareTestRequest(t, outsider.ID, "not-an-id")
	if _, err = resolveRequestWorkspaceAccess(request); !errors.Is(err, errInvalidWorkspaceHeader) {
		t.Fatalf("malformed workspace header error = %v", err)
	}
}

func TestWorkspaceRoleMiddlewareSeparatesReadAndOperatePermissions(t *testing.T) {
	setupUserRegistrationTestDB(t)
	t.Setenv("JWT_SECRET", "workspace-role-test-secret")
	t.Setenv("AUTH_REVOCATION_FAIL_OPEN", "true")

	owner, workspace := createWorkspaceMiddlewareTestUser(t, "owner@example.test")
	viewer, _ := createWorkspaceMiddlewareTestUser(t, "viewer@example.test")
	if _, err := database.AddWorkspaceMember(workspace.ID, viewer.Email, domain.WorkspaceRoleViewer, false); err != nil {
		t.Fatalf("add viewer: %v", err)
	}

	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	})

	request := workspaceMiddlewareTestRequest(t, viewer.ID, strconv.FormatUint(uint64(workspace.ID), 10))
	recorder := httptest.NewRecorder()
	withWorkspaceOperator(next).ServeHTTP(recorder, request)
	if recorder.Code != http.StatusForbidden || called {
		t.Fatalf("viewer operate result = status %d called %v, want 403/false", recorder.Code, called)
	}

	request = workspaceMiddlewareTestRequest(t, viewer.ID, strconv.FormatUint(uint64(workspace.ID), 10))
	recorder = httptest.NewRecorder()
	withWorkspaceViewer(next).ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNoContent || !called {
		t.Fatalf("viewer read result = status %d called %v, want 204/true", recorder.Code, called)
	}

	called = false
	request = workspaceMiddlewareTestRequest(t, owner.ID, strconv.FormatUint(uint64(workspace.ID), 10))
	recorder = httptest.NewRecorder()
	withWorkspaceOperator(next).ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNoContent || !called {
		t.Fatalf("owner operate result = status %d called %v, want 204/true", recorder.Code, called)
	}
}

func createWorkspaceMiddlewareTestUser(t *testing.T, email string) (domain.User, domain.Workspace) {
	t.Helper()
	user := domain.User{Email: email, Password: "hash", Role: "user"}
	if err := database.DB.Create(&user).Error; err != nil {
		t.Fatalf("create user %s: %v", email, err)
	}
	workspace, err := database.CreatePersonalWorkspaceForUser(database.DB, user)
	if err != nil {
		t.Fatalf("create personal workspace for %s: %v", email, err)
	}
	return user, workspace
}

func workspaceMiddlewareTestRequest(t *testing.T, userID uint, workspaceID string) *http.Request {
	t.Helper()
	token, err := auth.GenerateJWT(userID, "user")
	if err != nil {
		t.Fatalf("generate JWT: %v", err)
	}
	request := httptest.NewRequest(http.MethodGet, "/api/resource", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	if workspaceID != "" {
		request.Header.Set(workspaceHeader, workspaceID)
	}
	return request
}
