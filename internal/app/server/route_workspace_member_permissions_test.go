package server

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"magpie/internal/database"
	"magpie/internal/domain"
)

func TestWorkspaceAdminCannotManageAdministratorsOrPromoteToAdministrator(t *testing.T) {
	setupUserRegistrationTestDB(t)
	t.Setenv("JWT_SECRET", "workspace-member-permission-test-secret")
	t.Setenv("AUTH_REVOCATION_FAIL_OPEN", "true")
	_, workspace := createWorkspaceMiddlewareTestUser(t, "member-permission-owner@example.test")
	admin, _ := createWorkspaceMiddlewareTestUser(t, "member-permission-admin@example.test")
	otherAdmin, _ := createWorkspaceMiddlewareTestUser(t, "member-permission-other-admin@example.test")
	operator, _ := createWorkspaceMiddlewareTestUser(t, "member-permission-operator@example.test")
	if _, err := database.AddWorkspaceMember(workspace.ID, admin.Email, domain.WorkspaceRoleAdmin, false); err != nil {
		t.Fatalf("add acting admin: %v", err)
	}
	if _, err := database.AddWorkspaceMember(workspace.ID, otherAdmin.Email, domain.WorkspaceRoleAdmin, false); err != nil {
		t.Fatalf("add other admin: %v", err)
	}
	if _, err := database.AddWorkspaceMember(workspace.ID, operator.Email, domain.WorkspaceRoleOperator, false); err != nil {
		t.Fatalf("add operator: %v", err)
	}

	recorder := httptest.NewRecorder()
	updateWorkspaceMember(recorder, workspaceMemberPermissionRequest(t, admin.ID, workspace.ID, otherAdmin.ID, http.MethodPatch, `{"role":"operator","billing_admin":false}`))
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("admin demoting admin status = %d, body %s", recorder.Code, recorder.Body.String())
	}

	recorder = httptest.NewRecorder()
	updateWorkspaceMember(recorder, workspaceMemberPermissionRequest(t, admin.ID, workspace.ID, operator.ID, http.MethodPatch, `{"role":"admin","billing_admin":false}`))
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("admin promoting admin status = %d, body %s", recorder.Code, recorder.Body.String())
	}

	recorder = httptest.NewRecorder()
	removeWorkspaceMember(recorder, workspaceMemberPermissionRequest(t, admin.ID, workspace.ID, otherAdmin.ID, http.MethodDelete, ""))
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("admin removing admin status = %d, body %s", recorder.Code, recorder.Body.String())
	}

	recorder = httptest.NewRecorder()
	updateWorkspaceMember(recorder, workspaceMemberPermissionRequest(t, admin.ID, workspace.ID, operator.ID, http.MethodPatch, `{"role":"viewer","billing_admin":false}`))
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("admin updating operator status = %d, body %s", recorder.Code, recorder.Body.String())
	}
}

func workspaceMemberPermissionRequest(t *testing.T, actorID, workspaceID, memberID uint, method, body string) *http.Request {
	t.Helper()
	request := workspaceMiddlewareTestRequest(t, actorID, strconv.FormatUint(uint64(workspaceID), 10))
	request.Method = method
	request.SetPathValue("id", strconv.FormatUint(uint64(workspaceID), 10))
	request.SetPathValue("userId", strconv.FormatUint(uint64(memberID), 10))
	request.Body = http.NoBody
	if body != "" {
		request.Body = io.NopCloser(bytes.NewBufferString(body))
		request.Header.Set("Content-Type", "application/json")
	}
	return request
}
