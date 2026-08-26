package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"magpie/internal/api/dto"
	"magpie/internal/database"
	"magpie/internal/domain"
	"magpie/internal/support"
)

func TestCreateWorkspaceInvitationSurvivesEmailQueueFailure(t *testing.T) {
	setupUserRegistrationTestDB(t)
	t.Setenv("JWT_SECRET", "workspace-invitation-test-secret")
	t.Setenv("AUTH_REVOCATION_FAIL_OPEN", "true")
	owner, workspace := createWorkspaceMiddlewareTestUser(t, "invitation-owner@example.test")
	invitee, _ := createWorkspaceMiddlewareTestUser(t, "invitation-recipient@example.test")

	previousReadEmail := readEmailConfigFn
	previousReadInvitation := readWorkspaceInvitationConfigFn
	previousBuildURL := buildWorkspaceInvitationsURLFn
	previousQueue := queueWorkspaceInvitationEmailFn
	previousNow := nowFn
	t.Cleanup(func() {
		readEmailConfigFn = previousReadEmail
		readWorkspaceInvitationConfigFn = previousReadInvitation
		buildWorkspaceInvitationsURLFn = previousBuildURL
		queueWorkspaceInvitationEmailFn = previousQueue
		nowFn = previousNow
	})
	fixedNow := time.Now().UTC().Truncate(time.Second)
	nowFn = func() time.Time { return fixedNow }
	readEmailConfigFn = func() (support.EmailConfig, error) {
		return support.EmailConfig{FromAddress: "magpie@example.test", SMTPHost: "smtp.example.test", SMTPPort: 587}, nil
	}
	readWorkspaceInvitationConfigFn = func() (support.WorkspaceInvitationConfig, error) {
		return support.WorkspaceInvitationConfig{PublicAppURL: "https://magpie.example.test", TTL: 7 * 24 * time.Hour}, nil
	}
	buildWorkspaceInvitationsURLFn = func(support.WorkspaceInvitationConfig) (string, error) {
		return "https://magpie.example.test/invitations", nil
	}
	queueCalls := 0
	queueWorkspaceInvitationEmailFn = func(kind, toAddress, subject, body string) error {
		queueCalls++
		if kind != "workspace_invitation_created" || toAddress != invitee.Email {
			t.Fatalf("unexpected queued email metadata: %q %q", kind, toAddress)
		}
		if !strings.Contains(subject, "invited") || !strings.Contains(body, "Workspace:") || !strings.Contains(body, "Review invitation") {
			t.Fatalf("unexpected invitation email subject/body: %q %q", subject, body)
		}
		return errors.New("outbox unavailable")
	}

	request := workspaceInvitationHandlerRequest(t, owner.ID, workspace.ID, http.MethodPost, `{"email":"INVITATION-RECIPIENT@example.test","role":"operator","billing_admin":false}`)
	recorder := httptest.NewRecorder()
	createWorkspaceInvitation(recorder, request)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("create status = %d, body %s", recorder.Code, recorder.Body.String())
	}
	var response dto.WorkspaceInvitationResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode invitation response: %v", err)
	}
	if response.Warning == "" || response.Invitation.NotificationStatus != domain.WorkspaceInvitationNotificationFailed {
		t.Fatalf("queue-failure response = %#v", response)
	}
	if !response.Invitation.ExpiresAt.Equal(fixedNow.Add(7 * 24 * time.Hour)) {
		t.Fatalf("expiry = %s", response.Invitation.ExpiresAt)
	}
	stored, err := database.ListWorkspaceInvitations(workspace.ID, fixedNow)
	if err != nil || len(stored) != 1 || stored[0].ID != response.Invitation.ID {
		t.Fatalf("stored invitations = %#v, error %v", stored, err)
	}

	request = workspaceInvitationHandlerRequest(t, owner.ID, workspace.ID, http.MethodPatch, `{"role":"operator","billing_admin":false}`)
	request.SetPathValue("invitationId", strconv.FormatUint(uint64(response.Invitation.ID), 10))
	recorder = httptest.NewRecorder()
	updateWorkspaceInvitation(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("unchanged update status = %d, body %s", recorder.Code, recorder.Body.String())
	}
	if queueCalls != 1 {
		t.Fatalf("unchanged update queued %d emails total, want only the original one", queueCalls)
	}
}

func TestUpdateWorkspaceInvitationQueuesChangedEmailWithoutRenewingExpiry(t *testing.T) {
	setupUserRegistrationTestDB(t)
	t.Setenv("JWT_SECRET", "workspace-invitation-update-email-test-secret")
	t.Setenv("AUTH_REVOCATION_FAIL_OPEN", "true")
	owner, workspace := createWorkspaceMiddlewareTestUser(t, "update-invitation-owner@example.test")
	invitee, _ := createWorkspaceMiddlewareTestUser(t, "update-invitation-recipient@example.test")

	previousReadEmail := readEmailConfigFn
	previousReadInvitation := readWorkspaceInvitationConfigFn
	previousBuildURL := buildWorkspaceInvitationsURLFn
	previousQueue := queueWorkspaceInvitationEmailFn
	previousNow := nowFn
	t.Cleanup(func() {
		readEmailConfigFn = previousReadEmail
		readWorkspaceInvitationConfigFn = previousReadInvitation
		buildWorkspaceInvitationsURLFn = previousBuildURL
		queueWorkspaceInvitationEmailFn = previousQueue
		nowFn = previousNow
	})
	fixedNow := time.Now().UTC().Truncate(time.Second)
	nowFn = func() time.Time { return fixedNow }
	readEmailConfigFn = func() (support.EmailConfig, error) {
		return support.EmailConfig{FromAddress: "magpie@example.test", SMTPHost: "smtp.example.test", SMTPPort: 587}, nil
	}
	readWorkspaceInvitationConfigFn = func() (support.WorkspaceInvitationConfig, error) {
		return support.WorkspaceInvitationConfig{PublicAppURL: "https://magpie.example.test", TTL: 7 * 24 * time.Hour}, nil
	}
	buildWorkspaceInvitationsURLFn = func(support.WorkspaceInvitationConfig) (string, error) {
		return "https://magpie.example.test/invitations", nil
	}
	type queuedEmail struct {
		kind    string
		subject string
		body    string
	}
	queued := make([]queuedEmail, 0, 2)
	queueWorkspaceInvitationEmailFn = func(kind, toAddress, subject, body string) error {
		if toAddress != invitee.Email {
			t.Fatalf("queued email recipient = %q, want %q", toAddress, invitee.Email)
		}
		queued = append(queued, queuedEmail{kind: kind, subject: subject, body: body})
		return nil
	}

	request := workspaceInvitationHandlerRequest(t, owner.ID, workspace.ID, http.MethodPost, `{"email":"update-invitation-recipient@example.test","role":"viewer","billing_admin":false}`)
	recorder := httptest.NewRecorder()
	createWorkspaceInvitation(recorder, request)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("create status = %d, body %s", recorder.Code, recorder.Body.String())
	}
	var created dto.WorkspaceInvitationResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode created invitation: %v", err)
	}

	request = workspaceInvitationHandlerRequest(t, owner.ID, workspace.ID, http.MethodPatch, `{"role":"operator","billing_admin":true}`)
	request.SetPathValue("invitationId", strconv.FormatUint(uint64(created.Invitation.ID), 10))
	recorder = httptest.NewRecorder()
	updateWorkspaceInvitation(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("update status = %d, body %s", recorder.Code, recorder.Body.String())
	}
	var updated dto.WorkspaceInvitationResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &updated); err != nil {
		t.Fatalf("decode updated invitation: %v", err)
	}
	if !updated.Invitation.ExpiresAt.Equal(created.Invitation.ExpiresAt) {
		t.Fatalf("updated expiry = %s, want unchanged %s", updated.Invitation.ExpiresAt, created.Invitation.ExpiresAt)
	}
	if updated.Invitation.NotificationStatus != domain.WorkspaceInvitationNotificationQueued || updated.Warning != "" {
		t.Fatalf("updated notification response = %#v", updated)
	}
	if len(queued) != 2 {
		t.Fatalf("queued email count = %d, want 2", len(queued))
	}
	changed := queued[1]
	if changed.kind != "workspace_invitation_changed" || changed.subject != "Your Magpie workspace invitation changed" {
		t.Fatalf("changed email metadata = %#v", changed)
	}
	if !strings.Contains(changed.body, "Your invitation changed") || !strings.Contains(changed.body, "Billing access: <strong>Yes</strong>") {
		t.Fatalf("changed email body = %q", changed.body)
	}
}

func TestCreateWorkspaceInvitationWithoutEmailStillCreatesInboxOffer(t *testing.T) {
	setupUserRegistrationTestDB(t)
	t.Setenv("JWT_SECRET", "workspace-invitation-no-email-test-secret")
	t.Setenv("AUTH_REVOCATION_FAIL_OPEN", "true")
	owner, workspace := createWorkspaceMiddlewareTestUser(t, "no-email-invitation-owner@example.test")
	invitee, _ := createWorkspaceMiddlewareTestUser(t, "no-email-invitation-recipient@example.test")

	previousReadEmail := readEmailConfigFn
	previousReadInvitation := readWorkspaceInvitationConfigFn
	previousQueue := queueWorkspaceInvitationEmailFn
	t.Cleanup(func() {
		readEmailConfigFn = previousReadEmail
		readWorkspaceInvitationConfigFn = previousReadInvitation
		queueWorkspaceInvitationEmailFn = previousQueue
	})
	readEmailConfigFn = func() (support.EmailConfig, error) { return support.EmailConfig{}, nil }
	readWorkspaceInvitationConfigFn = func() (support.WorkspaceInvitationConfig, error) {
		return support.WorkspaceInvitationConfig{TTL: 7 * 24 * time.Hour}, nil
	}
	queueWorkspaceInvitationEmailFn = func(kind, toAddress, subject, body string) error {
		t.Fatal("email queue called without email configuration")
		return nil
	}

	request := workspaceInvitationHandlerRequest(t, owner.ID, workspace.ID, http.MethodPost, `{"email":"no-email-invitation-recipient@example.test","role":"viewer","billing_admin":false}`)
	recorder := httptest.NewRecorder()
	createWorkspaceInvitation(recorder, request)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("create status = %d, body %s", recorder.Code, recorder.Body.String())
	}
	var response dto.WorkspaceInvitationResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode invitation response: %v", err)
	}
	if response.Warning != "" || response.Invitation.NotificationStatus != domain.WorkspaceInvitationNotificationNotConfigured {
		t.Fatalf("no-email response = %#v", response)
	}
	inbox, err := database.ListUserWorkspaceInvitations(invitee.ID, time.Now().UTC())
	if err != nil || len(inbox) != 1 || inbox[0].ID != response.Invitation.ID {
		t.Fatalf("inbox invitations = %#v, error %v", inbox, err)
	}
}

func TestWorkspaceInvitationPermissionsKeepAdminsBelowAdmins(t *testing.T) {
	owner := database.WorkspaceAccess{Role: domain.WorkspaceRoleOwner}
	admin := database.WorkspaceAccess{Role: domain.WorkspaceRoleAdmin}
	if err := authorizeWorkspaceInvitationMutation(owner, domain.WorkspaceRoleAdmin, true); err != nil {
		t.Fatalf("owner invitation permission: %v", err)
	}
	if err := authorizeWorkspaceInvitationMutation(admin, domain.WorkspaceRoleOperator, false); err != nil {
		t.Fatalf("admin operator invitation permission: %v", err)
	}
	if err := authorizeWorkspaceInvitationMutation(admin, domain.WorkspaceRoleAdmin, false); err == nil {
		t.Fatal("admin must not invite another admin")
	}
	if err := authorizeWorkspaceInvitationMutation(admin, domain.WorkspaceRoleViewer, true); err == nil {
		t.Fatal("admin must not grant billing access")
	}
	if err := authorizeExistingWorkspaceInvitationMutation(admin, dto.WorkspaceInvitation{Role: domain.WorkspaceRoleAdmin}); err == nil {
		t.Fatal("admin must not manage an existing admin invitation")
	}
	if err := authorizeExistingWorkspaceInvitationMutation(admin, dto.WorkspaceInvitation{Role: domain.WorkspaceRoleOperator, BillingAdmin: true}); err == nil {
		t.Fatal("admin must not manage an existing billing invitation")
	}
}

func TestCreateWorkspaceInvitationReportsUnknownAccount(t *testing.T) {
	setupUserRegistrationTestDB(t)
	t.Setenv("JWT_SECRET", "workspace-invitation-unknown-test-secret")
	t.Setenv("AUTH_REVOCATION_FAIL_OPEN", "true")
	owner, workspace := createWorkspaceMiddlewareTestUser(t, "unknown-invitation-owner@example.test")

	previousReadInvitation := readWorkspaceInvitationConfigFn
	t.Cleanup(func() { readWorkspaceInvitationConfigFn = previousReadInvitation })
	readWorkspaceInvitationConfigFn = func() (support.WorkspaceInvitationConfig, error) {
		return support.WorkspaceInvitationConfig{TTL: 7 * 24 * time.Hour}, nil
	}

	request := workspaceInvitationHandlerRequest(t, owner.ID, workspace.ID, http.MethodPost, `{"email":"missing@example.test","role":"viewer","billing_admin":false}`)
	recorder := httptest.NewRecorder()
	createWorkspaceInvitation(recorder, request)
	if recorder.Code != http.StatusNotFound || !strings.Contains(recorder.Body.String(), "No Magpie account") {
		t.Fatalf("unknown account response = %d %s", recorder.Code, recorder.Body.String())
	}
}

func workspaceInvitationHandlerRequest(t *testing.T, userID, workspaceID uint, method, body string) *http.Request {
	t.Helper()
	request := workspaceMiddlewareTestRequest(t, userID, strconv.FormatUint(uint64(workspaceID), 10))
	request.Method = method
	request.URL.Path = "/api/workspaces/" + strconv.FormatUint(uint64(workspaceID), 10) + "/invitations"
	request.SetPathValue("id", strconv.FormatUint(uint64(workspaceID), 10))
	request.Body = http.NoBody
	if body != "" {
		request.Body = io.NopCloser(bytes.NewBufferString(body))
		request.Header.Set("Content-Type", "application/json")
	}
	return request
}
