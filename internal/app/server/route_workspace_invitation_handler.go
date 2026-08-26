package server

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"magpie/internal/api/dto"
	"magpie/internal/auth"
	"magpie/internal/database"
	"magpie/internal/domain"
	"magpie/internal/support"

	"github.com/charmbracelet/log"
)

const workspaceInvitationEmailWarning = "The invitation was saved, but its notification email could not be queued."

var (
	readWorkspaceInvitationConfigFn = support.ReadWorkspaceInvitationConfig
	buildWorkspaceInvitationsURLFn  = support.BuildWorkspaceInvitationsURL
	queueWorkspaceInvitationEmailFn = func(kind, toAddress, subject, body string) error {
		return database.EnqueueEmailOutbox(kind, toAddress, subject, body, resolveEmailOutboxMaxAttempts())
	}
)

func listWorkspaceInvitations(w http.ResponseWriter, r *http.Request) {
	workspaceID, _, ok := requirePathWorkspaceAccess(w, r, domain.WorkspaceRoleAdmin)
	if !ok {
		return
	}
	invitations, err := database.ListWorkspaceInvitations(workspaceID, nowFn().UTC())
	if err != nil {
		writeError(w, "Failed to list workspace invitations", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"invitations": invitations})
}

func createWorkspaceInvitation(w http.ResponseWriter, r *http.Request) {
	workspaceID, access, ok := requirePathWorkspaceAccess(w, r, domain.WorkspaceRoleAdmin)
	if !ok {
		return
	}
	var payload dto.WorkspaceInvitationCreateRequest
	if !decodeJSONBodyLimited(w, r, &payload, resolveJSONMaxBodyBytes()) {
		return
	}
	payload.Email = auth.NormalizeEmail(payload.Email)
	payload.Role = normalizeInvitationRole(payload.Role)
	if payload.Email == "" {
		writeError(w, "Invitee email is required", http.StatusBadRequest)
		return
	}
	if err := authorizeWorkspaceInvitationMutation(access, payload.Role, payload.BillingAdmin); err != nil {
		writeWorkspaceInvitationMutationAuthorizationError(w, err)
		return
	}

	invitationCfg, err := readWorkspaceInvitationConfigFn()
	if err != nil {
		log.Error("failed to read workspace invitation configuration", "error", err)
		writeError(w, "Workspace invitations are not configured correctly", http.StatusInternalServerError)
		return
	}
	invitation, err := database.CreateWorkspaceInvitation(
		workspaceID,
		access.UserID,
		payload.Email,
		payload.Role,
		payload.BillingAdmin,
		nowFn().UTC().Add(invitationCfg.TTL),
	)
	if err != nil {
		writeWorkspaceInvitationError(w, err)
		return
	}

	invitation, warning := queueWorkspaceInvitationNotification(invitation, invitationCfg, false)
	writeJSON(w, http.StatusCreated, dto.WorkspaceInvitationResponse{Invitation: invitation, Warning: warning})
}

func updateWorkspaceInvitation(w http.ResponseWriter, r *http.Request) {
	workspaceID, access, ok := requirePathWorkspaceAccess(w, r, domain.WorkspaceRoleAdmin)
	if !ok {
		return
	}
	invitationID, ok := parsePositivePathUint(w, r, "invitationId")
	if !ok {
		return
	}
	current, err := database.GetWorkspaceInvitationForWorkspace(workspaceID, invitationID, nowFn().UTC())
	if err != nil {
		writeWorkspaceInvitationError(w, err)
		return
	}
	if err := authorizeExistingWorkspaceInvitationMutation(access, current); err != nil {
		writeError(w, err.Error(), http.StatusForbidden)
		return
	}

	var payload dto.WorkspaceInvitationUpdateRequest
	if !decodeJSONBodyLimited(w, r, &payload, resolveJSONMaxBodyBytes()) {
		return
	}
	payload.Role = normalizeInvitationRole(payload.Role)
	if err := authorizeWorkspaceInvitationMutation(access, payload.Role, payload.BillingAdmin); err != nil {
		writeWorkspaceInvitationMutationAuthorizationError(w, err)
		return
	}
	if payload.Role == current.Role && payload.BillingAdmin == current.BillingAdmin {
		writeJSON(w, http.StatusOK, dto.WorkspaceInvitationResponse{Invitation: current})
		return
	}

	invitation, err := database.UpdateWorkspaceInvitation(
		workspaceID,
		invitationID,
		payload.Role,
		payload.BillingAdmin,
		nowFn().UTC(),
	)
	if err != nil {
		writeWorkspaceInvitationError(w, err)
		return
	}
	invitationCfg, cfgErr := readWorkspaceInvitationConfigFn()
	if cfgErr != nil {
		log.Error("failed to read workspace invitation configuration after invitation update", "error", cfgErr)
		invitation = setWorkspaceInvitationNotificationStatus(invitation, domain.WorkspaceInvitationNotificationFailed)
		writeJSON(w, http.StatusOK, dto.WorkspaceInvitationResponse{Invitation: invitation, Warning: workspaceInvitationEmailWarning})
		return
	}
	invitation, warning := queueWorkspaceInvitationNotification(invitation, invitationCfg, true)
	writeJSON(w, http.StatusOK, dto.WorkspaceInvitationResponse{Invitation: invitation, Warning: warning})
}

func revokeWorkspaceInvitation(w http.ResponseWriter, r *http.Request) {
	workspaceID, access, ok := requirePathWorkspaceAccess(w, r, domain.WorkspaceRoleAdmin)
	if !ok {
		return
	}
	invitationID, ok := parsePositivePathUint(w, r, "invitationId")
	if !ok {
		return
	}
	invitation, err := database.GetWorkspaceInvitationForWorkspace(workspaceID, invitationID, nowFn().UTC())
	if err != nil {
		writeWorkspaceInvitationError(w, err)
		return
	}
	if err := authorizeExistingWorkspaceInvitationMutation(access, invitation); err != nil {
		writeError(w, err.Error(), http.StatusForbidden)
		return
	}
	if err := database.RevokeWorkspaceInvitation(workspaceID, invitationID, nowFn().UTC()); err != nil {
		writeWorkspaceInvitationError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func listUserWorkspaceInvitations(w http.ResponseWriter, r *http.Request) {
	userID, err := auth.GetUserIDFromRequest(r)
	if err != nil {
		writeError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	invitations, err := database.ListUserWorkspaceInvitations(userID, nowFn().UTC())
	if err != nil {
		writeError(w, "Failed to list invitations", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"invitations": invitations})
}

func acceptWorkspaceInvitation(w http.ResponseWriter, r *http.Request) {
	userID, err := auth.GetUserIDFromRequest(r)
	if err != nil {
		writeError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	invitationID, ok := parsePositivePathUint(w, r, "invitationId")
	if !ok {
		return
	}
	accepted, err := database.AcceptWorkspaceInvitation(invitationID, userID, nowFn().UTC())
	if err != nil {
		writeWorkspaceInvitationError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, accepted)
}

func declineWorkspaceInvitation(w http.ResponseWriter, r *http.Request) {
	userID, err := auth.GetUserIDFromRequest(r)
	if err != nil {
		writeError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	invitationID, ok := parsePositivePathUint(w, r, "invitationId")
	if !ok {
		return
	}
	if err := database.DeclineWorkspaceInvitation(invitationID, userID, nowFn().UTC()); err != nil {
		writeWorkspaceInvitationError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func authorizeWorkspaceInvitationMutation(access database.WorkspaceAccess, role string, billingAdmin bool) error {
	if role != domain.WorkspaceRoleAdmin && role != domain.WorkspaceRoleOperator && role != domain.WorkspaceRoleViewer {
		return domain.ErrInvalidWorkspaceRole
	}
	if access.IsOwner() {
		return nil
	}
	if role == domain.WorkspaceRoleAdmin || billingAdmin {
		return errors.New("Only a workspace owner can invite administrators or grant billing access")
	}
	return nil
}

func authorizeExistingWorkspaceInvitationMutation(access database.WorkspaceAccess, invitation dto.WorkspaceInvitation) error {
	if access.IsOwner() {
		return nil
	}
	if invitation.Role == domain.WorkspaceRoleAdmin || invitation.BillingAdmin {
		return errors.New("Only a workspace owner can manage this invitation")
	}
	return nil
}

func writeWorkspaceInvitationMutationAuthorizationError(w http.ResponseWriter, err error) {
	if errors.Is(err, domain.ErrInvalidWorkspaceRole) {
		writeError(w, "Invitation role must be admin, operator, or viewer", http.StatusBadRequest)
		return
	}
	writeError(w, err.Error(), http.StatusForbidden)
}

func queueWorkspaceInvitationNotification(
	invitation dto.WorkspaceInvitation,
	invitationCfg support.WorkspaceInvitationConfig,
	changed bool,
) (dto.WorkspaceInvitation, string) {
	emailCfg, err := readEmailConfigFn()
	if err != nil {
		log.Error("failed to read email configuration for workspace invitation", "invitation_id", invitation.ID, "error", err)
		return setWorkspaceInvitationNotificationStatus(invitation, domain.WorkspaceInvitationNotificationFailed), workspaceInvitationEmailWarning
	}
	if !emailCfg.IsConfigured() {
		return setWorkspaceInvitationNotificationStatus(invitation, domain.WorkspaceInvitationNotificationNotConfigured), ""
	}
	invitationURL, err := buildWorkspaceInvitationsURLFn(invitationCfg)
	if err != nil {
		log.Error("failed to build workspace invitation URL", "invitation_id", invitation.ID, "error", err)
		return setWorkspaceInvitationNotificationStatus(invitation, domain.WorkspaceInvitationNotificationFailed), workspaceInvitationEmailWarning
	}
	subject, body := renderWorkspaceInvitationEmail(emailCfg, invitation, invitationURL, changed)
	kind := "workspace_invitation_created"
	if changed {
		kind = "workspace_invitation_changed"
	}
	if err := queueWorkspaceInvitationEmailFn(kind, invitation.InviteeEmail, subject, body); err != nil {
		log.Error("failed to queue workspace invitation email", "invitation_id", invitation.ID, "error", err)
		return setWorkspaceInvitationNotificationStatus(invitation, domain.WorkspaceInvitationNotificationFailed), workspaceInvitationEmailWarning
	}
	return setWorkspaceInvitationNotificationStatus(invitation, domain.WorkspaceInvitationNotificationQueued), ""
}

func setWorkspaceInvitationNotificationStatus(invitation dto.WorkspaceInvitation, status string) dto.WorkspaceInvitation {
	invitation.NotificationStatus = status
	if err := database.SetWorkspaceInvitationNotificationStatus(invitation.WorkspaceID, invitation.ID, status); err != nil {
		log.Error("failed to update workspace invitation notification status", "invitation_id", invitation.ID, "status", status, "error", err)
	}
	return invitation
}

func renderWorkspaceInvitationEmail(
	cfg support.EmailConfig,
	invitation dto.WorkspaceInvitation,
	invitationURL string,
	changed bool,
) (string, string) {
	subject := "You were invited to a Magpie workspace"
	preheader := fmt.Sprintf("%s invited you to join %s.", invitation.InviterEmail, invitation.WorkspaceName)
	heading := fmt.Sprintf("Join %s", invitation.WorkspaceName)
	intro := fmt.Sprintf("%s invited your Magpie account to this workspace.", invitation.InviterEmail)
	eyebrow := "Workspace invitation"
	if changed {
		subject = "Your Magpie workspace invitation changed"
		preheader = fmt.Sprintf("Your invitation to %s was updated.", invitation.WorkspaceName)
		heading = "Your invitation changed"
		intro = fmt.Sprintf("The invitation for your account to join %s was updated.", invitation.WorkspaceName)
		eyebrow = "Invitation updated"
	}
	billing := "No"
	if invitation.BillingAdmin {
		billing = "Yes"
	}
	detail := fmt.Sprintf(
		"Workspace: <strong>%s</strong><br>Invited by: <strong>%s</strong><br>Role: <strong>%s</strong><br>Billing access: <strong>%s</strong><br>Expires: <strong>%s</strong><br><br>Sign in to review, accept, or decline this invitation. No workspace access is granted until you accept.",
		escapeEmailHTML(invitation.WorkspaceName),
		escapeEmailHTML(invitation.InviterEmail),
		escapeEmailHTML(titleCaseWorkspaceRole(invitation.Role)),
		billing,
		escapeEmailHTML(invitation.ExpiresAt.UTC().Format(time.RFC1123)),
	)
	body := renderBrandedAuthEmail(
		cfg,
		subject,
		preheader,
		eyebrow,
		heading,
		intro,
		"Review invitation",
		invitationURL,
		detail,
	)
	return subject, body
}

func writeWorkspaceInvitationError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, database.ErrWorkspaceInviteeNotFound):
		writeError(w, "No Magpie account exists for that email address", http.StatusNotFound)
	case errors.Is(err, database.ErrWorkspaceInvitationNotFound):
		writeError(w, "Workspace invitation not found", http.StatusNotFound)
	case errors.Is(err, database.ErrWorkspaceInvitationExpired):
		writeError(w, "Workspace invitation has expired", http.StatusGone)
	case errors.Is(err, database.ErrWorkspaceInvitationExists):
		writeError(w, "A pending invitation already exists for that account", http.StatusConflict)
	case errors.Is(err, database.ErrWorkspaceMemberExists):
		writeError(w, "That account is already a workspace member", http.StatusConflict)
	case errors.Is(err, domain.ErrInvalidWorkspaceRole):
		writeError(w, "Invitation role must be admin, operator, or viewer", http.StatusBadRequest)
	default:
		log.Error("workspace invitation request failed", "error", err)
		writeError(w, "Workspace invitation request failed", http.StatusInternalServerError)
	}
}

func normalizeInvitationRole(role string) string {
	return strings.ToLower(strings.TrimSpace(role))
}

func titleCaseWorkspaceRole(role string) string {
	role = normalizeInvitationRole(role)
	if role == "" {
		return ""
	}
	return strings.ToUpper(role[:1]) + role[1:]
}
