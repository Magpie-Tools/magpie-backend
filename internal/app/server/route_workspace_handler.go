package server

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	"magpie/internal/api/dto"
	"magpie/internal/app/bootstrap"
	"magpie/internal/auth"
	"magpie/internal/config"
	"magpie/internal/database"
	"magpie/internal/domain"

	"github.com/charmbracelet/log"
	"gorm.io/gorm"
)

func listWorkspaces(w http.ResponseWriter, r *http.Request) {
	userID, err := auth.GetUserIDFromRequest(r)
	if err != nil {
		writeError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	workspaces, err := database.ListWorkspaces(userID)
	if err != nil {
		writeError(w, "Failed to list workspaces", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"workspaces": workspaces})
}

func createWorkspace(w http.ResponseWriter, r *http.Request) {
	userID, err := auth.GetUserIDFromRequest(r)
	if err != nil {
		writeError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	var payload dto.WorkspaceCreateRequest
	if !decodeJSONBodyLimited(w, r, &payload, resolveJSONMaxBodyBytes()) {
		return
	}
	workspace, err := database.CreateWorkspace(userID, payload.Name)
	if err != nil {
		writeError(w, err.Error(), http.StatusBadRequest)
		return
	}

	go bootstrap.AddDefaultJudgesToUsers()
	defaultSites, sitesErr := database.SaveScrapingSourcesOfUsers(workspace.ID, config.GetConfig().Scraper.ScrapeSites)
	if sitesErr != nil {
		log.Warn("Could not add default scraping sources to workspace", "workspace_id", workspace.ID, "error", sitesErr)
	} else if queueErr := enqueueScrapeSitesOrRollback(workspace.ID, defaultSites); queueErr != nil {
		log.Error("Could not queue default scraping sources for workspace", "workspace_id", workspace.ID, "error", queueErr)
	}
	writeJSON(w, http.StatusCreated, workspace)
}

func getWorkspace(w http.ResponseWriter, r *http.Request) {
	workspaceID, access, ok := requirePathWorkspaceAccess(w, r, domain.WorkspaceRoleViewer)
	if !ok {
		return
	}
	workspace, err := database.GetWorkspace(workspaceID, access.UserID)
	if err != nil {
		writeWorkspaceAccessError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, workspace)
}

func updateWorkspace(w http.ResponseWriter, r *http.Request) {
	workspaceID, _, ok := requirePathWorkspaceAccess(w, r, domain.WorkspaceRoleAdmin)
	if !ok {
		return
	}
	var payload dto.WorkspaceUpdateRequest
	if !decodeJSONBodyLimited(w, r, &payload, resolveJSONMaxBodyBytes()) {
		return
	}
	if err := database.RenameWorkspace(workspaceID, payload.Name); err != nil {
		writeError(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func selectWorkspace(w http.ResponseWriter, r *http.Request) {
	workspaceID, access, ok := requirePathWorkspaceAccess(w, r, domain.WorkspaceRoleViewer)
	if !ok {
		return
	}
	if err := database.SetDefaultWorkspace(access.UserID, workspaceID); err != nil {
		writeWorkspaceAccessError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func listWorkspaceMembers(w http.ResponseWriter, r *http.Request) {
	workspaceID, _, ok := requirePathWorkspaceAccess(w, r, domain.WorkspaceRoleViewer)
	if !ok {
		return
	}
	members, err := database.ListWorkspaceMembers(workspaceID)
	if err != nil {
		writeError(w, "Failed to list workspace members", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"members": members})
}

func addWorkspaceMember(w http.ResponseWriter, r *http.Request) {
	workspaceID, access, ok := requirePathWorkspaceAccess(w, r, domain.WorkspaceRoleAdmin)
	if !ok {
		return
	}
	var payload dto.WorkspaceMemberCreateRequest
	if !decodeJSONBodyLimited(w, r, &payload, resolveJSONMaxBodyBytes()) {
		return
	}
	payload.Role = strings.ToLower(strings.TrimSpace(payload.Role))
	if (payload.BillingAdmin || payload.Role == domain.WorkspaceRoleOwner) && !access.IsOwner() {
		writeError(w, "Only a workspace owner can grant ownership or billing access", http.StatusForbidden)
		return
	}
	member, err := database.AddWorkspaceMember(workspaceID, payload.Email, payload.Role, payload.BillingAdmin)
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, database.ErrWorkspaceMemberExists) {
			status = http.StatusConflict
		}
		writeError(w, err.Error(), status)
		return
	}
	writeJSON(w, http.StatusCreated, member)
}

func updateWorkspaceMember(w http.ResponseWriter, r *http.Request) {
	workspaceID, access, ok := requirePathWorkspaceAccess(w, r, domain.WorkspaceRoleAdmin)
	if !ok {
		return
	}
	memberUserID, ok := parsePositivePathUint(w, r, "userId")
	if !ok {
		return
	}
	var payload dto.WorkspaceMemberUpdateRequest
	if !decodeJSONBodyLimited(w, r, &payload, resolveJSONMaxBodyBytes()) {
		return
	}
	payload.Role = strings.ToLower(strings.TrimSpace(payload.Role))
	existing, err := database.ResolveWorkspaceAccess(memberUserID, workspaceID)
	if err != nil {
		writeError(w, "Workspace member not found", http.StatusNotFound)
		return
	}
	if !access.IsOwner() && (existing.IsOwner() || payload.Role == domain.WorkspaceRoleOwner || existing.BillingAdmin != payload.BillingAdmin) {
		writeError(w, "Only a workspace owner can change ownership or billing access", http.StatusForbidden)
		return
	}
	if err := database.UpdateWorkspaceMember(workspaceID, memberUserID, payload.Role, payload.BillingAdmin); err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, gorm.ErrRecordNotFound) {
			status = http.StatusNotFound
		}
		writeError(w, err.Error(), status)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func removeWorkspaceMember(w http.ResponseWriter, r *http.Request) {
	workspaceID, access, ok := requirePathWorkspaceAccess(w, r, domain.WorkspaceRoleAdmin)
	if !ok {
		return
	}
	memberUserID, ok := parsePositivePathUint(w, r, "userId")
	if !ok {
		return
	}
	existing, err := database.ResolveWorkspaceAccess(memberUserID, workspaceID)
	if err != nil {
		writeError(w, "Workspace member not found", http.StatusNotFound)
		return
	}
	if existing.IsOwner() && !access.IsOwner() {
		writeError(w, "Only a workspace owner can remove another owner", http.StatusForbidden)
		return
	}
	if err := database.RemoveWorkspaceMember(workspaceID, memberUserID); err != nil {
		writeError(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func requirePathWorkspaceAccess(w http.ResponseWriter, r *http.Request, role string) (uint, database.WorkspaceAccess, bool) {
	workspaceID, ok := parsePositivePathUint(w, r, "id")
	if !ok {
		return 0, database.WorkspaceAccess{}, false
	}
	userID, err := auth.GetUserIDFromRequest(r)
	if err != nil {
		writeError(w, "Unauthorized", http.StatusUnauthorized)
		return 0, database.WorkspaceAccess{}, false
	}
	access, err := database.ResolveWorkspaceAccess(userID, workspaceID)
	if err != nil {
		writeWorkspaceAccessError(w, err)
		return 0, database.WorkspaceAccess{}, false
	}
	if domain.WorkspaceRoleRank(access.Role) < domain.WorkspaceRoleRank(role) {
		writeError(w, "Workspace role does not permit this action", http.StatusForbidden)
		return 0, database.WorkspaceAccess{}, false
	}
	return workspaceID, access, true
}

func parsePositivePathUint(w http.ResponseWriter, r *http.Request, name string) (uint, bool) {
	raw := r.PathValue(name)
	parsed, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || parsed == 0 || uint64(uint(parsed)) != parsed {
		writeError(w, "Invalid "+name, http.StatusBadRequest)
		return 0, false
	}
	return uint(parsed), true
}
