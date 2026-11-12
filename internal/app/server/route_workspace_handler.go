package server

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"magpie/internal/api/dto"
	"magpie/internal/app/bootstrap"
	"magpie/internal/auth"
	"magpie/internal/config"
	"magpie/internal/database"
	"magpie/internal/domain"
	"magpie/internal/platform/pg"
	"magpie/internal/scrapestore"
	"magpie/internal/workspace"

	"github.com/charmbracelet/log"
	"gorm.io/gorm"
)

func (p pacedRoutes) listWorkspaces(w http.ResponseWriter, r *http.Request) {
	userID, err := auth.GetUserIDFromRequest(r)
	if err != nil {
		writeError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	workspaces, err := workspace.ListWorkspaces(userID, p.pace.Current())
	if err != nil {
		writeError(w, "Failed to list workspaces", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"workspaces": workspaces})
}

func (p pacedRoutes) createWorkspace(w http.ResponseWriter, r *http.Request) {
	userID, err := auth.GetUserIDFromRequest(r)
	if err != nil {
		writeError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	var payload dto.WorkspaceCreateRequest
	if !decodeJSONBodyLimited(w, r, &payload, resolveJSONMaxBodyBytes()) {
		return
	}
	workspace, err := workspace.CreateWorkspace(userID, payload.Name, p.pace.Current())
	if err != nil {
		writeError(w, err.Error(), http.StatusBadRequest)
		return
	}

	go bootstrap.AddDefaultJudgesToWorkspaces()
	defaultSites, sitesErr := scrapestore.SaveScrapingSourcesOfUsers(workspace.ID, config.GetConfig().Scraper.ScrapeSites)
	if sitesErr != nil {
		log.Warn("Could not add default scraping sources to workspace", "workspace_id", workspace.ID, "error", sitesErr)
	} else if queueErr := enqueueScrapeSitesOrRollback(workspace.ID, defaultSites); queueErr != nil {
		log.Error("Could not queue default scraping sources for workspace", "workspace_id", workspace.ID, "error", queueErr)
	}
	writeJSON(w, http.StatusCreated, workspace)
}

func (p pacedRoutes) getWorkspace(w http.ResponseWriter, r *http.Request) {
	workspaceID, access, ok := requirePathWorkspaceAccess(w, r, domain.WorkspaceRoleViewer)
	if !ok {
		return
	}
	workspace, err := workspace.GetWorkspace(workspaceID, access.UserID, p.pace.Current())
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
	if err := workspace.RenameWorkspace(workspaceID, payload.Name); err != nil {
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
	if err := workspace.SetDefaultWorkspace(access.UserID, workspaceID); err != nil {
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
	members, err := workspace.ListWorkspaceMembers(workspaceID)
	if err != nil {
		writeError(w, "Failed to list workspace members", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"members": members})
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
	existing, err := workspace.ResolveWorkspaceAccess(memberUserID, workspaceID)
	if err != nil {
		writeError(w, "Workspace member not found", http.StatusNotFound)
		return
	}
	if !access.IsOwner() && (existing.Role == domain.WorkspaceRoleOwner ||
		existing.Role == domain.WorkspaceRoleAdmin ||
		payload.Role == domain.WorkspaceRoleOwner ||
		payload.Role == domain.WorkspaceRoleAdmin ||
		existing.BillingAdmin ||
		payload.BillingAdmin) {
		writeError(w, "Only a workspace owner can manage owners, administrators, or billing access", http.StatusForbidden)
		return
	}
	if err := workspace.UpdateWorkspaceMember(workspaceID, memberUserID, payload.Role, payload.BillingAdmin); err != nil {
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
	existing, err := workspace.ResolveWorkspaceAccess(memberUserID, workspaceID)
	if err != nil {
		writeError(w, "Workspace member not found", http.StatusNotFound)
		return
	}
	if !access.IsOwner() && (existing.Role == domain.WorkspaceRoleOwner || existing.Role == domain.WorkspaceRoleAdmin || existing.BillingAdmin) {
		writeError(w, "Only a workspace owner can remove owners, administrators, or billing administrators", http.StatusForbidden)
		return
	}
	if err := workspace.RemoveWorkspaceMember(workspaceID, memberUserID); err != nil {
		writeError(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func requirePathWorkspaceAccess(w http.ResponseWriter, r *http.Request, role string) (uint, workspace.WorkspaceAccess, bool) {
	workspaceID, ok := parsePositivePathUint(w, r, "id")
	if !ok {
		return 0, workspace.WorkspaceAccess{}, false
	}
	userID, err := auth.GetUserIDFromRequest(r)
	if err != nil {
		writeError(w, "Unauthorized", http.StatusUnauthorized)
		return 0, workspace.WorkspaceAccess{}, false
	}
	access, err := workspace.ResolveWorkspaceAccess(userID, workspaceID)
	if err != nil {
		writeWorkspaceAccessError(w, err)
		return 0, workspace.WorkspaceAccess{}, false
	}
	if domain.WorkspaceRoleRank(access.Role) < domain.WorkspaceRoleRank(role) {
		writeError(w, "Workspace role does not permit this action", http.StatusForbidden)
		return 0, workspace.WorkspaceAccess{}, false
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

// updateWorkspaceAllowances lets a global administrator set one workspace's
// monthly check and scrape allowances. Zero inherits the instance default.
func (p pacedRoutes) updateWorkspaceAllowances(w http.ResponseWriter, r *http.Request) {
	workspaceID, err := strconv.ParseUint(r.PathValue("id"), 10, 64)
	if err != nil || workspaceID == 0 {
		writeError(w, "Invalid workspace id", http.StatusBadRequest)
		return
	}
	var payload dto.WorkspaceAllowanceUpdateRequest
	if !decodeJSONBodyLimited(w, r, &payload, resolveJSONMaxBodyBytes()) {
		return
	}
	if err := workspace.UpdateWorkspaceAllowances(r.Context(), uint(workspaceID), payload); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			writeError(w, "Workspace not found", http.StatusNotFound)
			return
		}
		log.Error("update workspace allowances", "workspace_id", workspaceID, "error", err)
		writeError(w, "Failed to update workspace allowances", http.StatusInternalServerError)
		return
	}
	p.refreshCheckPace(r.Context())
	writeJSON(w, http.StatusOK, map[string]any{
		"workspace_id":      workspaceID,
		"monthly_checks":    payload.MonthlyChecks,
		"monthly_scrapes":   payload.MonthlyScrapes,
		"unlimited_checks":  payload.UnlimitedChecks,
		"unlimited_scrapes": payload.UnlimitedScrapes,
	})
}

// updateWorkspaceCadence lets an operator choose fixed check and scrape
// intervals for the selected workspace, or zero for automatic spreading. A
// fixed interval runs until the monthly allowance is used up; the response
// warns when the interval needs more than the allowance. A workspace without a
// monthly limit must choose fixed intervals.
func (p pacedRoutes) updateWorkspaceCadence(w http.ResponseWriter, r *http.Request) {
	workspaceID, err := workspaceIDFromRequest(r)
	if err != nil {
		writeWorkspaceAccessError(w, err)
		return
	}
	var payload dto.WorkspaceCadenceUpdateRequest
	if !decodeJSONBodyLimited(w, r, &payload, resolveJSONMaxBodyBytes()) {
		return
	}
	if err := database.UpdateWorkspaceCadence(r.Context(), workspaceID, payload); err != nil {
		switch {
		case errors.Is(err, gorm.ErrRecordNotFound):
			writeError(w, "Workspace not found", http.StatusNotFound)
		case errors.Is(err, database.ErrCheckIntervalTooShort), errors.Is(err, database.ErrScrapeIntervalTooShort),
			errors.Is(err, database.ErrIntervalTooLong), errors.Is(err, database.ErrUnlimitedNeedsInterval):
			writeError(w, err.Error(), http.StatusBadRequest)
		default:
			log.Error("update workspace cadence", "workspace_id", workspaceID, "error", err)
			writeError(w, "Failed to update workspace intervals", http.StatusInternalServerError)
		}
		return
	}
	p.refreshCheckPace(r.Context())
	var subscription domain.WorkspaceSubscription
	pg.DB.WithContext(r.Context()).Where("workspace_id = ?", workspaceID).Limit(1).Find(&subscription)
	writeJSON(w, http.StatusOK, map[string]any{
		"check_interval_seconds":  payload.CheckIntervalSeconds,
		"scrape_interval_seconds": payload.ScrapeIntervalSeconds,
		"warnings":                database.CadenceWarnings(p.pace.Current(), workspaceID, subscription, payload),
	})
}

// refreshCheckPace publishes a snapshot that includes a just-saved allowance
// or interval before the handler responds, so the workspace data the client
// reloads next already shows the new cadence. If the load fails, the regular
// refresh picks the change up within a minute.
func (p pacedRoutes) refreshCheckPace(ctx context.Context) {
	refreshCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := p.pace.Refresh(refreshCtx); err != nil {
		log.Warn("refresh check pacing after a workspace change", "error", err)
		p.pace.RequestRefresh()
	}
}
