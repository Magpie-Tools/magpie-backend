package server

import (
	"errors"
	"net/http"
	"strconv"

	"gorm.io/gorm"
	"magpie/internal/api/dto"
	"magpie/internal/auth"
	"magpie/internal/database"
	"magpie/internal/domain"
)

func registerAlertRoutes(mux *http.ServeMux) {
	mux.Handle("GET /alerts", auth.RequireAuth(withWorkspaceViewer(http.HandlerFunc(getAlerts))))
	mux.Handle("GET /alerts/rotators", auth.RequireAuth(withWorkspaceViewer(http.HandlerFunc(getAlertRotators))))
	mux.Handle("POST /alerts/rules", auth.RequireAuth(withWorkspaceOperator(http.HandlerFunc(saveAlertRule))))
	mux.Handle("PUT /alerts/rules/{id}", auth.RequireAuth(withWorkspaceOperator(http.HandlerFunc(saveAlertRule))))
	mux.Handle("DELETE /alerts/rules/{id}", auth.RequireAuth(withWorkspaceOperator(http.HandlerFunc(deleteAlertRule))))
	admin := func(handler http.HandlerFunc) http.Handler {
		return auth.RequireAuth(withWorkspaceRole(domain.WorkspaceRoleAdmin, handler))
	}
	mux.Handle("POST /alerts/destinations", admin(saveAlertDestination))
	mux.Handle("PUT /alerts/destinations/{id}", admin(saveAlertDestination))
	mux.Handle("DELETE /alerts/destinations/{id}", admin(deleteAlertDestination))
	mux.Handle("POST /alerts/deliveries/{id}/retry", admin(retryAlertDelivery))
}

func getAlertRotators(w http.ResponseWriter, r *http.Request) {
	workspaceID, err := workspaceIDFromRequest(r)
	if err != nil {
		writeWorkspaceAccessError(w, err)
		return
	}
	rotators, err := database.GetAlertRotators(r.Context(), workspaceID)
	if err != nil {
		writeAlertError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, rotators)
}

func getAlerts(w http.ResponseWriter, r *http.Request) {
	workspaceID, err := workspaceIDFromRequest(r)
	if err != nil {
		writeWorkspaceAccessError(w, err)
		return
	}
	var before uint64
	if raw := r.URL.Query().Get("before"); raw != "" {
		before, err = strconv.ParseUint(raw, 10, 64)
		if err != nil || before == 0 {
			writeError(w, "before must be a positive incident ID", http.StatusBadRequest)
			return
		}
	}
	page, err := database.GetAlerts(r.Context(), workspaceID, before)
	if err != nil {
		writeAlertError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func alertResourceID(w http.ResponseWriter, r *http.Request) (uint64, bool) {
	if r.Method == http.MethodPost && r.PathValue("id") == "" {
		return 0, true
	}
	id, err := strconv.ParseUint(r.PathValue("id"), 10, 64)
	if err != nil || id == 0 {
		writeError(w, "Invalid alert resource ID", http.StatusBadRequest)
		return 0, false
	}
	return id, true
}

func saveAlertRule(w http.ResponseWriter, r *http.Request) {
	workspaceID, err := workspaceIDFromRequest(r)
	if err != nil {
		writeWorkspaceAccessError(w, err)
		return
	}
	id, ok := alertResourceID(w, r)
	if !ok {
		return
	}
	var input dto.AlertRuleWrite
	if !decodeJSONBodyLimited(w, r, &input, resolveJSONMaxBodyBytes()) {
		return
	}
	rule, err := database.SaveAlertRule(r.Context(), workspaceID, id, input)
	if err != nil {
		writeAlertError(w, err)
		return
	}
	status := http.StatusOK
	if id == 0 {
		status = http.StatusCreated
	}
	writeJSON(w, status, rule)
}

func deleteAlertRule(w http.ResponseWriter, r *http.Request) {
	workspaceID, err := workspaceIDFromRequest(r)
	if err != nil {
		writeWorkspaceAccessError(w, err)
		return
	}
	id, ok := alertResourceID(w, r)
	if !ok {
		return
	}
	if err := database.DeleteAlertRule(r.Context(), workspaceID, id); err != nil {
		writeAlertError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func saveAlertDestination(w http.ResponseWriter, r *http.Request) {
	workspaceID, err := workspaceIDFromRequest(r)
	if err != nil {
		writeWorkspaceAccessError(w, err)
		return
	}
	id, ok := alertResourceID(w, r)
	if !ok {
		return
	}
	var input dto.AlertDestinationWrite
	if !decodeJSONBodyLimited(w, r, &input, resolveJSONMaxBodyBytes()) {
		return
	}
	destination, err := database.SaveAlertDestination(r.Context(), workspaceID, id, input)
	if err != nil {
		writeAlertError(w, err)
		return
	}
	status := http.StatusOK
	if id == 0 {
		status = http.StatusCreated
	}
	writeJSON(w, status, destination)
}

func deleteAlertDestination(w http.ResponseWriter, r *http.Request) {
	workspaceID, err := workspaceIDFromRequest(r)
	if err != nil {
		writeWorkspaceAccessError(w, err)
		return
	}
	id, ok := alertResourceID(w, r)
	if !ok {
		return
	}
	if err := database.DeleteAlertDestination(r.Context(), workspaceID, id); err != nil {
		writeAlertError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func retryAlertDelivery(w http.ResponseWriter, r *http.Request) {
	workspaceID, err := workspaceIDFromRequest(r)
	if err != nil {
		writeWorkspaceAccessError(w, err)
		return
	}
	id, ok := alertResourceID(w, r)
	if !ok {
		return
	}
	if err := database.RetryAlertDelivery(r.Context(), workspaceID, id); err != nil {
		writeAlertError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func writeAlertError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, database.ErrAlertValidation):
		writeError(w, err.Error(), http.StatusBadRequest)
	case errors.Is(err, gorm.ErrRecordNotFound):
		writeError(w, "Alert resource not found", http.StatusNotFound)
	default:
		writeError(w, "Could not complete alert operation", http.StatusInternalServerError)
	}
}
