package server

import (
	"magpie/internal/config"
	"magpie/internal/database"
	"net/http"
)

func getGlobalSettings(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, config.GetConfig())
}

func getDashboardInfo(w http.ResponseWriter, r *http.Request) {
	userID, userErr := workspaceIDFromRequest(r)
	if userErr != nil {
		writeError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	dashInfo, err := database.GetDashboardInfoContext(r.Context(), userID)
	if err != nil {
		writeError(w, "Failed to load dashboard", http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, dashInfo)
}
