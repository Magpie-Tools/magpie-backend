package server

import (
	"encoding/json"
	"magpie/internal/config"
	"magpie/internal/database"
	"net/http"
)

func getGlobalSettings(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(config.GetConfig())
}

func getDashboardInfo(w http.ResponseWriter, r *http.Request) {
	userID, userErr := workspaceIDFromRequest(r)
	if userErr != nil {
		writeError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	dashInfo := database.GetDashboardInfo(userID)

	json.NewEncoder(w).Encode(dashInfo)
}
