package routing

import (
	"encoding/json"
	"magpie/authorization"
	"magpie/database"
	"magpie/settings"
	"net/http"
)

func getGlobalSettings(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(settings.GetConfig())
}

func getDashboardInfo(w http.ResponseWriter, r *http.Request) {
	userID, userErr := authorization.GetUserIDFromRequest(r)
	if userErr != nil {
		writeError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	dashInfo := database.GetDashboardInfo(userID)

	json.NewEncoder(w).Encode(dashInfo)
}
