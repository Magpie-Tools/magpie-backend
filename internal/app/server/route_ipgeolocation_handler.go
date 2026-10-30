package server

import (
	"net/http"
	"time"

	"magpie/internal/config"
	"magpie/internal/database"
)

func getIPGeolocationStatus(w http.ResponseWriter, r *http.Request) {
	state, err := database.GetIPGeolocationState(r.Context())
	if err != nil {
		writeError(w, "Could not load IPGeolocation.io status", http.StatusServiceUnavailable)
		return
	}
	now := time.Now().UTC()
	if state.BudgetDay != now.Format("2006-01-02") {
		state.UsedCredits = 0
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"used_credits": state.UsedCredits, "daily_credit_limit": config.GetConfig().Plugins.IPGeolocation.DailyCreditLimit,
		"reset_at": now.Truncate(24 * time.Hour).Add(24 * time.Hour), "next_request_at": state.NextRequestAt,
		"last_checked_at": state.LastCheckedAt, "last_error": state.LastError,
	})
}
