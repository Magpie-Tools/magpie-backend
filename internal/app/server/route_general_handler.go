package server

import (
	"magpie/internal/config"
	"net/http"
)

func getGlobalSettings(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, config.GetConfig())
}
