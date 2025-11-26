package server

import (
	"net/http"

	"github.com/charmbracelet/log"

	"magpie/internal/app/releases"
	"magpie/internal/app/version"
)

func getReleases(w http.ResponseWriter, r *http.Request) {
	items, err := releases.Get(r.Context())
	if err != nil {
		log.Warn("failed to fetch releases", "error", err)
		writeError(w, "Failed to load release notes", http.StatusBadGateway)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"releases": items,
		"build":    version.Get(),
	})
}
