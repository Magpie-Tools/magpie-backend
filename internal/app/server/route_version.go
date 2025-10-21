package server

import (
	"net/http"

	"magpie/internal/app/version"
)

func getVersion(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, version.GetInfo())
}
