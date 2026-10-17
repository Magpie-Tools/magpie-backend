package server

import (
	"errors"
	"net/http"
	"strings"

	"github.com/charmbracelet/log"

	"magpie/internal/auth"
	"magpie/internal/database"
	"magpie/internal/domain"
)

const maxNotificationReadIDs = 50

func registerNotificationRoutes(mux *http.ServeMux) {
	mux.Handle("GET /notifications/read-state", auth.RequireAuth(http.HandlerFunc(getNotificationReadState)))
	mux.Handle("PUT /notifications/{id}/read", auth.RequireAuth(http.HandlerFunc(markNotificationRead)))
}

func getNotificationReadState(w http.ResponseWriter, r *http.Request) {
	userID, err := auth.GetUserIDFromRequest(r)
	if err != nil {
		writeError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	ids := []string{}
	if raw := r.URL.Query().Get("ids"); raw != "" {
		ids = strings.Split(raw, ",")
	}
	if len(ids) > maxNotificationReadIDs {
		writeError(w, "At most 50 notification IDs are allowed", http.StatusBadRequest)
		return
	}
	for _, id := range ids {
		if _, _, ok := domain.ParseNotificationID(id); !ok {
			writeError(w, "Invalid notification ID", http.StatusBadRequest)
			return
		}
	}
	readIDs, err := database.ReadNotificationIDs(r.Context(), userID, ids)
	if err != nil {
		log.Error("failed to load notification read state", "error", err)
		writeError(w, "Failed to load notification read state", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, map[string]any{"read_ids": readIDs})
}

func markNotificationRead(w http.ResponseWriter, r *http.Request) {
	userID, err := auth.GetUserIDFromRequest(r)
	if err != nil {
		writeError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	err = database.MarkNotificationRead(r.Context(), userID, r.PathValue("id"))
	switch {
	case errors.Is(err, database.ErrInvalidNotificationID):
		writeError(w, "Invalid notification ID", http.StatusBadRequest)
	case errors.Is(err, database.ErrNotificationNotFound):
		writeError(w, "Notification not found", http.StatusNotFound)
	case err != nil:
		log.Error("failed to mark notification as read", "error", err)
		writeError(w, "Failed to mark notification as read", http.StatusInternalServerError)
	default:
		w.WriteHeader(http.StatusNoContent)
	}
}
