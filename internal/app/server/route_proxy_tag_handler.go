package server

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	"magpie/internal/api/dto"
	"magpie/internal/database"
	"magpie/internal/domain"

	"github.com/charmbracelet/log"
)

func listProxyTags(w http.ResponseWriter, r *http.Request) {
	userID, err := workspaceIDFromRequest(r)
	if err != nil {
		writeError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	tags, dbErr := database.GetProxyTags(userID)
	if dbErr != nil {
		log.Error("failed to list proxy tags", "error", dbErr)
		writeError(w, "Failed to load proxy tags", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, tags)
}

func createProxyTag(w http.ResponseWriter, r *http.Request) {
	userID, err := workspaceIDFromRequest(r)
	if err != nil {
		writeError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	var payload dto.ProxyTagWriteRequest
	if !decodeJSONBodyLimited(w, r, &payload, resolveJSONMaxBodyBytes()) {
		return
	}

	tag, dbErr := database.CreateProxyTag(userID, payload.Name, payload.Color)
	if dbErr != nil {
		writeProxyTagError(w, dbErr)
		return
	}
	writeJSON(w, http.StatusCreated, tag)
}

func updateProxyTag(w http.ResponseWriter, r *http.Request) {
	userID, err := workspaceIDFromRequest(r)
	if err != nil {
		writeError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	tagID, ok := parseProxyTagPathID(w, r)
	if !ok {
		return
	}

	var payload dto.ProxyTagWriteRequest
	if !decodeJSONBodyLimited(w, r, &payload, resolveJSONMaxBodyBytes()) {
		return
	}

	tag, dbErr := database.UpdateProxyTag(userID, tagID, payload.Name, payload.Color)
	if dbErr != nil {
		writeProxyTagError(w, dbErr)
		return
	}
	writeJSON(w, http.StatusOK, tag)
}

func deleteProxyTag(w http.ResponseWriter, r *http.Request) {
	userID, err := workspaceIDFromRequest(r)
	if err != nil {
		writeError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	tagID, ok := parseProxyTagPathID(w, r)
	if !ok {
		return
	}

	if dbErr := database.DeleteProxyTag(userID, tagID); dbErr != nil {
		writeProxyTagError(w, dbErr)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func replaceProxyTags(w http.ResponseWriter, r *http.Request) {
	userID, err := workspaceIDFromRequest(r)
	if err != nil {
		writeError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	proxyID, parseErr := strconv.ParseUint(strings.TrimSpace(r.PathValue("id")), 10, 64)
	if parseErr != nil || proxyID == 0 {
		writeError(w, "Invalid proxy id", http.StatusBadRequest)
		return
	}

	var payload dto.ProxyTagAssignmentRequest
	if !decodeJSONBodyLimited(w, r, &payload, resolveJSONMaxBodyBytes()) {
		return
	}

	tags, dbErr := database.ReplaceProxyTags(userID, proxyID, payload.TagIDs)
	if dbErr != nil {
		writeProxyTagError(w, dbErr)
		return
	}
	writeJSON(w, http.StatusOK, dto.ProxyTagAssignmentResponse{Tags: tags})
}

func parseProxyTagPathID(w http.ResponseWriter, r *http.Request) (uint64, bool) {
	tagID, err := strconv.ParseUint(strings.TrimSpace(r.PathValue("id")), 10, 64)
	if err != nil || tagID == 0 {
		writeError(w, "Invalid proxy tag id", http.StatusBadRequest)
		return 0, false
	}
	return tagID, true
}

func writeProxyTagError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, domain.ErrProxyTagNameRequired):
		writeError(w, "Tag name is required", http.StatusBadRequest)
	case errors.Is(err, domain.ErrProxyTagNameTooLong):
		writeError(w, "Tag name must be 40 characters or fewer", http.StatusBadRequest)
	case errors.Is(err, domain.ErrProxyTagColorInvalid):
		writeError(w, "Tag color must use #RRGGBB", http.StatusBadRequest)
	case errors.Is(err, database.ErrProxyTagNameConflict):
		writeError(w, "A tag with that name already exists", http.StatusConflict)
	case errors.Is(err, database.ErrProxyTagNotFound):
		writeError(w, "Proxy tag not found", http.StatusNotFound)
	case errors.Is(err, database.ErrProxyAccessNotFound):
		writeError(w, "Proxy not found", http.StatusNotFound)
	default:
		log.Error("proxy tag request failed", "error", err)
		writeError(w, "Could not save proxy tags", http.StatusInternalServerError)
	}
}

func parseStrictProxyTagIDs(values []string) ([]uint64, error) {
	seen := make(map[uint64]struct{}, len(values))
	result := make([]uint64, 0, len(values))
	for _, raw := range values {
		for _, part := range strings.Split(raw, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			id, err := strconv.ParseUint(part, 10, 64)
			if err != nil || id == 0 {
				return nil, errors.New("invalid proxy tag id")
			}
			if _, exists := seen[id]; exists {
				continue
			}
			seen[id] = struct{}{}
			result = append(result, id)
		}
	}
	return result, nil
}

func parseProxyTagFilterIDs(values []string) []uint64 {
	ids, err := parseStrictProxyTagIDs(values)
	if err != nil {
		return nil
	}
	return ids
}
