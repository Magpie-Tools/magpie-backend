package server

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"magpie/internal/auth"
	"magpie/internal/database"
	"magpie/internal/domain"
)

const workspaceHeader = "X-Workspace-ID"

type workspaceContextKey struct{}

func withWorkspaceRole(minimumRole string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		access, err := resolveRequestWorkspaceAccess(r)
		if err != nil {
			writeWorkspaceAccessError(w, err)
			return
		}
		if domain.WorkspaceRoleRank(access.Role) < domain.WorkspaceRoleRank(minimumRole) {
			writeError(w, "Workspace role does not permit this action", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), workspaceContextKey{}, access)))
	})
}

func withWorkspaceViewer(next http.Handler) http.Handler {
	return withWorkspaceRole(domain.WorkspaceRoleViewer, next)
}

func withWorkspaceOperator(next http.Handler) http.Handler {
	return withWorkspaceRole(domain.WorkspaceRoleOperator, next)
}

func resolveRequestWorkspaceAccess(r *http.Request) (database.WorkspaceAccess, error) {
	userID, err := auth.GetUserIDFromRequest(r)
	if err != nil {
		return database.WorkspaceAccess{}, err
	}

	var requested uint
	raw := strings.TrimSpace(r.Header.Get(workspaceHeader))
	if raw != "" {
		parsed, parseErr := strconv.ParseUint(raw, 10, 64)
		if parseErr != nil || parsed == 0 || uint64(uint(parsed)) != parsed {
			return database.WorkspaceAccess{}, errInvalidWorkspaceHeader
		}
		requested = uint(parsed)
	}
	return database.ResolveWorkspaceAccess(userID, requested)
}

var errInvalidWorkspaceHeader = errors.New("invalid workspace header")

func workspaceAccessFromRequest(r *http.Request) (database.WorkspaceAccess, error) {
	if r != nil && r.Context() != nil {
		if access, ok := r.Context().Value(workspaceContextKey{}).(database.WorkspaceAccess); ok && access.WorkspaceID > 0 {
			return access, nil
		}
	}
	return resolveRequestWorkspaceAccess(r)
}

func workspaceIDFromRequest(r *http.Request) (uint, error) {
	access, err := workspaceAccessFromRequest(r)
	if err != nil {
		return 0, err
	}
	return access.WorkspaceID, nil
}

func writeWorkspaceAccessError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errInvalidWorkspaceHeader):
		writeError(w, "X-Workspace-ID must be a positive integer", http.StatusBadRequest)
	case errors.Is(err, database.ErrWorkspaceNotFound):
		writeError(w, "Workspace not found or membership required", http.StatusForbidden)
	default:
		writeError(w, "Unauthorized", http.StatusUnauthorized)
	}
}
