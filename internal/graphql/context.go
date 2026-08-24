package graphql

import (
	"context"
	"errors"
)

type contextKey string

const userIDKey contextKey = "graphql.userID"
const workspaceIDKey contextKey = "graphql.workspaceID"
const workspaceRoleKey contextKey = "graphql.workspaceRole"

var ErrUnauthenticated = errors.New("unauthenticated")

func WithUserID(ctx context.Context, userID uint) context.Context {
	return context.WithValue(ctx, userIDKey, userID)
}

func UserIDFromContext(ctx context.Context) (uint, error) {
	if ctx == nil {
		return 0, ErrUnauthenticated
	}
	if raw, ok := ctx.Value(userIDKey).(uint); ok && raw > 0 {
		return raw, nil
	}
	return 0, ErrUnauthenticated
}

func WithWorkspaceAccess(ctx context.Context, workspaceID uint, role string) context.Context {
	ctx = context.WithValue(ctx, workspaceIDKey, workspaceID)
	return context.WithValue(ctx, workspaceRoleKey, role)
}

func WorkspaceAccessFromContext(ctx context.Context) (uint, string, error) {
	if ctx == nil {
		return 0, "", ErrUnauthenticated
	}
	workspaceID, ok := ctx.Value(workspaceIDKey).(uint)
	if !ok || workspaceID == 0 {
		return 0, "", ErrUnauthenticated
	}
	role, _ := ctx.Value(workspaceRoleKey).(string)
	if role == "" {
		return 0, "", ErrUnauthenticated
	}
	return workspaceID, role, nil
}
