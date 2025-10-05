package graphql

import (
	"context"
	"errors"
)

type contextKey string

const userIDKey contextKey = "graphql.userID"

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
