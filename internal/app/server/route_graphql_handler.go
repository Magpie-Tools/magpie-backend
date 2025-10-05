package server

import (
	"net/http"
	"strings"
	"sync"

	"github.com/charmbracelet/log"
	gqlhandler "github.com/graphql-go/handler"

	"magpie/internal/auth"
	gqlschema "magpie/internal/graphql"
)

var (
	graphQLHandler     http.Handler
	graphQLHandlerOnce sync.Once
	graphQLHandlerErr  error
)

func getGraphQLHandler() (http.Handler, error) {
	graphQLHandlerOnce.Do(func() {
		schema, err := gqlschema.NewSchema()
		if err != nil {
			graphQLHandlerErr = err
			return
		}

		base := gqlhandler.New(&gqlhandler.Config{
			Schema:   &schema,
			Pretty:   true,
			GraphiQL: false,
		})

		graphQLHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()

			if token := extractBearerToken(r.Header.Get("Authorization")); token != "" {
				if claims, err := auth.ValidateJWT(token); err == nil {
					if rawID, ok := claims["user_id"].(float64); ok && rawID > 0 {
						ctx = gqlschema.WithUserID(ctx, uint(rawID))
					}
				} else {
					log.Debug("GraphQL token rejected", "error", err)
				}
			}

			base.ContextHandler(ctx, w, r)
		})
	})

	return graphQLHandler, graphQLHandlerErr
}

func extractBearerToken(header string) string {
	if header == "" {
		return ""
	}
	const prefix = "Bearer "
	if len(header) < len(prefix) {
		return ""
	}
	if !strings.HasPrefix(header, prefix) {
		return ""
	}
	return strings.TrimSpace(header[len(prefix):])
}
