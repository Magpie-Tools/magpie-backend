package server

import (
	"net/http"

	"github.com/charmbracelet/log"
	gqlhandler "github.com/graphql-go/handler"

	"magpie/internal/auth"
	"magpie/internal/dashboard"
	gqlschema "magpie/internal/graphql"
)

// newGraphQLHandler builds the GraphQL handler. views is the process's
// dashboard service. Each request gets its own request scope.
func newGraphQLHandler(views *dashboard.Service) (http.Handler, error) {
	schema, err := gqlschema.NewSchema(views)
	if err != nil {
		return nil, err
	}

	base := gqlhandler.New(&gqlhandler.Config{
		Schema:   &schema,
		Pretty:   true,
		GraphiQL: false,
	})

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := gqlschema.WithRequestScope(r.Context())

		if userID, err := auth.GetUserIDFromRequest(r); err == nil && userID > 0 {
			ctx = gqlschema.WithUserID(ctx, userID)
		} else if err != nil {
			log.Debug("GraphQL token rejected", "error", err)
		}
		if access, err := workspaceAccessFromRequest(r); err == nil {
			ctx = gqlschema.WithWorkspaceAccess(ctx, access.WorkspaceID, access.Role)
		} else {
			log.Debug("GraphQL workspace rejected", "error", err)
		}

		base.ContextHandler(ctx, w, r)
	}), nil
}
