package graphql

import (
	"context"
	"fmt"

	"magpie/internal/account"
	"magpie/internal/domain"
	"magpie/internal/platform/pg"
	workspaces "magpie/internal/workspace"

	gql "github.com/graphql-go/graphql"
)

type viewerData struct {
	user       domain.User
	workspace  domain.Workspace
	preference domain.WorkspaceMemberPreference
	role       string
}

func fetchViewer(ctx context.Context) (interface{}, error) {
	userID, err := UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}

	user := account.GetUserFromId(userID)
	if user.ID == 0 {
		return nil, fmt.Errorf("user %d not found", userID)
	}

	workspaceID, role, err := WorkspaceAccessFromContext(ctx)
	if err != nil {
		return nil, err
	}
	workspace := workspaces.GetWorkspaceByID(workspaceID)
	if workspace.ID == 0 {
		return nil, fmt.Errorf("workspace %d not found", workspaceID)
	}
	var subscription domain.WorkspaceSubscription
	if pg.DB.Where("workspace_id = ?", workspaceID).First(&subscription).Error == nil {
		workspace.Subscription = subscription
	}
	return &viewerData{
		user:       user,
		workspace:  workspace,
		preference: workspaces.GetWorkspaceMemberPreference(workspaceID, userID),
		role:       role,
	}, nil
}

// viewerIdentityFields are the Viewer fields that describe the user account
// and the selected workspace.
func viewerIdentityFields() gql.Fields {
	return gql.Fields{

		"id": &gql.Field{
			Type: gql.NewNonNull(gql.ID),
			Resolve: func(p gql.ResolveParams) (interface{}, error) {
				if data, ok := p.Source.(*viewerData); ok {
					return fmt.Sprintf("%d", data.user.ID), nil
				}
				return nil, nil
			},
		},
		"email": &gql.Field{
			Type: gql.NewNonNull(gql.String),
			Resolve: func(p gql.ResolveParams) (interface{}, error) {
				if data, ok := p.Source.(*viewerData); ok {
					return data.user.Email, nil
				}
				return nil, nil
			},
		},
		"role": &gql.Field{
			Type: gql.NewNonNull(gql.String),
			Resolve: func(p gql.ResolveParams) (interface{}, error) {
				if data, ok := p.Source.(*viewerData); ok {
					return data.user.Role, nil
				}
				return nil, nil
			},
		},
		"workspaceId": &gql.Field{
			Type: gql.NewNonNull(gql.ID),
			Resolve: func(p gql.ResolveParams) (interface{}, error) {
				if data, ok := p.Source.(*viewerData); ok {
					return fmt.Sprintf("%d", data.workspace.ID), nil
				}
				return nil, nil
			},
		},
		"workspaceName": &gql.Field{
			Type: gql.NewNonNull(gql.String),
			Resolve: func(p gql.ResolveParams) (interface{}, error) {
				if data, ok := p.Source.(*viewerData); ok {
					return data.workspace.Name, nil
				}
				return nil, nil
			},
		},
		"workspaceRole": &gql.Field{
			Type: gql.NewNonNull(gql.String),
			Resolve: func(p gql.ResolveParams) (interface{}, error) {
				if data, ok := p.Source.(*viewerData); ok {
					return data.role, nil
				}
				return nil, nil
			},
		},
	}
}
