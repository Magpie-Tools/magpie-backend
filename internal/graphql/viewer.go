package graphql

import (
	"context"
	"fmt"

	"magpie/internal/account"
	"magpie/internal/domain"
	"magpie/internal/platform/pg"
	workspaces "magpie/internal/workspace"
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
