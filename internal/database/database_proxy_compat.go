package database

import "magpie/internal/domain"

// InsertAndGetProxiesWithUser resolves each user account's default workspace.
// For compatibility, an ID that cannot be resolved is treated as a workspace ID.
//
// Deprecated: Use InsertAndGetProxiesWithWorkspace with explicit workspace IDs.
func InsertAndGetProxiesWithUser(proxies []domain.Proxy, userIDs ...uint) ([]domain.Proxy, error) {
	workspaceIDs := make([]uint, 0, len(userIDs))
	for _, userID := range userIDs {
		if access, err := ResolveWorkspaceAccess(userID, 0); err == nil {
			workspaceIDs = append(workspaceIDs, access.WorkspaceID)
		} else {
			// Source compatibility for callers that already passed a workspace ID
			// before the function was renamed.
			workspaceIDs = append(workspaceIDs, userID)
		}
	}
	return InsertAndGetProxiesWithWorkspace(proxies, workspaceIDs...)
}

// GetAllProxyCountOfUser is the compatibility name for a workspace's proxy count.
// The ID identifies a workspace, not a user account.
//
// Deprecated: Use GetWorkspaceProxyCount.
func GetAllProxyCountOfUser(workspaceID uint) int64 {
	return GetWorkspaceProxyCount(workspaceID)
}

// GetQueuedProxyForUser is the compatibility name for a workspace's queued route.
// The ID identifies a workspace, not a user account.
//
// Deprecated: Use GetQueuedProxyForWorkspace.
func GetQueuedProxyForUser(workspaceID uint, proxyID uint64) (*domain.Proxy, error) {
	return GetQueuedProxyForWorkspace(workspaceID, proxyID)
}
