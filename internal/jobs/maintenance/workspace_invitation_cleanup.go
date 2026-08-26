package maintenance

import (
	"context"
	"errors"
	"time"

	"magpie/internal/database"
	"magpie/internal/support"

	"github.com/charmbracelet/log"
)

const (
	workspaceInvitationCleanupInterval = time.Hour
	workspaceInvitationCleanupLockKey  = "magpie:leader:workspace_invitation_cleanup"
)

func StartWorkspaceInvitationCleanupRoutine(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	err := support.RunWithLeader(ctx, workspaceInvitationCleanupLockKey, support.DefaultLeadershipTTL, func(leaderCtx context.Context) {
		runWorkspaceInvitationCleanupLoop(leaderCtx)
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Error("Workspace invitation cleanup routine stopped", "error", err)
	}
}

func runWorkspaceInvitationCleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(workspaceInvitationCleanupInterval)
	defer ticker.Stop()

	runWorkspaceInvitationCleanup(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			runWorkspaceInvitationCleanup(ctx)
		}
	}
}

func runWorkspaceInvitationCleanup(ctx context.Context) {
	removed, err := database.DeleteExpiredWorkspaceInvitations(ctx, time.Now().UTC())
	if err != nil {
		log.Error("Failed to cleanup expired workspace invitations", "error", err)
		return
	}
	if removed > 0 {
		log.Info("Expired workspace invitations removed", "count", removed)
	}
}
