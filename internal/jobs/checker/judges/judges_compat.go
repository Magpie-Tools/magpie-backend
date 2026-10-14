package judges

import "magpie/internal/domain"

// AddUserJudge is the compatibility name for adding a workspace's cached judge.
// The ID identifies a workspace, not a user account.
//
// Deprecated: Use AddWorkspaceJudge.
func AddUserJudge(workspaceID uint, judge *domain.Judge, regex string) {
	AddWorkspaceJudge(workspaceID, judge, regex)
}

// AddJudgesToUsers is the compatibility name for adding cached workspace judges.
// The IDs identify workspaces, not user accounts.
//
// Deprecated: Use AddJudgesToWorkspaces.
func AddJudgesToUsers(workspaceIDs []uint, judgesWithRegex []domain.JudgeWithRegex) {
	AddJudgesToWorkspaces(workspaceIDs, judgesWithRegex)
}

// SetUserJudges is the compatibility name for replacing a workspace's cached judges.
// The ID identifies a workspace, not a user account.
//
// Deprecated: Use SetWorkspaceJudges.
func SetUserJudges(workspaceID uint, judgesWithRegex []domain.JudgeWithRegex) {
	SetWorkspaceJudges(workspaceID, judgesWithRegex)
}
