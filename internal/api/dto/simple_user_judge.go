package dto

type SimpleWorkspaceJudge struct {
	Url   string `json:"url"`
	Regex string `json:"regex"`
}

// SimpleUserJudge is the compatibility name for workspace judge settings.
//
// Deprecated: Use SimpleWorkspaceJudge.
type SimpleUserJudge = SimpleWorkspaceJudge
