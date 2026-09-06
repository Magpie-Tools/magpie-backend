// Package settings applies workspace settings consistently across API transports.
package settings

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/log"

	"magpie/internal/api/dto"
	"magpie/internal/config"
	"magpie/internal/database"
	"magpie/internal/jobs/checker/judges"
)

type BlockedJudgesError struct {
	URLs []string
}

func (err *BlockedJudgesError) Error() string {
	return "One or more judges point to blocked websites: " + strings.Join(err.URLs, ", ")
}

// ValidateCheckerLimits checks integers before GraphQL narrows them to the DTO's
// unsigned types. REST's JSON decoder also rejects values outside these ranges.
func ValidateCheckerLimits(timeout, retries, failureThreshold int) error {
	for _, limit := range []struct {
		name       string
		value, max int
	}{
		{"timeout", timeout, 65535},
		{"retries", retries, 255},
		{"autoRemoveFailureThreshold", failureThreshold, 255},
	} {
		if limit.value < 0 || limit.value > limit.max {
			return fmt.Errorf("%s must be between 0 and %d", limit.name, limit.max)
		}
	}
	return nil
}

// SaveWorkspace persists settings, then updates the local checker judge cache
// and broadcasts the change to other instances. Scrape sources are managed by
// the scrape-source API and are read-only in settings responses.
func SaveWorkspace(workspaceID, userID uint, value dto.UserSettings) error {
	if err := ValidateCheckerLimits(int(value.Timeout), int(value.Retries), int(value.AutoRemoveFailureThreshold)); err != nil {
		return err
	}
	var blocked []string
	seen := make(map[string]struct{})
	for _, judge := range value.SimpleUserJudges {
		if config.IsWebsiteBlocked(judge.Url) {
			if _, exists := seen[judge.Url]; !exists {
				blocked = append(blocked, judge.Url)
				seen[judge.Url] = struct{}{}
			}
		}
	}
	if len(blocked) > 0 {
		return &BlockedJudgesError{URLs: blocked}
	}
	if err := database.UpdateWorkspaceSettings(workspaceID, userID, value); err != nil {
		return err
	}
	loaded, err := database.GetWorkspaceJudgesWithRegex(workspaceID)
	if err != nil {
		log.Warn("failed to refresh workspace judge cache after settings update", "workspace_id", workspaceID, "error", err)
	} else {
		judges.SetUserJudges(workspaceID, loaded)
	}
	return nil
}
