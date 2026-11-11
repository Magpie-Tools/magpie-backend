package graphql

import (
	"context"
	"fmt"

	"magpie/internal/api/dto"
	"magpie/internal/domain"
	"magpie/internal/judgestore"
	"magpie/internal/scrapestore"
	workspacesettings "magpie/internal/settings"
	workspaces "magpie/internal/workspace"
)

func buildUserSettings(workspace domain.Workspace, preference domain.WorkspaceMemberPreference, judges []dto.SimpleWorkspaceJudge, sources []string) map[string]interface{} {
	dtoSettings := workspace.ToUserSettings(judges, sources, preference)

	judgeList := make([]map[string]interface{}, 0, len(dtoSettings.SimpleUserJudges))
	for _, judge := range dtoSettings.SimpleUserJudges {
		judgeList = append(judgeList, map[string]interface{}{
			"url":   judge.Url,
			"regex": judge.Regex,
		})
	}

	return map[string]interface{}{
		"checkerSettings":            checkerSettingsOutput(dtoSettings.CheckerSettings),
		"transportProtocol":          dtoSettings.TransportProtocol,
		"httpProtocol":               dtoSettings.HTTPProtocol,
		"httpsProtocol":              dtoSettings.HTTPSProtocol,
		"socks4Protocol":             dtoSettings.SOCKS4Protocol,
		"socks5Protocol":             dtoSettings.SOCKS5Protocol,
		"timeout":                    int(dtoSettings.Timeout),
		"retries":                    int(dtoSettings.Retries),
		"useHttpsForSocks":           dtoSettings.UseHttpsForSocks,
		"autoRemoveFailingProxies":   dtoSettings.AutoRemoveFailingProxies,
		"autoRemoveFailureThreshold": int(dtoSettings.AutoRemoveFailureThreshold),
		"failureAction":              dtoSettings.FailureAction,
		"judges":                     judgeList,
		"scrapingSources":            dtoSettings.ScrapingSources,
		"proxyListColumns":           dtoSettings.ProxyListColumns,
		"scrapeSourceProxyColumns":   dtoSettings.ScrapeSourceProxyColumns,
		"scrapeSourceListColumns":    dtoSettings.ScrapeSourceListColumns,
	}
}

func applyUserSettings(ctx context.Context, input map[string]interface{}) error {
	if input == nil {
		return fmt.Errorf("missing input")
	}

	userID, err := UserIDFromContext(ctx)
	if err != nil {
		return err
	}
	workspaceID, role, err := WorkspaceAccessFromContext(ctx)
	if err != nil {
		return err
	}
	if domain.WorkspaceRoleRank(role) < domain.WorkspaceRoleRank(domain.WorkspaceRoleOperator) {
		return fmt.Errorf("workspace role does not permit settings changes")
	}
	timeout, _ := input["timeout"].(int)
	retries, _ := input["retries"].(int)
	failureThreshold, _ := input["autoRemoveFailureThreshold"].(int)
	if err := workspacesettings.ValidateCheckerLimits(timeout, retries, failureThreshold); err != nil {
		return err
	}
	workspace := workspaces.GetWorkspaceByID(workspaceID)
	if workspace.ID == 0 {
		return fmt.Errorf("workspace %d not found", workspaceID)
	}
	currentJudges := judgestore.GetWorkspaceJudges(workspaceID)
	currentSources := scrapestore.GetScrapingSourcesOfUsers(workspaceID)
	preference := workspaces.GetWorkspaceMemberPreference(workspaceID, userID)
	settings := workspace.ToUserSettings(currentJudges, currentSources, preference)
	settings.CheckerSettings = nil
	if raw, ok := input["checkerSettings"].(map[string]interface{}); ok {
		parsed, err := parseCheckerSettings(raw)
		if err != nil {
			return err
		}
		settings.CheckerSettings = parsed
	}
	if transport, ok := input["transportProtocol"].(string); ok {
		settings.TransportProtocol = transport
	}

	if v, ok := input["httpProtocol"].(bool); ok {
		settings.HTTPProtocol = v
	}
	if v, ok := input["httpsProtocol"].(bool); ok {
		settings.HTTPSProtocol = v
	}
	if v, ok := input["socks4Protocol"].(bool); ok {
		settings.SOCKS4Protocol = v
	}
	if v, ok := input["socks5Protocol"].(bool); ok {
		settings.SOCKS5Protocol = v
	}
	if v, ok := input["timeout"].(int); ok {
		settings.Timeout = uint16(v)
	}
	if v, ok := input["retries"].(int); ok {
		settings.Retries = uint8(v)
	}
	if v, ok := input["useHttpsForSocks"].(bool); ok {
		settings.UseHttpsForSocks = v
	}
	if v, ok := input["autoRemoveFailingProxies"].(bool); ok {
		settings.AutoRemoveFailingProxies = v
	}
	if v, ok := input["autoRemoveFailureThreshold"].(int); ok {
		settings.AutoRemoveFailureThreshold = uint8(v)
	}
	if v, ok := input["failureAction"].(string); ok {
		settings.FailureAction = v
	}

	if rawJudges, ok := input["judges"].([]interface{}); ok {
		judges := make([]dto.SimpleWorkspaceJudge, 0, len(rawJudges))
		for _, rawJudge := range rawJudges {
			if judgeMap, ok := rawJudge.(map[string]interface{}); ok {
				judge := dto.SimpleWorkspaceJudge{}
				if url, ok := judgeMap["url"].(string); ok {
					judge.Url = url
				}
				if regex, ok := judgeMap["regex"].(string); ok {
					judge.Regex = regex
				}
				judges = append(judges, judge)
			}
		}
		settings.SimpleUserJudges = judges
	}

	if rawColumns, ok := input["proxyListColumns"].([]interface{}); ok {
		columns := make([]string, 0, len(rawColumns))
		for _, raw := range rawColumns {
			if column, ok := raw.(string); ok {
				columns = append(columns, column)
			}
		}
		settings.ProxyListColumns = columns
	}
	if rawColumns, ok := input["scrapeSourceProxyColumns"].([]interface{}); ok {
		columns := make([]string, 0, len(rawColumns))
		for _, raw := range rawColumns {
			if column, ok := raw.(string); ok {
				columns = append(columns, column)
			}
		}
		settings.ScrapeSourceProxyColumns = columns
	}
	if rawColumns, ok := input["scrapeSourceListColumns"].([]interface{}); ok {
		columns := make([]string, 0, len(rawColumns))
		for _, raw := range rawColumns {
			if column, ok := raw.(string); ok {
				columns = append(columns, column)
			}
		}
		settings.ScrapeSourceListColumns = columns
	}

	if err := workspacesettings.SaveWorkspace(workspaceID, userID, settings); err != nil {
		return err
	}

	return nil
}
