package database

import (
	"magpie/internal/domain"

	"gorm.io/gorm"
)

// Keep namespace selection separate from key validation so PostgreSQL can reject
// other workspaces before ownership and sparse-override joins.
const workspaceLatestStatisticsKeySQL = `
 AND NOT w.checker_dirty
 AND (w.checker_generation = 0 OR (w.checker_generation > 0 AND pls.config_key <> ''
 AND pls.config_key = COALESCE(cp.config_key,
 CASE pls.protocol_id WHEN 1 THEN w.checker_http_key WHEN 2 THEN w.checker_https_key
 WHEN 3 THEN w.checker_socks4_key WHEN 4 THEN w.checker_socks5_key ELSE '' END)))
`

const workspaceLatestStatisticsSQL = `
SELECT pls.*, up.workspace_id AS checking_workspace_id
FROM workspaces w
JOIN user_proxies up ON up.workspace_id = w.id
JOIN proxy_latest_statistics pls ON pls.proxy_id = up.proxy_id
 AND pls.workspace_id = CASE WHEN w.checker_generation = 0 THEN 0 ELSE w.id END
LEFT JOIN proxy_checker_plans cp ON cp.workspace_id = up.workspace_id AND cp.proxy_id = pls.proxy_id AND cp.protocol_id = pls.protocol_id
WHERE up.workspace_id = ? AND pls.workspace_id IN (0, ?)
` + workspaceLatestStatisticsKeySQL

func workspaceLatestStatistics(tx *gorm.DB, workspaceID uint, activeOnly bool) *gorm.DB {
	query := workspaceLatestStatisticsSQL
	if activeOnly {
		query += " AND up.state = '" + domain.ManagedProxyStateActive + "'"
	}
	return tx.Table("("+query+") pls", workspaceID, workspaceID)
}

// Dashboard aggregates consume the whole active inventory. Materialize its
// ownership once so a low current-key estimate cannot turn the join into one
// route-only ownership probe per result, visiting every shared owner each time.
// Keep point/detail reads on the pushdown-friendly helper above.
func dashboardLatestStatistics(tx *gorm.DB, workspaceID uint) *gorm.DB {
	if tx.Dialector.Name() != "postgres" {
		return workspaceLatestStatistics(tx, workspaceID, true)
	}
	query := `WITH active_owners AS MATERIALIZED (
 SELECT proxy_id FROM user_proxies WHERE workspace_id = ? AND state = 'active'
)
SELECT pls.*, w.id AS checking_workspace_id
FROM workspaces w
JOIN active_owners up ON TRUE
JOIN proxy_latest_statistics pls ON pls.proxy_id = up.proxy_id
 AND pls.workspace_id = CASE WHEN w.checker_generation = 0 THEN 0 ELSE w.id END
LEFT JOIN proxy_checker_plans cp ON cp.workspace_id = w.id AND cp.proxy_id = pls.proxy_id AND cp.protocol_id = pls.protocol_id
WHERE w.id = ? AND pls.workspace_id IN (0, ?)
` + workspaceLatestStatisticsKeySQL
	return tx.Table("("+query+") pls", workspaceID, workspaceID, workspaceID)
}
