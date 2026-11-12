package evidence

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

func WorkspaceLatestStatistics(tx *gorm.DB, workspaceID uint, activeOnly bool) *gorm.DB {
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
func DashboardLatestStatistics(tx *gorm.DB, workspaceID uint) *gorm.DB {
	if tx.Dialector.Name() != "postgres" {
		return WorkspaceLatestStatistics(tx, workspaceID, true)
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

// Current evidence uses the sparse override key, or the workspace default.
// Generation zero is solely the compatibility path for unmigrated test data.
const CurrentLatestStatisticsSQL = `
SELECT pls.*, up.workspace_id AS checking_workspace_id
FROM proxy_latest_statistics pls
JOIN user_proxies up ON up.proxy_id = pls.proxy_id
JOIN workspaces w ON w.id = up.workspace_id
LEFT JOIN proxy_checker_plans cp ON cp.workspace_id = up.workspace_id AND cp.proxy_id = pls.proxy_id AND cp.protocol_id = pls.protocol_id
WHERE NOT w.checker_dirty AND ((w.checker_generation = 0 AND pls.workspace_id = 0)
OR (w.checker_generation > 0 AND pls.workspace_id = up.workspace_id AND pls.config_key = COALESCE(cp.config_key,
CASE pls.protocol_id WHEN 1 THEN w.checker_http_key WHEN 2 THEN w.checker_https_key WHEN 3 THEN w.checker_socks4_key WHEN 4 THEN w.checker_socks5_key ELSE '' END) AND pls.config_key <> ''))
`

func CurrentLatestStatistics(tx *gorm.DB, workspaceID uint) *gorm.DB {
	return WorkspaceLatestStatistics(tx, workspaceID, false)
}

const CurrentOverallStatusesSQL = `SELECT up.workspace_id, pos.proxy_id, pos.overall_alive, pos.last_checked_at
FROM proxy_overall_statuses pos JOIN user_proxies up ON up.proxy_id = pos.proxy_id
JOIN workspaces w ON w.id = up.workspace_id AND w.checker_generation = 0 AND NOT w.checker_dirty
UNION ALL
SELECT checking_workspace_id AS workspace_id, proxy_id,
CASE WHEN MAX(CASE WHEN alive THEN 1 ELSE 0 END) > 0 THEN TRUE ELSE FALSE END AS overall_alive,
MAX(checked_at) AS last_checked_at FROM (` + CurrentLatestStatisticsSQL + `) scoped WHERE scoped.workspace_id > 0 GROUP BY checking_workspace_id, proxy_id`

// Correlate candidates before aggregation. A grouped workspace-wide subquery
// cannot push an outer proxy filter down through GROUP BY.
func CurrentOverallStatusForProxySQL(workspaceExpr, proxyExpr string) string {
	return `SELECT legacy_up.workspace_id, pos.proxy_id, pos.overall_alive, pos.last_checked_at
FROM proxy_overall_statuses pos JOIN user_proxies legacy_up ON legacy_up.proxy_id = pos.proxy_id
JOIN workspaces w ON w.id = legacy_up.workspace_id AND w.checker_generation = 0 AND NOT w.checker_dirty
WHERE legacy_up.workspace_id = ` + workspaceExpr + ` AND pos.proxy_id = ` + proxyExpr + `
UNION ALL
SELECT checking_workspace_id AS workspace_id, proxy_id,
MAX(CASE WHEN alive THEN 1 ELSE 0 END) > 0 AS overall_alive, MAX(checked_at) AS last_checked_at
FROM (` + CurrentLatestStatisticsSQL + `) evidence
WHERE evidence.workspace_id > 0 AND evidence.checking_workspace_id = ` + workspaceExpr + ` AND evidence.proxy_id = ` + proxyExpr + `
GROUP BY checking_workspace_id, proxy_id`
}

func CurrentOverallStatusJoin(tx *gorm.DB, kind, workspaceExpr, proxyExpr string) string {
	if tx.Dialector.Name() == "postgres" {
		return kind + " JOIN LATERAL (" + CurrentOverallStatusForProxySQL(workspaceExpr, proxyExpr) + ") pos ON TRUE"
	}
	return kind + " JOIN (" + CurrentOverallStatusesSQL + ") pos ON pos.proxy_id = " + proxyExpr + " AND pos.workspace_id = " + workspaceExpr
}
