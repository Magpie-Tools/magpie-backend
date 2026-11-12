package evidence

import (
	"fmt"
	"gorm.io/gorm"

	"magpie/internal/domain"
	"magpie/internal/platform/pg"
)

func AliveProxyCountByWorkspace(tx *gorm.DB, workspaceIDs []uint) (map[uint]int64, error) {
	counts := make(map[uint]int64, len(workspaceIDs))
	for _, workspaceID := range workspaceIDs {
		counts[workspaceID] = 0
	}
	if len(workspaceIDs) == 0 {
		return counts, nil
	}

	var rows []struct {
		WorkspaceID uint
		AliveCount  int64
	}

	activeOwnership := `SELECT workspace_id, proxy_id FROM user_proxies
WHERE workspace_id IN ? AND state = ?`
	if pg.IsPostgresDialect(tx) {
		// Keep the requested ownership scope as one input to the join. Dense
		// current evidence can otherwise cause an underestimated nested loop
		// to probe the ownership primary key once for every alive route.
		activeOwnership += ` OFFSET 0`
	}

	// Count matching evidence before joining ownership. Correlating an overall
	// status aggregate to every owned route repeats the evidence lookup millions
	// of times, even when most routes have no current alive result.
	if err := tx.Raw(`SELECT alive.workspace_id, COUNT(*) AS alive_count
FROM (
    SELECT DISTINCT pls.workspace_id, pls.proxy_id
    FROM workspaces w
    JOIN proxy_latest_statistics pls ON pls.workspace_id = w.id AND pls.alive = TRUE
    LEFT JOIN proxy_checker_plans cp ON cp.workspace_id = w.id AND cp.proxy_id = pls.proxy_id AND cp.protocol_id = pls.protocol_id
    WHERE w.id IN ? AND w.checker_generation > 0 AND NOT w.checker_dirty
      AND pls.config_key <> '' AND pls.config_key = COALESCE(cp.config_key,
          CASE pls.protocol_id WHEN 1 THEN w.checker_http_key WHEN 2 THEN w.checker_https_key
          WHEN 3 THEN w.checker_socks4_key WHEN 4 THEN w.checker_socks5_key ELSE '' END)
) alive
JOIN (`+activeOwnership+`) up ON up.workspace_id = alive.workspace_id AND up.proxy_id = alive.proxy_id
GROUP BY alive.workspace_id
UNION ALL
SELECT up.workspace_id, COUNT(*) AS alive_count
FROM workspaces w
JOIN user_proxies up ON up.workspace_id = w.id AND up.state = ?
JOIN proxy_overall_statuses pos ON pos.proxy_id = up.proxy_id AND pos.overall_alive = TRUE
WHERE w.id IN ? AND w.checker_generation = 0 AND NOT w.checker_dirty
GROUP BY up.workspace_id`, workspaceIDs, workspaceIDs, domain.ManagedProxyStateActive,
		domain.ManagedProxyStateActive, workspaceIDs).Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("proxy snapshot: aggregate alive counts: %w", err)
	}

	for _, row := range rows {
		counts[row.WorkspaceID] = row.AliveCount
	}

	return counts, nil
}
