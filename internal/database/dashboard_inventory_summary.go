package database

import (
	"magpie/internal/domain"

	"gorm.io/gorm"
)

type dashboardCountryCount struct {
	Country string
	Count   uint
}

type dashboardReputationCount struct {
	Label string
	Count uint
}

type dashboardTopReputationProxy struct {
	ProxyID uint64
	IP      string
	Port    uint
	Score   float32
	Label   string
}

type dashboardInventorySummary struct {
	Countries     []dashboardCountryCount
	Reputations   []dashboardReputationCount
	TopProxy      dashboardTopReputationProxy
	TopProxyFound bool
}

// The three dashboard summaries use the same active ownership projection. Read
// that projection once; separate queries each scanned its wide table under cold
// dashboard demand. Materialization trades a bounded set of selected columns in
// temporary storage for those repeated base-table reads.
func loadDashboardInventorySummary(db *gorm.DB, workspaceID uint) (dashboardInventorySummary, error) {
	var rows []struct {
		Kind    string
		Value   string
		Count   uint
		ProxyID uint64
		IP      string
		Port    uint
		Score   float32
	}
	err := db.Raw(`WITH active_inventory AS MATERIALIZED (
		SELECT ufi.proxy_id, ufi.host, ufi.port, ufi.reputation_score, ufi.reputation_label,
			CASE WHEN ufi.country_key IN ('n/a', 'unknown', 'unk') THEN 'Unknown' ELSE ufi.country END AS country
		FROM user_proxy_filter_indexes ufi
		WHERE ufi.workspace_id = ? AND ufi.state = ?
	)
	SELECT 'country' AS kind, country AS value, COUNT(*) AS count,
		CAST(0 AS BIGINT) AS proxy_id, '' AS ip, CAST(0 AS BIGINT) AS port, CAST(NULL AS REAL) AS score
	FROM active_inventory GROUP BY country
	UNION ALL
	SELECT 'reputation' AS kind, reputation_label AS value, COUNT(*) AS count,
		CAST(0 AS BIGINT) AS proxy_id, '' AS ip, CAST(0 AS BIGINT) AS port, CAST(NULL AS REAL) AS score
	FROM active_inventory GROUP BY reputation_label
	UNION ALL
	SELECT 'top' AS kind, reputation_label AS value, CAST(0 AS BIGINT) AS count,
		proxy_id, host AS ip, port, reputation_score AS score
	FROM (SELECT proxy_id, host, port, reputation_score, reputation_label
		FROM active_inventory WHERE reputation_score IS NOT NULL
		ORDER BY reputation_score DESC, proxy_id ASC LIMIT 1) top_proxy
	ORDER BY kind ASC, count DESC, value ASC`, workspaceID, domain.ManagedProxyStateActive).Scan(&rows).Error
	if err != nil {
		return dashboardInventorySummary{}, err
	}
	var summary dashboardInventorySummary
	for _, row := range rows {
		switch row.Kind {
		case "country":
			summary.Countries = append(summary.Countries, dashboardCountryCount{Country: row.Value, Count: row.Count})
		case "reputation":
			summary.Reputations = append(summary.Reputations, dashboardReputationCount{Label: row.Value, Count: row.Count})
		case "top":
			summary.TopProxy = dashboardTopReputationProxy{ProxyID: row.ProxyID, IP: row.IP, Port: row.Port, Score: row.Score, Label: row.Value}
			summary.TopProxyFound = row.ProxyID != 0
		}
	}
	return summary, nil
}
