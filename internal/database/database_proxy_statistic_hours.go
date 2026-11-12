package database

import (
	"database/sql"
	"time"

	"magpie/internal/api/dto"
	"magpie/internal/platform/pg"

	"gorm.io/gorm"
)

// MaxProxyStatisticHours bounds the hourly history window to seven days.
const MaxProxyStatisticHours = 7 * 24

type proxyStatisticHourRow struct {
	HourIndex       int64           `gorm:"column:hour_index"`
	Checks          int64           `gorm:"column:checks"`
	Failed          int64           `gorm:"column:failed"`
	AvgResponseTime sql.NullFloat64 `gorm:"column:avg_response_time"`
}

// GetProxyStatisticHours groups a proxy's checks since the given time into UTC
// hours for one workspace. A check counts as alive using the workspace's own
// verdict when the row carries per-workspace evidence, the same rule the check
// history uses. Hours without checks are omitted.
func GetProxyStatisticHours(workspaceID uint, proxyID uint64, since time.Time) ([]dto.ProxyStatisticHour, error) {
	if proxyID == 0 || pg.DB == nil {
		return []dto.ProxyStatisticHour{}, nil
	}
	return getProxyStatisticHoursWithDB(pg.DB, workspaceID, proxyID, since)
}

func getProxyStatisticHoursWithDB(db *gorm.DB, workspaceID uint, proxyID uint64, since time.Time) ([]dto.ProxyStatisticHour, error) {
	hourExpr := "CAST(FLOOR(EXTRACT(EPOCH FROM proxy_statistics.created_at) / 3600) AS bigint)"
	aliveExpr := "CASE WHEN COALESCE(jsonb_array_length(proxy_statistics.check_evidence), 0) = 0 THEN proxy_statistics.alive " +
		"ELSE proxy_statistics.check_evidence @> jsonb_build_array(jsonb_build_object('workspace_id', CAST(? AS bigint), 'alive', true)) END"
	if db.Dialector.Name() == "sqlite" {
		hourExpr = "CAST(strftime('%s', proxy_statistics.created_at) AS INTEGER) / 3600"
		aliveExpr = "CASE WHEN COALESCE(json_array_length(proxy_statistics.check_evidence), 0) = 0 THEN proxy_statistics.alive " +
			"ELSE EXISTS (SELECT 1 FROM json_each(proxy_statistics.check_evidence) e WHERE json_extract(e.value, '$.workspace_id') = ? AND json_extract(e.value, '$.alive') = 1) END"
	}

	checks := db.Table("proxy_statistics").
		Select(hourExpr+" AS hour_index, "+aliveExpr+" AS ok, proxy_statistics.response_time", workspaceID).
		Joins("JOIN user_proxies up ON up.proxy_id = proxy_statistics.proxy_id").
		Where("proxy_statistics.proxy_id = ? AND up.workspace_id = ? AND proxy_statistics.created_at >= ?", proxyID, workspaceID, since.UTC())
	checks = filterWorkspaceStatisticEvidence(checks, workspaceID)

	var rows []proxyStatisticHourRow
	err := db.Table("(?) AS checks", checks).
		Select("hour_index, COUNT(*) AS checks, " +
			"SUM(CASE WHEN ok THEN 0 ELSE 1 END) AS failed, " +
			"AVG(CASE WHEN ok THEN response_time END) AS avg_response_time").
		Group("hour_index").
		Order("hour_index").
		Scan(&rows).Error
	if err != nil {
		return nil, err
	}

	hours := make([]dto.ProxyStatisticHour, 0, len(rows))
	for _, row := range rows {
		hour := dto.ProxyStatisticHour{
			Hour:   time.Unix(row.HourIndex*3600, 0).UTC(),
			Checks: row.Checks,
			Failed: row.Failed,
		}
		if row.AvgResponseTime.Valid {
			hour.AvgResponseTime = int64(row.AvgResponseTime.Float64 + 0.5)
		}
		hours = append(hours, hour)
	}
	return hours, nil
}
