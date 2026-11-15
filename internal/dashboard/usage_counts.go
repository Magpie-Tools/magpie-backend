package dashboard

import (
	"time"

	"magpie/internal/checkpace"

	"gorm.io/gorm"
)

// dashboardUsageCounts are the dashboard's check figures, read from the
// workspace usage meter. Their unit is outbound check requests, retries
// included: the number the monthly allowance charges.
type dashboardUsageCounts struct {
	// TotalChecks sums the larger of checks and check_attempts per period.
	// checks is metered by the checker since November 2026; check_attempts is
	// written by statistics ingestion and is lossy under load. Both charge
	// every served workspace for the same requests, so they are never added.
	TotalChecks int64 `gorm:"column:total_checks"`
	// ChecksThisMonth is checks for the current allowance period, the column
	// the allowance uses. A period has no row until its first write, so it is
	// 0 at the start of every month.
	ChecksThisMonth int64 `gorm:"column:checks_this_month"`
}

// The COALESCE wraps the second scalar subquery: a subquery that finds no row
// yields NULL, which a COALESCE inside it would never see. CASE instead of
// GREATEST keeps the statement valid on SQLite.
const dashboardUsageCountsSQL = `SELECT
	(SELECT COALESCE(SUM(CASE WHEN u.checks > u.check_attempts THEN u.checks ELSE u.check_attempts END), 0)
	   FROM workspace_usage_periods u WHERE u.workspace_id = ?) AS total_checks,
	COALESCE((SELECT u.checks FROM workspace_usage_periods u
	           WHERE u.workspace_id = ? AND u.period_start = ?), 0) AS checks_this_month`

// loadDashboardUsageCounts reads both figures with two primary-key range reads
// over one row per month for the workspace. The cost does not depend on route
// count or on other workspaces.
func loadDashboardUsageCounts(db *gorm.DB, workspaceID uint, now time.Time) (dashboardUsageCounts, error) {
	var counts dashboardUsageCounts
	if db == nil || workspaceID == 0 {
		return counts, nil
	}
	periodStart, _ := checkpace.UsagePeriod(now)
	err := db.Raw(dashboardUsageCountsSQL, workspaceID, workspaceID, periodStart).Scan(&counts).Error
	return counts, err
}
