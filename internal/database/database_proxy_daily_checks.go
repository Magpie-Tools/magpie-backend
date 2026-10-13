package database

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"magpie/internal/domain"

	"github.com/charmbracelet/log"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type dashboardCheckCounts struct {
	TotalChecks     int64 `gorm:"column:total_checks"`
	TotalChecksWeek int64 `gorm:"column:total_checks_week"`
}

type dailyCheckAggregationKey struct {
	ProxyID uint64
	Day     time.Time
}

const proxyDailyBackfillAdvisoryLockBase int64 = 941_843_229_900

const (
	proxyDailyBackfillBatchSize  = 500
	proxyDailyBackfillStartDelay = 2 * time.Second
)

var proxyDailyBackfillInFlight sync.Map

func incrementProxyDailyChecks(tx *gorm.DB, statistics []domain.ProxyStatistic) error {
	if tx == nil || len(statistics) == 0 {
		return nil
	}

	aggregated := make(map[dailyCheckAggregationKey]int64, len(statistics))
	for _, stat := range statistics {
		if stat.ProxyID == 0 {
			continue
		}

		checkTime := stat.CreatedAt
		if checkTime.IsZero() {
			checkTime = time.Now().UTC()
		}
		day := startOfUTCDay(checkTime)
		key := dailyCheckAggregationKey{
			ProxyID: stat.ProxyID,
			Day:     day,
		}
		aggregated[key]++
	}

	if len(aggregated) == 0 {
		return nil
	}

	entries := make([]domain.ProxyDailyCheck, 0, len(aggregated))
	for key, count := range aggregated {
		entries = append(entries, domain.ProxyDailyCheck{
			ProxyID:     key.ProxyID,
			Day:         key.Day,
			ChecksCount: count,
		})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].ProxyID == entries[j].ProxyID {
			return entries[i].Day.Before(entries[j].Day)
		}
		return entries[i].ProxyID < entries[j].ProxyID
	})

	if err := tx.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "proxy_id"},
			{Name: "day"},
		},
		DoUpdates: clause.Assignments(map[string]interface{}{
			"checks_count": gorm.Expr("proxy_daily_checks.checks_count + EXCLUDED.checks_count"),
			"updated_at":   gorm.Expr("CURRENT_TIMESTAMP"),
		}),
	}).Create(&entries).Error; err != nil {
		return fmt.Errorf("daily checks: upsert counters: %w", err)
	}

	return nil
}

func loadDashboardCheckCounts(userID uint, weekAgo time.Time) (dashboardCheckCounts, error) {
	if DB == nil || userID == 0 {
		return dashboardCheckCounts{}, nil
	}

	counts, err := queryDashboardCheckCountsFromDaily(userID, weekAgo)
	if err == nil {
		queueProxyDailyChecksBackfill(userID)
	}
	return counts, err
}

func queueProxyDailyChecksBackfill(userID uint) {
	if DB == nil || userID == 0 {
		return
	}

	if _, loaded := proxyDailyBackfillInFlight.LoadOrStore(userID, struct{}{}); loaded {
		return
	}

	go func() {
		defer proxyDailyBackfillInFlight.Delete(userID)

		timer := time.NewTimer(proxyDailyBackfillStartDelay)
		defer timer.Stop()
		<-timer.C

		if _, err := ensureProxyDailyChecksBackfilled(userID); err != nil {
			log.Warn("dashboard: async daily check backfill failed", "workspace_id", userID, "error", err)
		}
	}()
}

func ensureProxyDailyChecksBackfilled(userID uint) (bool, error) {
	if DB == nil || userID == 0 {
		return false, nil
	}

	processed := false
	err := DB.Transaction(func(tx *gorm.DB) error {
		if tx.Dialector.Name() == "postgres" {
			var acquired bool
			if err := tx.Raw("SELECT pg_try_advisory_xact_lock(?)", proxyDailyBackfillAdvisoryLockBase).Scan(&acquired).Error; err != nil {
				return fmt.Errorf("daily checks: acquire backfill lock: %w", err)
			}
			if !acquired {
				return nil
			}
		}

		missingProxyIDs, err := loadUserProxyIDsMissingBackfill(tx, userID)
		if err != nil {
			return err
		}
		if len(missingProxyIDs) == 0 {
			return nil
		}

		if err := backfillProxyDailyChecksForProxyIDsTx(tx, missingProxyIDs, startOfUTCDay(time.Now())); err != nil {
			return err
		}

		if err := markProxyDailyChecksBackfilled(tx, missingProxyIDs); err != nil {
			return err
		}

		processed = true
		return nil
	})
	return processed, err
}

func loadUserProxyIDsMissingBackfill(tx *gorm.DB, userID uint) ([]uint64, error) {
	var rows []struct {
		ProxyID uint64 `gorm:"column:proxy_id"`
	}

	if err := tx.Table("user_proxies up").
		Select("up.proxy_id").
		Joins("LEFT JOIN proxy_daily_check_proxy_backfills b ON b.proxy_id = up.proxy_id").
		Where("up.workspace_id = ? AND b.proxy_id IS NULL", userID).
		Order("up.proxy_id").
		Limit(proxyDailyBackfillBatchSize).
		Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("daily checks: load missing proxy backfills: %w", err)
	}

	if len(rows) == 0 {
		return nil, nil
	}

	proxyIDs := make([]uint64, 0, len(rows))
	seen := make(map[uint64]struct{}, len(rows))
	for _, row := range rows {
		if row.ProxyID == 0 {
			continue
		}
		if _, exists := seen[row.ProxyID]; exists {
			continue
		}
		seen[row.ProxyID] = struct{}{}
		proxyIDs = append(proxyIDs, row.ProxyID)
	}

	return proxyIDs, nil
}

func markProxyDailyChecksBackfilled(tx *gorm.DB, proxyIDs []uint64) error {
	if tx == nil || len(proxyIDs) == 0 {
		return nil
	}

	entries := make([]domain.ProxyDailyCheckProxyBackfill, 0, len(proxyIDs))
	for _, proxyID := range proxyIDs {
		if proxyID == 0 {
			continue
		}
		entries = append(entries, domain.ProxyDailyCheckProxyBackfill{ProxyID: proxyID})
	}

	if len(entries) == 0 {
		return nil
	}

	if err := tx.Clauses(clause.OnConflict{DoNothing: true}).CreateInBatches(entries, 1000).Error; err != nil {
		return fmt.Errorf("daily checks: persist proxy backfill markers: %w", err)
	}

	return nil
}

func queryDashboardCheckCountsFromDaily(userID uint, weekAgo time.Time) (dashboardCheckCounts, error) {
	var counts dashboardCheckCounts
	todayStart := startOfUTCDay(time.Now())
	cutoff := weekAgo.UTC()
	cutoffDayStart := startOfUTCDay(cutoff)
	nextDayStart := cutoffDayStart.Add(24 * time.Hour)

	err := DB.Table("(SELECT proxy_id, day, checks_count FROM proxy_daily_checks UNION ALL SELECT proxy_id, NULL AS day, checks_count FROM proxy_check_totals) pdc").
		Select(
			"COALESCE(SUM(pdc.checks_count), 0) AS total_checks, "+
				"COALESCE(SUM(CASE WHEN pdc.day >= ? THEN pdc.checks_count ELSE 0 END), 0) AS total_checks_week",
			nextDayStart,
		).
		Joins("JOIN user_proxies up ON up.proxy_id = pdc.proxy_id AND up.workspace_id = ?", userID).
		Where("pdc.day < ? OR pdc.day IS NULL", todayStart).
		Scan(&counts).Error
	if err != nil {
		return dashboardCheckCounts{}, err
	}

	var boundaryCounts struct {
		PartialCutoffDay int64 `gorm:"column:partial_cutoff_day"`
		CurrentDay       int64 `gorm:"column:current_day"`
	}
	err = DB.Table("proxy_statistics ps").
		Select(
			"COALESCE(SUM(CASE WHEN ps.created_at < ? THEN 1 ELSE 0 END), 0) AS partial_cutoff_day, "+
				"COALESCE(SUM(CASE WHEN ps.created_at >= ? THEN 1 ELSE 0 END), 0) AS current_day",
			nextDayStart,
			todayStart,
		).
		Joins("JOIN user_proxies up ON up.proxy_id = ps.proxy_id AND up.workspace_id = ?", userID).
		Where("ps.created_at >= ?", cutoff).
		Scan(&boundaryCounts).Error
	if err != nil {
		return dashboardCheckCounts{}, err
	}

	counts.TotalChecks += boundaryCounts.CurrentDay
	counts.TotalChecksWeek += boundaryCounts.PartialCutoffDay + boundaryCounts.CurrentDay
	return counts, nil
}

func loadDashboardCheckCountsDirect(userID uint, weekAgo time.Time) (dashboardCheckCounts, error) {
	var counts dashboardCheckCounts
	if DB == nil || userID == 0 {
		return counts, nil
	}

	err := DB.Model(&domain.ProxyStatistic{}).
		Select(
			"COUNT(*) AS total_checks, "+
				"COALESCE(SUM(CASE WHEN proxy_statistics.created_at >= ? THEN 1 ELSE 0 END), 0) AS total_checks_week",
			weekAgo,
		).
		Joins("JOIN user_proxies up ON up.proxy_id = proxy_statistics.proxy_id").
		Where("up.workspace_id = ?", userID).
		Scan(&counts).Error
	if err != nil {
		return dashboardCheckCounts{}, err
	}
	return counts, nil
}

func backfillProxyDailyChecksForProxyIDsTx(tx *gorm.DB, proxyIDs []uint64, before time.Time) error {
	if tx == nil || len(proxyIDs) == 0 {
		return nil
	}

	query := `
INSERT INTO proxy_daily_checks (proxy_id, day, checks_count, created_at, updated_at)
SELECT
	ps.proxy_id,
	DATE(ps.created_at AT TIME ZONE 'UTC') AS day,
	COUNT(*) AS checks_count,
	CURRENT_TIMESTAMP,
	CURRENT_TIMESTAMP
FROM proxy_statistics ps
LEFT JOIN proxy_check_totals pct ON pct.proxy_id = ps.proxy_id
WHERE ps.proxy_id IN ?
	AND ps.created_at < ?
	AND (pct.proxy_id IS NULL OR ps.created_at >= pct.compacted_before)
GROUP BY ps.proxy_id, DATE(ps.created_at AT TIME ZONE 'UTC')
ON CONFLICT (proxy_id, day) DO UPDATE
SET checks_count = GREATEST(proxy_daily_checks.checks_count, EXCLUDED.checks_count),
	updated_at = CURRENT_TIMESTAMP;
`

	for start := 0; start < len(proxyIDs); start += proxyDailyBackfillBatchSize {
		end := start + proxyDailyBackfillBatchSize
		if end > len(proxyIDs) {
			end = len(proxyIDs)
		}
		if err := tx.Exec(query, proxyIDs[start:end], before).Error; err != nil {
			return fmt.Errorf("daily checks: backfill proxy daily counts: %w", err)
		}
	}

	return nil
}

func startOfUTCDay(value time.Time) time.Time {
	value = value.UTC()
	year, month, day := value.Date()
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

// CompactProxyDailyChecks replaces old days with lifetime totals atomically.
// Late statistics still enter daily counters and are folded on a later pass.
func CompactProxyDailyChecks(ctx context.Context, before time.Time, limit int) (int64, error) {
	if DB == nil {
		return 0, nil
	}
	before = startOfUTCDay(before)
	var count int64
	err := DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if isPostgresDialector(tx) {
			var acquired bool
			if err := tx.Raw("SELECT pg_try_advisory_xact_lock(?)", proxyDailyBackfillAdvisoryLockBase).Scan(&acquired).Error; err != nil {
				return err
			}
			if !acquired {
				return nil
			}
			var missing []uint64
			if err := tx.Table("proxy_daily_checks d").Select("DISTINCT d.proxy_id").
				Joins("LEFT JOIN proxy_daily_check_proxy_backfills b ON b.proxy_id = d.proxy_id").
				Where("d.day < ? AND b.proxy_id IS NULL", before).Order("d.proxy_id").Limit(proxyDailyBackfillBatchSize).Pluck("d.proxy_id", &missing).Error; err != nil {
				return err
			}
			if err := backfillProxyDailyChecksForProxyIDsTx(tx, missing, startOfUTCDay(time.Now())); err != nil {
				return err
			}
			if err := markProxyDailyChecksBackfilled(tx, missing); err != nil {
				return err
			}
			query := `WITH candidates AS (
				SELECT proxy_id, day FROM proxy_daily_checks d WHERE day < ?
				AND EXISTS (SELECT 1 FROM proxy_daily_check_proxy_backfills b WHERE b.proxy_id = d.proxy_id)
				ORDER BY day, proxy_id LIMIT ? FOR UPDATE SKIP LOCKED
			), removed AS (
				DELETE FROM proxy_daily_checks d USING candidates c
				WHERE d.proxy_id = c.proxy_id AND d.day = c.day
				RETURNING d.proxy_id, d.day, d.checks_count
			), compacted AS (
				INSERT INTO proxy_check_totals (proxy_id, checks_count, compacted_before)
				SELECT proxy_id, SUM(checks_count), MAX(day) + INTERVAL '1 day'
				FROM removed GROUP BY proxy_id ORDER BY proxy_id
				ON CONFLICT (proxy_id) DO UPDATE SET
				checks_count = proxy_check_totals.checks_count + EXCLUDED.checks_count,
				compacted_before = GREATEST(proxy_check_totals.compacted_before, EXCLUDED.compacted_before)
				RETURNING proxy_id
			) SELECT COUNT(*) FROM removed`
			return tx.Raw(query, before, max(1, limit)).Scan(&count).Error
		}
		var days []domain.ProxyDailyCheck
		if err := tx.Where("day < ?", before).Order("proxy_id, day").Limit(max(1, limit)).Find(&days).Error; err != nil {
			return err
		}
		totals := make(map[uint64]domain.ProxyCheckTotal)
		for _, day := range days {
			total := totals[day.ProxyID]
			total.ProxyID = day.ProxyID
			total.ChecksCount += day.ChecksCount
			if next := day.Day.AddDate(0, 0, 1); next.After(total.CompactedBefore) {
				total.CompactedBefore = next
			}
			totals[day.ProxyID] = total
			if err := tx.Where("proxy_id = ? AND day = ?", day.ProxyID, day.Day).Delete(&domain.ProxyDailyCheck{}).Error; err != nil {
				return err
			}
		}
		for _, total := range totals {
			if err := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "proxy_id"}}, DoUpdates: clause.Assignments(map[string]any{
				"checks_count":     gorm.Expr("proxy_check_totals.checks_count + EXCLUDED.checks_count"),
				"compacted_before": gorm.Expr("MAX(proxy_check_totals.compacted_before, EXCLUDED.compacted_before)"),
			})}).Create(&total).Error; err != nil {
				return err
			}
		}
		count = int64(len(days))
		return nil
	})
	return count, err
}

func DeleteOrphanProxyCheckCounters(ctx context.Context, limit int) error {
	if DB == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	limit = min(deleteChunkSize, max(1, limit))
	type counterTable struct {
		name  string
		after uint64
		done  bool
	}
	var tables []counterTable
	existingTables, err := DB.WithContext(ctx).Migrator().GetTables()
	if err != nil {
		return err
	}
	present := make(map[string]bool, len(existingTables))
	for _, table := range existingTables {
		present[table] = true
	}
	for _, table := range []string{"proxy_daily_checks", "proxy_daily_check_proxy_backfills", "proxy_check_totals"} {
		if present[table] {
			tables = append(tables, counterTable{name: table})
		}
	}
	// Visit every table each round so an interrupted pass makes progress on
	// totals and backfill markers as well as daily rows. The ID cursor avoids
	// rescanning the live prefix of each table for every batch.
	for {
		unfinished := false
		for i := range tables {
			if err := ctx.Err(); err != nil {
				return err
			}
			table := &tables[i]
			if table.done {
				continue
			}
			var ids []uint64
			query := DB.WithContext(ctx).Table(table.name + " AS c")
			if table.after != 0 {
				query = query.Where("c.proxy_id > ?", table.after)
			}
			if err := query.
				Where("NOT EXISTS (SELECT 1 FROM proxies p WHERE p.id = c.proxy_id)").
				Distinct("c.proxy_id").Order("c.proxy_id").Limit(limit).
				Pluck("c.proxy_id", &ids).Error; err != nil {
				return err
			}
			if len(ids) > 0 {
				query := `DELETE FROM ` + table.name + ` WHERE proxy_id IN ? AND NOT EXISTS (SELECT 1 FROM proxies p WHERE p.id = ` + table.name + `.proxy_id)`
				if err := DB.WithContext(ctx).Exec(query, ids).Error; err != nil {
					return err
				}
				table.after = ids[len(ids)-1]
			}
			table.done = len(ids) < limit
			unfinished = unfinished || !table.done
		}
		if !unfinished {
			return ctx.Err()
		}
	}
}
