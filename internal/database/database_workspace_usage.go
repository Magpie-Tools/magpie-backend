package database

import (
	"context"
	"fmt"
	"sort"
	"time"

	"magpie/internal/domain"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type workspaceUsageKey struct {
	WorkspaceID uint
	PeriodStart time.Time
}

func recordWorkspaceCheckUsage(tx *gorm.DB, statistics []domain.ProxyStatistic) error {
	if tx == nil || len(statistics) == 0 || !tx.Migrator().HasTable(&domain.WorkspaceUsagePeriod{}) {
		return nil
	}

	usageByPeriod := make(map[workspaceUsageKey]*domain.WorkspaceUsagePeriod)
	workspaceSet := make(map[uint]struct{})
	for _, statistic := range statistics {
		if len(statistic.WorkspaceIDs) == 0 {
			continue
		}
		checkedAt := statistic.CreatedAt
		if checkedAt.IsZero() {
			checkedAt = time.Now().UTC()
		}
		periodStart, periodEnd := workspaceUsageMonth(checkedAt)
		seen := make(map[uint]struct{}, len(statistic.WorkspaceIDs))
		for _, workspaceID := range statistic.WorkspaceIDs {
			if workspaceID == 0 {
				continue
			}
			if _, duplicate := seen[workspaceID]; duplicate {
				continue
			}
			seen[workspaceID] = struct{}{}
			workspaceSet[workspaceID] = struct{}{}
			key := workspaceUsageKey{WorkspaceID: workspaceID, PeriodStart: periodStart}
			usage := usageByPeriod[key]
			if usage == nil {
				usage = &domain.WorkspaceUsagePeriod{
					WorkspaceID: workspaceID,
					PeriodStart: periodStart,
					PeriodEnd:   periodEnd,
				}
				usageByPeriod[key] = usage
			}
			// Attempt is zero-based: zero means one outbound request.
			usage.CheckAttempts += uint64(statistic.Attempt) + 1
		}
	}
	if len(usageByPeriod) == 0 {
		return nil
	}

	workspaceIDs := make([]uint, 0, len(workspaceSet))
	for workspaceID := range workspaceSet {
		workspaceIDs = append(workspaceIDs, workspaceID)
	}
	var activeRows []struct {
		WorkspaceID uint
		Count       uint64
	}
	if err := tx.Model(&domain.ManagedProxy{}).
		Select("workspace_id, COUNT(*) AS count").
		Where("workspace_id IN ? AND state = ?", workspaceIDs, domain.ManagedProxyStateActive).
		Group("workspace_id").
		Scan(&activeRows).Error; err != nil {
		return err
	}
	activeByWorkspace := make(map[uint]uint64, len(activeRows))
	for _, row := range activeRows {
		activeByWorkspace[row.WorkspaceID] = row.Count
	}

	periods := make([]domain.WorkspaceUsagePeriod, 0, len(usageByPeriod))
	for _, usage := range usageByPeriod {
		usage.ActiveRoutes = activeByWorkspace[usage.WorkspaceID]
		usage.PeakActiveRoutes = usage.ActiveRoutes
		periods = append(periods, *usage)
	}
	sort.Slice(periods, func(i, j int) bool {
		if periods[i].WorkspaceID != periods[j].WorkspaceID {
			return periods[i].WorkspaceID < periods[j].WorkspaceID
		}
		return periods[i].PeriodStart.Before(periods[j].PeriodStart)
	})

	return tx.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "workspace_id"}, {Name: "period_start"}},
		DoUpdates: clause.Assignments(map[string]any{
			"period_end":         gorm.Expr("EXCLUDED.period_end"),
			"active_routes":      gorm.Expr("EXCLUDED.active_routes"),
			"peak_active_routes": gorm.Expr("CASE WHEN workspace_usage_periods.peak_active_routes > EXCLUDED.peak_active_routes THEN workspace_usage_periods.peak_active_routes ELSE EXCLUDED.peak_active_routes END"),
			"check_attempts":     gorm.Expr("workspace_usage_periods.check_attempts + EXCLUDED.check_attempts"),
			"updated_at":         gorm.Expr("CURRENT_TIMESTAMP"),
		}),
	}).Create(&periods).Error
}

func UpdateWorkspaceUsageActiveRoutes(tx *gorm.DB, workspaceID uint, active uint64) error {
	if tx == nil || workspaceID == 0 || !tx.Migrator().HasTable(&domain.WorkspaceUsagePeriod{}) {
		return nil
	}
	periodStart, periodEnd := workspaceUsageMonth(time.Now().UTC())
	usage := domain.WorkspaceUsagePeriod{
		WorkspaceID:      workspaceID,
		PeriodStart:      periodStart,
		PeriodEnd:        periodEnd,
		ActiveRoutes:     active,
		PeakActiveRoutes: active,
	}
	return tx.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "workspace_id"}, {Name: "period_start"}},
		DoUpdates: clause.Assignments(map[string]any{
			"period_end":         gorm.Expr("EXCLUDED.period_end"),
			"active_routes":      gorm.Expr("EXCLUDED.active_routes"),
			"peak_active_routes": gorm.Expr("CASE WHEN workspace_usage_periods.peak_active_routes > EXCLUDED.peak_active_routes THEN workspace_usage_periods.peak_active_routes ELSE EXCLUDED.peak_active_routes END"),
			"updated_at":         gorm.Expr("CURRENT_TIMESTAMP"),
		}),
	}).Create(&usage).Error
}

func refreshWorkspaceUsageActiveRoutes(tx *gorm.DB, workspaceID uint) error {
	if tx == nil || workspaceID == 0 || !tx.Migrator().HasTable(&domain.WorkspaceUsagePeriod{}) {
		return nil
	}
	var active int64
	if err := tx.Model(&domain.ManagedProxy{}).
		Where("workspace_id = ? AND state = ?", workspaceID, domain.ManagedProxyStateActive).
		Count(&active).Error; err != nil {
		return err
	}
	return UpdateWorkspaceUsageActiveRoutes(tx, workspaceID, uint64(active))
}

type WorkspaceManagedTrafficSample struct {
	WorkspaceID uint
	Requests    uint64
	Bytes       uint64
	RecordedAt  time.Time
}

// RecordWorkspaceManagedTraffic adds one already-aggregated rotator usage
// sample. The batch form is preferred by the runtime flusher.
func RecordWorkspaceManagedTraffic(ctx context.Context, workspaceID uint, requests, bytes uint64, recordedAt time.Time) error {
	return RecordWorkspaceManagedTrafficBatch(ctx, []WorkspaceManagedTrafficSample{{
		WorkspaceID: workspaceID,
		Requests:    requests,
		Bytes:       bytes,
		RecordedAt:  recordedAt,
	}})
}

// RecordWorkspaceManagedTrafficBatch writes aggregate deltas for any number of
// workspaces in one upsert. Rotator request handling only touches in-memory
// counters and never calls this function directly.
func RecordWorkspaceManagedTrafficBatch(ctx context.Context, samples []WorkspaceManagedTrafficSample) error {
	if len(samples) == 0 {
		return nil
	}
	if DB == nil {
		return fmt.Errorf("database not initialised")
	}
	tx := DB
	if ctx != nil {
		tx = tx.WithContext(ctx)
	}
	return recordWorkspaceManagedTrafficBatch(tx, samples)
}

func recordWorkspaceManagedTraffic(tx *gorm.DB, workspaceID uint, requests, bytes uint64, recordedAt time.Time) error {
	return recordWorkspaceManagedTrafficBatch(tx, []WorkspaceManagedTrafficSample{{
		WorkspaceID: workspaceID,
		Requests:    requests,
		Bytes:       bytes,
		RecordedAt:  recordedAt,
	}})
}

func recordWorkspaceManagedTrafficBatch(tx *gorm.DB, samples []WorkspaceManagedTrafficSample) error {
	if tx == nil || len(samples) == 0 {
		return nil
	}
	usageByPeriod := make(map[workspaceUsageKey]*domain.WorkspaceUsagePeriod, len(samples))
	for _, sample := range samples {
		if sample.WorkspaceID == 0 || (sample.Requests == 0 && sample.Bytes == 0) {
			continue
		}
		recordedAt := sample.RecordedAt
		if recordedAt.IsZero() {
			recordedAt = time.Now().UTC()
		}
		periodStart, periodEnd := workspaceUsageMonth(recordedAt)
		key := workspaceUsageKey{WorkspaceID: sample.WorkspaceID, PeriodStart: periodStart}
		usage := usageByPeriod[key]
		if usage == nil {
			usage = &domain.WorkspaceUsagePeriod{
				WorkspaceID: sample.WorkspaceID,
				PeriodStart: periodStart,
				PeriodEnd:   periodEnd,
			}
			usageByPeriod[key] = usage
		}
		usage.ManagedRequests += sample.Requests
		usage.ManagedBytes += sample.Bytes
	}
	if len(usageByPeriod) == 0 {
		return nil
	}
	periods := make([]domain.WorkspaceUsagePeriod, 0, len(usageByPeriod))
	for _, usage := range usageByPeriod {
		periods = append(periods, *usage)
	}
	sort.Slice(periods, func(i, j int) bool {
		if periods[i].WorkspaceID != periods[j].WorkspaceID {
			return periods[i].WorkspaceID < periods[j].WorkspaceID
		}
		return periods[i].PeriodStart.Before(periods[j].PeriodStart)
	})
	return tx.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "workspace_id"}, {Name: "period_start"}},
		DoUpdates: clause.Assignments(map[string]any{
			"period_end":       gorm.Expr("EXCLUDED.period_end"),
			"managed_requests": gorm.Expr("workspace_usage_periods.managed_requests + EXCLUDED.managed_requests"),
			"managed_bytes":    gorm.Expr("workspace_usage_periods.managed_bytes + EXCLUDED.managed_bytes"),
			"updated_at":       gorm.Expr("CURRENT_TIMESTAMP"),
		}),
	}).Create(&periods).Error
}

func workspaceUsageMonth(value time.Time) (time.Time, time.Time) {
	utc := value.UTC()
	start := time.Date(utc.Year(), utc.Month(), 1, 0, 0, 0, 0, time.UTC)
	return start, start.AddDate(0, 1, 0)
}
