package database

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"magpie/internal/checkpace"
	"magpie/internal/domain"
	"magpie/internal/platform/pg"

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
			// Allowance usage (checks) is metered by the checker itself, not
			// here: this pipeline may drop statistics under load.
			usage.CheckAttempts += uint64(statistic.Attempt) + 1
		}
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

	rows := workspaceUsageDeltaRows(periods)
	return tx.Model(&domain.WorkspaceUsagePeriod{}).Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "workspace_id"}, {Name: "period_start"}},
		DoUpdates: clause.Assignments(map[string]any{
			"period_end":     gorm.Expr("EXCLUDED.period_end"),
			"check_attempts": gorm.Expr("workspace_usage_periods.check_attempts + EXCLUDED.check_attempts"),
			"updated_at":     gorm.Expr("CURRENT_TIMESTAMP"),
		}),
	}).CreateInBatches(&rows, 1000).Error
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
	// Which routes are active changed, so tag-rule protocol counts must be
	// recomputed for check pacing.
	invalidatePlanDeltas(workspaceID)
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
	if pg.DB == nil {
		return fmt.Errorf("database not initialised")
	}
	tx := pg.DB
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
	rows := workspaceUsageDeltaRows(periods)
	return tx.Model(&domain.WorkspaceUsagePeriod{}).Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "workspace_id"}, {Name: "period_start"}},
		DoUpdates: clause.Assignments(map[string]any{
			"period_end":       gorm.Expr("EXCLUDED.period_end"),
			"managed_requests": gorm.Expr("workspace_usage_periods.managed_requests + EXCLUDED.managed_requests"),
			"managed_bytes":    gorm.Expr("workspace_usage_periods.managed_bytes + EXCLUDED.managed_bytes"),
			"updated_at":       gorm.Expr("CURRENT_TIMESTAMP"),
		}),
	}).CreateInBatches(&rows, 1000).Error
}

// On the first write of a new month, carry the last recorded capacity into
// the new period. Both checks and traffic can create a period. Existing rows
// only receive deltas, and lifecycle writes remain authoritative for capacity.
func workspaceUsageDeltaRows(periods []domain.WorkspaceUsagePeriod) []map[string]any {
	rows := make([]map[string]any, 0, len(periods))
	now := time.Now().UTC()
	for _, period := range periods {
		capacity := gorm.Expr(`COALESCE((SELECT active_routes FROM workspace_usage_periods
			WHERE workspace_id = ? AND period_start < ? ORDER BY period_start DESC LIMIT 1), 0)`, period.WorkspaceID, period.PeriodStart)
		rows = append(rows, map[string]any{
			"workspace_id": period.WorkspaceID, "period_start": period.PeriodStart, "period_end": period.PeriodEnd,
			"active_routes": capacity, "peak_active_routes": capacity,
			"check_attempts": period.CheckAttempts, "managed_requests": period.ManagedRequests, "managed_bytes": period.ManagedBytes,
			"checks": period.Checks, "protocol_checks": period.ProtocolChecks, "scrapes": period.Scrapes,
			"created_at": now, "updated_at": now,
		})
	}
	return rows
}

// FlushWorkspaceCheckUsage adds a batch of check usage metered in memory by a
// checker process (checkpace.Pacer.ChargeRequest). It runs every
// checkpace.FlushInterval, never per check: one transaction with one mark
// upsert and one usage upsert row per workspace and month. A batch whose
// sequence the process already applied changes nothing.
func FlushWorkspaceCheckUsage(ctx context.Context, batch checkpace.UsageBatch) error {
	if pg.DB == nil {
		return fmt.Errorf("database not initialised")
	}
	tx := pg.DB
	if ctx != nil {
		tx = tx.WithContext(ctx)
	}
	return flushWorkspaceCheckUsage(tx, batch)
}

func flushWorkspaceCheckUsage(db *gorm.DB, batch checkpace.UsageBatch) error {
	if db == nil {
		return nil
	}
	if batch.Instance == "" || batch.Sequence == 0 {
		return fmt.Errorf("check usage batch needs an instance and a sequence")
	}
	periods := make([]domain.WorkspaceUsagePeriod, 0, len(batch.Usage))
	for _, delta := range batch.Usage {
		if delta.WorkspaceID == 0 || (delta.Attempts == 0 && delta.ProtocolChecks == 0) {
			continue
		}
		periodStart, periodEnd := workspaceUsageMonth(delta.PeriodStart)
		periods = append(periods, domain.WorkspaceUsagePeriod{
			WorkspaceID: delta.WorkspaceID, PeriodStart: periodStart, PeriodEnd: periodEnd,
			Checks: delta.Attempts, ProtocolChecks: delta.ProtocolChecks,
		})
	}
	// An empty batch only refreshes the replay mark of an idle process.
	// A consistent row order keeps concurrent flushers from deadlocking.
	sort.Slice(periods, func(i, j int) bool {
		if periods[i].WorkspaceID != periods[j].WorkspaceID {
			return periods[i].WorkspaceID < periods[j].WorkspaceID
		}
		return periods[i].PeriodStart.Before(periods[j].PeriodStart)
	})
	if err := registerUsageFlushInstance(db, batch.Instance); err != nil {
		return err
	}
	return db.Transaction(func(tx *gorm.DB) error {
		// The mark row serializes one process's batches, and a replayed
		// sequence updates nothing.
		now := time.Now().UTC()
		mark := tx.Model(&domain.WorkspaceUsageFlush{}).
			Where("instance = ? AND sequence < ?", batch.Instance, batch.Sequence).
			Updates(map[string]any{"sequence": batch.Sequence, "updated_at": now})
		if mark.Error != nil {
			return mark.Error
		}
		if mark.RowsAffected == 0 {
			var marks int64
			if err := tx.Model(&domain.WorkspaceUsageFlush{}).Where("instance = ?", batch.Instance).Count(&marks).Error; err != nil {
				return err
			}
			if marks > 0 {
				return nil // already applied
			}
			// The mark was registered and later pruned. A first send cannot
			// have been applied before, but a resend may have been.
			if batch.Resend {
				return checkpace.ErrBatchOutcomeUnknown
			}
			if err := tx.Create(&domain.WorkspaceUsageFlush{Instance: batch.Instance, Sequence: batch.Sequence, UpdatedAt: now}).Error; err != nil {
				return err
			}
		}
		if len(periods) == 0 {
			return nil
		}
		rows := workspaceUsageDeltaRows(periods)
		return tx.Model(&domain.WorkspaceUsagePeriod{}).Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "workspace_id"}, {Name: "period_start"}},
			DoUpdates: clause.Assignments(map[string]any{
				"period_end":      gorm.Expr("EXCLUDED.period_end"),
				"checks":          gorm.Expr("workspace_usage_periods.checks + EXCLUDED.checks"),
				"protocol_checks": gorm.Expr("workspace_usage_periods.protocol_checks + EXCLUDED.protocol_checks"),
				"updated_at":      gorm.Expr("CURRENT_TIMESTAMP"),
			}),
		}).CreateInBatches(&rows, 1000).Error
	})
}

// registeredUsageFlushInstances holds the processes whose replay mark was
// written in this process. Each mark is registered once, acknowledged, before
// its first batch, so a mark missing later can only have been pruned.
var registeredUsageFlushInstances sync.Map

func registerUsageFlushInstance(db *gorm.DB, instance string) error {
	if _, ok := registeredUsageFlushInstances.Load(instance); ok {
		return nil
	}
	err := db.Clauses(clause.OnConflict{DoNothing: true}).
		Create(&domain.WorkspaceUsageFlush{Instance: instance, Sequence: 0, UpdatedAt: time.Now().UTC()}).Error
	if err != nil {
		return fmt.Errorf("register check usage instance: %w", err)
	}
	registeredUsageFlushInstances.Store(instance, struct{}{})
	return nil
}

// workspaceUsageFlushRetention is how long a replay mark is kept after its
// last write. A process resends a batch until it succeeds, so a mark can be
// needed for as long as an outage lasts. A resend that finds its mark pruned
// is dropped rather than applied twice (checkpace.ErrBatchOutcomeUnknown);
// this retention makes that practically unreachable.
const workspaceUsageFlushRetention = 30 * 24 * time.Hour

// PruneWorkspaceUsageFlushes deletes batch marks of processes that stopped
// flushing long ago. Each process start adds one mark; this runs at startup.
func PruneWorkspaceUsageFlushes(ctx context.Context) error {
	if pg.DB == nil {
		return fmt.Errorf("database not initialised")
	}
	return pg.DB.WithContext(ctx).Where("updated_at < ?", time.Now().UTC().Add(-workspaceUsageFlushRetention)).
		Delete(&domain.WorkspaceUsageFlush{}).Error
}

// RecordWorkspaceScrapeUsage charges one scrape to each workspace served by a
// source fetch. Scrapes are infrequent, so each fetch writes one small upsert.
func RecordWorkspaceScrapeUsage(ctx context.Context, workspaceIDs []uint, fetchedAt time.Time) error {
	if pg.DB == nil {
		return fmt.Errorf("database not initialised")
	}
	tx := pg.DB
	if ctx != nil {
		tx = tx.WithContext(ctx)
	}
	return recordWorkspaceScrapeUsage(tx, workspaceIDs, fetchedAt)
}

func recordWorkspaceScrapeUsage(tx *gorm.DB, workspaceIDs []uint, fetchedAt time.Time) error {
	if tx == nil || len(workspaceIDs) == 0 {
		return nil
	}
	if fetchedAt.IsZero() {
		fetchedAt = time.Now().UTC()
	}
	periodStart, periodEnd := workspaceUsageMonth(fetchedAt)
	periods := make([]domain.WorkspaceUsagePeriod, 0, len(workspaceIDs))
	for _, workspaceID := range normalizeWorkspaceIDs(workspaceIDs) {
		periods = append(periods, domain.WorkspaceUsagePeriod{
			WorkspaceID: workspaceID, PeriodStart: periodStart, PeriodEnd: periodEnd, Scrapes: 1,
		})
	}
	if len(periods) == 0 {
		return nil
	}
	rows := workspaceUsageDeltaRows(periods)
	return tx.Model(&domain.WorkspaceUsagePeriod{}).Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "workspace_id"}, {Name: "period_start"}},
		DoUpdates: clause.Assignments(map[string]any{
			"period_end": gorm.Expr("EXCLUDED.period_end"),
			"scrapes":    gorm.Expr("workspace_usage_periods.scrapes + EXCLUDED.scrapes"),
			"updated_at": gorm.Expr("CURRENT_TIMESTAMP"),
		}),
	}).Create(&rows).Error
}

func workspaceUsageMonth(value time.Time) (time.Time, time.Time) {
	utc := value.UTC()
	start := time.Date(utc.Year(), utc.Month(), 1, 0, 0, 0, 0, time.UTC)
	return start, start.AddDate(0, 1, 0)
}
