package database

import (
	"context"
	"errors"
	"fmt"
	"magpie/internal/checkerconfig"
	"magpie/internal/config"
	"magpie/internal/domain"
	"magpie/internal/jobs/checker/judges"
	"magpie/internal/platform/pg"
	"magpie/internal/readmodel"
	"sort"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func ListCheckerWorkspaces(ctx context.Context) ([]uint, error) {
	var ids []uint
	err := pg.DB.WithContext(ctx).Model(&domain.Workspace{}).Pluck("id", &ids).Error
	return ids, err
}

// This loader reads identifiers and settings only. Loading ManagedProxy rows
// here would decrypt credentials and is deliberately unnecessary.
func LoadCheckerWorkspace(ctx context.Context, workspaceID uint) (*checkerconfig.Workspace, error) {
	var snapshot *checkerconfig.Workspace
	projectionChanged := false
	err := pg.DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var workspace domain.Workspace
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&workspace, workspaceID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			return err
		}
		var rows []struct {
			ID         uint
			FullString string
			Regex      string
		}
		if err := tx.Table("user_judges uj").Select("j.id, j.full_string, uj.regex").Joins("JOIN judges j ON j.id = uj.judge_id").Where("uj.workspace_id = ?", workspaceID).Scan(&rows).Error; err != nil {
			return err
		}
		known := make(map[uint]*domain.Judge)
		for _, judge := range judges.GetSortedJudgesByID() {
			known[judge.ID] = judge
		}
		list := make([]domain.JudgeWithRegex, 0, len(rows))
		for _, row := range rows {
			if config.IsWebsiteBlocked(row.FullString) {
				continue
			}
			judge := known[row.ID]
			if judge == nil || judge.FullString != row.FullString {
				judge = &domain.Judge{ID: row.ID, FullString: row.FullString}
				judge.SetUp()
				judge.UpdateIp()
			}
			list = append(list, domain.JudgeWithRegex{Judge: judge, Regex: row.Regex})
		}
		previous, _ := checkerconfig.Lookup(workspaceID)
		seed := checkerconfig.Build(workspace, nil, list, config.GetConfig().Checker.StandardHeader)
		if previous != nil && !workspace.CheckerDirty && previous.Generation == workspace.CheckerGeneration && previous.Value.CheckerRevision == workspace.CheckerRevision && previous.InputSignature == seed.InputSignature {
			snapshot = previous
			return nil
		}
		// The durable watermark includes notifications projected by other
		// instances. Coalesced route changes also recover a missed reversion.
		incremental := previous != nil && previous.BaseSignature == seed.BaseSignature && previous.Value.CheckerRevision >= workspace.CheckerFullRevision && previous.Value.CheckerRevision <= workspace.CheckerRevision
		if workspace.CheckerDirty && workspace.CheckerRevision <= workspace.CheckerProjectedRevision {
			incremental = false
		}
		if previous != nil && previous.Generation != workspace.CheckerGeneration && previous.Value.CheckerRevision == workspace.CheckerRevision {
			incremental = false
		}
		var affected []uint64
		if incremental {
			if err := tx.Model(&domain.CheckerProxyChange{}).Where("workspace_id = ? AND revision > ? AND revision <= ?", workspaceID, previous.Value.CheckerRevision, workspace.CheckerRevision).Order("proxy_id").Pluck("proxy_id", &affected).Error; err != nil {
				return err
			}
		}
		settings := workspace.DefaultCheckerSettings()
		tagIDs := make([]uint64, 0, len(settings.Rules))
		for _, rule := range settings.Rules {
			tagIDs = append(tagIDs, rule.TagID)
		}
		assignments, err := loadCheckerAssignments(tx, workspaceID, tagIDs, affected, incremental)
		if err != nil {
			return err
		}
		snapshot = checkerconfig.Build(workspace, assignments, list, config.GetConfig().Checker.StandardHeader)
		if incremental {
			snapshot = checkerconfig.Merge(previous, snapshot, affected)
		}
		keys := snapshot.Default.Keys
		defaultChanged := workspace.CheckerGeneration == 0 || keys != [4]string{workspace.CheckerHTTPKey, workspace.CheckerHTTPSKey, workspace.CheckerSOCKS4Key, workspace.CheckerSOCKS5Key}
		changedIDs, err := checkerProjectionDelta(tx, workspaceID, snapshot, affected, incremental)
		if err != nil {
			return err
		}
		if !incremental && workspace.CheckerRevision == workspace.CheckerProjectedRevision && (defaultChanged || len(changedIDs) > 0 || (previous != nil && previous.BaseSignature != seed.BaseSignature)) {
			workspace.CheckerRevision++
			workspace.CheckerFullRevision = workspace.CheckerRevision
			workspace.CheckerDirty = true
			snapshot.Value.CheckerRevision = workspace.CheckerRevision
			snapshot.Value.CheckerFullRevision = workspace.CheckerFullRevision
		}
		if !defaultChanged && len(changedIDs) == 0 && !workspace.CheckerDirty {
			return nil
		}
		projectionChanged = true
		sort.Slice(changedIDs, func(i, j int) bool { return changedIDs[i] < changedIDs[j] })
		workspace.CheckerGeneration++
		snapshot.Generation = workspace.CheckerGeneration
		snapshot.Value.CheckerGeneration = workspace.CheckerGeneration
		snapshot.Value.CheckerDirty = false
		snapshot.Value.CheckerProjectedRevision = workspace.CheckerRevision
		if err := tx.Model(&domain.Workspace{}).Where("id = ?", workspaceID).UpdateColumns(map[string]any{
			"checker_generation": workspace.CheckerGeneration, "checker_dirty": false, "checker_projected_revision": workspace.CheckerRevision, "checker_revision": workspace.CheckerRevision, "checker_full_revision": workspace.CheckerFullRevision,
			"checker_http_key": keys[0], "checker_https_key": keys[1], "checker_socks4_key": keys[2], "checker_socks5_key": keys[3],
		}).Error; err != nil {
			return err
		}
		// Rewrite only changed routes, including overrides that disappeared.
		for start := 0; start < len(changedIDs); start += pg.DeleteChunkSize {
			end := min(start+pg.DeleteChunkSize, len(changedIDs))
			if err := tx.Where("workspace_id = ? AND proxy_id IN ?", workspaceID, changedIDs[start:end]).Delete(&domain.ProxyCheckerPlan{}).Error; err != nil {
				return err
			}
			plans := make([]domain.ProxyCheckerPlan, 0)
			for _, proxyID := range changedIDs[start:end] {
				if plan := snapshot.Proxies.Get(proxyID); plan != nil {
					for i, key := range plan.Keys {
						if key != keys[i] {
							plans = append(plans, domain.ProxyCheckerPlan{WorkspaceID: workspaceID, ProxyID: proxyID, ProtocolID: i + 1, ConfigKey: key, Transport: plan.Settings[i].Transport})
						}
					}
				}
			}
			if len(plans) > 0 {
				if err := tx.CreateInBatches(plans, 1000).Error; err != nil {
					return err
				}
			}
		}
		if tx.Dialector.Name() == "postgres" {
			if defaultChanged {
				if err := readmodel.RefreshWorkspaceProxyFilterIndexes(tx, "WHERE up.workspace_id = ?", workspaceID); err != nil {
					return err
				}
			} else if err := readmodel.RefreshWorkspaceProxyFilterIndexesForWorkspaceProxyIDs(tx, workspaceID, changedIDs); err != nil {
				return err
			}
			if defaultChanged {
				if err := readmodel.RefreshWorkspaceScrapeSourceStats(tx, "WHERE uss.workspace_id = ?", workspaceID); err != nil {
					return err
				}
			} else if err := readmodel.RefreshWorkspaceScrapeSourceStatsForWorkspaceProxyIDs(tx, workspaceID, changedIDs); err != nil {
				return err
			}
		}
		return nil
	})
	if err == nil && (projectionChanged || snapshot == nil) {
		invalidateDashboardWorkspace(workspaceID)
	}
	return snapshot, err
}

// Load identifiers with database/sql instead of retaining GORM-decoded rows.
func loadCheckerAssignments(tx *gorm.DB, workspaceID uint, tagIDs, affected []uint64, incremental bool) ([]domain.ProxyTagAssignment, error) {
	if len(tagIDs) == 0 || (incremental && len(affected) == 0) {
		return nil, nil
	}
	var assignments []domain.ProxyTagAssignment
	load := func(ids []uint64) error {
		query := tx.Model(&domain.ProxyTagAssignment{}).Select("proxy_id, proxy_tag_id").Where("workspace_id = ? AND proxy_tag_id IN ?", workspaceID, tagIDs).Order("proxy_id")
		if incremental {
			query = query.Where("proxy_id IN ?", ids)
		}
		rows, err := query.Rows()
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			assignment := domain.ProxyTagAssignment{WorkspaceID: workspaceID}
			if err := rows.Scan(&assignment.ProxyID, &assignment.ProxyTagID); err != nil {
				return err
			}
			assignments = append(assignments, assignment)
		}
		return rows.Err()
	}
	if !incremental {
		err := load(nil)
		return assignments, err
	}
	for start := 0; start < len(affected); start += pg.DeleteChunkSize {
		if err := load(affected[start:min(start+pg.DeleteChunkSize, len(affected))]); err != nil {
			return nil, err
		}
	}
	return assignments, nil
}

// Compare only affected persisted routes for an incremental refresh. A cold
// snapshot streams the full sparse projection, without a protocol-row map.
func checkerProjectionDelta(tx *gorm.DB, workspaceID uint, snapshot *checkerconfig.Workspace, affected []uint64, incremental bool) ([]uint64, error) {
	remaining := make(map[uint64]uint8)
	add := func(id uint64, plan *checkerconfig.Plan) {
		var mask uint8
		for i, key := range plan.Keys {
			if key != snapshot.Default.Keys[i] {
				mask |= 1 << i
			}
		}
		if mask != 0 {
			remaining[id] = mask
		}
	}
	if incremental {
		for _, id := range affected {
			if plan := snapshot.Proxies.Get(id); plan != nil {
				add(id, plan)
			}
		}
	} else {
		snapshot.Proxies.Range(add)
	}
	changed := make(map[uint64]bool)
	load := func(ids []uint64) error {
		query := tx.Model(&domain.ProxyCheckerPlan{}).Select("proxy_id,protocol_id,config_key,transport").Where("workspace_id = ?", workspaceID)
		if incremental {
			query = query.Where("proxy_id IN ?", ids)
		}
		rows, err := query.Rows()
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id uint64
			var protocol int
			var key, transport string
			if err := rows.Scan(&id, &protocol, &key, &transport); err != nil {
				return err
			}
			if protocol < 1 || protocol > 4 {
				changed[id] = true
				continue
			}
			plan := snapshot.Plan(id)
			index := protocol - 1
			if plan.Keys[index] == snapshot.Default.Keys[index] || plan.Keys[index] != key || plan.Settings[index].Transport != transport {
				changed[id] = true
			}
			remaining[id] &^= 1 << index
		}
		return rows.Err()
	}
	if incremental {
		for start := 0; start < len(affected); start += pg.DeleteChunkSize {
			if err := load(affected[start:min(start+pg.DeleteChunkSize, len(affected))]); err != nil {
				return nil, err
			}
		}
	} else if err := load(nil); err != nil {
		return nil, err
	}
	for id, mask := range remaining {
		if mask != 0 {
			changed[id] = true
		}
	}
	ids := make([]uint64, 0, len(changed))
	for id := range changed {
		ids = append(ids, id)
	}
	return ids, nil
}

// Legacy history has no attribution. Keep it visible without treating it as
// current evidence once a workspace has compiled settings.
func filterWorkspaceStatisticEvidence(query *gorm.DB, workspaceID uint) *gorm.DB {
	if query.Dialector.Name() == "sqlite" {
		return query.Where("COALESCE(json_array_length(proxy_statistics.check_evidence), 0) = 0 OR EXISTS (SELECT 1 FROM json_each(proxy_statistics.check_evidence) e WHERE json_extract(e.value, '$.workspace_id') = ?)", workspaceID)
	}
	return query.Where("COALESCE(jsonb_array_length(proxy_statistics.check_evidence), 0) = 0 OR proxy_statistics.check_evidence @> jsonb_build_array(jsonb_build_object('workspace_id', CAST(? AS bigint)))", workspaceID)
}

// Removing obsolete latest pointers lets the existing retention job expire
// historical rows. A delayed old event may recreate a pointer; the next sweep
// drops it again, and current-health reads always reject it immediately.
func PruneObsoleteCheckerEvidence(ctx context.Context, limit int) (int64, error) {
	if pg.DB == nil {
		return 0, fmt.Errorf("checker evidence pruning: database not initialised")
	}
	if limit <= 0 {
		limit = pg.DeleteChunkSize
	}
	result := pg.DB.WithContext(ctx).Exec(`DELETE FROM proxy_latest_statistics WHERE (workspace_id, config_key, proxy_id, protocol_id) IN (
SELECT candidate.workspace_id, candidate.config_key, candidate.proxy_id, candidate.protocol_id
FROM proxy_latest_statistics candidate WHERE `+obsoleteCheckerEvidencePredicateSQL+` LIMIT ?)`, limit)
	return result.RowsAffected, result.Error
}

// Only changes to tags used by checker rules invalidate the projection.
func MarkCheckerAssignmentsDirty(tx *gorm.DB, workspaceID uint, tagIDs, proxyIDs []uint64) (bool, error) {
	if len(tagIDs) == 0 || len(proxyIDs) == 0 {
		return false, nil
	}
	var workspace domain.Workspace
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&workspace, workspaceID).Error; err != nil {
		return false, err
	}
	changed := make(map[uint64]bool, len(tagIDs))
	for _, id := range tagIDs {
		changed[id] = true
	}
	for _, rule := range workspace.DefaultCheckerSettings().Rules {
		if changed[rule.TagID] {
			workspace.CheckerRevision++
			if err := tx.Model(&domain.Workspace{}).Where("id = ?", workspaceID).UpdateColumns(map[string]any{"checker_dirty": true, "checker_revision": workspace.CheckerRevision}).Error; err != nil {
				return false, err
			}
			for start := 0; start < len(proxyIDs); start += pg.DeleteChunkSize {
				if err := tx.Exec(`INSERT INTO checker_proxy_changes (workspace_id,proxy_id,revision)
SELECT workspace_id,proxy_id,CAST(? AS bigint) FROM user_proxies WHERE workspace_id=? AND proxy_id IN ?
ON CONFLICT (workspace_id,proxy_id) DO UPDATE SET revision=excluded.revision, deleted=FALSE, updated_at=CURRENT_TIMESTAMP`, workspace.CheckerRevision, workspaceID, proxyIDs[start:min(start+pg.DeleteChunkSize, len(proxyIDs))]).Error; err != nil {
					return false, err
				}
			}
			return true, nil
		}
	}
	return false, nil
}

// Rule changes enumerate only assignments of the changed tags. The latest
// revision per route is retained for instances that missed the notification.
func recordCheckerRuleChanges(tx *gorm.DB, workspaceID uint, revision uint64, tagIDs []uint64) error {
	if len(tagIDs) == 0 {
		return nil
	}
	return tx.Exec(`INSERT INTO checker_proxy_changes (workspace_id,proxy_id,revision)
SELECT DISTINCT workspace_id,proxy_id,CAST(? AS bigint) FROM proxy_tag_assignments WHERE workspace_id=? AND proxy_tag_id IN ?
ON CONFLICT (workspace_id,proxy_id) DO UPDATE SET revision=excluded.revision, deleted=FALSE, updated_at=CURRENT_TIMESTAMP`, revision, workspaceID, tagIDs).Error
}

// Capture removal in the ownership DELETE statement, before cascades erase
// assignments and overrides. No additional failure-deletion SQL call is needed.
func deleteManagedRoutes(tx *gorm.DB, workspaceID uint, proxyIDs []uint64, state string) (int64, bool, error) {
	if len(proxyIDs) == 0 {
		return 0, false, nil
	}
	if tx.Dialector.Name() != "postgres" {
		query := tx.Where("workspace_id=? AND proxy_id IN ?", workspaceID, proxyIDs)
		if state != "" {
			query = query.Where("state=?", state)
		}
		result := query.Delete(&domain.ManagedProxy{})
		return result.RowsAffected, false, result.Error
	}
	var removed int64
	var changed bool
	err := tx.Raw(`WITH affected AS MATERIALIZED (
SELECT up.proxy_id FROM user_proxies up WHERE up.workspace_id=? AND up.proxy_id IN ?
AND (? = '' OR up.state=?) AND (
EXISTS (SELECT 1 FROM proxy_checker_plans cp WHERE cp.workspace_id=up.workspace_id AND cp.proxy_id=up.proxy_id)
OR EXISTS (SELECT 1 FROM checker_proxy_changes c WHERE c.workspace_id=up.workspace_id AND c.proxy_id=up.proxy_id))
), revision AS (
UPDATE workspaces SET checker_revision=checker_revision+1,checker_dirty=TRUE
WHERE id=? AND EXISTS (SELECT 1 FROM affected) RETURNING id,checker_revision
), recorded AS (
INSERT INTO checker_proxy_changes (workspace_id,proxy_id,revision,deleted)
SELECT r.id,a.proxy_id,r.checker_revision,TRUE FROM revision r CROSS JOIN affected a
ON CONFLICT (workspace_id,proxy_id) DO UPDATE SET revision=excluded.revision,deleted=TRUE,updated_at=CURRENT_TIMESTAMP
RETURNING proxy_id
), removed AS (
DELETE FROM user_proxies WHERE workspace_id=? AND proxy_id IN ? AND (? = '' OR state=?)
AND (SELECT COUNT(*) FROM recorded)>=0 RETURNING proxy_id
)
SELECT COUNT(*),(SELECT COUNT(*)>0 FROM recorded) FROM removed`, workspaceID, proxyIDs, state, state, workspaceID, workspaceID, proxyIDs, state, state).Row().Scan(&removed, &changed)
	return removed, changed, err
}

// Tombstones survive missed notifications. Once old deleted rows expire,
// advance the full-refresh watermark so an older process cannot miss removal.
func PruneDeletedCheckerRoutes(ctx context.Context, before time.Time, limit int) (int64, error) {
	if pg.DB == nil || pg.DB.Dialector.Name() != "postgres" {
		return 0, nil
	}
	if limit <= 0 {
		limit = pg.DeleteChunkSize
	}
	var count int64
	err := pg.DB.WithContext(ctx).Raw(`WITH candidates AS MATERIALIZED (
SELECT workspace_id,proxy_id FROM checker_proxy_changes
WHERE deleted AND updated_at < ? ORDER BY updated_at,workspace_id,proxy_id LIMIT ?
), locked AS MATERIALIZED (
SELECT id,checker_dirty,checker_projected_revision FROM workspaces
WHERE id IN (SELECT workspace_id FROM candidates) ORDER BY id FOR UPDATE
), restored AS (
UPDATE checker_proxy_changes c SET deleted=FALSE FROM locked w
WHERE c.workspace_id=w.id AND (c.workspace_id,c.proxy_id) IN (SELECT workspace_id,proxy_id FROM candidates)
AND EXISTS (SELECT 1 FROM user_proxies up WHERE up.workspace_id=c.workspace_id AND up.proxy_id=c.proxy_id)
RETURNING c.workspace_id
), removed AS (
DELETE FROM checker_proxy_changes c USING locked w
WHERE c.workspace_id=w.id AND NOT w.checker_dirty AND c.revision<=w.checker_projected_revision
AND (c.workspace_id,c.proxy_id) IN (SELECT workspace_id,proxy_id FROM candidates)
AND c.deleted AND c.updated_at < ?
AND NOT EXISTS (SELECT 1 FROM user_proxies up WHERE up.workspace_id=c.workspace_id AND up.proxy_id=c.proxy_id)
RETURNING c.workspace_id,c.revision
), advanced AS (
UPDATE workspaces w SET checker_full_revision=GREATEST(w.checker_full_revision,r.revision)
FROM (SELECT workspace_id,MAX(revision) AS revision FROM removed GROUP BY workspace_id) r
WHERE w.id=r.workspace_id RETURNING w.id
)
SELECT COUNT(*) FROM removed`, before, limit, before).Row().Scan(&count)
	return count, err
}

// Health caches must not expose the preceding configuration while a committed
// settings or membership change is waiting for projection. This is an API read,
// never a checker-worker lookup. Database failures also suppress cached health.
type checkerHealthVersion struct {
	Generation uint64 `gorm:"column:checker_generation"`
	Dirty      bool   `gorm:"column:checker_dirty"`
}

// Read the committed generation on API cache reads, including across instances.
// A refresh records it before querying and rechecks it before publishing.
func currentCheckerHealthVersion(workspaceID uint) checkerHealthVersion {
	version, err := currentCheckerHealthVersionWithDB(pg.DB, workspaceID)
	if err != nil {
		version.Dirty = true
	}
	return version
}

func currentCheckerHealthVersionWithDB(db *gorm.DB, workspaceID uint) (checkerHealthVersion, error) {
	var version checkerHealthVersion
	if db == nil {
		return version, nil
	}
	err := db.Model(&domain.Workspace{}).Select("checker_generation, checker_dirty").Where("id = ?", workspaceID).Scan(&version).Error
	return version, err
}

func checkerProjectionPending(workspaceID uint) bool {
	return currentCheckerHealthVersion(workspaceID).Dirty
}

type dashboardHealthCacheEntry[T any] struct {
	version     checkerHealthVersion
	value       T
	completedAt time.Time
}
