package database

import (
	"context"
	"fmt"
	"time"

	"magpie/internal/config"
	"magpie/internal/domain"

	"gorm.io/gorm"
)

const autoRemoveCleanupBatchSize = 2000

func CleanupAutoRemovalViolations(ctx context.Context) (int64, []domain.Proxy, []domain.Proxy, error) {
	if DB == nil {
		return 0, nil, nil, fmt.Errorf("database not initialised")
	}

	db := DB
	if ctx != nil {
		db = db.WithContext(ctx)
	}

	type target struct {
		WorkspaceID uint
		ProxyID     uint64
	}

	var (
		batch          []target
		total          int64
		pausedProxyIDs = make(map[uint64]struct{})
		queryBase      = db.Table("user_proxies up").
				Select("up.workspace_id, up.proxy_id").
				Joins("JOIN workspaces w ON w.id = up.workspace_id").
				Where("w.auto_remove_failing_proxies = ?", true).
				Where("w.auto_remove_failure_threshold > 0").
			// Delete requires a new failed check, even after a restart.
			Where("w.failure_action <> ?", domain.FailureActionDelete).
			Where("up.state = ?", domain.ManagedProxyStateActive).
			Where("up.consecutive_failures >= w.auto_remove_failure_threshold")
	)

	result := queryBase.FindInBatches(&batch, autoRemoveCleanupBatchSize, func(tx *gorm.DB, _ int) error {
		if len(batch) == 0 {
			return nil
		}

		perWorkspace := make(map[uint][]uint64)
		for _, item := range batch {
			perWorkspace[item.WorkspaceID] = append(perWorkspace[item.WorkspaceID], item.ProxyID)
		}

		now := time.Now().UTC()
		for workspaceID, proxyIDs := range perWorkspace {
			result := tx.Model(&domain.ManagedProxy{}).
				Where("workspace_id = ? AND proxy_id IN ? AND state = ?", workspaceID, proxyIDs, domain.ManagedProxyStateActive).
				Updates(map[string]any{
					"state":        domain.ManagedProxyStatePaused,
					"pause_reason": domain.ManagedProxyPauseReasonFailure,
					"paused_at":    now,
				})
			if result.Error != nil {
				return result.Error
			}
			if err := refreshManagedProxyLifecycleReadModels(tx, workspaceID, proxyIDs); err != nil {
				return err
			}
			total += result.RowsAffected
			for _, proxyID := range proxyIDs {
				pausedProxyIDs[proxyID] = struct{}{}
			}
		}

		return nil
	})
	if result.Error != nil {
		return 0, nil, nil, result.Error
	}

	inactive, refresh, err := loadProxyQueueChangesForUint64(db, mapKeysUint64(pausedProxyIDs))
	return total, inactive, refresh, err
}

func CleanupProxyLimitViolations(ctx context.Context) (int64, []domain.Proxy, []domain.Proxy, error) {
	if DB == nil {
		return 0, nil, nil, fmt.Errorf("database not initialised")
	}
	db := DB
	if ctx != nil {
		db = db.WithContext(ctx)
	}
	var subscriptions []domain.WorkspaceSubscription
	if err := db.Find(&subscriptions).Error; err != nil {
		return 0, nil, nil, err
	}
	var total int64
	allPaused := make(map[uint64]struct{})
	for _, subscription := range subscriptions {
		limit, unlimited := subscription.ActivationLimit()
		if unlimited {
			continue
		}
		paused, proxyIDs, err := pauseWorkspaceCapacityOverflow(db, subscription.WorkspaceID, limit)
		if err != nil {
			return 0, nil, nil, err
		}
		total += paused
		for _, proxyID := range proxyIDs {
			allPaused[proxyID] = struct{}{}
		}
	}
	inactive, refresh, err := loadProxyQueueChangesForUint64(db, mapKeysUint64(allPaused))
	return total, inactive, refresh, err
}

func cleanupProxyLimitViolationsWithConfig(ctx context.Context, limitCfg config.ProxyLimitConfig) (int64, []domain.Proxy, []domain.Proxy, error) {
	if !limitCfg.Enabled {
		return 0, nil, nil, nil
	}

	if DB == nil {
		return 0, nil, nil, fmt.Errorf("database not initialised")
	}

	db := DB
	if ctx != nil {
		db = db.WithContext(ctx)
	}

	maxPerUser := uint64(limitCfg.MaxPerUser)

	query := db.Table("user_proxies up").
		Select("up.workspace_id").
		Group("up.workspace_id").
		Where("up.state = ?", domain.ManagedProxyStateActive).
		Having("COUNT(*) > ?", maxPerUser)
	if limitCfg.ExcludeAdmins {
		query = query.Joins("LEFT JOIN workspace_memberships wm ON wm.workspace_id = up.workspace_id AND wm.role = 'owner'").
			Joins("LEFT JOIN users u ON u.id = wm.user_id").
			Where("COALESCE(u.role, 'user') <> ?", "admin")
	}

	var workspaceIDs []uint
	if err := query.Pluck("up.workspace_id", &workspaceIDs).Error; err != nil {
		return 0, nil, nil, err
	}
	workspaceIDs = normalizeWorkspaceIDs(workspaceIDs)
	if len(workspaceIDs) == 0 {
		return 0, nil, nil, nil
	}

	totalPaused := int64(0)
	pausedIDs := make(map[uint64]struct{})

	for _, workspaceID := range workspaceIDs {
		paused, proxyIDs, err := pauseWorkspaceCapacityOverflow(db, workspaceID, maxPerUser)
		if err != nil {
			return 0, nil, nil, err
		}
		totalPaused += paused
		for _, proxyID := range proxyIDs {
			pausedIDs[proxyID] = struct{}{}
		}
	}
	inactive, refresh, err := loadProxyQueueChangesForUint64(db, mapKeysUint64(pausedIDs))
	return totalPaused, inactive, refresh, err
}

func pauseWorkspaceCapacityOverflow(db *gorm.DB, workspaceID uint, limit uint64) (int64, []uint64, error) {
	var paused int64
	var proxyIDs []uint64
	err := db.Transaction(func(tx *gorm.DB) error {
		var activeCount int64
		if err := tx.Model(&domain.ManagedProxy{}).
			Where("workspace_id = ? AND state = ?", workspaceID, domain.ManagedProxyStateActive).
			Count(&activeCount).Error; err != nil {
			return err
		}
		overflow := activeCount - int64(limit)
		if overflow <= 0 {
			return nil
		}
		if err := tx.Model(&domain.ManagedProxy{}).
			Select("proxy_id").
			Where("workspace_id = ? AND state = ?", workspaceID, domain.ManagedProxyStateActive).
			Order("COALESCE(activated_at, created_at) DESC, proxy_id DESC").
			Limit(int(overflow)).
			Pluck("proxy_id", &proxyIDs).Error; err != nil {
			return err
		}
		if len(proxyIDs) == 0 {
			return nil
		}
		now := time.Now().UTC()
		result := tx.Model(&domain.ManagedProxy{}).
			Where("workspace_id = ? AND proxy_id IN ? AND state = ?", workspaceID, proxyIDs, domain.ManagedProxyStateActive).
			Updates(map[string]any{
				"state":        domain.ManagedProxyStatePaused,
				"pause_reason": domain.ManagedProxyPauseReasonCapacity,
				"paused_at":    now,
			})
		if result.Error != nil {
			return result.Error
		}
		paused = result.RowsAffected
		if err := refreshManagedProxyLifecycleReadModels(tx, workspaceID, proxyIDs); err != nil {
			return err
		}
		return refreshWorkspaceUsageActiveRoutes(tx, workspaceID)
	})
	if err != nil {
		return 0, nil, err
	}
	return paused, proxyIDs, nil
}

func proxiesWithoutActiveManagement(db *gorm.DB, proxyIDs []uint64) ([]domain.Proxy, error) {
	if len(proxyIDs) == 0 {
		return nil, nil
	}
	var proxies []domain.Proxy
	err := db.Model(&domain.Proxy{}).
		Where("id IN ?", proxyIDs).
		Where("NOT EXISTS (SELECT 1 FROM user_proxies up WHERE up.proxy_id = proxies.id AND up.state = ?)", domain.ManagedProxyStateActive).
		Find(&proxies).Error
	return proxies, err
}

func collectOrphanProxyIDs(candidateIDs []int) ([]uint64, error) {
	if len(candidateIDs) == 0 {
		return nil, nil
	}

	var stillInUse []int
	if err := DB.Model(&domain.ManagedProxy{}).
		Where("proxy_id IN ?", candidateIDs).
		Distinct("proxy_id").
		Pluck("proxy_id", &stillInUse).Error; err != nil {
		return nil, err
	}

	inUseSet := make(map[int]struct{}, len(stillInUse))
	for _, id := range stillInUse {
		inUseSet[id] = struct{}{}
	}

	seen := make(map[int]struct{}, len(candidateIDs))
	orphanIDs := make([]uint64, 0, len(candidateIDs))
	for _, id := range candidateIDs {
		if _, alreadySeen := seen[id]; alreadySeen {
			continue
		}
		seen[id] = struct{}{}

		if _, inUse := inUseSet[id]; inUse {
			continue
		}

		orphanIDs = append(orphanIDs, uint64(id))
	}

	if len(orphanIDs) == 0 {
		return nil, nil
	}

	return orphanIDs, nil
}

func GetExistingProxyIDSet(ctx context.Context, proxyIDs []uint64) (map[uint64]struct{}, error) {
	if len(proxyIDs) == 0 {
		return nil, nil
	}
	if DB == nil {
		return nil, fmt.Errorf("database not initialised")
	}

	db := DB
	if ctx != nil {
		db = db.WithContext(ctx)
	}

	result := make(map[uint64]struct{})
	for start := 0; start < len(proxyIDs); start += maxParamsPerBatch {
		end := start + maxParamsPerBatch
		if end > len(proxyIDs) {
			end = len(proxyIDs)
		}

		var ids []uint64
		if err := db.Model(&domain.Proxy{}).
			Where("id IN ?", proxyIDs[start:end]).
			Pluck("id", &ids).Error; err != nil {
			return nil, err
		}

		for _, id := range ids {
			result[id] = struct{}{}
		}
	}

	return result, nil
}

func DeleteOrphanProxies(ctx context.Context) (int64, error) {
	if DB == nil {
		return 0, fmt.Errorf("database not initialised")
	}
	db := DB
	if ctx != nil {
		db = db.WithContext(ctx)
	}

	result := db.
		Where("NOT EXISTS (SELECT 1 FROM user_proxies up WHERE up.proxy_id = proxies.id)").
		// Ownership deletion removes a managed proxy immediately, but retain
		// the route's recent check history for the rolling alert window.
		Where("NOT EXISTS (SELECT 1 FROM proxy_statistics ps WHERE ps.proxy_id = proxies.id AND ps.created_at >= ?)", time.Now().UTC().Add(-AlertWindow)).
		Delete(&domain.Proxy{})
	if result.Error != nil {
		return 0, result.Error
	}
	if err := DeleteOrphanProxyCheckCounters(ctx, deleteChunkSize); err != nil {
		return result.RowsAffected, err
	}
	return result.RowsAffected, nil
}
