package database

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"magpie/internal/api/dto"
	"magpie/internal/checkerconfig"
	"magpie/internal/domain"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const deleteChunkSize = 5000 // Keep large deletes under Postgres parameter limits

var ErrNoProxiesSelected = errors.New("no proxies selected for deletion")

var ErrWorkspaceCapacityReached = errors.New("workspace active route capacity reached")

func DeleteProxyRelation(workspaceID uint, proxies []int) (totalDeleted int64, orphans []domain.Proxy, err error) {
	if len(proxies) == 0 {
		return 0, nil, nil
	}

	checkerChanged := false
	var committedProxyIDs []uint64
	// Refresh once after all committed batches, including when a later batch
	// fails. Recounting overlapping sources after each chunk repeats the same
	// health lookups while the user waits for the bulk deletion to finish.
	defer func() {
		if totalDeleted == 0 {
			return
		}
		err = errors.Join(err,
			refreshWorkspaceScrapeSourceStatsForWorkspaceProxyIDs(DB, workspaceID, committedProxyIDs),
			refreshWorkspaceUsageActiveRoutes(DB, workspaceID))
		if checkerChanged {
			checkerconfig.Notify(workspaceID)
		} else {
			checkerconfig.TagAssignmentsChanged(workspaceID)
		}
	}()
	hasFilterIndex := DB.Migrator().HasTable(&domain.WorkspaceProxyFilterIndex{})
	chunkSize := deleteChunkSize
	if chunkSize > len(proxies) {
		chunkSize = len(proxies)
	}
	if chunkSize <= 0 {
		chunkSize = len(proxies)
	}

	orphanSet := make(map[uint64]struct{})

	for start := 0; start < len(proxies); start += chunkSize {
		end := start + chunkSize
		if end > len(proxies) {
			end = len(proxies)
		}

		chunk := proxies[start:end]
		proxyIDs := make([]uint64, 0, len(chunk))
		for _, id := range chunk {
			if id > 0 {
				proxyIDs = append(proxyIDs, uint64(id))
			}
		}
		var removed int64
		var changed bool
		if err := DB.Transaction(func(tx *gorm.DB) error {
			var deleteErr error
			removed, changed, deleteErr = deleteManagedRoutes(tx, workspaceID, proxyIDs, "")
			return deleteErr
		}); err != nil {
			return totalDeleted, nil, err
		}
		totalDeleted += removed
		checkerChanged = checkerChanged || changed
		committedProxyIDs = append(committedProxyIDs, proxyIDs...)
		if len(proxyIDs) > 0 {
			if hasFilterIndex {
				if err := DB.
					Where("workspace_id = ?", workspaceID).
					Where("proxy_id IN ?", proxyIDs).
					Delete(&domain.WorkspaceProxyFilterIndex{}).Error; err != nil {
					return totalDeleted, nil, err
				}
			}
		}

		orphanIDs, err := collectOrphanProxyIDs(chunk)
		if err != nil {
			return totalDeleted, nil, err
		}
		if len(orphanIDs) == 0 {
			continue
		}

		for _, id := range orphanIDs {
			orphanSet[id] = struct{}{}
		}
	}
	if len(orphanSet) == 0 {
		return totalDeleted, nil, nil
	}

	uniqueIDs := make([]uint64, 0, len(orphanSet))
	for id := range orphanSet {
		uniqueIDs = append(uniqueIDs, id)
	}

	for start := 0; start < len(uniqueIDs); start += deleteChunkSize {
		var batch []domain.Proxy
		if err := DB.Where("id IN ?", uniqueIDs[start:min(start+deleteChunkSize, len(uniqueIDs))]).Find(&batch).Error; err != nil {
			return totalDeleted, nil, err
		}
		orphans = append(orphans, batch...)
	}

	return totalDeleted, orphans, nil
}

func PauseManagedProxy(workspaceID uint, proxyID uint64, reason string) (bool, []domain.Proxy, error) {
	if DB == nil {
		return false, nil, fmt.Errorf("database not initialised")
	}
	paused := false
	err := DB.Transaction(func(tx *gorm.DB) error {
		now := time.Now().UTC()
		result := tx.Model(&domain.ManagedProxy{}).
			Where("workspace_id = ? AND proxy_id = ? AND state = ?", workspaceID, proxyID, domain.ManagedProxyStateActive).
			Updates(map[string]any{
				"state":        domain.ManagedProxyStatePaused,
				"pause_reason": reason,
				"paused_at":    now,
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return nil
		}
		paused = true
		if err := refreshManagedProxyLifecycleReadModels(tx, workspaceID, []uint64{proxyID}); err != nil {
			return err
		}
		return refreshWorkspaceUsageActiveRoutes(tx, workspaceID)
	})
	if err != nil || !paused {
		return false, nil, err
	}
	inactive, err := proxiesWithoutActiveManagement(DB, []uint64{proxyID})
	return true, inactive, err
}

// DeleteActiveManagedProxy applies failure deletion atomically. A manual pause
// or archive during an in-flight check must not turn into an automatic deletion.
func DeleteActiveManagedProxy(workspaceID uint, proxyID uint64) (bool, []domain.Proxy, error) {
	if DB == nil {
		return false, nil, fmt.Errorf("database not initialised")
	}
	deleted := false
	checkerChanged := false
	err := DB.Transaction(func(tx *gorm.DB) error {
		var err error
		var removed int64
		removed, checkerChanged, err = deleteManagedRoutes(tx, workspaceID, []uint64{proxyID}, domain.ManagedProxyStateActive)
		if err != nil {
			return err
		}
		if removed == 0 {
			return nil
		}
		deleted = true
		if tx.Migrator().HasTable(&domain.WorkspaceProxyFilterIndex{}) {
			if err := tx.Where("workspace_id = ? AND proxy_id = ?", workspaceID, proxyID).
				Delete(&domain.WorkspaceProxyFilterIndex{}).Error; err != nil {
				return err
			}
		}
		if err := refreshWorkspaceScrapeSourceStatsForWorkspaceProxyIDs(tx, workspaceID, []uint64{proxyID}); err != nil {
			return err
		}
		return refreshWorkspaceUsageActiveRoutes(tx, workspaceID)
	})
	if err == nil && checkerChanged {
		checkerconfig.Notify(workspaceID)
	}
	if err != nil || !deleted {
		return false, nil, err
	}
	// Paused or archived management in another workspace still retains the route,
	// but only active management should keep it in checker and rotator queues.
	inactive, err := proxiesWithoutActiveManagement(DB, []uint64{proxyID})
	return true, inactive, err
}

func SetManagedProxyState(workspaceID uint, proxyID uint64, state string) error {
	state = strings.ToLower(strings.TrimSpace(state))
	switch state {
	case domain.ManagedProxyStateActive, domain.ManagedProxyStatePaused, domain.ManagedProxyStateArchived:
	default:
		return domain.ErrInvalidManagedProxyState
	}

	return DB.Transaction(func(tx *gorm.DB) error {
		var managed domain.ManagedProxy
		if err := tx.Where("workspace_id = ? AND proxy_id = ?", workspaceID, proxyID).First(&managed).Error; err != nil {
			return err
		}
		if managed.State == state {
			return nil
		}

		now := time.Now().UTC()
		updates := map[string]any{"state": state}
		switch state {
		case domain.ManagedProxyStateActive:
			if err := ensureWorkspaceActivationAvailable(tx, workspaceID); err != nil {
				return err
			}
			updates["pause_reason"] = ""
			updates["activated_at"] = now
			updates["paused_at"] = nil
			updates["archived_at"] = nil
		case domain.ManagedProxyStatePaused:
			updates["pause_reason"] = domain.ManagedProxyPauseReasonManual
			updates["paused_at"] = now
			updates["archived_at"] = nil
		case domain.ManagedProxyStateArchived:
			updates["pause_reason"] = ""
			updates["archived_at"] = now
		}
		if err := tx.Model(&managed).Updates(updates).Error; err != nil {
			return err
		}
		if err := refreshManagedProxyLifecycleReadModels(tx, workspaceID, []uint64{proxyID}); err != nil {
			return err
		}
		return refreshWorkspaceUsageActiveRoutes(tx, workspaceID)
	})
}

func ensureWorkspaceActivationAvailable(tx *gorm.DB, workspaceID uint) error {
	var subscription domain.WorkspaceSubscription
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("workspace_id = ?", workspaceID).
		First(&subscription).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	limit, unlimited := subscription.ActivationLimit()
	if unlimited {
		return nil
	}
	var activeCount int64
	if err := tx.Model(&domain.ManagedProxy{}).
		Where("workspace_id = ? AND state = ?", workspaceID, domain.ManagedProxyStateActive).
		Count(&activeCount).Error; err != nil {
		return err
	}
	if uint64(activeCount) >= limit {
		return ErrWorkspaceCapacityReached
	}
	return nil
}

func refreshManagedProxyLifecycleReadModels(tx *gorm.DB, workspaceID uint, proxyIDs []uint64) error {
	if err := refreshWorkspaceProxyFilterIndexesForWorkspaceProxyIDs(tx, workspaceID, proxyIDs); err != nil {
		return err
	}
	return refreshWorkspaceScrapeSourceStatsForWorkspaceProxyIDs(tx, workspaceID, proxyIDs)
}

func DeleteProxiesWithSettings(workspaceID uint, settings dto.DeleteSettings) (int64, []domain.Proxy, error) {
	if settings.Scope == "selected" && len(settings.Proxies) == 0 {
		return 0, nil, ErrNoProxiesSelected
	}

	proxyIDs, err := collectProxyIDsForDeletion(workspaceID, settings)
	if err != nil {
		return 0, nil, err
	}

	if len(proxyIDs) == 0 {
		return 0, nil, nil
	}

	intIDs := make([]int, 0, len(proxyIDs))
	for _, id := range proxyIDs {
		intIDs = append(intIDs, int(id))
	}

	return DeleteProxyRelation(workspaceID, intIDs)
}
