package database

import (
	"context"
	"errors"
	"fmt"

	"magpie/internal/domain"

	"gorm.io/gorm"
)

var ErrWorkspaceOwnershipTransferRequired = errors.New("workspace ownership must be transferred before deleting the account")

// DeleteUserAccountResult describes the runtime state that must be reconciled
// after the database transaction commits.
type DeleteUserAccountResult struct {
	InactiveProxies     []domain.Proxy
	RefreshProxies      []domain.Proxy
	OrphanedScrapeSites []domain.ScrapeSite
	DeletedWorkspaceIDs []uint
}

// ValidateUserAccountDeletion performs the ownership check without changing
// state. The transaction repeats the check to protect against concurrent
// membership changes.
func ValidateUserAccountDeletion(ctx context.Context, userID uint) error {
	if DB == nil {
		return fmt.Errorf("database not initialised")
	}
	db := DB
	if ctx != nil {
		db = db.WithContext(ctx)
	}
	return validateUserAccountDeletion(db, userID)
}

// DeleteUserAccountWithResult removes the user identity and its solitary
// personal workspace. Memberships in all other workspaces are revoked without
// changing those workspaces' resources, subscriptions, or capacity.
func DeleteUserAccountWithResult(ctx context.Context, userID uint) (DeleteUserAccountResult, error) {
	if DB == nil {
		return DeleteUserAccountResult{}, fmt.Errorf("database not initialised")
	}

	db := DB
	if ctx != nil {
		db = db.WithContext(ctx)
	}

	var result DeleteUserAccountResult
	err := db.Transaction(func(tx *gorm.DB) error {
		if err := validateUserAccountDeletion(tx, userID); err != nil {
			return err
		}

		workspaceIDs, err := deletablePersonalWorkspaceIDs(tx, userID)
		if err != nil {
			return err
		}
		result.DeletedWorkspaceIDs = workspaceIDs

		proxyIDs, err := workspaceProxyIDs(tx, workspaceIDs)
		if err != nil {
			return err
		}
		scrapeSiteIDs, err := workspaceScrapeSiteIDs(tx, workspaceIDs)
		if err != nil {
			return err
		}

		if err := deleteWorkspaceOwnedResources(tx, workspaceIDs); err != nil {
			return err
		}

		// These deletes also remove preferences and memberships from shared
		// workspaces. No workspace-owned resource uses user_id.
		if tx.Migrator().HasTable(&domain.WorkspaceMemberPreference{}) {
			if err := tx.Where("user_id = ?", userID).Delete(&domain.WorkspaceMemberPreference{}).Error; err != nil {
				return err
			}
		}
		if tx.Migrator().HasTable(&domain.WorkspaceMembership{}) {
			if err := tx.Where("user_id = ?", userID).Delete(&domain.WorkspaceMembership{}).Error; err != nil {
				return err
			}
		}
		if len(workspaceIDs) > 0 {
			if err := tx.Where("id IN ?", workspaceIDs).Delete(&domain.Workspace{}).Error; err != nil {
				return err
			}
		}

		if tx.Migrator().HasTable(&domain.PasswordResetToken{}) {
			if err := tx.Where("user_id = ?", userID).Delete(&domain.PasswordResetToken{}).Error; err != nil {
				return err
			}
		}
		if err := tx.Delete(&domain.User{}, userID).Error; err != nil {
			return err
		}

		result.InactiveProxies, result.RefreshProxies, err = loadProxyQueueChanges(tx, proxyIDs)
		if err != nil {
			return err
		}
		result.OrphanedScrapeSites, err = loadOrphanedScrapeSites(tx, scrapeSiteIDs)
		return err
	})
	if err != nil {
		return DeleteUserAccountResult{}, err
	}
	return result, nil
}

// DeleteUserAccount preserves the original API for callers that do not need
// to rewrite queue payloads after workspace ownership changes.
func DeleteUserAccount(ctx context.Context, userID uint) ([]domain.Proxy, []domain.ScrapeSite, error) {
	result, err := DeleteUserAccountWithResult(ctx, userID)
	if err != nil {
		return nil, nil, err
	}
	return result.InactiveProxies, result.OrphanedScrapeSites, nil
}

func validateUserAccountDeletion(tx *gorm.DB, userID uint) error {
	if userID == 0 {
		return gorm.ErrRecordNotFound
	}
	if !tx.Migrator().HasTable(&domain.WorkspaceMembership{}) {
		return nil
	}

	var blockingCount int64
	err := tx.Table("workspace_memberships owner_membership").
		Joins("JOIN workspaces w ON w.id = owner_membership.workspace_id").
		Where("owner_membership.user_id = ? AND owner_membership.role = ?", userID, domain.WorkspaceRoleOwner).
		Where(`
			NOT EXISTS (
				SELECT 1 FROM workspace_memberships other_owner
				WHERE other_owner.workspace_id = owner_membership.workspace_id
				  AND other_owner.user_id <> owner_membership.user_id
				  AND other_owner.role = ?
			)
		`, domain.WorkspaceRoleOwner).
		Where(`
			w.personal = ? OR EXISTS (
				SELECT 1 FROM workspace_memberships other_member
				WHERE other_member.workspace_id = owner_membership.workspace_id
				  AND other_member.user_id <> owner_membership.user_id
			)
		`, false).
		Count(&blockingCount).Error
	if err != nil {
		return err
	}
	if blockingCount > 0 {
		return ErrWorkspaceOwnershipTransferRequired
	}
	return nil
}

func deletablePersonalWorkspaceIDs(tx *gorm.DB, userID uint) ([]uint, error) {
	if !tx.Migrator().HasTable(&domain.WorkspaceMembership{}) {
		return nil, nil
	}
	var ids []uint
	err := tx.Table("workspaces w").
		Select("w.id").
		Joins("JOIN workspace_memberships wm ON wm.workspace_id = w.id").
		Where("wm.user_id = ? AND wm.role = ? AND w.personal = ?", userID, domain.WorkspaceRoleOwner, true).
		Where(`NOT EXISTS (
			SELECT 1 FROM workspace_memberships other_member
			WHERE other_member.workspace_id = w.id AND other_member.user_id <> ?
		)`, userID).
		Pluck("w.id", &ids).Error
	return ids, err
}

func workspaceProxyIDs(tx *gorm.DB, workspaceIDs []uint) ([]int, error) {
	if len(workspaceIDs) == 0 || !tx.Migrator().HasTable(&domain.ManagedProxy{}) {
		return nil, nil
	}
	var ids []int
	err := tx.Model(&domain.ManagedProxy{}).
		Where("workspace_id IN ?", workspaceIDs).
		Distinct().
		Pluck("proxy_id", &ids).Error
	return ids, err
}

func workspaceScrapeSiteIDs(tx *gorm.DB, workspaceIDs []uint) ([]int, error) {
	if len(workspaceIDs) == 0 || !tx.Migrator().HasTable(&domain.WorkspaceScrapeSite{}) {
		return nil, nil
	}
	var ids []int
	err := tx.Model(&domain.WorkspaceScrapeSite{}).
		Where("workspace_id IN ?", workspaceIDs).
		Distinct().
		Pluck("scrape_site_id", &ids).Error
	return ids, err
}

func deleteWorkspaceOwnedResources(tx *gorm.DB, workspaceIDs []uint) error {
	if len(workspaceIDs) == 0 {
		return nil
	}

	models := []any{
		&domain.ProxyTagAssignment{},
		&domain.WorkspaceProxyFilterIndex{},
		&domain.ManagedProxy{},
		&domain.ProxyTag{},
		&domain.WorkspaceScrapeSourceStat{},
		&domain.WorkspaceScrapeSite{},
		&domain.WorkspaceJudge{},
		&domain.RotatingProxy{},
		&domain.ProxyHistory{},
		&domain.ProxySnapshot{},
		&domain.WorkspaceUsagePeriod{},
		&domain.WorkspaceSubscription{},
	}
	for _, model := range models {
		if !tx.Migrator().HasTable(model) {
			continue
		}
		if err := tx.Where("workspace_id IN ?", workspaceIDs).Delete(model).Error; err != nil {
			return err
		}
	}
	return nil
}

func loadProxyQueueChanges(tx *gorm.DB, proxyIDs []int) (inactive, refresh []domain.Proxy, err error) {
	if len(proxyIDs) == 0 {
		return nil, nil, nil
	}

	if err = tx.
		Where("id IN ?", proxyIDs).
		Where("NOT EXISTS (SELECT 1 FROM user_proxies up WHERE up.proxy_id = proxies.id AND up.state = ?)", domain.ManagedProxyStateActive).
		Find(&inactive).Error; err != nil {
		return nil, nil, err
	}

	if err = tx.
		Distinct("proxies.*").
		Joins("JOIN user_proxies active_up ON active_up.proxy_id = proxies.id AND active_up.state = ?", domain.ManagedProxyStateActive).
		Where("proxies.id IN ?", proxyIDs).
		Find(&refresh).Error; err != nil {
		return nil, nil, err
	}
	if err = hydrateActiveProxyWorkspaces(tx, refresh); err != nil {
		return nil, nil, err
	}
	if err = hydrateProxyCredentials(tx, refresh, 0); err != nil {
		return nil, nil, err
	}
	return inactive, refresh, nil
}

func loadProxyQueueChangesForUint64(tx *gorm.DB, proxyIDs []uint64) (inactive, refresh []domain.Proxy, err error) {
	if len(proxyIDs) == 0 {
		return nil, nil, nil
	}
	ids := make([]int, 0, len(proxyIDs))
	for _, proxyID := range proxyIDs {
		if proxyID > uint64(^uint(0)>>1) {
			return nil, nil, fmt.Errorf("proxy id %d exceeds platform integer range", proxyID)
		}
		ids = append(ids, int(proxyID))
	}
	return loadProxyQueueChanges(tx, ids)
}

func loadOrphanedScrapeSites(tx *gorm.DB, scrapeSiteIDs []int) ([]domain.ScrapeSite, error) {
	if len(scrapeSiteIDs) == 0 {
		return nil, nil
	}

	chunkSize := deleteChunkSize
	if chunkSize <= 0 || chunkSize > len(scrapeSiteIDs) {
		chunkSize = len(scrapeSiteIDs)
	}

	orphans := make([]domain.ScrapeSite, 0)
	for start := 0; start < len(scrapeSiteIDs); start += chunkSize {
		end := start + chunkSize
		if end > len(scrapeSiteIDs) {
			end = len(scrapeSiteIDs)
		}

		var batch []domain.ScrapeSite
		if err := tx.
			Where("id IN ?", scrapeSiteIDs[start:end]).
			Where("NOT EXISTS (SELECT 1 FROM user_scrape_site us WHERE us.scrape_site_id = scrape_sites.id)").
			Find(&batch).Error; err != nil {
			return nil, err
		}

		orphans = append(orphans, batch...)
	}

	return orphans, nil
}
