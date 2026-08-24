package database

import (
	"errors"
	"strings"
	"time"

	"magpie/internal/api/dto"
	"magpie/internal/auth"
	"magpie/internal/domain"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	ErrWorkspaceNotFound      = errors.New("workspace not found")
	ErrWorkspaceMemberExists  = errors.New("workspace member already exists")
	ErrWorkspaceLastOwner     = errors.New("workspace must keep at least one owner")
	ErrWorkspacePersonalOwner = errors.New("the owner of a personal workspace cannot be removed")
)

type WorkspaceAccess struct {
	WorkspaceID  uint
	UserID       uint
	Role         string
	BillingAdmin bool
	IsDefault    bool
}

func (access WorkspaceAccess) CanRead() bool {
	return domain.WorkspaceRoleRank(access.Role) >= domain.WorkspaceRoleRank(domain.WorkspaceRoleViewer)
}

func (access WorkspaceAccess) CanOperate() bool {
	return domain.WorkspaceRoleRank(access.Role) >= domain.WorkspaceRoleRank(domain.WorkspaceRoleOperator)
}

func (access WorkspaceAccess) CanAdminister() bool {
	return domain.WorkspaceRoleRank(access.Role) >= domain.WorkspaceRoleRank(domain.WorkspaceRoleAdmin)
}

func (access WorkspaceAccess) IsOwner() bool          { return access.Role == domain.WorkspaceRoleOwner }
func (access WorkspaceAccess) CanManageBilling() bool { return access.IsOwner() || access.BillingAdmin }

func ResolveWorkspaceAccess(userID, requestedWorkspaceID uint) (WorkspaceAccess, error) {
	if DB == nil || userID == 0 {
		return WorkspaceAccess{}, ErrWorkspaceNotFound
	}

	query := DB.Model(&domain.WorkspaceMembership{}).Where("user_id = ?", userID)
	if requestedWorkspaceID > 0 {
		query = query.Where("workspace_id = ?", requestedWorkspaceID)
	} else {
		query = query.Order("is_default DESC, created_at ASC, workspace_id ASC")
	}

	var membership domain.WorkspaceMembership
	if err := query.First(&membership).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return WorkspaceAccess{}, ErrWorkspaceNotFound
		}
		return WorkspaceAccess{}, err
	}

	return WorkspaceAccess{
		WorkspaceID:  membership.WorkspaceID,
		UserID:       membership.UserID,
		Role:         membership.Role,
		BillingAdmin: membership.BillingAdmin,
		IsDefault:    membership.IsDefault,
	}, nil
}

func CreatePersonalWorkspaceForUser(tx *gorm.DB, user domain.User) (domain.Workspace, error) {
	if tx == nil {
		tx = DB
	}
	if tx == nil || user.ID == 0 {
		return domain.Workspace{}, errors.New("cannot create workspace without a stored user")
	}

	workspace := domain.Workspace{
		Name:                       personalWorkspaceName(user.Email),
		Personal:                   true,
		HTTPProtocol:               user.HTTPProtocol,
		HTTPSProtocol:              user.HTTPSProtocol,
		SOCKS4Protocol:             user.SOCKS4Protocol,
		SOCKS5Protocol:             user.SOCKS5Protocol,
		Timeout:                    user.Timeout,
		Retries:                    user.Retries,
		UseHttpsForSocks:           user.UseHttpsForSocks,
		TransportProtocol:          user.TransportProtocol,
		AutoRemoveFailingProxies:   user.AutoRemoveFailingProxies,
		AutoRemoveFailureThreshold: user.AutoRemoveFailureThreshold,
	}
	if err := tx.Create(&workspace).Error; err != nil {
		return domain.Workspace{}, err
	}

	membership := domain.WorkspaceMembership{
		WorkspaceID:  workspace.ID,
		UserID:       user.ID,
		Role:         domain.WorkspaceRoleOwner,
		BillingAdmin: true,
		IsDefault:    true,
	}
	preference := domain.WorkspaceMemberPreference{
		WorkspaceID:              workspace.ID,
		UserID:                   user.ID,
		ProxyListColumns:         user.ProxyListColumns,
		ScrapeSourceProxyColumns: user.ScrapeSourceProxyColumns,
		ScrapeSourceListColumns:  user.ScrapeSourceListColumns,
	}
	subscription := defaultWorkspaceSubscription(user.Role)
	subscription.WorkspaceID = workspace.ID

	if err := tx.Create(&membership).Error; err != nil {
		return domain.Workspace{}, err
	}
	if err := tx.Create(&preference).Error; err != nil {
		return domain.Workspace{}, err
	}
	if err := tx.Create(&subscription).Error; err != nil {
		return domain.Workspace{}, err
	}
	workspace.Subscription = subscription
	return workspace, nil
}

func CreateWorkspace(userID uint, name string) (dto.Workspace, error) {
	name = normalizeWorkspaceName(name)
	if name == "" {
		return dto.Workspace{}, errors.New("workspace name is required")
	}
	if len([]rune(name)) > 120 {
		return dto.Workspace{}, errors.New("workspace name is too long")
	}

	var result dto.Workspace
	err := DB.Transaction(func(tx *gorm.DB) error {
		var user domain.User
		if err := tx.Select("id", "role").First(&user, userID).Error; err != nil {
			return err
		}
		workspace := domain.Workspace{
			Name:                       name,
			HTTPSProtocol:              true,
			Timeout:                    7500,
			Retries:                    2,
			UseHttpsForSocks:           true,
			TransportProtocol:          "tcp",
			AutoRemoveFailureThreshold: 3,
		}
		if err := tx.Create(&workspace).Error; err != nil {
			return err
		}
		membership := domain.WorkspaceMembership{
			WorkspaceID:  workspace.ID,
			UserID:       userID,
			Role:         domain.WorkspaceRoleOwner,
			BillingAdmin: true,
		}
		if err := tx.Create(&membership).Error; err != nil {
			return err
		}
		if err := tx.Create(&domain.WorkspaceMemberPreference{WorkspaceID: workspace.ID, UserID: userID}).Error; err != nil {
			return err
		}
		subscription := defaultWorkspaceSubscription(user.Role)
		subscription.WorkspaceID = workspace.ID
		if err := tx.Create(&subscription).Error; err != nil {
			return err
		}
		workspace.Subscription = subscription
		result = workspaceDTO(workspace, membership, 0, 0)
		return nil
	})
	return result, err
}

func ListWorkspaces(userID uint) ([]dto.Workspace, error) {
	var memberships []domain.WorkspaceMembership
	if err := DB.Where("user_id = ?", userID).
		Preload("Workspace.Subscription").
		Order("is_default DESC, created_at ASC, workspace_id ASC").
		Find(&memberships).Error; err != nil {
		return nil, err
	}

	result := make([]dto.Workspace, 0, len(memberships))
	for _, membership := range memberships {
		active, stored, err := workspaceManagedProxyCounts(DB, membership.WorkspaceID)
		if err != nil {
			return nil, err
		}
		result = append(result, workspaceDTO(membership.Workspace, membership, active, stored))
	}
	return result, nil
}

func GetWorkspace(workspaceID, userID uint) (dto.Workspace, error) {
	var membership domain.WorkspaceMembership
	if err := DB.Where("workspace_id = ? AND user_id = ?", workspaceID, userID).
		Preload("Workspace.Subscription").First(&membership).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return dto.Workspace{}, ErrWorkspaceNotFound
		}
		return dto.Workspace{}, err
	}
	active, stored, err := workspaceManagedProxyCounts(DB, workspaceID)
	if err != nil {
		return dto.Workspace{}, err
	}
	return workspaceDTO(membership.Workspace, membership, active, stored), nil
}

func RenameWorkspace(workspaceID uint, name string) error {
	name = normalizeWorkspaceName(name)
	if name == "" {
		return errors.New("workspace name is required")
	}
	if len([]rune(name)) > 120 {
		return errors.New("workspace name is too long")
	}
	result := DB.Model(&domain.Workspace{}).Where("id = ?", workspaceID).Update("name", name)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return ErrWorkspaceNotFound
	}
	return nil
}

func SetDefaultWorkspace(userID, workspaceID uint) error {
	return DB.Transaction(func(tx *gorm.DB) error {
		var membership domain.WorkspaceMembership
		if err := tx.Where("user_id = ? AND workspace_id = ?", userID, workspaceID).First(&membership).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrWorkspaceNotFound
			}
			return err
		}
		if err := tx.Model(&domain.WorkspaceMembership{}).Where("user_id = ?", userID).Update("is_default", false).Error; err != nil {
			return err
		}
		return tx.Model(&domain.WorkspaceMembership{}).
			Where("user_id = ? AND workspace_id = ?", userID, workspaceID).
			Update("is_default", true).Error
	})
}

func ListWorkspaceMembers(workspaceID uint) ([]dto.WorkspaceMember, error) {
	var rows []struct {
		UserID       uint      `gorm:"column:user_id"`
		Email        string    `gorm:"column:email"`
		Role         string    `gorm:"column:role"`
		BillingAdmin bool      `gorm:"column:billing_admin"`
		CreatedAt    time.Time `gorm:"column:created_at"`
	}
	if err := DB.Table("workspace_memberships wm").
		Select("wm.user_id, u.email, wm.role, wm.billing_admin, wm.created_at").
		Joins("JOIN users u ON u.id = wm.user_id").
		Where("wm.workspace_id = ?", workspaceID).
		Order("CASE wm.role WHEN 'owner' THEN 0 WHEN 'admin' THEN 1 WHEN 'operator' THEN 2 ELSE 3 END, LOWER(u.email)").
		Scan(&rows).Error; err != nil {
		return nil, err
	}
	result := make([]dto.WorkspaceMember, 0, len(rows))
	for _, row := range rows {
		result = append(result, dto.WorkspaceMember{
			UserID:       row.UserID,
			Email:        row.Email,
			Role:         row.Role,
			BillingAdmin: row.BillingAdmin,
			JoinedAt:     row.CreatedAt,
		})
	}
	return result, nil
}

func AddWorkspaceMember(workspaceID uint, email, role string, billingAdmin bool) (dto.WorkspaceMember, error) {
	email = auth.NormalizeEmail(email)
	role = strings.ToLower(strings.TrimSpace(role))
	membership := domain.WorkspaceMembership{WorkspaceID: workspaceID, Role: role, BillingAdmin: billingAdmin}
	if err := membership.Normalize(); err != nil {
		return dto.WorkspaceMember{}, err
	}
	if role == domain.WorkspaceRoleOwner {
		return dto.WorkspaceMember{}, errors.New("transfer ownership instead of adding another owner")
	}

	var user domain.User
	if err := DB.Where("LOWER(email) = ?", email).First(&user).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return dto.WorkspaceMember{}, errors.New("user account not found")
		}
		return dto.WorkspaceMember{}, err
	}
	membership.UserID = user.ID
	if err := DB.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&membership).Error; err != nil {
			if isUniqueConstraintError(err) {
				return ErrWorkspaceMemberExists
			}
			return err
		}
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&domain.WorkspaceMemberPreference{
			WorkspaceID: workspaceID,
			UserID:      user.ID,
		}).Error; err != nil {
			return err
		}
		// A generated personal workspace becomes shared as soon as another
		// account is invited. This makes later ownership-transfer checks clear.
		return tx.Model(&domain.Workspace{}).
			Where("id = ? AND personal = ?", workspaceID, true).
			Update("personal", false).Error
	}); err != nil {
		return dto.WorkspaceMember{}, err
	}
	return dto.WorkspaceMember{UserID: user.ID, Email: user.Email, Role: membership.Role, BillingAdmin: membership.BillingAdmin, JoinedAt: membership.CreatedAt}, nil
}

func UpdateWorkspaceMember(workspaceID, memberUserID uint, role string, billingAdmin bool) error {
	role = strings.ToLower(strings.TrimSpace(role))
	updated := domain.WorkspaceMembership{Role: role, BillingAdmin: billingAdmin}
	if err := updated.Normalize(); err != nil {
		return err
	}
	return DB.Transaction(func(tx *gorm.DB) error {
		var existing domain.WorkspaceMembership
		if err := tx.Where("workspace_id = ? AND user_id = ?", workspaceID, memberUserID).First(&existing).Error; err != nil {
			return err
		}
		if existing.Role == domain.WorkspaceRoleOwner && role != domain.WorkspaceRoleOwner {
			var ownerCount int64
			if err := tx.Model(&domain.WorkspaceMembership{}).Where("workspace_id = ? AND role = ?", workspaceID, domain.WorkspaceRoleOwner).Count(&ownerCount).Error; err != nil {
				return err
			}
			if ownerCount <= 1 {
				return ErrWorkspaceLastOwner
			}
		}
		return tx.Model(&existing).Updates(map[string]any{"role": updated.Role, "billing_admin": updated.BillingAdmin}).Error
	})
}

func RemoveWorkspaceMember(workspaceID, memberUserID uint) error {
	return DB.Transaction(func(tx *gorm.DB) error {
		var workspace domain.Workspace
		if err := tx.Select("id", "personal").First(&workspace, workspaceID).Error; err != nil {
			return err
		}
		var membership domain.WorkspaceMembership
		if err := tx.Where("workspace_id = ? AND user_id = ?", workspaceID, memberUserID).First(&membership).Error; err != nil {
			return err
		}
		if workspace.Personal && membership.Role == domain.WorkspaceRoleOwner {
			return ErrWorkspacePersonalOwner
		}
		if membership.Role == domain.WorkspaceRoleOwner {
			var ownerCount int64
			if err := tx.Model(&domain.WorkspaceMembership{}).Where("workspace_id = ? AND role = ?", workspaceID, domain.WorkspaceRoleOwner).Count(&ownerCount).Error; err != nil {
				return err
			}
			if ownerCount <= 1 {
				return ErrWorkspaceLastOwner
			}
		}
		if err := tx.Where("workspace_id = ? AND user_id = ?", workspaceID, memberUserID).Delete(&domain.WorkspaceMemberPreference{}).Error; err != nil {
			return err
		}
		if err := tx.Delete(&membership).Error; err != nil {
			return err
		}
		var replacement domain.WorkspaceMembership
		if err := tx.Where("user_id = ?", memberUserID).Order("created_at ASC, workspace_id ASC").First(&replacement).Error; err == nil {
			return tx.Model(&replacement).Update("is_default", true).Error
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		return nil
	})
}

func GetWorkspaceByID(workspaceID uint) domain.Workspace {
	var workspace domain.Workspace
	DB.First(&workspace, workspaceID)
	return workspace
}

func GetWorkspacesByIDsForChecker(ids []uint) (map[uint]domain.Workspace, error) {
	ids = normalizeUserIDs(ids)
	if len(ids) == 0 {
		return nil, nil
	}
	var workspaces []domain.Workspace
	if err := DB.Where("id IN ?", ids).Find(&workspaces).Error; err != nil {
		return nil, err
	}
	result := make(map[uint]domain.Workspace, len(workspaces))
	for _, workspace := range workspaces {
		result[workspace.ID] = workspace
	}
	return result, nil
}

func GetWorkspaceMemberPreference(workspaceID, userID uint) domain.WorkspaceMemberPreference {
	var preference domain.WorkspaceMemberPreference
	if err := DB.Where("workspace_id = ? AND user_id = ?", workspaceID, userID).First(&preference).Error; err != nil {
		preference.WorkspaceID = workspaceID
		preference.UserID = userID
	}
	return preference
}

func workspaceManagedProxyCounts(tx *gorm.DB, workspaceID uint) (active, stored uint64, err error) {
	var activeCount, storedCount int64
	if tx.Migrator().HasTable(&domain.ManagedProxy{}) {
		if err = tx.Model(&domain.ManagedProxy{}).Where("workspace_id = ?", workspaceID).Count(&storedCount).Error; err != nil {
			return 0, 0, err
		}
		if err = tx.Model(&domain.ManagedProxy{}).Where("workspace_id = ? AND state = ?", workspaceID, domain.ManagedProxyStateActive).Count(&activeCount).Error; err != nil {
			return 0, 0, err
		}
	}
	return uint64(activeCount), uint64(storedCount), nil
}

func workspaceDTO(workspace domain.Workspace, membership domain.WorkspaceMembership, active, stored uint64) dto.Workspace {
	subscription := workspace.Subscription
	limit, unlimited := subscription.ActivationLimit()
	var activationLimit *uint64
	if !unlimited {
		activationLimit = &limit
	}
	return dto.Workspace{
		ID:           workspace.ID,
		Name:         workspace.Name,
		Personal:     workspace.Personal,
		Role:         membership.Role,
		BillingAdmin: membership.BillingAdmin,
		IsDefault:    membership.IsDefault,
		Capacity: dto.WorkspaceCapacity{
			ActiveRoutes:     active,
			StoredRoutes:     stored,
			IncludedRoutes:   subscription.IncludedActiveRoutes,
			AdditionalRoutes: subscription.AdditionalActiveRoutes,
			OverageRoutes:    subscription.OverageActiveRoutes,
			ActivationLimit:  activationLimit,
			OverageMode:      subscription.OverageMode,
		},
		Subscription: dto.WorkspaceSubscription{
			PlanCode:                    subscription.PlanCode,
			Status:                      subscription.Status,
			IncludedOperators:           subscription.IncludedOperators,
			StatisticsRetentionDays:     subscription.StatisticsRetentionDays,
			MinimumCheckIntervalSeconds: subscription.MinimumCheckIntervalSeconds,
			CurrentPeriodStart:          subscription.CurrentPeriodStart,
			CurrentPeriodEnd:            subscription.CurrentPeriodEnd,
			CancelAtPeriodEnd:           subscription.CancelAtPeriodEnd,
		},
		CreatedAt: workspace.CreatedAt,
	}
}

func normalizeWorkspaceName(name string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(name)), " ")
}
