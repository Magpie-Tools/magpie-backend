package database

import (
	"context"
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
	ErrWorkspaceInvitationExists   = errors.New("a pending invitation already exists for this account")
	ErrWorkspaceInvitationNotFound = errors.New("workspace invitation not found")
	ErrWorkspaceInvitationExpired  = errors.New("workspace invitation expired")
	ErrWorkspaceInviteeNotFound    = errors.New("user account not found")
)

func CreateWorkspaceInvitation(workspaceID, inviterUserID uint, email, role string, billingAdmin bool, expiresAt time.Time) (dto.WorkspaceInvitation, error) {
	email = auth.NormalizeEmail(email)
	role = strings.ToLower(strings.TrimSpace(role))
	if workspaceID == 0 || inviterUserID == 0 || email == "" {
		return dto.WorkspaceInvitation{}, errors.New("workspace, inviter, and invitee are required")
	}
	if !expiresAt.After(time.Now().UTC()) {
		return dto.WorkspaceInvitation{}, errors.New("invitation expiry must be in the future")
	}

	var created domain.WorkspaceInvitation
	err := DB.Transaction(func(tx *gorm.DB) error {
		var invitee domain.User
		if err := tx.Where("LOWER(email) = ?", email).First(&invitee).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrWorkspaceInviteeNotFound
			}
			return err
		}
		var inviter domain.User
		if err := tx.Select("id", "email").First(&inviter, inviterUserID).Error; err != nil {
			return err
		}
		var membershipCount int64
		if err := tx.Model(&domain.WorkspaceMembership{}).
			Where("workspace_id = ? AND user_id = ?", workspaceID, invitee.ID).
			Count(&membershipCount).Error; err != nil {
			return err
		}
		if membershipCount > 0 {
			return ErrWorkspaceMemberExists
		}
		if err := tx.Where(
			"workspace_id = ? AND invitee_user_id = ? AND expires_at <= ?",
			workspaceID,
			invitee.ID,
			time.Now().UTC(),
		).Delete(&domain.WorkspaceInvitation{}).Error; err != nil {
			return err
		}

		inviterID := inviter.ID
		created = domain.WorkspaceInvitation{
			WorkspaceID:   workspaceID,
			InviteeUserID: invitee.ID,
			InviterUserID: &inviterID,
			InviterEmail:  auth.NormalizeEmail(inviter.Email),
			Role:          role,
			BillingAdmin:  billingAdmin,
			ExpiresAt:     expiresAt.UTC(),
		}
		if err := created.Normalize(); err != nil {
			return err
		}
		if err := tx.Create(&created).Error; err != nil {
			if isUniqueConstraintError(err) {
				return ErrWorkspaceInvitationExists
			}
			return err
		}
		return nil
	})
	if err != nil {
		return dto.WorkspaceInvitation{}, err
	}
	return GetWorkspaceInvitationForWorkspace(workspaceID, created.ID, time.Now().UTC())
}

func ListWorkspaceInvitations(workspaceID uint, now time.Time) ([]dto.WorkspaceInvitation, error) {
	var invitations []domain.WorkspaceInvitation
	if err := invitationQuery(DB).
		Where("workspace_invitations.workspace_id = ? AND workspace_invitations.expires_at > ?", workspaceID, now.UTC()).
		Order("workspace_invitations.created_at ASC, workspace_invitations.id ASC").
		Find(&invitations).Error; err != nil {
		return nil, err
	}
	return workspaceInvitationDTOs(invitations), nil
}

func ListUserWorkspaceInvitations(userID uint, now time.Time) ([]dto.WorkspaceInvitation, error) {
	var invitations []domain.WorkspaceInvitation
	if err := invitationQuery(DB).
		Where("workspace_invitations.invitee_user_id = ? AND workspace_invitations.expires_at > ?", userID, now.UTC()).
		Order("workspace_invitations.expires_at ASC, workspace_invitations.id ASC").
		Find(&invitations).Error; err != nil {
		return nil, err
	}
	return workspaceInvitationDTOs(invitations), nil
}

func GetWorkspaceInvitationForWorkspace(workspaceID, invitationID uint, now time.Time) (dto.WorkspaceInvitation, error) {
	var invitation domain.WorkspaceInvitation
	err := invitationQuery(DB).
		Where("workspace_invitations.id = ? AND workspace_invitations.workspace_id = ?", invitationID, workspaceID).
		First(&invitation).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return dto.WorkspaceInvitation{}, ErrWorkspaceInvitationNotFound
		}
		return dto.WorkspaceInvitation{}, err
	}
	if !invitation.ExpiresAt.After(now.UTC()) {
		return dto.WorkspaceInvitation{}, ErrWorkspaceInvitationExpired
	}
	return workspaceInvitationDTO(invitation), nil
}

func UpdateWorkspaceInvitation(workspaceID, invitationID uint, role string, billingAdmin bool, now time.Time) (dto.WorkspaceInvitation, error) {
	role = strings.ToLower(strings.TrimSpace(role))
	err := DB.Transaction(func(tx *gorm.DB) error {
		var invitation domain.WorkspaceInvitation
		query := tx.Where("id = ? AND workspace_id = ?", invitationID, workspaceID)
		if isPostgresDialect(tx) {
			query = query.Clauses(clause.Locking{Strength: "UPDATE"})
		}
		if err := query.First(&invitation).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrWorkspaceInvitationNotFound
			}
			return err
		}
		if !invitation.ExpiresAt.After(now.UTC()) {
			return ErrWorkspaceInvitationExpired
		}
		candidate := invitation
		candidate.Role = role
		candidate.BillingAdmin = billingAdmin
		if err := candidate.Normalize(); err != nil {
			return err
		}
		return tx.Model(&invitation).Updates(map[string]any{
			"role":          candidate.Role,
			"billing_admin": candidate.BillingAdmin,
		}).Error
	})
	if err != nil {
		return dto.WorkspaceInvitation{}, err
	}
	return GetWorkspaceInvitationForWorkspace(workspaceID, invitationID, now)
}

func SetWorkspaceInvitationNotificationStatus(workspaceID, invitationID uint, status string) error {
	candidate := domain.WorkspaceInvitation{Role: domain.WorkspaceRoleViewer, Notification: status}
	if err := candidate.Normalize(); err != nil {
		return err
	}
	result := DB.Model(&domain.WorkspaceInvitation{}).
		Where("id = ? AND workspace_id = ?", invitationID, workspaceID).
		Update("notification", candidate.Notification)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return ErrWorkspaceInvitationNotFound
	}
	return nil
}

func RevokeWorkspaceInvitation(workspaceID, invitationID uint, now time.Time) error {
	result := DB.Where("id = ? AND workspace_id = ? AND expires_at > ?", invitationID, workspaceID, now.UTC()).
		Delete(&domain.WorkspaceInvitation{})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected > 0 {
		return nil
	}
	return workspaceInvitationMissingReason(workspaceID, invitationID, now)
}

func AcceptWorkspaceInvitation(invitationID, userID uint, now time.Time) (dto.WorkspaceInvitationAcceptance, error) {
	var accepted dto.WorkspaceInvitationAcceptance
	err := DB.Transaction(func(tx *gorm.DB) error {
		var invitation domain.WorkspaceInvitation
		query := tx.Where("id = ? AND invitee_user_id = ?", invitationID, userID)
		if isPostgresDialect(tx) {
			query = query.Clauses(clause.Locking{Strength: "UPDATE"})
		}
		if err := query.First(&invitation).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrWorkspaceInvitationNotFound
			}
			return err
		}
		if !invitation.ExpiresAt.After(now.UTC()) {
			return ErrWorkspaceInvitationExpired
		}

		membership := domain.WorkspaceMembership{
			WorkspaceID:  invitation.WorkspaceID,
			UserID:       userID,
			Role:         invitation.Role,
			BillingAdmin: invitation.BillingAdmin,
		}
		if err := membership.Normalize(); err != nil {
			return err
		}
		if err := tx.Create(&membership).Error; err != nil {
			if isUniqueConstraintError(err) {
				return ErrWorkspaceMemberExists
			}
			return err
		}
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&domain.WorkspaceMemberPreference{
			WorkspaceID: invitation.WorkspaceID,
			UserID:      userID,
		}).Error; err != nil {
			return err
		}
		if err := tx.Model(&domain.Workspace{}).
			Where("id = ? AND personal = ?", invitation.WorkspaceID, true).
			Update("personal", false).Error; err != nil {
			return err
		}
		var workspace domain.Workspace
		if err := tx.Select("id", "name").First(&workspace, invitation.WorkspaceID).Error; err != nil {
			return err
		}
		result := tx.Where("id = ? AND invitee_user_id = ?", invitation.ID, userID).Delete(&domain.WorkspaceInvitation{})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return ErrWorkspaceInvitationNotFound
		}
		accepted = dto.WorkspaceInvitationAcceptance{WorkspaceID: workspace.ID, WorkspaceName: workspace.Name}
		return nil
	})
	return accepted, err
}

func DeclineWorkspaceInvitation(invitationID, userID uint, now time.Time) error {
	result := DB.Where("id = ? AND invitee_user_id = ? AND expires_at > ?", invitationID, userID, now.UTC()).
		Delete(&domain.WorkspaceInvitation{})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected > 0 {
		return nil
	}
	var invitation domain.WorkspaceInvitation
	err := DB.Select("id", "expires_at").Where("id = ? AND invitee_user_id = ?", invitationID, userID).First(&invitation).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return ErrWorkspaceInvitationNotFound
	}
	if err != nil {
		return err
	}
	if !invitation.ExpiresAt.After(now.UTC()) {
		return ErrWorkspaceInvitationExpired
	}
	return ErrWorkspaceInvitationNotFound
}

func DeleteExpiredWorkspaceInvitations(ctx context.Context, before time.Time) (int64, error) {
	tx := DB
	if ctx != nil {
		tx = tx.WithContext(ctx)
	}
	result := tx.Where("expires_at <= ?", before.UTC()).Delete(&domain.WorkspaceInvitation{})
	return result.RowsAffected, result.Error
}

func invitationQuery(db *gorm.DB) *gorm.DB {
	return db.Model(&domain.WorkspaceInvitation{}).
		Preload("Workspace").
		Preload("Invitee")
}

func workspaceInvitationDTOs(invitations []domain.WorkspaceInvitation) []dto.WorkspaceInvitation {
	result := make([]dto.WorkspaceInvitation, 0, len(invitations))
	for _, invitation := range invitations {
		result = append(result, workspaceInvitationDTO(invitation))
	}
	return result
}

func workspaceInvitationDTO(invitation domain.WorkspaceInvitation) dto.WorkspaceInvitation {
	return dto.WorkspaceInvitation{
		ID:                 invitation.ID,
		WorkspaceID:        invitation.WorkspaceID,
		WorkspaceName:      invitation.Workspace.Name,
		InviteeUserID:      invitation.InviteeUserID,
		InviteeEmail:       invitation.Invitee.Email,
		InviterUserID:      invitation.InviterUserID,
		InviterEmail:       invitation.InviterEmail,
		Role:               invitation.Role,
		BillingAdmin:       invitation.BillingAdmin,
		NotificationStatus: invitation.Notification,
		ExpiresAt:          invitation.ExpiresAt,
		CreatedAt:          invitation.CreatedAt,
		UpdatedAt:          invitation.UpdatedAt,
	}
}

func workspaceInvitationMissingReason(workspaceID, invitationID uint, now time.Time) error {
	var invitation domain.WorkspaceInvitation
	err := DB.Select("id", "expires_at").Where("id = ? AND workspace_id = ?", invitationID, workspaceID).First(&invitation).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return ErrWorkspaceInvitationNotFound
	}
	if err != nil {
		return err
	}
	if !invitation.ExpiresAt.After(now.UTC()) {
		return ErrWorkspaceInvitationExpired
	}
	return ErrWorkspaceInvitationNotFound
}
