package database

import (
	"context"
	"errors"
	"time"

	"magpie/internal/domain"

	"gorm.io/gorm/clause"
)

var (
	ErrInvalidNotificationID = errors.New("invalid notification ID")
	ErrNotificationNotFound  = errors.New("notification not found")
)

func ReadNotificationIDs(ctx context.Context, userID uint, ids []string) ([]string, error) {
	readIDs := make([]string, 0)
	if len(ids) == 0 {
		return readIDs, nil
	}
	err := DB.WithContext(ctx).Model(&domain.NotificationRead{}).
		Where("user_id = ? AND notification_id IN ?", userID, ids).
		Order("notification_id").Pluck("notification_id", &readIDs).Error
	return readIDs, err
}

func MarkNotificationRead(ctx context.Context, userID uint, id string) error {
	kind, eventID, valid := domain.ParseNotificationID(id)
	if !valid {
		return ErrInvalidNotificationID
	}
	db := DB.WithContext(ctx)
	var accessible int64
	var err error
	switch kind {
	case "invitation":
		err = db.Model(&domain.WorkspaceInvitation{}).
			Where("id = ? AND invitee_user_id = ? AND expires_at > ?", eventID, userID, time.Now().UTC()).
			Count(&accessible).Error
	case "alert":
		err = db.Model(&domain.AlertIncident{}).
			Joins("JOIN workspace_memberships wm ON wm.workspace_id = alert_incidents.workspace_id").
			Where("alert_incidents.id = ? AND wm.user_id = ?", eventID, userID).
			Count(&accessible).Error
	case "release":
		// Releases are public external events. A receipt stores only the ID;
		// reading it does not require another request to GitHub.
		err = db.Model(&domain.User{}).Where("id = ?", userID).Count(&accessible).Error
	}
	if err != nil {
		return err
	}
	if accessible == 0 {
		return ErrNotificationNotFound
	}
	return db.Clauses(clause.OnConflict{DoNothing: true}).Create(&domain.NotificationRead{
		UserID: userID, NotificationID: id, ReadAt: time.Now().UTC(),
	}).Error
}
