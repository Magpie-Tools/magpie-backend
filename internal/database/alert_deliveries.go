package database

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"magpie/internal/domain"
)

const AlertMaximumDeliveryAttempts = 4

func ClaimAlertDeliveries(ctx context.Context, limit int, now time.Time) ([]domain.AlertDelivery, error) {
	if limit <= 0 {
		return nil, nil
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, err
	}
	token := hex.EncodeToString(nonce[:])
	var messages []domain.AlertDelivery
	err := DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&domain.AlertDelivery{}).Where("status = ? AND last_attempt_at < ?", "processing", now.Add(-5*time.Minute)).Updates(map[string]any{
			"status":      gorm.Expr("CASE WHEN attempts >= ? THEN 'failed' ELSE 'pending' END", AlertMaximumDeliveryAttempts),
			"last_error":  gorm.Expr("CASE WHEN attempts >= ? THEN 'Delivery attempt interrupted' ELSE last_error END", AlertMaximumDeliveryAttempts),
			"claim_token": "",
		}).Error; err != nil {
			return err
		}
		query := tx.Where("status = ? AND next_attempt_at <= ?", "pending", now).
			Where("NOT EXISTS (SELECT 1 FROM alert_deliveries older WHERE older.incident_id = alert_deliveries.incident_id AND older.destination_id = alert_deliveries.destination_id AND older.id < alert_deliveries.id AND older.status IN ('pending','processing'))").Order("id").Limit(limit)
		if isPostgresDialect(tx) {
			query = query.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"})
		}
		if err := query.Find(&messages).Error; err != nil {
			return err
		}
		for i := range messages {
			message := &messages[i]
			if err := tx.Model(message).Updates(map[string]any{"status": "processing", "claim_token": token, "attempts": gorm.Expr("attempts + 1"), "last_attempt_at": now}).Error; err != nil {
				return err
			}
			message.Status, message.ClaimToken, message.LastAttemptAt = "processing", token, &now
			// Updates assigns expressions to model fields on some dialects. Reload
			// the stored attempt count rather than relying on that side effect.
			if err := tx.Select("attempts").First(message, message.ID).Error; err != nil {
				return err
			}
		}
		return nil
	})
	return messages, err
}

func GetAlertDeliveryDestination(ctx context.Context, message domain.AlertDelivery) (domain.AlertDestination, error) {
	var destination domain.AlertDestination
	// Recheck the fenced claim and destination immediately before sending.
	err := DB.WithContext(ctx).Where("workspace_id = ? AND id = ? AND enabled = ?", message.WorkspaceID, message.DestinationID, true).
		Where("EXISTS (SELECT 1 FROM alert_deliveries d WHERE d.id = ? AND d.workspace_id = ? AND d.status = 'processing' AND d.claim_token = ?)", message.ID, message.WorkspaceID, message.ClaimToken).First(&destination).Error
	return destination, err
}

func FinishAlertDelivery(ctx context.Context, message domain.AlertDelivery, status, lastError string, next time.Time, now time.Time) error {
	updates := map[string]any{"status": status, "last_error": lastError, "next_attempt_at": next, "claim_token": ""}
	if status == "sent" {
		updates["sent_at"] = now
	}
	return DB.WithContext(ctx).Model(&domain.AlertDelivery{}).Where("id = ? AND workspace_id = ? AND status = 'processing' AND claim_token = ?", message.ID, message.WorkspaceID, message.ClaimToken).Updates(updates).Error
}

func RetryAlertDelivery(ctx context.Context, workspaceID uint, id uint64) error {
	return DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var workspace domain.Workspace
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Select("id").First(&workspace, workspaceID).Error; err != nil {
			return err
		}
		result := tx.Model(&domain.AlertDelivery{}).Where("id = ? AND workspace_id = ? AND status = 'failed'", id, workspaceID).
			Where("EXISTS (SELECT 1 FROM alert_destinations a WHERE a.id = alert_deliveries.destination_id AND a.workspace_id = ? AND a.enabled = TRUE AND a.deleted_at IS NULL)", workspaceID).
			Where("NOT EXISTS (SELECT 1 FROM alert_deliveries newer WHERE newer.incident_id = alert_deliveries.incident_id AND newer.destination_id = alert_deliveries.destination_id AND newer.id > alert_deliveries.id)").
			Where("EXISTS (SELECT 1 FROM alert_incidents i WHERE i.id = alert_deliveries.incident_id AND i.workspace_id = ? AND i.close_reason <> 'configuration_changed')", workspaceID).
			Where("EXISTS (SELECT 1 FROM alert_incidents i JOIN alert_rules r ON r.id = i.rule_id AND r.workspace_id = i.workspace_id AND r.revision = i.rule_revision WHERE i.id = alert_deliveries.incident_id AND r.deleted_at IS NULL)").
			Updates(map[string]any{"status": "pending", "attempts": 0, "last_error": "", "next_attempt_at": time.Now().UTC(), "claim_token": ""})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return alertInvalid("only the latest failed delivery with an enabled destination and unchanged rule can be retried")
		}
		return nil
	})
}

func CleanupAlertHistory(ctx context.Context, now time.Time) error {
	return DB.WithContext(ctx).Where("id IN (SELECT id FROM alert_incidents WHERE closed_at < ? ORDER BY id LIMIT 1000)", now.Add(-90*24*time.Hour)).Delete(&domain.AlertIncident{}).Error
}
