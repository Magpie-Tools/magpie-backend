package database

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/mail"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"magpie/internal/api/dto"
	"magpie/internal/domain"
	"magpie/internal/security"
	"magpie/internal/support"
)

var ErrAlertValidation = errors.New("invalid alert configuration")

func alertInvalid(message string) error { return fmt.Errorf("%w: %s", ErrAlertValidation, message) }

type AlertsPage struct {
	Rules        []domain.AlertRule     `json:"rules"`
	Destinations []dto.AlertDestination `json:"destinations"`
	Incidents    []domain.AlertIncident `json:"incidents"`
	Deliveries   []domain.AlertDelivery `json:"deliveries"`
	NextCursor   uint64                 `json:"next_cursor"`
}

func GetAlertRotators(ctx context.Context, workspaceID uint) ([]dto.AlertRotator, error) {
	rotators := []dto.AlertRotator{}
	err := DB.WithContext(ctx).Table("rotating_proxies r").
		Select("r.id, r.name, p.name AS protocol").Joins("JOIN protocols p ON p.id = r.protocol_id").
		Where("r.workspace_id = ?", workspaceID).Order("r.id").Scan(&rotators).Error
	return rotators, err
}

func GetAlerts(ctx context.Context, workspaceID uint, before uint64) (AlertsPage, error) {
	page := AlertsPage{Rules: []domain.AlertRule{}, Destinations: []dto.AlertDestination{}, Incidents: []domain.AlertIncident{}, Deliveries: []domain.AlertDelivery{}}
	tx := DB.WithContext(ctx)
	if err := tx.Where("workspace_id = ?", workspaceID).Order("id DESC").Find(&page.Rules).Error; err != nil {
		return page, err
	}
	var destinations []domain.AlertDestination
	if err := tx.Where("workspace_id = ?", workspaceID).Order("id DESC").Find(&destinations).Error; err != nil {
		return page, err
	}
	for _, d := range destinations {
		page.Destinations = append(page.Destinations, alertDestinationDTO(d))
	}
	query := tx.Where("workspace_id = ?", workspaceID)
	if before > 0 {
		query = query.Where("id < ?", before)
	}
	if err := query.Order("id DESC").Limit(50).Find(&page.Incidents).Error; err != nil {
		return page, err
	}
	if len(page.Incidents) == 50 {
		page.NextCursor = page.Incidents[49].ID
	}
	ids := make([]uint64, 0, len(page.Incidents))
	for _, incident := range page.Incidents {
		ids = append(ids, incident.ID)
	}
	if len(ids) > 0 {
		if err := tx.Where("workspace_id = ? AND incident_id IN ?", workspaceID, ids).Order("id DESC").Find(&page.Deliveries).Error; err != nil {
			return page, err
		}
	}
	return page, nil
}

func alertDestinationDTO(d domain.AlertDestination) dto.AlertDestination {
	mode := d.MentionMode
	if mode == "" {
		mode = "none"
	}
	return dto.AlertDestination{ID: d.ID, Name: d.Name, Kind: d.Kind, Enabled: d.Enabled, TargetConfigured: d.TargetEncrypted != "", SigningConfigured: d.SigningSecretEncrypted != "", MentionMode: mode, MentionID: d.MentionID}
}

func SaveAlertRule(ctx context.Context, workspaceID uint, id uint64, input dto.AlertRuleWrite) (domain.AlertRule, error) {
	var rule domain.AlertRule
	name := strings.TrimSpace(input.Name)
	if name == "" || len(name) > 120 || strings.ContainsAny(name, "\r\n") {
		return rule, alertInvalid("name is required and must be at most 120 bytes")
	}
	if input.Threshold == nil || math.IsNaN(*input.Threshold) || math.IsInf(*input.Threshold, 0) {
		return rule, alertInvalid("a finite threshold is required")
	}
	threshold := *input.Threshold
	switch input.Metric {
	case domain.AlertMetricUsableRoutes:
		if threshold < 1 || threshold > 1e9 || math.Trunc(threshold) != threshold {
			return rule, alertInvalid("usable-route threshold must be an integer between 1 and 1000000000")
		}
	case domain.AlertMetricSuccessRate:
		if threshold < 0 || threshold > 100 {
			return rule, alertInvalid("success-rate threshold must be between 0 and 100")
		}
	case domain.AlertMetricLatency:
		if threshold <= 0 || threshold > 65535 {
			return rule, alertInvalid("latency threshold must be between 0 and 65535 milliseconds")
		}
	default:
		return rule, alertInvalid("unknown metric")
	}
	if len(input.DestinationIDs) > 10 {
		return rule, alertInvalid("at most ten destinations may be attached")
	}
	err := DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Serialize configuration and count limits with workspace/rotator deletion.
		var workspace domain.Workspace
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Select("id").First(&workspace, workspaceID).Error; err != nil {
			return err
		}
		if input.RotatorID != nil {
			var count int64
			if err := tx.Model(&domain.RotatingProxy{}).Where("workspace_id = ? AND id = ?", workspaceID, *input.RotatorID).Count(&count).Error; err != nil {
				return err
			}
			if count != 1 {
				return alertInvalid("rotator must belong to this workspace")
			}
		}
		ids := make([]uint64, 0, len(input.DestinationIDs))
		seen := map[uint64]bool{}
		for _, destinationID := range input.DestinationIDs {
			if seen[destinationID] {
				continue
			}
			seen[destinationID] = true
			var count int64
			if err := tx.Model(&domain.AlertDestination{}).Where("workspace_id = ? AND id = ?", workspaceID, destinationID).Count(&count).Error; err != nil {
				return err
			}
			if count != 1 {
				return alertInvalid("destination must belong to this workspace")
			}
			ids = append(ids, destinationID)
		}
		if id > 0 {
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("workspace_id = ? AND id = ?", workspaceID, id).First(&rule).Error; err != nil {
				return err
			}
			if err := closeAlertForConfiguration(tx, &rule, time.Now().UTC()); err != nil {
				return err
			}
			rule.Revision++
		} else {
			var count int64
			if err := tx.Model(&domain.AlertRule{}).Where("workspace_id = ?", workspaceID).Count(&count).Error; err != nil {
				return err
			}
			if count >= 100 {
				return alertInvalid("workspace has reached its limit of 100 alert rules")
			}
			rule.WorkspaceID = workspaceID
			rule.Revision = 1
		}
		rule.Name, rule.RotatorID, rule.Metric, rule.Threshold, rule.Enabled, rule.DestinationIDs = name, input.RotatorID, input.Metric, threshold, input.Enabled, ids
		rule.Status, rule.UnknownReason = "unknown", "Waiting for evaluation"
		if !rule.Enabled {
			rule.Status, rule.UnknownReason = "disabled", ""
		}
		rule.LastValue, rule.LastEvaluatedAt, rule.BreachSince, rule.RecoverySince, rule.ActiveIncidentID = nil, nil, nil, nil, nil
		rule.LastEvaluationAttemptAt = nil
		rule.SampleCount = 0
		return tx.Save(&rule).Error
	})
	return rule, err
}

func closeAlertForConfiguration(tx *gorm.DB, rule *domain.AlertRule, now time.Time) error {
	if rule.ActiveIncidentID != nil {
		if err := tx.Model(&domain.AlertIncident{}).Where("workspace_id = ? AND id = ? AND closed_at IS NULL", rule.WorkspaceID, *rule.ActiveIncidentID).Updates(map[string]any{"closed_at": now, "close_reason": "configuration_changed"}).Error; err != nil {
			return err
		}
	}
	return tx.Model(&domain.AlertDelivery{}).Where("workspace_id = ? AND incident_id IN (SELECT id FROM alert_incidents WHERE workspace_id = ? AND rule_id = ?) AND status IN ?", rule.WorkspaceID, rule.WorkspaceID, rule.ID, []string{"pending", "processing"}).Updates(map[string]any{"status": "canceled", "claim_token": "", "last_error": "Configuration changed"}).Error
}

func DeleteAlertRule(ctx context.Context, workspaceID uint, id uint64) error {
	return DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var workspace domain.Workspace
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Select("id").First(&workspace, workspaceID).Error; err != nil {
			return err
		}
		var rule domain.AlertRule
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("workspace_id = ? AND id = ?", workspaceID, id).First(&rule).Error; err != nil {
			return err
		}
		if err := closeAlertForConfiguration(tx, &rule, time.Now().UTC()); err != nil {
			return err
		}
		return tx.Delete(&rule).Error
	})
}

func SaveAlertDestination(ctx context.Context, workspaceID uint, id uint64, input dto.AlertDestinationWrite) (dto.AlertDestination, error) {
	var destination domain.AlertDestination
	name := strings.TrimSpace(input.Name)
	if name == "" || len(name) > 120 || strings.ContainsAny(name, "\r\n") {
		return dto.AlertDestination{}, alertInvalid("name is required and must be at most 120 bytes")
	}
	if input.Kind != "email" && input.Kind != "slack" && input.Kind != "discord" && input.Kind != "webhook" {
		return dto.AlertDestination{}, alertInvalid("unknown destination kind")
	}
	if input.SigningSecret != nil && (input.Kind != "webhook" || len(*input.SigningSecret) > 4096) {
		return dto.AlertDestination{}, alertInvalid("signing secrets apply only to generic webhooks and must be at most 4096 bytes")
	}
	var encryptedTarget, encryptedSecret string
	if input.Target != nil {
		target := strings.TrimSpace(*input.Target)
		if err := ValidateAlertTarget(input.Kind, target); err != nil {
			return dto.AlertDestination{}, err
		}
		var err error
		encryptedTarget, err = security.EncryptProxySecret(target)
		if err != nil {
			return dto.AlertDestination{}, err
		}
	}
	if input.SigningSecret != nil && *input.SigningSecret != "" {
		var err error
		encryptedSecret, err = security.EncryptProxySecret(*input.SigningSecret)
		if err != nil {
			return dto.AlertDestination{}, err
		}
	}
	err := DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var workspace domain.Workspace
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Select("id").First(&workspace, workspaceID).Error; err != nil {
			return err
		}
		if id > 0 {
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("workspace_id = ? AND id = ?", workspaceID, id).First(&destination).Error; err != nil {
				return err
			}
			if destination.Kind != input.Kind {
				return alertInvalid("destination kind cannot change")
			}
			if err := cancelAlertDestinationDeliveries(tx, workspaceID, id); err != nil {
				return err
			}
		} else {
			if input.Target == nil {
				return alertInvalid("target is required")
			}
			var count int64
			if err := tx.Model(&domain.AlertDestination{}).Where("workspace_id = ?", workspaceID).Count(&count).Error; err != nil {
				return err
			}
			if count >= 20 {
				return alertInvalid("workspace has reached its limit of 20 alert destinations")
			}
			destination.WorkspaceID = workspaceID
		}
		destination.Name, destination.Kind, destination.Enabled = name, input.Kind, input.Enabled
		if input.MentionMode != nil {
			destination.MentionMode = strings.TrimSpace(*input.MentionMode)
		}
		if input.MentionID != nil {
			destination.MentionID = strings.TrimSpace(*input.MentionID)
		}
		if destination.MentionMode == "" {
			destination.MentionMode = "none"
		}
		if destination.MentionMode != "role" && destination.MentionMode != "user_group" {
			destination.MentionID = ""
		}
		if err := support.ValidateAlertMention(destination.Kind, support.AlertMention{Mode: destination.MentionMode, ID: destination.MentionID}); err != nil {
			return alertInvalid(err.Error())
		}
		if input.Target != nil {
			destination.TargetEncrypted = encryptedTarget
		}
		if input.SigningSecret != nil {
			destination.SigningSecretEncrypted = encryptedSecret
		}
		return tx.Save(&destination).Error
	})
	return alertDestinationDTO(destination), err
}

func ValidateAlertTarget(kind, target string) error {
	if len(target) == 0 || len(target) > 4096 || strings.ContainsAny(target, "\r\n") {
		return alertInvalid("target is required and must be at most 4096 bytes")
	}
	if kind == "email" {
		address, err := mail.ParseAddress(target)
		if err != nil || address.Address != target || len(target) > 254 {
			return alertInvalid("enter one email address without a display name")
		}
		return nil
	}
	parsed, err := support.ValidateOutboundHTTPLiteral(target)
	if err != nil {
		return alertInvalid("webhook URL is invalid or blocked by outbound network policy")
	}
	if parsed.Scheme != "https" && !(kind == "webhook" && support.PrivateNetworkEgressAllowed()) {
		return alertInvalid("webhook URL must use HTTPS")
	}
	if parsed.Fragment != "" {
		return alertInvalid("webhook URL cannot contain a fragment")
	}
	host := strings.ToLower(parsed.Hostname())
	if kind == "slack" && ((host != "hooks.slack.com" && host != "hooks.slack-gov.com") || !strings.HasPrefix(parsed.Path, "/services/") || parsed.Port() != "") {
		return alertInvalid("enter a Slack incoming webhook URL")
	}
	if kind == "discord" && (host != "discord.com" || !strings.HasPrefix(parsed.Path, "/api/webhooks/") || parsed.Port() != "") {
		return alertInvalid("enter a Discord webhook URL")
	}
	return nil
}

func cancelAlertDestinationDeliveries(tx *gorm.DB, workspaceID uint, id uint64) error {
	return tx.Model(&domain.AlertDelivery{}).Where("workspace_id = ? AND destination_id = ? AND status IN ?", workspaceID, id, []string{"pending", "processing"}).Updates(map[string]any{"status": "canceled", "claim_token": "", "last_error": "Destination changed"}).Error
}

func DeleteAlertDestination(ctx context.Context, workspaceID uint, id uint64) error {
	return DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var workspace domain.Workspace
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Select("id").First(&workspace, workspaceID).Error; err != nil {
			return err
		}
		var destination domain.AlertDestination
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("workspace_id = ? AND id = ?", workspaceID, id).First(&destination).Error; err != nil {
			return err
		}
		if err := cancelAlertDestinationDeliveries(tx, workspaceID, id); err != nil {
			return err
		}
		return tx.Delete(&destination).Error
	})
}
