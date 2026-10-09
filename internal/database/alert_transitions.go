package database

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"magpie/internal/api/dto"
	"magpie/internal/domain"
)

type alertRuleObservation struct {
	snapshot       domain.AlertRule
	scopeName      string
	observation    AlertObservation
	missingRotator bool
}

type alertRuleState struct {
	ID               uint64     `json:"id"`
	Enabled          bool       `json:"enabled"`
	Revision         uint64     `json:"revision"`
	Status           string     `json:"status"`
	UnknownReason    string     `json:"unknown_reason"`
	LastValue        *float64   `json:"last_value"`
	SampleCount      int64      `json:"sample_count"`
	LastEvaluatedAt  *time.Time `json:"last_evaluated_at"`
	LastAttemptAt    *time.Time `json:"last_evaluation_attempt_at"`
	BreachSince      *time.Time `json:"breach_since"`
	RecoverySince    *time.Time `json:"recovery_since"`
	ActiveIncidentID *uint64    `json:"active_incident_id"`
}

type alertIncidentClosure struct {
	ID    uint64  `json:"id"`
	Value float64 `json:"value"`
}

type alertTransition struct {
	rule     *domain.AlertRule
	incident domain.AlertIncident
	event    string
	value    float64
}

// Scheduling is one atomic update per workspace batch, outside the observation
// transaction. Failed/canceled measurements still advance workspace priority.
func recordAlertEvaluationAttempts(ctx context.Context, snapshots []domain.AlertRule, now time.Time) (map[uint64]bool, error) {
	started := map[uint64]bool{}
	if len(snapshots) == 0 {
		return started, nil
	}
	tx := DB.WithContext(ctx)
	workspaceID := snapshots[0].WorkspaceID
	var ids []struct{ ID uint64 }
	if tx.Dialector.Name() == "postgres" {
		type version struct {
			ID       uint64 `json:"id"`
			Revision uint64 `json:"revision"`
		}
		versions := make([]version, len(snapshots))
		for i, rule := range snapshots {
			versions[i] = version{rule.ID, rule.Revision}
		}
		payload, err := json.Marshal(versions)
		if err != nil {
			return nil, err
		}
		if err := tx.Raw(`WITH candidate AS (
SELECT a.id FROM alert_rules a JOIN jsonb_to_recordset(CAST(? AS jsonb)) s(id bigint, revision bigint)
ON a.id = s.id AND a.revision = s.revision
WHERE a.workspace_id = ? AND a.enabled AND a.deleted_at IS NULL
AND (a.last_evaluation_attempt_at IS NULL OR a.last_evaluation_attempt_at < ?)
ORDER BY a.id LIMIT 1 FOR UPDATE OF a)
UPDATE alert_rules a SET last_evaluation_attempt_at = ? FROM candidate c WHERE a.id = c.id
AND NOT EXISTS (SELECT 1 FROM alert_rules newer WHERE newer.workspace_id = ? AND newer.enabled
AND newer.deleted_at IS NULL AND newer.last_evaluation_attempt_at >= ?) RETURNING a.id`, string(payload), workspaceID, now, now, workspaceID, now).Scan(&ids).Error; err != nil {
			return nil, err
		}
	} else {
		versions := make([]clause.Expression, len(snapshots))
		for i, rule := range snapshots {
			versions[i] = clause.And(clause.Eq{Column: "id", Value: rule.ID}, clause.Eq{Column: "revision", Value: rule.Revision})
		}
		var newer int64
		if err := tx.Model(&domain.AlertRule{}).Where("workspace_id = ? AND enabled = ? AND last_evaluation_attempt_at >= ?", workspaceID, true, now).Count(&newer).Error; err != nil {
			return nil, err
		}
		if newer > 0 {
			return started, nil
		}
		if err := tx.Model(&domain.AlertRule{}).Select("id").Where("workspace_id = ? AND enabled = ?", workspaceID, true).
			Where(clause.Or(versions...)).Order("id").Limit(1).Scan(&ids).Error; err != nil {
			return nil, err
		}
		if len(ids) > 0 {
			if err := tx.Model(&domain.AlertRule{}).Where("id = ?", ids[0].ID).UpdateColumn("last_evaluation_attempt_at", now).Error; err != nil {
				return nil, err
			}
		}
	}
	if len(ids) > 0 {
		for _, rule := range snapshots {
			started[rule.ID] = true
		}
	}
	return started, nil
}

// A batch belongs to one workspace. Configuration takes the same workspace
// lock, so incident transitions, rule state and their outbox writes stay atomic.
func applyAlertObservations(ctx context.Context, workspace domain.Workspace, observations []alertRuleObservation, now time.Time) error {
	if len(observations) == 0 {
		return nil
	}
	return DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var currentWorkspace domain.Workspace
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Select("id", "checker_dirty", "checker_generation").First(&currentWorkspace, workspace.ID).Error; err != nil {
			return err
		}
		byID := make(map[uint64]alertRuleObservation, len(observations))
		ids := make([]uint64, len(observations))
		for i, observation := range observations {
			byID[observation.snapshot.ID] = observation
			ids[i] = observation.snapshot.ID
		}
		var rules []domain.AlertRule
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("workspace_id = ? AND id IN ? AND enabled = ?", workspace.ID, ids, true).Order("id").Find(&rules).Error; err != nil {
			return err
		}
		changed := make([]*domain.AlertRule, 0, len(rules))
		opened := []domain.AlertIncident{}
		openingRules, closingRules := []*domain.AlertRule{}, []*domain.AlertRule{}
		for i := range rules {
			rule := &rules[i]
			input := byID[rule.ID]
			if rule.Revision != input.snapshot.Revision {
				continue
			}
			if rule.LastEvaluationAttemptAt == nil || rule.LastEvaluationAttemptAt.Before(now) {
				rule.LastEvaluationAttemptAt = &now
			}
			if input.missingRotator {
				if err := closeAlertForConfiguration(tx, rule, now); err != nil {
					return err
				}
				rule.Enabled, rule.Status, rule.UnknownReason = false, "disabled", "Rotator deleted"
				rule.ActiveIncidentID, rule.BreachSince, rule.RecoverySince = nil, nil, nil
				rule.Revision++
				changed = append(changed, rule)
				continue
			}
			if rule.LastEvaluatedAt != nil && !now.After(*rule.LastEvaluatedAt) {
				continue
			}
			observation := input.observation
			if currentWorkspace.CheckerDirty || currentWorkspace.CheckerGeneration != workspace.CheckerGeneration {
				observation = AlertObservation{UnknownReason: "Checker settings changed during evaluation"}
			}
			if rule.LastEvaluatedAt != nil && now.Sub(*rule.LastEvaluatedAt) > 90*time.Second {
				rule.BreachSince, rule.RecoverySince = nil, nil
			}
			rule.LastEvaluatedAt, rule.LastValue, rule.SampleCount, rule.UnknownReason = &now, observation.Value, observation.Samples, observation.UnknownReason
			changed = append(changed, rule)
			if observation.Value == nil {
				rule.Status, rule.BreachSince, rule.RecoverySince = "unknown", nil, nil
				continue
			}
			breached := *observation.Value < rule.Threshold
			if rule.Metric == domain.AlertMetricLatency {
				breached = *observation.Value > rule.Threshold
			}
			if breached {
				rule.Status, rule.RecoverySince = "breaching", nil
				if rule.ActiveIncidentID == nil {
					if rule.BreachSince == nil {
						rule.BreachSince = &now
					}
					if now.Sub(*rule.BreachSince) >= AlertSustainDuration {
						opened = append(opened, domain.AlertIncident{WorkspaceID: rule.WorkspaceID, RuleID: rule.ID, RuleRevision: rule.Revision, RuleName: rule.Name, ScopeName: input.scopeName, Metric: rule.Metric, Threshold: rule.Threshold, OpeningValue: *observation.Value, OpenedAt: now})
						openingRules = append(openingRules, rule)
					}
				}
			} else {
				rule.Status, rule.BreachSince = "healthy", nil
				if rule.ActiveIncidentID != nil {
					if rule.RecoverySince == nil {
						rule.RecoverySince = &now
					}
					if now.Sub(*rule.RecoverySince) >= AlertSustainDuration {
						closingRules = append(closingRules, rule)
					}
				} else {
					rule.RecoverySince = nil
				}
			}
		}
		transitions := make([]alertTransition, 0, len(openingRules)+len(closingRules))
		if len(opened) > 0 {
			if err := tx.Omit(clause.Associations).CreateInBatches(&opened, 500).Error; err != nil {
				return err
			}
			for i, incident := range opened {
				rule := openingRules[i]
				rule.ActiveIncidentID, rule.BreachSince = &opened[i].ID, nil
				transitions = append(transitions, alertTransition{rule, incident, "opened", incident.OpeningValue})
			}
		}
		if len(closingRules) > 0 {
			closingIDs := make([]uint64, len(closingRules))
			for i, rule := range closingRules {
				closingIDs[i] = *rule.ActiveIncidentID
			}
			var incidents []domain.AlertIncident
			if err := tx.Where("workspace_id = ? AND id IN ? AND closed_at IS NULL", workspace.ID, closingIDs).Find(&incidents).Error; err != nil {
				return err
			}
			if len(incidents) != len(closingRules) {
				return gorm.ErrRecordNotFound
			}
			byIncident := map[uint64]domain.AlertIncident{}
			for _, incident := range incidents {
				byIncident[incident.ID] = incident
			}
			closures := make([]alertIncidentClosure, len(closingRules))
			for i, rule := range closingRules {
				incident := byIncident[*rule.ActiveIncidentID]
				if incident.RuleID != rule.ID || incident.RuleRevision != rule.Revision {
					return fmt.Errorf("alert incident %d does not match rule revision", incident.ID)
				}
				closures[i] = alertIncidentClosure{incident.ID, *rule.LastValue}
				incident.ClosedAt, incident.ClosingValue, incident.CloseReason = &now, rule.LastValue, "recovered"
				transitions = append(transitions, alertTransition{rule, incident, "recovered", *rule.LastValue})
				rule.ActiveIncidentID, rule.RecoverySince = nil, nil
			}
			if err := saveAlertIncidentClosures(tx, workspace.ID, closures, now); err != nil {
				return err
			}
		}
		if err := queueAlertTransitions(tx, workspace, transitions, now); err != nil {
			return err
		}
		return saveAlertRuleStates(tx, workspace.ID, changed)
	})
}

func saveAlertRuleStates(tx *gorm.DB, workspaceID uint, rules []*domain.AlertRule) error {
	if len(rules) == 0 {
		return nil
	}
	if tx.Dialector.Name() == "postgres" {
		states := make([]alertRuleState, len(rules))
		for i, rule := range rules {
			states[i] = alertRuleState{rule.ID, rule.Enabled, rule.Revision, rule.Status, rule.UnknownReason, rule.LastValue, rule.SampleCount, rule.LastEvaluatedAt, rule.LastEvaluationAttemptAt, rule.BreachSince, rule.RecoverySince, rule.ActiveIncidentID}
		}
		payload, err := json.Marshal(states)
		if err != nil {
			return err
		}
		result := tx.Exec(`UPDATE alert_rules a SET enabled = s.enabled, revision = s.revision, status = s.status,
unknown_reason = s.unknown_reason, last_value = s.last_value, sample_count = s.sample_count,
last_evaluated_at = s.last_evaluated_at, last_evaluation_attempt_at = s.last_evaluation_attempt_at,
breach_since = s.breach_since, recovery_since = s.recovery_since,
active_incident_id = s.active_incident_id, updated_at = ?
FROM jsonb_to_recordset(CAST(? AS jsonb)) s(id bigint, enabled boolean, revision bigint, status text, unknown_reason text,
last_value double precision, sample_count bigint, last_evaluated_at timestamptz, last_evaluation_attempt_at timestamptz, breach_since timestamptz,
recovery_since timestamptz, active_incident_id bigint)
WHERE a.workspace_id = ? AND a.id = s.id`, time.Now().UTC(), string(payload), workspaceID)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != int64(len(rules)) {
			return fmt.Errorf("alert state update affected %d of %d rules", result.RowsAffected, len(rules))
		}
		return nil
	}
	for _, rule := range rules {
		if err := tx.Model(&domain.AlertRule{}).Where("workspace_id = ? AND id = ?", workspaceID, rule.ID).Updates(map[string]any{
			"enabled": rule.Enabled, "revision": rule.Revision, "status": rule.Status, "unknown_reason": rule.UnknownReason,
			"last_value": rule.LastValue, "sample_count": rule.SampleCount, "last_evaluated_at": rule.LastEvaluatedAt,
			"last_evaluation_attempt_at": rule.LastEvaluationAttemptAt,
			"breach_since":               rule.BreachSince, "recovery_since": rule.RecoverySince, "active_incident_id": rule.ActiveIncidentID,
		}).Error; err != nil {
			return err
		}
	}
	return nil
}

func saveAlertIncidentClosures(tx *gorm.DB, workspaceID uint, closures []alertIncidentClosure, now time.Time) error {
	if tx.Dialector.Name() == "postgres" {
		payload, err := json.Marshal(closures)
		if err != nil {
			return err
		}
		return tx.Exec(`UPDATE alert_incidents i SET closed_at = ?, close_reason = 'recovered', closing_value = s.value
FROM jsonb_to_recordset(CAST(? AS jsonb)) s(id bigint, value double precision)
WHERE i.workspace_id = ? AND i.id = s.id`, now, string(payload), workspaceID).Error
	}
	for _, closure := range closures {
		if err := tx.Model(&domain.AlertIncident{}).Where("workspace_id = ? AND id = ?", workspaceID, closure.ID).
			Updates(map[string]any{"closed_at": now, "close_reason": "recovered", "closing_value": closure.Value}).Error; err != nil {
			return err
		}
	}
	return nil
}

func queueAlertTransitions(tx *gorm.DB, workspace domain.Workspace, transitions []alertTransition, now time.Time) error {
	destinationIDs, rotatorIDs := map[uint64]bool{}, map[uint64]bool{}
	for _, transition := range transitions {
		for _, id := range transition.rule.DestinationIDs {
			destinationIDs[id] = true
		}
		if transition.rule.RotatorID != nil && len(transition.rule.DestinationIDs) > 0 {
			rotatorIDs[*transition.rule.RotatorID] = true
		}
	}
	if len(destinationIDs) == 0 {
		return nil
	}
	ids := make([]uint64, 0, len(destinationIDs))
	for id := range destinationIDs {
		ids = append(ids, id)
	}
	var destinations []domain.AlertDestination
	if err := tx.Select("id", "name", "kind").Where("workspace_id = ? AND id IN ? AND enabled = ?", workspace.ID, ids, true).Find(&destinations).Error; err != nil {
		return err
	}
	byDestination := map[uint64]domain.AlertDestination{}
	for _, destination := range destinations {
		byDestination[destination.ID] = destination
	}
	protocols := map[uint64]string{}
	if len(rotatorIDs) > 0 {
		ids = ids[:0]
		for id := range rotatorIDs {
			ids = append(ids, id)
		}
		var rows []dto.AlertRotator
		if err := tx.Table("rotating_proxies r").Select("r.id, p.name AS protocol").Joins("JOIN protocols p ON p.id = r.protocol_id").Where("r.workspace_id = ? AND r.id IN ?", workspace.ID, ids).Scan(&rows).Error; err != nil {
			return err
		}
		for _, row := range rows {
			protocols[row.ID] = row.Protocol
		}
	}
	messages := []domain.AlertDelivery{}
	for _, transition := range transitions {
		rule, incident := transition.rule, transition.incident
		measurementScope, protocol := "workspace_checks", ""
		if rule.Metric == domain.AlertMetricUsableRoutes {
			measurementScope = "current_routing_eligibility"
		}
		if rule.RotatorID != nil {
			protocol = protocols[*rule.RotatorID]
			if rule.Metric != domain.AlertMetricUsableRoutes {
				measurementScope = "workspace_protocol_tcp_checks"
			}
		}
		payload, err := json.Marshal(dto.AlertEvent{Version: 1, IncidentID: incident.ID, WorkspaceID: workspace.ID, WorkspaceName: workspace.Name, RuleID: rule.ID, RuleName: rule.Name, ScopeName: incident.ScopeName, RotatorID: rule.RotatorID, MeasurementScope: measurementScope, Protocol: protocol, Metric: rule.Metric, Threshold: rule.Threshold, Value: transition.value, Event: transition.event, OccurredAt: now})
		if err != nil {
			return err
		}
		seen := map[uint64]bool{}
		for _, id := range rule.DestinationIDs {
			if destination, exists := byDestination[id]; exists && !seen[id] {
				seen[id] = true
				messages = append(messages, domain.AlertDelivery{WorkspaceID: workspace.ID, IncidentID: incident.ID, DestinationID: id, DestinationName: destination.Name, Kind: destination.Kind, Event: transition.event, Payload: string(payload), Status: "pending", NextAttemptAt: now})
			}
		}
	}
	if len(messages) == 0 {
		return nil
	}
	return tx.Omit(clause.Associations).CreateInBatches(&messages, 500).Error
}

func applyAlertObservation(ctx context.Context, snapshot domain.AlertRule, workspace domain.Workspace, scopeName string, observation AlertObservation, now time.Time) error {
	return applyAlertObservations(ctx, workspace, []alertRuleObservation{{snapshot: snapshot, scopeName: scopeName, observation: observation}}, now)
}
