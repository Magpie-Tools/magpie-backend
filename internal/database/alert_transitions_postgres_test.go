package database

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"magpie/internal/api/dto"
	"magpie/internal/domain"
)

type alertSQLCounter struct {
	logger.Interface
	statements atomic.Int64
}

func (c *alertSQLCounter) Trace(ctx context.Context, begin time.Time, sql func() (string, int64), err error) {
	c.statements.Add(1)
	c.Interface.Trace(ctx, begin, sql, err)
}

func TestAlertsBatchedTransitionsPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	workspace := domain.Workspace{Name: "Batch operations", CheckerGeneration: 1}
	other := domain.Workspace{Name: "Other tenant"}
	alertMust(t, db.Create(&workspace).Error)
	alertMust(t, db.Create(&other).Error)
	ids := []uint64{}
	for i := 0; i < 12; i++ {
		owner := workspace.ID
		if i == 11 {
			owner = other.ID
		}
		destination, err := SaveAlertDestination(context.Background(), owner, 0, dto.AlertDestinationWrite{Name: fmt.Sprintf("Ops %d", i), Kind: "email", Enabled: i != 10, Target: alertString("ops@example.test")})
		alertMust(t, err)
		ids = append(ids, destination.ID)
	}
	// A stored duplicate, disabled destination and another tenant's ID must not
	// multiply or leak notification records, even when batching transitions.
	ids = append(ids, ids[0])
	now := time.Now().UTC().Truncate(time.Microsecond)
	last, breach := now.Add(-time.Minute), now.Add(-2*time.Minute)
	rules := make([]domain.AlertRule, 100)
	for i := range rules {
		rules[i] = domain.AlertRule{WorkspaceID: workspace.ID, Name: fmt.Sprintf("Routes %d", i), Metric: domain.AlertMetricUsableRoutes, Threshold: float64(i + 1), Enabled: true, Revision: uint64(i + 1), DestinationIDs: ids, Status: "breaching", UnknownReason: "Previous measurement", LastValue: alertFloat(0), SampleCount: 20, LastEvaluatedAt: &last, BreachSince: &breach}
	}
	alertMust(t, db.Create(&rules).Error)
	inputs := make([]alertRuleObservation, len(rules))
	for i, rule := range rules {
		inputs[i] = alertRuleObservation{snapshot: rule, scopeName: workspace.Name, observation: AlertObservation{Value: alertFloat(0)}}
	}
	counter := &alertSQLCounter{Interface: db.Logger}
	DB = db.Session(&gorm.Session{Logger: counter})
	alertMust(t, applyAlertObservations(context.Background(), workspace, inputs, now))
	if count := counter.statements.Load(); count > 8 {
		t.Fatalf("opening 100 incidents and 1,000 messages used %d statements", count)
	}
	// Equal and older observations must not duplicate transitions or change state.
	alertMust(t, applyAlertObservations(context.Background(), workspace, inputs, now))
	alertMust(t, applyAlertObservations(context.Background(), workspace, inputs, now.Add(-time.Minute)))
	for i := range inputs {
		inputs[i].observation.Value = alertFloat(1000)
	}
	for minute := 1; minute <= 3; minute++ {
		counter.statements.Store(0)
		alertMust(t, applyAlertObservations(context.Background(), workspace, inputs, now.Add(time.Duration(minute)*time.Minute)))
		if minute == 3 && counter.statements.Load() > 9 {
			t.Fatalf("recovering 100 incidents and 1,000 messages used %d statements", counter.statements.Load())
		}
	}
	var stored []domain.AlertRule
	alertMust(t, db.Order("id").Find(&stored).Error)
	for i, rule := range stored {
		if rule.ID != rules[i].ID || rule.Name != rules[i].Name || rule.Revision != rules[i].Revision || rule.Threshold != rules[i].Threshold || !reflect.DeepEqual(rule.DestinationIDs, ids) {
			t.Fatalf("batch changed rule configuration: %#v", rule)
		}
		if rule.Status != "healthy" || rule.LastValue == nil || *rule.LastValue != 1000 || rule.UnknownReason != "" || rule.SampleCount != 0 || rule.ActiveIncidentID != nil || rule.BreachSince != nil || rule.RecoverySince != nil {
			t.Fatalf("batch did not clear finished incident state: %#v", rule)
		}
	}
	var incidents []domain.AlertIncident
	alertMust(t, db.Find(&incidents).Error)
	if len(incidents) != len(rules) {
		t.Fatalf("incidents = %d, want %d", len(incidents), len(rules))
	}
	byIncident := map[uint64]domain.AlertIncident{}
	byRule := map[uint64]domain.AlertRule{}
	for _, rule := range rules {
		byRule[rule.ID] = rule
	}
	for _, incident := range incidents {
		rule := byRule[incident.RuleID]
		if incident.RuleRevision != rule.Revision || incident.RuleName != rule.Name || incident.Threshold != rule.Threshold || incident.ScopeName != workspace.Name || incident.OpeningValue != 0 || incident.ClosingValue == nil || *incident.ClosingValue != 1000 || !incident.OpenedAt.Equal(now) || incident.ClosedAt == nil || !incident.ClosedAt.Equal(now.Add(3*time.Minute)) || incident.CloseReason != "recovered" {
			t.Fatalf("batch paired an incident with the wrong rule: %#v", incident)
		}
		byIncident[incident.ID] = incident
	}
	var messages []domain.AlertDelivery
	alertMust(t, db.Order("id").Find(&messages).Error)
	if len(messages) != 2000 {
		t.Fatalf("notifications = %d, want 2000", len(messages))
	}
	seen := map[string]bool{}
	for _, message := range messages {
		incident := byIncident[message.IncidentID]
		key := fmt.Sprintf("%d/%d/%s", message.IncidentID, message.DestinationID, message.Event)
		if seen[key] || message.WorkspaceID != workspace.ID || message.Status != "pending" || message.DestinationID < ids[0] || message.DestinationID > ids[9] {
			t.Fatalf("duplicate or incorrectly scoped outbox row: %#v", message)
		}
		seen[key] = true
		var event dto.AlertEvent
		alertMust(t, json.Unmarshal([]byte(message.Payload), &event))
		value, at := 0.0, now
		if message.Event == "recovered" {
			value, at = 1000, now.Add(3*time.Minute)
		}
		if event.RuleID != incident.RuleID || event.IncidentID != incident.ID || event.RuleName != incident.RuleName || event.Value != value || !event.OccurredAt.Equal(at) || event.Event != message.Event || event.MeasurementScope != "current_routing_eligibility" || event.WorkspaceID != workspace.ID {
			t.Fatalf("outbox snapshot describes another transition: %#v", event)
		}
	}
}

func TestAlertsBatchRollsBackWhenOutboxWriteFailsPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	workspace := domain.Workspace{Name: "Atomic transitions"}
	alertMust(t, db.Create(&workspace).Error)
	destination, err := SaveAlertDestination(context.Background(), workspace.ID, 0, dto.AlertDestinationWrite{Name: "Ops", Kind: "email", Enabled: true, Target: alertString("ops@example.test")})
	alertMust(t, err)
	now := time.Now().UTC().Truncate(time.Microsecond)
	last, breach := now.Add(-time.Minute), now.Add(-2*time.Minute)
	rules := []domain.AlertRule{
		{WorkspaceID: workspace.ID, Name: "A", Metric: domain.AlertMetricUsableRoutes, Threshold: 1, Enabled: true, DestinationIDs: []uint64{destination.ID}, Status: "breaching", LastEvaluatedAt: &last, BreachSince: &breach},
		{WorkspaceID: workspace.ID, Name: "B", Metric: domain.AlertMetricUsableRoutes, Threshold: 1, Enabled: true, DestinationIDs: []uint64{destination.ID}, Status: "breaching", LastEvaluatedAt: &last, BreachSince: &breach},
	}
	alertMust(t, db.Create(&rules).Error)
	inputs := []alertRuleObservation{}
	for _, rule := range rules {
		inputs = append(inputs, alertRuleObservation{snapshot: rule, scopeName: workspace.Name, observation: AlertObservation{Value: alertFloat(0)}})
	}
	alertMust(t, db.Exec("ALTER TABLE alert_deliveries ADD CONSTRAINT reject_opened_for_test CHECK (event <> 'opened')").Error)
	if err := applyAlertObservations(context.Background(), workspace, inputs, now); err == nil {
		t.Fatal("outbox constraint did not abort the batch")
	}
	for _, snapshot := range rules {
		var rule domain.AlertRule
		alertMust(t, db.First(&rule, snapshot.ID).Error)
		if !rule.LastEvaluatedAt.Equal(last) || !rule.BreachSince.Equal(breach) || rule.ActiveIncidentID != nil || rule.LastEvaluationAttemptAt != nil {
			t.Fatalf("failed batch partially committed rule state: %#v", rule)
		}
	}
	var count int64
	alertMust(t, db.Model(&domain.AlertIncident{}).Count(&count).Error)
	if count != 0 {
		t.Fatal("failed batch left incidents without their outbox records")
	}
	alertMust(t, db.Model(&domain.AlertDelivery{}).Count(&count).Error)
	if count != 0 {
		t.Fatal("failed batch left partial outbox records")
	}
	alertMust(t, db.Exec("ALTER TABLE alert_deliveries DROP CONSTRAINT reject_opened_for_test").Error)
	alertMust(t, applyAlertObservations(context.Background(), workspace, inputs, now))
	alertMust(t, db.Model(&domain.AlertDelivery{}).Count(&count).Error)
	if count != 2 {
		t.Fatalf("retry did not commit both transitions: %d", count)
	}
}
