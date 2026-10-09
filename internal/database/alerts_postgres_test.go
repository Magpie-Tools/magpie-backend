package database

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gorm.io/gorm"
	"magpie/internal/api/dto"
	"magpie/internal/domain"
)

func TestAlertsConcurrentCountsAndStateTimeoutPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	a, b, _, _ := seedCheckerEvidenceTest(t, db)
	rules := []domain.AlertRule{createAlertRouteRule(t, a.ID), createAlertRouteRule(t, a.ID), createAlertRouteRule(t, b.ID), createAlertRouteRule(t, b.ID)}
	// Simulate a configuration transaction holding one workspace lock. Its
	// timeout must not cancel other workspaces or duplicate shared count scans.
	locked := db.Begin()
	alertMust(t, locked.Error)
	defer locked.Rollback()
	alertMust(t, locked.Exec("SELECT id FROM workspaces WHERE id = ? FOR UPDATE", a.ID).Error)
	config := defaultAlertEvaluationConfig()
	config.stateTimeout = 100 * time.Millisecond
	var aCounts, bCounts atomic.Int32
	config.observeRoutes = func(_ *gorm.DB, workspace domain.Workspace, _ *alertRotator, _ time.Time) AlertObservation {
		if workspace.ID == a.ID {
			aCounts.Add(1)
		} else {
			bCounts.Add(1)
		}
		return AlertObservation{Value: alertFloat(0)}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	now := time.Now().UTC().Truncate(time.Microsecond)
	if err := evaluateAlerts(ctx, now, config); !errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
		t.Fatalf("workspace lock exhausted the whole pass: %v, parent %v", err, ctx.Err())
	}
	if aCounts.Load() != 1 || bCounts.Load() != 1 {
		t.Fatalf("duplicate count scans: A=%d B=%d", aCounts.Load(), bCounts.Load())
	}
	attempted := map[uint]int{}
	for _, snapshot := range rules {
		var rule domain.AlertRule
		alertMust(t, db.First(&rule, snapshot.ID).Error)
		if rule.LastEvaluationAttemptAt != nil {
			attempted[rule.WorkspaceID]++
		}
		if rule.WorkspaceID == a.ID && rule.LastEvaluatedAt != nil {
			t.Fatal("a blocked state transaction counted as an observation")
		}
		if rule.WorkspaceID == b.ID && (rule.LastEvaluatedAt == nil || rule.LastValue == nil || *rule.LastValue != 0) {
			t.Fatalf("later workspace missed evaluation: %#v", rule)
		}
	}
	if attempted[a.ID] != 1 || attempted[b.ID] != 2 {
		t.Fatalf("workspace scheduling markers = %#v", attempted)
	}
	alertMust(t, locked.Rollback().Error)
	alertMust(t, evaluateAlerts(ctx, now.Add(time.Minute), config))
	if aCounts.Load() != 2 || bCounts.Load() != 2 {
		t.Fatal("counts were cached beyond the current pass")
	}
}

func TestAlertEvaluationSchedulingMigrationPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	workspace, _, _, _ := seedCheckerEvidenceTest(t, db)
	rule := createAlertRouteRule(t, workspace.ID)
	now := time.Now().UTC().Truncate(time.Microsecond)
	alertMust(t, applyAlertObservation(context.Background(), rule, workspace, workspace.Name, AlertObservation{Value: alertFloat(100)}, now))
	alertMust(t, db.Exec("ALTER TABLE alert_rules DROP COLUMN last_evaluation_attempt_at").Error)
	alertMust(t, db.AutoMigrate(&domain.AlertRule{}))
	var stored domain.AlertRule
	alertMust(t, db.First(&stored, rule.ID).Error)
	if stored.LastEvaluationAttemptAt != nil || stored.LastEvaluatedAt == nil || stored.LastValue == nil || *stored.LastValue != 100 {
		t.Fatal("scheduling migration changed existing observations")
	}
	alertMust(t, db.Model(&stored).UpdateColumn("last_evaluation_attempt_at", now).Error)
	page, err := GetAlerts(context.Background(), workspace.ID, 0)
	alertMust(t, err)
	encoded, err := json.Marshal(page)
	alertMust(t, err)
	if strings.Contains(string(encoded), "last_evaluation_attempt_at") {
		t.Fatal("internal scheduling was exposed as a measurement")
	}
}

func TestAlertRotatorNotificationScopePostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	workspace, _, _, _ := seedCheckerEvidenceTest(t, db)
	rotator := domain.RotatingProxy{WorkspaceID: workspace.ID, Name: "HTTP pool", ProtocolID: 1}
	alertMust(t, db.Create(&rotator).Error)
	destination, err := SaveAlertDestination(context.Background(), workspace.ID, 0, dto.AlertDestinationWrite{Name: "Ops", Kind: "email", Enabled: true, Target: alertString("ops@example.com")})
	alertMust(t, err)
	rule, err := SaveAlertRule(context.Background(), workspace.ID, 0, dto.AlertRuleWrite{Name: "Pool check failures", RotatorID: &rotator.ID, Metric: domain.AlertMetricSuccessRate, Threshold: alertFloat(80), Enabled: true, DestinationIDs: []uint64{destination.ID}})
	alertMust(t, err)
	now := time.Now().UTC().Truncate(time.Microsecond)
	for i := 0; i <= 2; i++ {
		alertMust(t, applyAlertObservation(context.Background(), rule, workspace, rotator.Name, AlertObservation{Value: alertFloat(0), Samples: 20}, now.Add(time.Duration(i)*time.Minute)))
	}
	var message domain.AlertDelivery
	alertMust(t, db.First(&message).Error)
	var event dto.AlertEvent
	alertMust(t, json.Unmarshal([]byte(message.Payload), &event))
	if event.MeasurementScope != "workspace_protocol_tcp_checks" || event.Protocol != "http" || event.RotatorID == nil || *event.RotatorID != rotator.ID {
		t.Fatalf("notification has the wrong checker scope: %#v", event)
	}
}

func TestAlertDestinationMentionMigrationPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	workspace, _, _, _ := seedCheckerEvidenceTest(t, db)
	destination, err := SaveAlertDestination(context.Background(), workspace.ID, 0, dto.AlertDestinationWrite{Name: "Discord", Kind: "discord", Enabled: true, Target: alertString("https://discord.com/api/webhooks/123/token")})
	alertMust(t, err)
	alertMust(t, db.Exec("ALTER TABLE alert_destinations DROP COLUMN mention_mode, DROP COLUMN mention_id").Error)
	alertMust(t, db.AutoMigrate(&domain.AlertDestination{}))
	var stored domain.AlertDestination
	alertMust(t, db.First(&stored, destination.ID).Error)
	if stored.MentionMode != "none" || stored.MentionID != "" || stored.TargetEncrypted == "" {
		t.Fatalf("migration changed an existing destination or enabled mentions: %#v", alertDestinationDTO(stored))
	}
}

func TestAlertsPostgresEvidenceClaimsAndRetention(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	a, b, proxy, judge := seedCheckerEvidenceTest(t, db)
	now := time.Now().UTC().Truncate(time.Microsecond)
	rows := make([]domain.ProxyStatistic, 20)
	for i := range rows {
		rows[i] = domain.ProxyStatistic{ProxyID: proxy.ID, ProtocolID: 1, JudgeID: judge.ID, Alive: true, ResponseTime: 120, TransportProtocol: "tcp", CreatedAt: now, CheckEvidence: []domain.WorkspaceCheckEvidence{{WorkspaceID: a.ID, ConfigKey: "a-current", Alive: false}, {WorkspaceID: b.ID, ConfigKey: "b-current", Alive: true}}}
	}
	alertMust(t, db.Create(&rows).Error)
	rule, err := SaveAlertRule(context.Background(), a.ID, 0, dto.AlertRuleWrite{Name: "Failures", Metric: domain.AlertMetricSuccessRate, Threshold: alertFloat(80), Enabled: true})
	alertMust(t, err)
	for i := 0; i < 10; i++ {
		destination, err := SaveAlertDestination(context.Background(), a.ID, 0, dto.AlertDestinationWrite{Name: "Ops", Kind: "email", Enabled: true, Target: alertString("ops@example.com")})
		alertMust(t, err)
		rule.DestinationIDs = append(rule.DestinationIDs, destination.ID)
	}
	alertMust(t, db.Save(&rule).Error)
	for i := 0; i <= 2; i++ {
		alertMust(t, EvaluateAlerts(context.Background(), now.Add(time.Duration(i)*time.Minute)))
	}
	var stored domain.AlertRule
	alertMust(t, db.First(&stored, rule.ID).Error)
	if stored.LastValue == nil || *stored.LastValue != 0 || stored.SampleCount != 20 || stored.ActiveIncidentID == nil {
		t.Fatalf("PostgreSQL workspace verdicts were mixed: %#v", stored)
	}
	var group sync.WaitGroup
	results := make(chan []domain.AlertDelivery, 2)
	failures := make(chan error, 2)
	for i := 0; i < 2; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			messages, err := ClaimAlertDeliveries(context.Background(), 10, now.Add(3*time.Minute))
			results <- messages
			failures <- err
		}()
	}
	group.Wait()
	close(results)
	close(failures)
	for err := range failures {
		alertMust(t, err)
	}
	seen := map[uint64]bool{}
	for batch := range results {
		for _, message := range batch {
			if seen[message.ID] {
				t.Fatal("two workers claimed the same notification")
			}
			seen[message.ID] = true
		}
	}
	if len(seen) != 10 {
		t.Fatalf("claimed %d messages, want 10", len(seen))
	}
	incidentID := *stored.ActiveIncidentID
	alertMust(t, db.Model(&domain.AlertRule{}).Where("id = ?", stored.ID).Update("active_incident_id", nil).Error)
	alertMust(t, db.Model(&domain.AlertIncident{}).Where("id = ?", incidentID).Updates(map[string]any{"closed_at": now.Add(-91 * 24 * time.Hour), "close_reason": "recovered"}).Error)
	alertMust(t, CleanupAlertHistory(context.Background(), now))
	var count int64
	alertMust(t, db.Model(&domain.AlertDelivery{}).Count(&count).Error)
	if count != 0 {
		t.Fatal("PostgreSQL retention did not cascade deliveries")
	}
}
