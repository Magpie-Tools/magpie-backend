package database

import (
	"gorm.io/gorm"

	"context"
	"encoding/json"
	"testing"
	"time"

	"magpie/internal/alerts"
	"magpie/internal/api/dto"
	"magpie/internal/domain"
	testdb "magpie/internal/testkit/db"
)

// These tests cross the alerts and core boundary (orphan cleanup and rotator
// deletion), so they stay white-box in the core while it exists.

func alertMust(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func alertFloat(value float64) *float64 { return &value }
func alertString(value string) *string  { return &value }

func setupAlertsTest(t *testing.T) (*gorm.DB, domain.Workspace, domain.AlertRule, time.Time) {
	t.Helper()
	db := setupRotatingProxyTestDB(t)
	workspace := domain.Workspace{Name: "Production", CheckerGeneration: 1, CheckerHTTPKey: "current-http", CheckerHTTPSKey: "current-https"}
	alertMust(t, db.Create(&workspace).Error)
	destination, err := alerts.SaveAlertDestination(context.Background(), workspace.ID, 0, dto.AlertDestinationWrite{Name: "Operations", Kind: "email", Enabled: true, Target: alertString("ops@example.com")})
	alertMust(t, err)
	rule, err := alerts.SaveAlertRule(context.Background(), workspace.ID, 0, dto.AlertRuleWrite{Name: "Check failures", Metric: domain.AlertMetricSuccessRate, Threshold: alertFloat(80), Enabled: true, DestinationIDs: []uint64{destination.ID}})
	alertMust(t, err)
	return db, workspace, rule, time.Now().UTC().Truncate(time.Second)
}

func TestAlertsRollingMetricsKeepDeletedFailuresAndRejectUnattributedHistory(t *testing.T) {
	db, workspace, rule, now := setupAlertsTest(t)
	proxy := testdb.SeedAlertCheckRoute(t, db)
	// No current managed association: these are failures from a deleted route.
	testdb.SeedAlertChecks(t, db, proxy, workspace.ID, "previous-settings", false, 20, now.Add(-time.Minute))
	testdb.SeedAlertChecks(t, db, proxy, workspace.ID+1, "other-workspace", true, 20, now.Add(-time.Minute))
	testdb.SeedAlertChecks(t, db, proxy, workspace.ID, "current-http", true, 20, now.Add(-16*time.Minute))
	testdb.SeedAlertChecks(t, db, proxy, workspace.ID, "current-http", true, 20, now.Add(time.Hour))
	testdb.SeedAlertChecks(t, db, proxy, workspace.ID, "", true, 20, now.Add(-time.Minute))
	for i := 0; i <= 2; i++ {
		alertMust(t, alerts.EvaluateAlerts(context.Background(), now.Add(time.Duration(i)*time.Minute)))
	}
	var stored domain.AlertRule
	alertMust(t, db.First(&stored, rule.ID).Error)
	if stored.LastValue == nil || *stored.LastValue != 0 || stored.SampleCount != 20 || stored.ActiveIncidentID == nil {
		t.Fatalf("wrong attributed rolling metric: %#v", stored)
	}
	removed, err := DeleteOrphanProxies(context.Background())
	alertMust(t, err)
	if removed != 0 {
		t.Fatal("orphan cleanup erased recent failure history")
	}
	alertMust(t, db.Model(&domain.ProxyStatistic{}).Where("proxy_id = ?", proxy.ID).Update("created_at", now.Add(-16*time.Minute)).Error)
	removed, err = DeleteOrphanProxies(context.Background())
	alertMust(t, err)
	if removed != 1 {
		t.Fatal("old orphan was not reclaimed")
	}
}

func TestAlertsRotatorAvailabilityUsesCurrentWorkspaceTCPEvidence(t *testing.T) {
	db, workspace, workspaceRule, now := setupAlertsTest(t)
	proxy := testdb.SeedAlertCheckRoute(t, db)
	alertMust(t, db.Create(&domain.ManagedProxy{WorkspaceID: workspace.ID, ProxyID: proxy.ID, State: domain.ManagedProxyStateActive}).Error)
	rotator := domain.RotatingProxy{WorkspaceID: workspace.ID, Name: "HTTP pool", ProtocolID: 1}
	alertMust(t, db.Create(&rotator).Error)
	rule, err := alerts.SaveAlertRule(context.Background(), workspace.ID, 0, dto.AlertRuleWrite{Name: "Empty pool", RotatorID: &rotator.ID, Metric: domain.AlertMetricUsableRoutes, Threshold: alertFloat(1), Enabled: true, DestinationIDs: workspaceRule.DestinationIDs})
	alertMust(t, err)
	alertMust(t, db.Create(&domain.ProxyLatestStatistic{WorkspaceID: workspace.ID, ConfigKey: "obsolete", ProxyID: proxy.ID, ProtocolID: 1, Alive: true, TransportProtocol: "tcp", CheckedAt: now}).Error)
	alertMust(t, db.Create(&domain.ProxyLatestStatistic{WorkspaceID: workspace.ID, ConfigKey: "current-https", ProxyID: proxy.ID, ProtocolID: 2, Alive: true, TransportProtocol: "tcp", CheckedAt: now}).Error)
	alertMust(t, alerts.EvaluateAlerts(context.Background(), now))
	var stored domain.AlertRule
	alertMust(t, db.First(&stored, rule.ID).Error)
	if stored.LastValue != nil || stored.Status != "unknown" {
		t.Fatal("unrelated HTTPS/obsolete evidence qualified HTTP pool")
	}
	alertMust(t, db.Create(&domain.ProxyLatestStatistic{WorkspaceID: workspace.ID, ConfigKey: "current-http", ProxyID: proxy.ID, ProtocolID: 1, Alive: false, TransportProtocol: "tcp", CheckedAt: now}).Error)
	for i := 1; i <= 3; i++ {
		alertMust(t, alerts.EvaluateAlerts(context.Background(), now.Add(time.Duration(i)*time.Minute)))
	}
	alertMust(t, db.First(&stored, rule.ID).Error)
	if stored.LastValue == nil || *stored.LastValue != 0 || stored.ActiveIncidentID == nil {
		t.Fatal("failed current TCP evidence should open an empty-pool incident")
	}
	incidentID := *stored.ActiveIncidentID
	var message domain.AlertDelivery
	alertMust(t, db.Where("incident_id = ?", incidentID).First(&message).Error)
	var event dto.AlertEvent
	alertMust(t, json.Unmarshal([]byte(message.Payload), &event))
	if event.MeasurementScope != "current_routing_eligibility" || event.Protocol != "http" || event.RotatorID == nil || *event.RotatorID != rotator.ID {
		t.Fatalf("notification has the wrong routing scope: %#v", event)
	}
	alertMust(t, DeleteRotatingProxy(workspace.ID, rotator.ID))
	alertMust(t, db.First(&stored, rule.ID).Error)
	if stored.Enabled || stored.ActiveIncidentID != nil {
		t.Fatal("deleted rotator still has an enabled/open rule")
	}
	var incident domain.AlertIncident
	alertMust(t, db.First(&incident, incidentID).Error)
	if incident.ClosedAt == nil || incident.CloseReason != "configuration_changed" {
		t.Fatal("rotator deletion sent a false recovery")
	}
}
