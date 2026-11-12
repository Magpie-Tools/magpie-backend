package alerts

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"
	"magpie/internal/api/dto"
	"magpie/internal/domain"
	testdb "magpie/internal/testkit/db"
)

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
	db := setupAlertsSQLite(t)
	workspace := domain.Workspace{Name: "Production", CheckerGeneration: 1, CheckerHTTPKey: "current-http", CheckerHTTPSKey: "current-https"}
	alertMust(t, db.Create(&workspace).Error)
	destination, err := SaveAlertDestination(context.Background(), workspace.ID, 0, dto.AlertDestinationWrite{Name: "Operations", Kind: "email", Enabled: true, Target: alertString("ops@example.com")})
	alertMust(t, err)
	rule, err := SaveAlertRule(context.Background(), workspace.ID, 0, dto.AlertRuleWrite{Name: "Check failures", Metric: domain.AlertMetricSuccessRate, Threshold: alertFloat(80), Enabled: true, DestinationIDs: []uint64{destination.ID}})
	alertMust(t, err)
	return db, workspace, rule, time.Now().UTC().Truncate(time.Second)
}

func TestAlertsSustainUnknownRecoveryAndDeduplicatedDelivery(t *testing.T) {
	db, workspace, rule, now := setupAlertsTest(t)
	observe := func(minute int, value *float64) {
		t.Helper()
		alertMust(t, applyAlertObservation(context.Background(), rule, workspace, workspace.Name, AlertObservation{Value: value, Samples: 20}, now.Add(time.Duration(minute)*time.Minute)))
	}
	observe(0, alertFloat(50))
	observe(1, alertFloat(50))
	observe(2, nil)
	var count int64
	alertMust(t, db.Model(&domain.AlertIncident{}).Count(&count).Error)
	if count != 0 {
		t.Fatal("unknown must interrupt a pending breach")
	}
	observe(3, alertFloat(50))
	observe(4, alertFloat(50))
	observe(5, alertFloat(50))
	observe(6, alertFloat(50))
	alertMust(t, db.Model(&domain.AlertIncident{}).Count(&count).Error)
	if count != 1 {
		t.Fatalf("incidents = %d, want one", count)
	}
	observe(7, nil)
	observe(8, alertFloat(100))
	observe(9, nil)
	observe(10, alertFloat(100))
	observe(11, alertFloat(100))
	var incident domain.AlertIncident
	alertMust(t, db.First(&incident).Error)
	if incident.ClosedAt != nil {
		t.Fatal("unknown or short recovery closed incident")
	}
	observe(12, alertFloat(100))
	observe(13, alertFloat(100))
	alertMust(t, db.First(&incident).Error)
	if incident.CloseReason != "recovered" || incident.ClosedAt == nil {
		t.Fatalf("not recovered: %#v", incident)
	}
	var messages []domain.AlertDelivery
	alertMust(t, db.Order("id").Find(&messages).Error)
	if len(messages) != 2 || messages[0].Event != "opened" || messages[1].Event != "recovered" {
		t.Fatalf("notifications = %#v", messages)
	}
}

func TestAlertsIgnoreStaleEvaluationsAndRestartAfterGap(t *testing.T) {
	db, workspace, rule, now := setupAlertsTest(t)
	alertMust(t, applyAlertObservation(context.Background(), rule, workspace, workspace.Name, AlertObservation{Value: alertFloat(100)}, now.Add(5*time.Minute)))
	alertMust(t, applyAlertObservation(context.Background(), rule, workspace, workspace.Name, AlertObservation{Value: alertFloat(0)}, now))
	var stored domain.AlertRule
	alertMust(t, db.First(&stored, rule.ID).Error)
	if stored.Status != "healthy" || stored.LastValue == nil || *stored.LastValue != 100 {
		t.Fatal("stale evaluation overwrote newer state")
	}
	alertMust(t, applyAlertObservation(context.Background(), rule, workspace, workspace.Name, AlertObservation{Value: alertFloat(0)}, now.Add(6*time.Minute)))
	alertMust(t, applyAlertObservation(context.Background(), rule, workspace, workspace.Name, AlertObservation{Value: alertFloat(0)}, now.Add(10*time.Minute)))
	alertMust(t, db.First(&stored, rule.ID).Error)
	if stored.ActiveIncidentID != nil || !stored.BreachSince.Equal(now.Add(10*time.Minute)) {
		t.Fatal("observation gap incorrectly counted as a sustained breach")
	}
}

func TestAlertsLatencyUsesSuccessfulSamplesAndSuppressesInsufficientData(t *testing.T) {
	db, workspace, _, now := setupAlertsTest(t)
	proxy := testdb.SeedAlertCheckRoute(t, db)
	rule, err := SaveAlertRule(context.Background(), workspace.ID, 0, dto.AlertRuleWrite{Name: "Latency", Metric: domain.AlertMetricLatency, Threshold: alertFloat(100), Enabled: true})
	alertMust(t, err)
	testdb.SeedAlertChecks(t, db, proxy, workspace.ID, "current-http", true, 19, now)
	testdb.SeedAlertChecks(t, db, proxy, workspace.ID, "current-http", false, 40, now)
	alertMust(t, EvaluateAlerts(context.Background(), now))
	var stored domain.AlertRule
	alertMust(t, db.First(&stored, rule.ID).Error)
	if stored.Status != "unknown" || stored.SampleCount != 19 {
		t.Fatal("insufficient successful latency samples did not remain unknown")
	}
	testdb.SeedAlertChecks(t, db, proxy, workspace.ID, "current-http", true, 1, now)
	alertMust(t, EvaluateAlerts(context.Background(), now.Add(time.Minute)))
	alertMust(t, db.First(&stored, rule.ID).Error)
	if stored.LastValue == nil || *stored.LastValue != 120 || stored.SampleCount != 20 {
		t.Fatalf("wrong latency metric: %#v", stored)
	}
	alertMust(t, db.Model(&domain.Workspace{}).Where("id = ?", workspace.ID).Update("checker_dirty", true).Error)
	alertMust(t, EvaluateAlerts(context.Background(), now.Add(2*time.Minute)))
	alertMust(t, db.First(&stored, rule.ID).Error)
	if stored.Status != "unknown" || stored.LastValue != nil {
		t.Fatal("dirty checker projection produced a verdict")
	}
}

func TestAlertsConfigurationClosesWithoutRecoveryAndSecretsStayWriteOnly(t *testing.T) {
	db, workspace, rule, now := setupAlertsTest(t)
	for i := 0; i <= 2; i++ {
		alertMust(t, applyAlertObservation(context.Background(), rule, workspace, workspace.Name, AlertObservation{Value: alertFloat(0)}, now.Add(time.Duration(i)*time.Minute)))
	}
	_, err := SaveAlertRule(context.Background(), workspace.ID, rule.ID, dto.AlertRuleWrite{Name: rule.Name, Metric: rule.Metric, Threshold: alertFloat(50), Enabled: false})
	alertMust(t, err)
	var incident domain.AlertIncident
	alertMust(t, db.First(&incident).Error)
	if incident.ClosedAt == nil || incident.CloseReason != "configuration_changed" {
		t.Fatal("edit did not close incident as configuration changed")
	}
	var messages []domain.AlertDelivery
	alertMust(t, db.Find(&messages).Error)
	if len(messages) != 1 || messages[0].Status != "canceled" {
		t.Fatal("edit sent false recovery or failed to cancel queued opening")
	}
	destination, err := SaveAlertDestination(context.Background(), workspace.ID, 0, dto.AlertDestinationWrite{Name: "Hook", Kind: "webhook", Enabled: true, Target: alertString("https://example.com/secret-url"), SigningSecret: alertString("shared-secret")})
	alertMust(t, err)
	var stored domain.AlertDestination
	alertMust(t, db.First(&stored, destination.ID).Error)
	if strings.Contains(stored.TargetEncrypted, "secret-url") || strings.Contains(stored.SigningSecretEncrypted, "shared-secret") {
		t.Fatal("destination stored in plaintext")
	}
	page, err := GetAlerts(context.Background(), workspace.ID, 0)
	alertMust(t, err)
	encoded, err := json.Marshal(page)
	alertMust(t, err)
	if strings.Contains(string(encoded), "secret-url") || strings.Contains(string(encoded), "shared-secret") || strings.Contains(string(encoded), stored.TargetEncrypted) {
		t.Fatal("API exposed destination secrets")
	}
	other := domain.Workspace{Name: "Other"}
	alertMust(t, db.Create(&other).Error)
	_, err = SaveAlertRule(context.Background(), other.ID, 0, dto.AlertRuleWrite{Name: "Cross tenant", Metric: rule.Metric, Threshold: alertFloat(10), DestinationIDs: []uint64{destination.ID}})
	if !errors.Is(err, ErrAlertValidation) {
		t.Fatal("cross-workspace destination allowed")
	}
	if err := DeleteAlertRule(context.Background(), other.ID, rule.ID); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatal("cross-workspace rule deletion allowed")
	}
}

func TestAlertsDeliveryOrderingFencingRetryAndRetention(t *testing.T) {
	db, workspace, rule, now := setupAlertsTest(t)
	for i := 0; i <= 2; i++ {
		alertMust(t, applyAlertObservation(context.Background(), rule, workspace, workspace.Name, AlertObservation{Value: alertFloat(0)}, now.Add(time.Duration(i)*time.Minute)))
	}
	for i := 3; i <= 5; i++ {
		alertMust(t, applyAlertObservation(context.Background(), rule, workspace, workspace.Name, AlertObservation{Value: alertFloat(100)}, now.Add(time.Duration(i)*time.Minute)))
	}
	first, err := ClaimAlertDeliveries(context.Background(), 10, now.Add(6*time.Minute))
	alertMust(t, err)
	if len(first) != 1 || first[0].Event != "opened" || first[0].Attempts != 1 {
		t.Fatalf("claim = %#v", first)
	}
	second, err := ClaimAlertDeliveries(context.Background(), 10, now.Add(12*time.Minute))
	alertMust(t, err)
	if len(second) != 1 || second[0].ID != first[0].ID || second[0].ClaimToken == first[0].ClaimToken || second[0].Attempts != 2 {
		t.Fatal("stale claim did not recover with a new fence")
	}
	alertMust(t, FinishAlertDelivery(context.Background(), first[0], "sent", "", now, now))
	var stored domain.AlertDelivery
	alertMust(t, db.First(&stored, first[0].ID).Error)
	if stored.Status != "processing" {
		t.Fatal("stale worker overwrote reclaimed delivery")
	}
	alertMust(t, FinishAlertDelivery(context.Background(), second[0], "failed", "Webhook returned HTTP 400", now, now))
	if err := RetryAlertDelivery(context.Background(), workspace.ID, second[0].ID); !errors.Is(err, ErrAlertValidation) {
		t.Fatal("opening can be retried after a recovery event exists")
	}
	recovery, err := ClaimAlertDeliveries(context.Background(), 10, now.Add(13*time.Minute))
	alertMust(t, err)
	if len(recovery) != 1 || recovery[0].Event != "recovered" {
		t.Fatal("recovery was not delivered after terminal opening failure")
	}
	alertMust(t, FinishAlertDelivery(context.Background(), recovery[0], "failed", "SMTP delivery failed", now, now))
	alertMust(t, RetryAlertDelivery(context.Background(), workspace.ID, recovery[0].ID))
	retried, err := ClaimAlertDeliveries(context.Background(), 10, now.Add(14*time.Minute))
	alertMust(t, err)
	if len(retried) != 1 || retried[0].ID != recovery[0].ID {
		t.Fatal("latest recovery for an unchanged rule could not be retried")
	}
	alertMust(t, FinishAlertDelivery(context.Background(), retried[0], "failed", "SMTP delivery failed", now, now))
	_, err = SaveAlertRule(context.Background(), workspace.ID, rule.ID, dto.AlertRuleWrite{Name: rule.Name, Metric: rule.Metric, Threshold: alertFloat(60), Enabled: true, DestinationIDs: rule.DestinationIDs})
	alertMust(t, err)
	if err := RetryAlertDelivery(context.Background(), workspace.ID, recovery[0].ID); !errors.Is(err, ErrAlertValidation) {
		t.Fatal("old recovery can be retried after the rule was edited")
	}
	alertMust(t, CleanupAlertHistory(context.Background(), now.Add(96*24*time.Hour)))
	var count int64
	alertMust(t, db.Model(&domain.AlertDelivery{}).Count(&count).Error)
	if count != 0 {
		t.Fatal("retained closed-incident deliveries did not cascade with history cleanup")
	}
}

func TestAlertsInterruptedDeliveriesStopAfterFourAttempts(t *testing.T) {
	db, workspace, rule, now := setupAlertsTest(t)
	for i := 0; i <= 2; i++ {
		alertMust(t, applyAlertObservation(context.Background(), rule, workspace, workspace.Name, AlertObservation{Value: alertFloat(0)}, now.Add(time.Duration(i)*time.Minute)))
	}
	for attempt := 1; attempt <= AlertMaximumDeliveryAttempts; attempt++ {
		messages, err := ClaimAlertDeliveries(context.Background(), 10, now.Add(time.Duration(3+(attempt-1)*6)*time.Minute))
		alertMust(t, err)
		if len(messages) != 1 || messages[0].Attempts != attempt {
			t.Fatalf("interrupted delivery attempt %d: %#v", attempt, messages)
		}
	}
	messages, err := ClaimAlertDeliveries(context.Background(), 10, now.Add(27*time.Minute))
	alertMust(t, err)
	if len(messages) != 0 {
		t.Fatal("interrupted delivery exceeded the automatic attempt limit")
	}
	var stored domain.AlertDelivery
	alertMust(t, db.First(&stored).Error)
	if stored.Status != "failed" || stored.Attempts != AlertMaximumDeliveryAttempts || stored.LastError != "Delivery attempt interrupted" {
		t.Fatalf("exhausted delivery was not marked failed: %#v", stored)
	}
}

func TestAlertDestinationMentionsPersistRetainClearAndValidate(t *testing.T) {
	_, workspace, _, _ := setupAlertsTest(t)
	input := dto.AlertDestinationWrite{Name: "Discord operations", Kind: "discord", Enabled: true, Target: alertString("https://discord.com/api/webhooks/123/token")}
	destination, err := SaveAlertDestination(context.Background(), workspace.ID, 0, input)
	alertMust(t, err)
	if destination.MentionMode != "none" || destination.MentionID != "" {
		t.Fatal("new destinations must default to no mentions")
	}
	input.Target, input.MentionMode, input.MentionID = nil, alertString("role"), alertString("165511591545143296")
	destination, err = SaveAlertDestination(context.Background(), workspace.ID, destination.ID, input)
	alertMust(t, err)
	if destination.MentionMode != "role" || destination.MentionID != "165511591545143296" {
		t.Fatal("role mention was not saved")
	}
	input.MentionMode, input.MentionID = nil, nil
	destination, err = SaveAlertDestination(context.Background(), workspace.ID, destination.ID, input)
	alertMust(t, err)
	if destination.MentionMode != "role" || destination.MentionID != "165511591545143296" {
		t.Fatal("an update without mention fields lost saved settings")
	}
	input.MentionMode = alertString("none")
	destination, err = SaveAlertDestination(context.Background(), workspace.ID, destination.ID, input)
	alertMust(t, err)
	if destination.MentionMode != "none" || destination.MentionID != "" {
		t.Fatal("turning off mentions retained a stale role ID")
	}
	for _, mode := range []string{"here", "everyone"} {
		input.MentionMode = alertString(mode)
		_, err = SaveAlertDestination(context.Background(), workspace.ID, destination.ID, input)
		alertMust(t, err)
	}
	input.MentionMode, input.MentionID = alertString("role"), alertString("@everyone")
	_, err = SaveAlertDestination(context.Background(), workspace.ID, destination.ID, input)
	if !errors.Is(err, ErrAlertValidation) {
		t.Fatal("invalid role ID accepted")
	}
	input.Kind, input.Target, input.MentionMode, input.MentionID = "slack", alertString("https://hooks.slack.com/services/T/B/secret"), alertString("user_group"), alertString("SAZ94GDB8")
	group, err := SaveAlertDestination(context.Background(), workspace.ID, 0, input)
	alertMust(t, err)
	if group.MentionID != "SAZ94GDB8" {
		t.Fatal("Slack group ID not returned")
	}
	input.Kind, input.Target, input.MentionMode = "email", alertString("ops@example.com"), alertString("everyone")
	_, err = SaveAlertDestination(context.Background(), workspace.ID, 0, input)
	if !errors.Is(err, ErrAlertValidation) {
		t.Fatal("email accepted a chat mention")
	}
}
