package database

import (
	"context"
	"errors"
	"testing"
	"time"

	"gorm.io/gorm"
	"magpie/internal/api/dto"
	"magpie/internal/domain"
)

func createAlertRouteRule(t *testing.T, workspaceID uint) domain.AlertRule {
	t.Helper()
	rule, err := SaveAlertRule(context.Background(), workspaceID, 0, dto.AlertRuleWrite{Name: "Minimum routes", Metric: domain.AlertMetricUsableRoutes, Threshold: alertFloat(1), Enabled: true})
	alertMust(t, err)
	return rule
}

func TestAlertsResumeUnattemptedWorkspacesAfterCanceledPasses(t *testing.T) {
	db := setupRotatingProxyTestDB(t)
	workspaces := make([]domain.Workspace, 3)
	for i := range workspaces {
		workspaces[i] = domain.Workspace{Name: "Workspace"}
		alertMust(t, db.Create(&workspaces[i]).Error)
		createAlertRouteRule(t, workspaces[i].ID)
	}
	// A busy first workspace must not put its remaining rules ahead of other
	// workspaces after every restart. None of these attempts can commit a verdict.
	for i := 0; i < 10; i++ {
		createAlertRouteRule(t, workspaces[0].ID)
	}
	now := time.Now().UTC().Truncate(time.Second)
	for pass := range workspaces {
		ctx, cancel := context.WithCancel(context.Background())
		config := defaultAlertEvaluationConfig()
		config.workers = 1
		var attempted uint
		config.observeRoutes = func(_ *gorm.DB, workspace domain.Workspace, _ *alertRotator, _ time.Time) AlertObservation {
			attempted = workspace.ID
			cancel()
			return AlertObservation{UnknownReason: "Interrupted"}
		}
		err := evaluateAlerts(ctx, now.Add(time.Duration(pass)*time.Minute), config)
		cancel()
		if !errors.Is(err, context.Canceled) || attempted != workspaces[pass].ID {
			t.Fatalf("pass %d restarted at workspace %d: %v", pass, attempted, err)
		}
	}
	var rules []domain.AlertRule
	alertMust(t, db.Find(&rules).Error)
	attempts := map[uint]int{}
	for _, rule := range rules {
		if rule.LastEvaluationAttemptAt != nil {
			attempts[rule.WorkspaceID]++
		}
		if rule.LastEvaluatedAt != nil || rule.BreachSince != nil {
			t.Fatal("an interrupted attempt counted toward incident timing")
		}
	}
	for _, workspace := range workspaces {
		want := 1
		if attempts[workspace.ID] != want {
			t.Fatalf("workspace %d scheduled %d/%d rules", workspace.ID, attempts[workspace.ID], want)
		}
	}
	alertMust(t, EvaluateAlerts(context.Background(), now.Add(3*time.Minute)))
	alertMust(t, db.Find(&rules).Error)
	for _, rule := range rules {
		if rule.LastEvaluatedAt == nil || rule.ActiveIncidentID != nil || rule.BreachSince == nil || !rule.BreachSince.Equal(now.Add(3*time.Minute)) {
			t.Fatalf("resumed evaluation used unobserved time: %#v", rule)
		}
	}
}

func TestAlertsBatchRulesAcrossWorkspaces(t *testing.T) {
	rules := []domain.AlertRule{{ID: 1, WorkspaceID: 1}, {ID: 2, WorkspaceID: 1}, {ID: 3, WorkspaceID: 1}, {ID: 4, WorkspaceID: 2}, {ID: 5, WorkspaceID: 2}, {ID: 6, WorkspaceID: 3}}
	batches := fairAlertWorkspaceBatches(rules)
	want := [][]uint64{{1, 2, 3}, {4, 5}, {6}}
	for i, batch := range batches {
		if batch.workspaceID != uint(i+1) || len(batch.rules) != len(want[i]) {
			t.Fatalf("wrong workspace batch: %#v", batch)
		}
		for j, rule := range batch.rules {
			if rule.ID != want[i][j] {
				t.Fatalf("rule %d:%d = %d, want %d", i, j, rule.ID, want[i][j])
			}
		}
	}
}

func TestAlertBatchFencesEditedDeletedAndReconfiguredRules(t *testing.T) {
	db := setupRotatingProxyTestDB(t)
	workspace := domain.Workspace{Name: "Operations", CheckerGeneration: 1}
	other := domain.Workspace{Name: "Other workspace"}
	alertMust(t, db.Create(&workspace).Error)
	alertMust(t, db.Create(&other).Error)
	rules := []domain.AlertRule{}
	inputs := []alertRuleObservation{}
	for i := 0; i < 5; i++ {
		owner := workspace.ID
		if i == 4 {
			owner = other.ID
		}
		rule := createAlertRouteRule(t, owner)
		rules = append(rules, rule)
		inputs = append(inputs, alertRuleObservation{snapshot: rule, scopeName: workspace.Name, observation: AlertObservation{Value: alertFloat(0)}})
	}
	edited, err := SaveAlertRule(context.Background(), workspace.ID, rules[0].ID, dto.AlertRuleWrite{Name: "Edited", Metric: domain.AlertMetricUsableRoutes, Threshold: alertFloat(2), Enabled: true})
	alertMust(t, err)
	_, err = SaveAlertRule(context.Background(), workspace.ID, rules[2].ID, dto.AlertRuleWrite{Name: "Disabled", Metric: domain.AlertMetricUsableRoutes, Threshold: alertFloat(1), Enabled: false})
	alertMust(t, err)
	alertMust(t, DeleteAlertRule(context.Background(), workspace.ID, rules[3].ID))
	now := time.Now().UTC().Truncate(time.Second)
	for minute := 0; minute <= 2; minute++ {
		alertMust(t, applyAlertObservations(context.Background(), workspace, inputs, now.Add(time.Duration(minute)*time.Minute)))
	}
	var stored []domain.AlertRule
	alertMust(t, db.Find(&stored).Error)
	var incidentID uint64
	for _, rule := range stored {
		if rule.ID == rules[1].ID {
			if rule.ActiveIncidentID == nil {
				t.Fatal("an unrelated edit prevented the current rule from opening")
			}
			incidentID = *rule.ActiveIncidentID
		} else if rule.LastEvaluatedAt != nil || rule.BreachSince != nil || rule.ActiveIncidentID != nil {
			t.Fatalf("stale, disabled or foreign rule was evaluated: %#v", rule)
		}
		if rule.ID == edited.ID && (rule.Name != edited.Name || rule.Threshold != edited.Threshold || rule.Revision != edited.Revision) {
			t.Fatal("old batch overwrote an edited rule")
		}
	}
	// Measurements from an earlier checker generation cannot continue the
	// incident timer or report recovery, even when other rule revisions match.
	alertMust(t, db.Model(&workspace).UpdateColumn("checker_generation", 2).Error)
	for i := range inputs {
		inputs[i].observation = AlertObservation{Value: alertFloat(100), Samples: 50}
	}
	// Retain the generation that was used to take the measurements.
	workspace.CheckerGeneration = 1
	alertMust(t, applyAlertObservations(context.Background(), workspace, inputs, now.Add(3*time.Minute)))
	var rule domain.AlertRule
	alertMust(t, db.First(&rule, rules[1].ID).Error)
	if rule.Status != "unknown" || rule.LastValue != nil || rule.SampleCount != 0 || rule.RecoverySince != nil || rule.BreachSince != nil || rule.ActiveIncidentID == nil || *rule.ActiveIncidentID != incidentID {
		t.Fatalf("checker generation change accepted old measurements: %#v", rule)
	}
	var incident domain.AlertIncident
	alertMust(t, db.First(&incident, incidentID).Error)
	if incident.ClosedAt != nil {
		t.Fatal("unknown batch reported a recovery")
	}
}

func TestAlertsMeasurementTimeoutLeavesTimeForStateAndLaterWorkspaces(t *testing.T) {
	db := setupRotatingProxyTestDB(t)
	a, b := domain.Workspace{Name: "Slow"}, domain.Workspace{Name: "Fast"}
	alertMust(t, db.Create(&a).Error)
	alertMust(t, db.Create(&b).Error)
	slow, fast := createAlertRouteRule(t, a.ID), createAlertRouteRule(t, b.ID)
	config := defaultAlertEvaluationConfig()
	config.workers, config.measurementTimeout = 1, 20*time.Millisecond
	config.observeRoutes = func(tx *gorm.DB, workspace domain.Workspace, rotator *alertRotator, now time.Time) AlertObservation {
		if workspace.ID == a.ID {
			<-tx.Statement.Context.Done()
			return AlertObservation{Value: alertFloat(100)} // A timed-out result cannot be trusted.
		}
		return observeUsableAlertRoutes(tx, workspace, rotator, now)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	now := time.Now().UTC().Truncate(time.Second)
	alertMust(t, evaluateAlerts(ctx, now, config))
	alertMust(t, db.First(&slow, slow.ID).Error)
	alertMust(t, db.First(&fast, fast.ID).Error)
	if ctx.Err() != nil || slow.Status != "unknown" || slow.LastValue != nil || slow.LastEvaluatedAt == nil || slow.BreachSince != nil {
		t.Fatalf("slow query consumed the state budget or produced a verdict: %#v", slow)
	}
	if fast.LastEvaluatedAt == nil || fast.LastValue == nil || *fast.LastValue != 0 || fast.Status != "breaching" {
		t.Fatalf("later workspace missed evaluation: %#v", fast)
	}
}

func TestAlertsAggregateTimeoutStillEvaluatesCountsAndKeepsIncidentOpen(t *testing.T) {
	db, workspace, metric, now := setupAlertsTest(t)
	for minute := -2; minute <= 0; minute++ {
		alertMust(t, applyAlertObservation(context.Background(), metric, workspace, workspace.Name, AlertObservation{Value: alertFloat(0), Samples: 20}, now.Add(time.Duration(minute)*time.Minute)))
	}
	count := createAlertRouteRule(t, workspace.ID)
	config := defaultAlertEvaluationConfig()
	config.workers, config.aggregateTimeout = 1, 20*time.Millisecond
	calls := 0
	config.loadAggregates = func(tx *gorm.DB, _ []uint, _ time.Time) (map[alertMetricKey]alertCheckAggregate, error) {
		calls++
		<-tx.Statement.Context.Done()
		return nil, tx.Statement.Context.Err()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := evaluateAlerts(ctx, now.Add(time.Minute), config); !errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
		t.Fatalf("history timeout exhausted the pass: %v, parent %v", err, ctx.Err())
	}
	alertMust(t, db.First(&metric, metric.ID).Error)
	alertMust(t, db.First(&count, count.ID).Error)
	if calls != 1 || metric.Status != "unknown" || metric.LastValue != nil || metric.ActiveIncidentID == nil || metric.LastEvaluatedAt == nil {
		t.Fatalf("history timeout hid rule state or closed the incident: %#v", metric)
	}
	var incident domain.AlertIncident
	alertMust(t, db.First(&incident, *metric.ActiveIncidentID).Error)
	if incident.ClosedAt != nil || count.LastEvaluatedAt == nil || count.LastValue == nil || *count.LastValue != 0 {
		t.Fatal("history timeout hid counts or reported a false recovery")
	}
}

func TestAlertRotatorMetadataIsWorkspaceScopedWithoutCountsOrCredentials(t *testing.T) {
	db := setupRotatingProxyTestDB(t)
	a, b := domain.Workspace{Name: "A"}, domain.Workspace{Name: "B"}
	alertMust(t, db.Create(&a).Error)
	alertMust(t, db.Create(&b).Error)
	alertMust(t, db.Create(&domain.Protocol{ID: 1, Name: "http"}).Error)
	rotator := domain.RotatingProxy{WorkspaceID: a.ID, Name: "Production", ProtocolID: 1, ListenPort: 9001}
	alertMust(t, db.Create(&rotator).Error)
	alertMust(t, db.Create(&domain.RotatingProxy{WorkspaceID: b.ID, Name: "Other tenant", ProtocolID: 1, ListenPort: 9002}).Error)
	// Neither health tables nor a working credential cipher are needed to select
	// a scope. The full rotating-proxy endpoint would fail on both conditions.
	alertMust(t, db.Exec("DROP TABLE proxy_latest_statistics").Error)
	alertMust(t, db.Model(&domain.RotatingProxy{}).Where("id = ?", rotator.ID).UpdateColumn("auth_password", "invalid-encrypted-credential").Error)
	metadata, err := GetAlertRotators(context.Background(), a.ID)
	alertMust(t, err)
	if len(metadata) != 1 || metadata[0] != (dto.AlertRotator{ID: rotator.ID, Name: rotator.Name, Protocol: "http"}) {
		t.Fatalf("wrong metadata: %#v", metadata)
	}
	empty, err := GetAlertRotators(context.Background(), b.ID+1)
	alertMust(t, err)
	if empty == nil || len(empty) != 0 {
		t.Fatal("empty metadata must encode as an empty array")
	}
}
