package database

import (
	"context"
	"os"
	"testing"
	"time"

	"magpie/internal/domain"
	"magpie/internal/security"
)

// Run explicitly against disposable PostgreSQL with two CPUs:
// MAGPIE_TEST_ALERT_SCALE=1 MAGPIE_TEST_POSTGRES_DSN=... GOMAXPROCS=2
// go test ./internal/database -run TestAlertsScaleCadencePostgres -count=1 -v
// This checks real reads/writes and transitions, not a mocked measurement loop.
// Also set MAGPIE_TEST_ALERT_SCALE_DESTINATIONS=1 to queue one notification
// per incident transition. This test never sends messages over the network.
func TestAlertsScaleCadencePostgres(t *testing.T) {
	if os.Getenv("MAGPIE_TEST_ALERT_SCALE") != "1" {
		t.Skip("set MAGPIE_TEST_ALERT_SCALE=1 for the 200,000-rule cadence check")
	}
	db := setupProxyIngestionPostgresTest(t)
	sqlDB, err := db.DB()
	alertMust(t, err)
	sqlDB.SetMaxOpenConns(32)
	sqlDB.SetMaxIdleConns(32)
	const workspaces, rulesPerWorkspace, total = 2000, 100, 200000
	alertMust(t, db.Exec("INSERT INTO workspaces (name, checker_generation, checker_http_key) SELECT 'Scale ' || n, 1, 'scale-http' FROM generate_series(1, ?) n", workspaces).Error)
	alertMust(t, db.Exec(`INSERT INTO alert_rules (workspace_id, name, metric, threshold, enabled, destination_ids, revision, status, sample_count, created_at, updated_at)
SELECT w.id, 'Minimum routes ' || n, 'usable_routes', 1, TRUE, '[]'::jsonb, 1, 'unknown', 0, NOW(), NOW()
FROM workspaces w CROSS JOIN generate_series(1, ?) n`, rulesPerWorkspace).Error)
	withDestinations := os.Getenv("MAGPIE_TEST_ALERT_SCALE_DESTINATIONS") == "1"
	if withDestinations {
		target, err := security.EncryptProxySecret("scale-alerts@example.test")
		alertMust(t, err)
		alertMust(t, db.Exec(`INSERT INTO alert_destinations (workspace_id, name, kind, enabled, target_encrypted, created_at, updated_at)
SELECT id, 'Scale operations', 'email', TRUE, ?, NOW(), NOW() FROM workspaces`, target).Error)
		alertMust(t, db.Exec(`UPDATE alert_rules r SET destination_ids = jsonb_build_array(d.id)
FROM alert_destinations d WHERE d.workspace_id = r.workspace_id`).Error)
	}
	alertMust(t, db.Exec("ANALYZE workspaces").Error)
	alertMust(t, db.Exec("ANALYZE alert_rules").Error)
	now := time.Now().UTC().Truncate(time.Microsecond)
	for pass := 0; pass < 6; pass++ {
		if pass == 3 {
			// Recover using actual current workspace evidence. Sharing one physical
			// route still requires an independently attributed verdict per workspace.
			alertMust(t, db.Create(&domain.Protocol{ID: 1, Name: "http"}).Error)
			proxy := domain.Proxy{IP: "192.0.2.1", Port: 8080}
			alertMust(t, db.Create(&proxy).Error)
			alertMust(t, db.Exec("INSERT INTO user_proxies (workspace_id, proxy_id, state) SELECT id, ?, 'active' FROM workspaces", proxy.ID).Error)
			alertMust(t, db.Exec(`INSERT INTO proxy_latest_statistics (workspace_id, config_key, proxy_id, protocol_id, statistic_id, alive, transport_protocol, checked_at)
SELECT id, 'scale-http', ?, 1, 0, TRUE, 'tcp', ? FROM workspaces`, proxy.ID, now.Add(3*time.Minute)).Error)
		}
		observedAt := now.Add(time.Duration(pass) * time.Minute)
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		started := time.Now()
		err := EvaluateAlerts(ctx, observedAt)
		elapsed := time.Since(started)
		cancel()
		var evaluated int64
		alertMust(t, db.Model(&domain.AlertRule{}).Where("last_evaluated_at = ?", observedAt).Count(&evaluated).Error)
		t.Logf("pass=%d evaluated=%d/%d duration=%s", pass, evaluated, total, elapsed.Round(time.Millisecond))
		if err != nil || evaluated != total || elapsed >= 45*time.Second {
			t.Fatalf("cadence missed: evaluated %d/%d in %s: %v", evaluated, total, elapsed, err)
		}
		var valid int64
		if pass < 3 {
			alertMust(t, db.Model(&domain.AlertRule{}).Where("status = 'breaching' AND last_value = 0").Count(&valid).Error)
		} else {
			alertMust(t, db.Model(&domain.AlertRule{}).Where("status = 'healthy' AND last_value = 1").Count(&valid).Error)
		}
		if valid != total {
			t.Fatalf("pass %d has only %d correct measurements", pass, valid)
		}
		if pass == 2 || pass == 5 {
			query := db.Model(&domain.AlertIncident{})
			if pass == 2 {
				query = query.Where("closed_at IS NULL AND opened_at = ?", observedAt)
			} else {
				query = query.Where("close_reason = 'recovered' AND closed_at = ? AND closing_value = 1", observedAt)
			}
			var transitioned int64
			alertMust(t, query.Count(&transitioned).Error)
			if transitioned != total {
				t.Fatalf("pass %d transitioned %d/%d incidents", pass, transitioned, total)
			}
			if withDestinations {
				event := "opened"
				if pass == 5 {
					event = "recovered"
				}
				var queued int64
				alertMust(t, db.Model(&domain.AlertDelivery{}).Where("event = ? AND status = 'pending' AND next_attempt_at = ?", event, observedAt).Count(&queued).Error)
				if queued != total {
					t.Fatalf("pass %d queued %d/%d %s notifications", pass, queued, total, event)
				}
				t.Logf("pass=%d queued=%d event=%s", pass, queued, event)
			}
		}
	}
	var incidents, active, deliveries int64
	alertMust(t, db.Model(&domain.AlertIncident{}).Count(&incidents).Error)
	alertMust(t, db.Model(&domain.AlertRule{}).Where("active_incident_id IS NOT NULL OR breach_since IS NOT NULL OR recovery_since IS NOT NULL").Count(&active).Error)
	alertMust(t, db.Model(&domain.AlertDelivery{}).Count(&deliveries).Error)
	wantDeliveries := int64(0)
	if withDestinations {
		wantDeliveries = 2 * total
	}
	if incidents != total || active != 0 || deliveries != wantDeliveries {
		t.Fatalf("final state: incidents=%d active=%d deliveries=%d, want %d/0/%d", incidents, active, deliveries, total, wantDeliveries)
	}
}
