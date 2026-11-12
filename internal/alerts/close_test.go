package alerts

import (
	"testing"
	"time"

	"magpie/internal/domain"
	testdb "magpie/internal/testkit/db"

	"gorm.io/gorm"
)

// CloseAlertForConfiguration runs inside its caller's transaction: the core
// calls it while deleting a rotator. Its writes are the caller's to commit, so
// a rollback by the caller must remove them.
func TestCloseAlertForConfigurationRollsBackWithItsCaller(t *testing.T) {
	backends := []struct {
		name string
		open func(testing.TB) *gorm.DB
	}{{"sqlite", testdb.OpenSQLite}, {"postgres", testdb.OpenPostgres}}
	for _, backend := range backends {
		t.Run(backend.name, func(t *testing.T) {
			db := backend.open(t)
			user := domain.User{Email: "rollback@example.test", Password: "hash"}
			if err := db.Create(&user).Error; err != nil {
				t.Fatal(err)
			}
			workspace := testdb.CreateWorkspaceForUser(t, db, user)
			opened := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
			rule := domain.AlertRule{WorkspaceID: workspace.ID, Name: "pool", Metric: domain.AlertMetricUsableRoutes, Threshold: 1, Enabled: true, Status: "breached"}
			if err := db.Create(&rule).Error; err != nil {
				t.Fatal(err)
			}
			incident := domain.AlertIncident{WorkspaceID: workspace.ID, RuleID: rule.ID, RuleRevision: 1, RuleName: rule.Name, ScopeName: "workspace", Metric: rule.Metric, OpenedAt: opened}
			if err := db.Create(&incident).Error; err != nil {
				t.Fatal(err)
			}
			rule.ActiveIncidentID = &incident.ID
			if err := db.Model(&rule).Update("active_incident_id", incident.ID).Error; err != nil {
				t.Fatal(err)
			}
			delivery := domain.AlertDelivery{WorkspaceID: workspace.ID, IncidentID: incident.ID, DestinationID: 1, Payload: "{}", Status: "pending", NextAttemptAt: opened}
			if err := db.Create(&delivery).Error; err != nil {
				t.Fatal(err)
			}

			state := func(handle *gorm.DB) (closed bool, status string) {
				t.Helper()
				var stored domain.AlertIncident
				if err := handle.First(&stored, incident.ID).Error; err != nil {
					t.Fatal(err)
				}
				if err := handle.Model(&domain.AlertDelivery{}).Where("id = ?", delivery.ID).Pluck("status", &status).Error; err != nil {
					t.Fatal(err)
				}
				return stored.ClosedAt != nil, status
			}

			tx := db.Begin()
			if tx.Error != nil {
				t.Fatal(tx.Error)
			}
			if err := CloseAlertForConfiguration(tx, &rule, time.Now().UTC()); err != nil {
				_ = tx.Rollback()
				t.Fatal(err)
			}
			if closed, status := state(tx); !closed || status != "canceled" {
				_ = tx.Rollback()
				t.Fatalf("inside the transaction: closed=%v delivery=%q, want closed and canceled", closed, status)
			}
			// A function on its caller's handle must leave the transaction open.
			if err := tx.Rollback().Error; err != nil {
				t.Fatalf("the caller's transaction was already ended: %v", err)
			}
			if closed, status := state(db); closed || status != "pending" {
				t.Fatalf("after the caller's rollback: closed=%v delivery=%q, want open and pending", closed, status)
			}
		})
	}
}
