package checker

import (
	"context"
	"testing"
	"time"

	"magpie/internal/domain"
)

// A saturated streak is not rewritten, yet still triggers the failure action
// without a follow-up read.
func TestSaturatedFailureStreakIsNotRewritten(t *testing.T) {
	r := startTestRuntime(t)
	db := setupCheckerTestDB(t)
	workspaces := []domain.Workspace{{Name: "observe"}, {Name: "pause", AutoRemoveFailingProxies: true, AutoRemoveFailureThreshold: 3}}
	if err := db.Create(&workspaces).Error; err != nil {
		t.Fatal(err)
	}
	proxy := domain.Proxy{IP: "192.0.2.9", Port: 8080}
	if err := db.Create(&proxy).Error; err != nil {
		t.Fatal(err)
	}
	for _, workspace := range workspaces {
		if err := db.Exec(`INSERT INTO user_proxies (workspace_id,proxy_id,consecutive_failures,state) VALUES (?,?,?,'active')`,
			workspace.ID, proxy.ID, failureStreakSaturation).Error; err != nil {
			t.Fatal(err)
		}
	}
	for _, sql := range []string{
		`CREATE TABLE observed_failure_updates (workspace_id bigint, consecutive_failures bigint)`,
		`CREATE TRIGGER observe_failure_update AFTER UPDATE OF consecutive_failures ON user_proxies BEGIN
INSERT INTO observed_failure_updates VALUES (NEW.workspace_id, NEW.consecutive_failures); END`,
	} {
		if err := db.Exec(sql).Error; err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	removed, _, err := r.processFailureEvents(ctx, proxy.ID, []failureEvent{
		{WorkspaceID: workspaces[0].ID, HasEligibleChecks: true},
		{WorkspaceID: workspaces[1].ID, AutoRemove: true, FailureThreshold: 3, FailureAction: domain.FailureActionPause, HasEligibleChecks: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := removed[workspaces[1].ID]; !ok || len(removed) != 1 {
		t.Fatal("saturated streak skipped its failure action", removed)
	}
	var rewrites int64
	if err := db.Table("observed_failure_updates").Count(&rewrites).Error; err != nil || rewrites != 0 {
		t.Fatal("saturated streak was rewritten", rewrites, err)
	}
	var streaks []uint16
	if err := db.Table("user_proxies").Order("workspace_id").Pluck("consecutive_failures", &streaks).Error; err != nil {
		t.Fatal(err)
	}
	if len(streaks) != 2 || streaks[0] != failureStreakSaturation || streaks[1] != failureStreakSaturation {
		t.Fatal(streaks)
	}
}
