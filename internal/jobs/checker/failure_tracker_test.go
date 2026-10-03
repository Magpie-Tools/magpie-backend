package checker

import (
	"context"
	"errors"
	"testing"
	"time"

	"magpie/internal/domain"

	"gorm.io/gorm"
)

func TestFailureTrackerIncrementAndDefaultAutoPause(t *testing.T) {
	db := setupCheckerTestDB(t)

	user := domain.User{
		Email:                      "tracker-auto@example.com",
		Password:                   "password123",
		AutoRemoveFailingProxies:   true,
		AutoRemoveFailureThreshold: 2,
		HTTPSProtocol:              true,
		UseHttpsForSocks:           true,
	}
	if err := db.Create(&user).Error; err != nil {
		t.Fatalf("create user: %v", err)
	}
	createCheckerWorkspace(t, db, user)

	proxy := domain.Proxy{
		IP:            "10.0.0.41",
		Port:          8080,
		Country:       "AA",
		EstimatedType: "residential",
	}
	if err := db.Create(&proxy).Error; err != nil {
		t.Fatalf("create proxy: %v", err)
	}

	link := domain.UserProxy{WorkspaceID: user.ID, ProxyID: proxy.ID}
	if err := db.Create(&link).Error; err != nil {
		t.Fatalf("link proxy: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	event := failureEvent{
		WorkspaceID:       user.ID,
		Success:           false,
		AutoRemove:        true,
		FailureThreshold:  user.AutoRemoveFailureThreshold,
		HasEligibleChecks: true,
	}

	if _, _, err := processFailureEvents(ctx, proxy.ID, []failureEvent{event}); err != nil {
		t.Fatalf("first failure: %v", err)
	}

	var state domain.UserProxy
	if err := db.First(&state, "workspace_id = ? AND proxy_id = ?", user.ID, proxy.ID).Error; err != nil {
		t.Fatalf("load user proxy state: %v", err)
	}
	if state.ConsecutiveFailures != 1 {
		t.Fatalf("consecutive failures = %d, want 1", state.ConsecutiveFailures)
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()

	removed, orphaned, err := processFailureEvents(ctx2, proxy.ID, []failureEvent{event})
	if err != nil {
		t.Fatalf("second failure: %v", err)
	}
	if removed == nil {
		t.Fatalf("expected removed map to be populated")
	}
	if _, ok := removed[user.ID]; !ok {
		t.Fatalf("user %d not reported as removed", user.ID)
	}
	if len(orphaned) != 1 {
		t.Fatalf("orphaned proxies = %d, want 1", len(orphaned))
	}
	if orphaned[0].ID != proxy.ID {
		t.Fatalf("orphaned proxy id = %d, want %d", orphaned[0].ID, proxy.ID)
	}

	var remaining int64
	if err := db.Model(&domain.ManagedProxy{}).
		Where("workspace_id = ? AND proxy_id = ?", user.ID, proxy.ID).
		Count(&remaining).Error; err != nil {
		t.Fatalf("count managed proxy: %v", err)
	}
	if remaining != 1 {
		t.Fatalf("managed proxy rows remaining = %d, want 1", remaining)
	}
	var managed domain.ManagedProxy
	if err := db.First(&managed, "workspace_id = ? AND proxy_id = ?", user.ID, proxy.ID).Error; err != nil {
		t.Fatalf("load paused managed proxy: %v", err)
	}
	if managed.State != domain.ManagedProxyStatePaused || managed.PauseReason != domain.ManagedProxyPauseReasonFailure {
		t.Fatalf("managed proxy lifecycle = %q/%q, want paused/failure", managed.State, managed.PauseReason)
	}
}

func TestHandleFailureTrackingUsesEachWorkspaceAction(t *testing.T) {
	db := setupCheckerTestDB(t)
	workspaces := []domain.Workspace{
		{Name: "Pause workspace", AutoRemoveFailingProxies: true, AutoRemoveFailureThreshold: 2},
		{Name: "Delete workspace", AutoRemoveFailingProxies: true, AutoRemoveFailureThreshold: 2, FailureAction: domain.FailureActionDelete},
	}
	if err := db.Create(&workspaces).Error; err != nil {
		t.Fatal(err)
	}
	proxy := domain.Proxy{IP: "192.0.2.43", Port: 8080}
	if err := db.Create(&proxy).Error; err != nil {
		t.Fatal(err)
	}
	for _, workspace := range workspaces {
		if err := db.Create(&domain.ManagedProxy{WorkspaceID: workspace.ID, ProxyID: proxy.ID, ConsecutiveFailures: 1}).Error; err != nil {
			t.Fatal(err)
		}
	}
	proxy.Workspaces = workspaces
	removed, inactive := handleFailureTracking(proxy, map[uint]bool{}, map[uint]bool{workspaces[0].ID: true, workspaces[1].ID: true})
	if len(removed) != 2 || len(inactive) != 1 || inactive[0].ID != proxy.ID {
		t.Fatalf("failure actions returned removed=%v inactive=%v", removed, inactive)
	}
	var paused domain.ManagedProxy
	if err := db.First(&paused, "workspace_id = ? AND proxy_id = ?", workspaces[0].ID, proxy.ID).Error; err != nil {
		t.Fatal(err)
	}
	if paused.State != domain.ManagedProxyStatePaused || paused.PauseReason != domain.ManagedProxyPauseReasonFailure {
		t.Fatalf("default action produced %q/%q", paused.State, paused.PauseReason)
	}
	var count int64
	if err := db.Model(&domain.ManagedProxy{}).Where("workspace_id = ? AND proxy_id = ?", workspaces[1].ID, proxy.ID).Count(&count).Error; err != nil || count != 0 {
		t.Fatalf("delete workspace still manages route: count=%d err=%v", count, err)
	}
	if err := db.Model(&domain.Proxy{}).Where("id = ?", proxy.ID).Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("shared route was removed: count=%d err=%v", count, err)
	}
}

func TestDeleteFailureActionRequiresEligibleFailureAndEnabledThreshold(t *testing.T) {
	for _, test := range []struct {
		name         string
		enabled      bool
		eligible     bool
		success      bool
		threshold    uint8
		wantFailures uint16
	}{
		{name: "disabled", eligible: true, threshold: 2, wantFailures: 2},
		{name: "zero threshold", enabled: true, eligible: true, wantFailures: 2},
		{name: "below threshold", enabled: true, eligible: true, threshold: 3, wantFailures: 2},
		{name: "no eligible checks", enabled: true, threshold: 2, wantFailures: 1},
		{name: "success resets streak", enabled: true, eligible: true, success: true, threshold: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := setupCheckerTestDB(t)
			workspace := domain.Workspace{Name: "Delete guards", FailureAction: domain.FailureActionDelete}
			if err := db.Create(&workspace).Error; err != nil {
				t.Fatal(err)
			}
			proxy := domain.Proxy{IP: "192.0.2.44", Port: 8080}
			if err := db.Create(&proxy).Error; err != nil {
				t.Fatal(err)
			}
			if err := db.Create(&domain.ManagedProxy{WorkspaceID: workspace.ID, ProxyID: proxy.ID, ConsecutiveFailures: 1}).Error; err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			removed, inactive, err := processFailureEvents(ctx, proxy.ID, []failureEvent{{WorkspaceID: workspace.ID, AutoRemove: test.enabled, FailureThreshold: test.threshold, FailureAction: domain.FailureActionDelete, HasEligibleChecks: test.eligible, Success: test.success}})
			if err != nil {
				t.Fatal(err)
			}
			if len(removed) != 0 || len(inactive) != 0 {
				t.Fatalf("unexpected lifecycle action: removed=%v inactive=%v", removed, inactive)
			}
			var managed domain.ManagedProxy
			if err := db.First(&managed, "workspace_id = ? AND proxy_id = ?", workspace.ID, proxy.ID).Error; err != nil {
				t.Fatal(err)
			}
			if managed.State != domain.ManagedProxyStateActive || managed.ConsecutiveFailures != test.wantFailures {
				t.Fatalf("managed proxy = %+v, want active with %d failures", managed, test.wantFailures)
			}
		})
	}
}

func TestFailureTrackingRetainsCommittedDeletionsOnActionError(t *testing.T) {
	for _, failure := range []string{"later deletion", "inactive lookup"} {
		t.Run(failure, func(t *testing.T) {
			db := setupCheckerTestDB(t)
			workspaces := []domain.Workspace{
				{Name: "First delete", AutoRemoveFailingProxies: true, AutoRemoveFailureThreshold: 1, FailureAction: domain.FailureActionDelete},
				{Name: "Second delete", AutoRemoveFailingProxies: true, AutoRemoveFailureThreshold: 1, FailureAction: domain.FailureActionDelete},
			}
			if err := db.Create(&workspaces).Error; err != nil {
				t.Fatal(err)
			}
			proxy := domain.Proxy{IP: "192.0.2.45", Port: 8080}
			if err := db.Create(&proxy).Error; err != nil {
				t.Fatal(err)
			}
			for _, workspace := range workspaces {
				if err := db.Create(&domain.ManagedProxy{WorkspaceID: workspace.ID, ProxyID: proxy.ID}).Error; err != nil {
					t.Fatal(err)
				}
			}
			injected := errors.New("test lifecycle failure")
			if failure == "later deletion" {
				deletions := 0
				if err := db.Callback().Delete().Before("gorm:delete").Register("test:fail_second_deletion", func(tx *gorm.DB) {
					if tx.Statement.Table == "user_proxies" {
						deletions++
						if deletions == 2 {
							tx.AddError(injected)
						}
					}
				}); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := db.Callback().Query().Before("gorm:query").Register("test:fail_inactive_lookup", func(tx *gorm.DB) {
					if tx.Statement.Table == "proxies" {
						tx.AddError(injected)
					}
				}); err != nil {
					t.Fatal(err)
				}
			}
			proxy.Workspaces = workspaces
			removed, _ := handleFailureTracking(proxy, map[uint]bool{}, map[uint]bool{workspaces[0].ID: true, workspaces[1].ID: true})
			if len(removed) != 1 {
				t.Fatalf("committed removal lost after %s: %v", failure, removed)
			}
			if _, exists := removed[workspaces[0].ID]; !exists {
				t.Fatalf("wrong workspace removed: %v", removed)
			}
			filtered := filterRemovedUsers(proxy, removed)
			if len(filtered.Workspaces) != 1 || filtered.Workspaces[0].ID != workspaces[1].ID {
				t.Fatal("deleted workspace would remain in the queue payload")
			}
			var count int64
			if err := db.Model(&domain.ManagedProxy{}).Where("workspace_id = ? AND proxy_id = ?", workspaces[0].ID, proxy.ID).Count(&count).Error; err != nil || count != 0 {
				t.Fatalf("first deletion did not commit: count=%d error=%v", count, err)
			}
		})
	}
}

func TestFailureTrackerResetsOnSuccess(t *testing.T) {
	db := setupCheckerTestDB(t)

	user := domain.User{
		Email:                      "tracker-reset@example.com",
		Password:                   "password123",
		AutoRemoveFailingProxies:   true,
		AutoRemoveFailureThreshold: 3,
		HTTPSProtocol:              true,
		UseHttpsForSocks:           true,
	}
	if err := db.Create(&user).Error; err != nil {
		t.Fatalf("create user: %v", err)
	}
	createCheckerWorkspace(t, db, user)

	proxy := domain.Proxy{
		IP:            "10.0.0.42",
		Port:          8081,
		Country:       "AA",
		EstimatedType: "residential",
	}
	if err := db.Create(&proxy).Error; err != nil {
		t.Fatalf("create proxy: %v", err)
	}

	link := domain.UserProxy{
		WorkspaceID:         user.ID,
		ProxyID:             proxy.ID,
		ConsecutiveFailures: 3,
	}
	if err := db.Create(&link).Error; err != nil {
		t.Fatalf("create user proxy: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	event := failureEvent{
		WorkspaceID:       user.ID,
		Success:           true,
		AutoRemove:        true,
		FailureThreshold:  user.AutoRemoveFailureThreshold,
		HasEligibleChecks: true,
	}

	if _, _, err := processFailureEvents(ctx, proxy.ID, []failureEvent{event}); err != nil {
		t.Fatalf("reset event: %v", err)
	}

	var state domain.UserProxy
	if err := db.First(&state, "workspace_id = ? AND proxy_id = ?", user.ID, proxy.ID).Error; err != nil {
		t.Fatalf("reload user proxy: %v", err)
	}
	if state.ConsecutiveFailures != 0 {
		t.Fatalf("consecutive failures after reset = %d, want 0", state.ConsecutiveFailures)
	}
}
