package database

import (
	"context"
	"testing"

	"magpie/internal/domain"
)

func TestDeleteActiveManagedProxyPreservesSharedRouteAndOtherWorkspaceData(t *testing.T) {
	for _, siblingState := range []string{domain.ManagedProxyStateActive, domain.ManagedProxyStatePaused, domain.ManagedProxyStateArchived} {
		t.Run(siblingState, func(t *testing.T) {
			db := setupProxyTagTestDB(t)
			if err := db.AutoMigrate(&domain.WorkspaceUsagePeriod{}); err != nil {
				t.Fatal(err)
			}
			workspaces := []domain.Workspace{{Name: "Deleted management"}, {Name: "Retained management"}}
			if err := db.Create(&workspaces).Error; err != nil {
				t.Fatal(err)
			}
			proxy := domain.Proxy{IP: "192.0.2.90", Port: 8080}
			if err := db.Create(&proxy).Error; err != nil {
				t.Fatal(err)
			}
			for index, workspace := range workspaces {
				state := domain.ManagedProxyStateActive
				if index == 1 {
					state = siblingState
				}
				if err := db.Create(&domain.ManagedProxy{WorkspaceID: workspace.ID, ProxyID: proxy.ID, State: state, ConsecutiveFailures: 3}).Error; err != nil {
					t.Fatal(err)
				}
				tag := domain.ProxyTag{WorkspaceID: workspace.ID, Name: "Keep scope", Color: "#aabbcc"}
				if err := db.Create(&tag).Error; err != nil {
					t.Fatal(err)
				}
				if err := db.Create(&domain.ProxyTagAssignment{WorkspaceID: workspace.ID, ProxyID: proxy.ID, ProxyTagID: tag.ID}).Error; err != nil {
					t.Fatal(err)
				}
				if err := db.Create(&domain.WorkspaceProxyFilterIndex{WorkspaceID: workspace.ID, ProxyID: proxy.ID, State: state}).Error; err != nil {
					t.Fatal(err)
				}
			}
			deleted, inactive, err := DeleteActiveManagedProxy(workspaces[0].ID, proxy.ID)
			if err != nil || !deleted {
				t.Fatalf("delete = %v, error = %v", deleted, err)
			}
			wantInactive := 1
			if siblingState == domain.ManagedProxyStateActive {
				wantInactive = 0
			}
			if len(inactive) != wantInactive {
				t.Fatalf("inactive routes = %d, want %d", len(inactive), wantInactive)
			}
			for _, model := range []any{&domain.ManagedProxy{}, &domain.ProxyTagAssignment{}, &domain.WorkspaceProxyFilterIndex{}} {
				for index, workspace := range workspaces {
					var count int64
					if err := db.Model(model).Where("workspace_id = ? AND proxy_id = ?", workspace.ID, proxy.ID).Count(&count).Error; err != nil {
						t.Fatal(err)
					}
					if count != int64(index) {
						t.Fatalf("%T workspace %d count = %d, want %d", model, workspace.ID, count, index)
					}
				}
			}
			var retained domain.ManagedProxy
			if err := db.First(&retained, "workspace_id = ? AND proxy_id = ?", workspaces[1].ID, proxy.ID).Error; err != nil {
				t.Fatal(err)
			}
			if retained.State != siblingState || retained.ConsecutiveFailures != 3 {
				t.Fatalf("sibling management changed: %+v", retained)
			}
			var usage domain.WorkspaceUsagePeriod
			if err := db.First(&usage, "workspace_id = ?", workspaces[0].ID).Error; err != nil {
				t.Fatal(err)
			}
			if usage.ActiveRoutes != 0 {
				t.Fatalf("deleted management still consumes %d active routes", usage.ActiveRoutes)
			}
			var count int64
			if err := db.Model(&domain.Proxy{}).Where("id = ?", proxy.ID).Count(&count).Error; err != nil || count != 1 {
				t.Fatalf("shared route count=%d error=%v", count, err)
			}
		})
	}
}

func TestDeleteActiveManagedProxySkipsPausedAndArchived(t *testing.T) {
	for _, state := range []string{domain.ManagedProxyStatePaused, domain.ManagedProxyStateArchived} {
		t.Run(state, func(t *testing.T) {
			db := setupProxyTagTestDB(t)
			workspace := domain.Workspace{Name: "Manual lifecycle change"}
			if err := db.Create(&workspace).Error; err != nil {
				t.Fatal(err)
			}
			proxy := domain.Proxy{IP: "192.0.2.91", Port: 8080}
			if err := db.Create(&proxy).Error; err != nil {
				t.Fatal(err)
			}
			if err := db.Create(&domain.ManagedProxy{WorkspaceID: workspace.ID, ProxyID: proxy.ID, State: state}).Error; err != nil {
				t.Fatal(err)
			}
			deleted, inactive, err := DeleteActiveManagedProxy(workspace.ID, proxy.ID)
			if err != nil || deleted || len(inactive) != 0 {
				t.Fatalf("inactive proxy deleted: deleted=%v inactive=%v error=%v", deleted, inactive, err)
			}
			var managed domain.ManagedProxy
			if err := db.First(&managed, "workspace_id = ? AND proxy_id = ?", workspace.ID, proxy.ID).Error; err != nil {
				t.Fatal(err)
			}
			if managed.State != state {
				t.Fatalf("state changed to %q", managed.State)
			}
		})
	}
}

func TestStartupFailureCleanupLeavesDeleteModeForNextFailedCheck(t *testing.T) {
	db := setupProxyTagTestDB(t)
	// SQLite cannot execute the PostgreSQL filter-index refresh statement.
	if err := db.Migrator().DropTable(&domain.WorkspaceProxyFilterIndex{}); err != nil {
		t.Fatal(err)
	}
	workspaces := []domain.Workspace{
		{Name: "Pause", AutoRemoveFailingProxies: true, AutoRemoveFailureThreshold: 2},
		{Name: "Delete", AutoRemoveFailingProxies: true, AutoRemoveFailureThreshold: 2, FailureAction: domain.FailureActionDelete},
	}
	if err := db.Create(&workspaces).Error; err != nil {
		t.Fatal(err)
	}
	proxy := domain.Proxy{IP: "192.0.2.92", Port: 8080}
	if err := db.Create(&proxy).Error; err != nil {
		t.Fatal(err)
	}
	for _, workspace := range workspaces {
		if err := db.Create(&domain.ManagedProxy{WorkspaceID: workspace.ID, ProxyID: proxy.ID, ConsecutiveFailures: 2}).Error; err != nil {
			t.Fatal(err)
		}
	}
	paused, inactive, refreshed, err := CleanupAutoRemovalViolations(context.Background())
	if err != nil || paused != 1 || len(inactive) != 0 || len(refreshed) != 1 {
		t.Fatalf("startup cleanup = %d inactive=%v refreshed=%v error=%v", paused, inactive, refreshed, err)
	}
	for index, workspace := range workspaces {
		var managed domain.ManagedProxy
		if err := db.First(&managed, "workspace_id = ? AND proxy_id = ?", workspace.ID, proxy.ID).Error; err != nil {
			t.Fatal(err)
		}
		wantState := domain.ManagedProxyStatePaused
		if index == 1 {
			wantState = domain.ManagedProxyStateActive
		}
		if managed.State != wantState || managed.ConsecutiveFailures != 2 {
			t.Fatalf("workspace %d state=%q failures=%d", workspace.ID, managed.State, managed.ConsecutiveFailures)
		}
	}
	if len(refreshed[0].Workspaces) != 1 || refreshed[0].Workspaces[0].FailureAction != domain.FailureActionDelete {
		t.Fatal("checker hydration lost the remaining workspace's failure action")
	}
}
