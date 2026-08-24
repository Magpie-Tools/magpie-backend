package database

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"magpie/internal/domain"

	"gorm.io/gorm"
)

func TestManagedProxyCapacityStoresOverflowAndBlocksActivation(t *testing.T) {
	db := setupRotatingProxyTestDB(t)
	if err := db.AutoMigrate(&domain.WorkspaceMemberPreference{}); err != nil {
		t.Fatalf("migrate workspace preferences: %v", err)
	}

	owner := domain.User{Email: "capacity-owner@example.test", Password: "hash", Role: "user"}
	if err := db.Create(&owner).Error; err != nil {
		t.Fatalf("create owner: %v", err)
	}
	workspace := createTestWorkspaceForUser(t, db, owner)
	if err := db.Model(&domain.WorkspaceSubscription{}).
		Where("workspace_id = ?", workspace.ID).
		Updates(map[string]any{
			"included_active_routes":   2,
			"additional_active_routes": 0,
			"overage_active_routes":    0,
			"overage_mode":             domain.WorkspaceOverageDisabled,
		}).Error; err != nil {
		t.Fatalf("set workspace capacity: %v", err)
	}

	proxies := make([]domain.Proxy, 0, 3)
	for i := 0; i < 3; i++ {
		proxy := domain.Proxy{
			IP:            fmt.Sprintf("192.0.2.%d", i+10),
			Port:          uint16(8010 + i),
			Country:       "N/A",
			EstimatedType: "N/A",
		}
		proxies = append(proxies, proxy)
	}
	inserted, err := InsertAndGetProxiesWithWorkspace(proxies, workspace.ID)
	if err != nil {
		t.Fatalf("import routes: %v", err)
	}
	if len(inserted) != 3 {
		t.Fatalf("stored import count = %d, want 3", len(inserted))
	}

	var managed []domain.ManagedProxy
	if err := db.Where("workspace_id = ?", workspace.ID).Order("proxy_id").Find(&managed).Error; err != nil {
		t.Fatalf("load managed routes: %v", err)
	}
	if len(managed) != 3 {
		t.Fatalf("stored managed routes = %d, want 3", len(managed))
	}
	var activeIDs, pausedIDs []uint64
	for _, route := range managed {
		switch route.State {
		case domain.ManagedProxyStateActive:
			activeIDs = append(activeIDs, route.ProxyID)
		case domain.ManagedProxyStatePaused:
			pausedIDs = append(pausedIDs, route.ProxyID)
			if route.PauseReason != domain.ManagedProxyPauseReasonCapacity {
				t.Fatalf("overflow pause reason = %q, want capacity", route.PauseReason)
			}
		default:
			t.Fatalf("unexpected managed route state %q", route.State)
		}
	}
	if len(activeIDs) != 2 || len(pausedIDs) != 1 {
		t.Fatalf("active/paused routes = %d/%d, want 2/1", len(activeIDs), len(pausedIDs))
	}
	queueable := 0
	for _, proxy := range inserted {
		if len(proxy.Workspaces) > 0 {
			queueable++
		}
	}
	if queueable != 2 {
		t.Fatalf("queueable imported routes = %d, want 2", queueable)
	}

	if err := SetManagedProxyState(workspace.ID, pausedIDs[0], domain.ManagedProxyStateActive); !errors.Is(err, ErrWorkspaceCapacityReached) {
		t.Fatalf("activate overflow route error = %v, want capacity error", err)
	}
	if err := SetManagedProxyState(workspace.ID, activeIDs[0], domain.ManagedProxyStatePaused); err != nil {
		t.Fatalf("pause active route: %v", err)
	}
	if err := SetManagedProxyState(workspace.ID, pausedIDs[0], domain.ManagedProxyStateActive); err != nil {
		t.Fatalf("activate route after freeing capacity: %v", err)
	}

	member := domain.User{Email: "capacity-member@example.test", Password: "hash", Role: "user"}
	if err := db.Create(&member).Error; err != nil {
		t.Fatalf("create member: %v", err)
	}
	if _, err := AddWorkspaceMember(workspace.ID, member.Email, domain.WorkspaceRoleOperator, false); err != nil {
		t.Fatalf("add workspace member: %v", err)
	}
	if err := RemoveWorkspaceMember(workspace.ID, member.ID); err != nil {
		t.Fatalf("remove workspace member: %v", err)
	}

	active, stored, err := workspaceManagedProxyCounts(db, workspace.ID)
	if err != nil {
		t.Fatalf("load capacity after member removal: %v", err)
	}
	if active != 2 || stored != 3 {
		t.Fatalf("capacity changed after member removal: active/stored = %d/%d", active, stored)
	}
	var subscription domain.WorkspaceSubscription
	if err := db.First(&subscription, "workspace_id = ?", workspace.ID).Error; err != nil {
		t.Fatalf("load subscription after member removal: %v", err)
	}
	if limit, unlimited := subscription.ActivationLimit(); unlimited || limit != 2 {
		t.Fatalf("subscription capacity changed after member removal: limit=%d unlimited=%v", limit, unlimited)
	}
}

func TestCapacityCleanupRefreshesSharedRouteQueueOwnership(t *testing.T) {
	db := setupRotatingProxyTestDB(t)

	firstOwner := domain.User{Email: "first-capacity-owner@example.test", Password: "hash", Role: "user"}
	secondOwner := domain.User{Email: "second-capacity-owner@example.test", Password: "hash", Role: "user"}
	if err := db.Create(&firstOwner).Error; err != nil {
		t.Fatalf("create first owner: %v", err)
	}
	if err := db.Create(&secondOwner).Error; err != nil {
		t.Fatalf("create second owner: %v", err)
	}
	firstWorkspace := createTestWorkspaceForUser(t, db, firstOwner)
	secondWorkspace := createTestWorkspaceForUser(t, db, secondOwner)

	proxy := domain.Proxy{IP: "198.51.100.99", Port: 8099, Country: "N/A", EstimatedType: "N/A"}
	inserted, err := InsertAndGetProxiesWithWorkspace([]domain.Proxy{proxy}, firstWorkspace.ID, secondWorkspace.ID)
	if err != nil || len(inserted) != 1 {
		t.Fatalf("create shared managed route: count=%d error=%v", len(inserted), err)
	}
	if len(inserted[0].Workspaces) != 2 {
		t.Fatalf("initial queue workspaces = %#v, want both workspaces", inserted[0].Workspaces)
	}

	if err := db.Model(&domain.WorkspaceSubscription{}).
		Where("workspace_id = ?", firstWorkspace.ID).
		Updates(map[string]any{
			"included_active_routes":   0,
			"additional_active_routes": 0,
			"overage_active_routes":    0,
			"overage_mode":             domain.WorkspaceOverageDisabled,
		}).Error; err != nil {
		t.Fatalf("downgrade first workspace capacity: %v", err)
	}

	paused, inactive, refresh, err := CleanupProxyLimitViolations(context.Background())
	if err != nil {
		t.Fatalf("cleanup workspace capacity: %v", err)
	}
	if paused != 1 || len(inactive) != 0 || len(refresh) != 1 {
		t.Fatalf("queue changes = paused %d, inactive %d, refresh %d; want 1/0/1", paused, len(inactive), len(refresh))
	}
	if refresh[0].ID != inserted[0].ID || len(refresh[0].Workspaces) != 1 || refresh[0].Workspaces[0].ID != secondWorkspace.ID {
		t.Fatalf("refreshed queue ownership = %#v, want only workspace %d", refresh[0].Workspaces, secondWorkspace.ID)
	}
}

func TestWorkspaceUsageMetersRetriesAndPreservesPeakCapacity(t *testing.T) {
	db := setupRotatingProxyTestDB(t)
	if err := db.AutoMigrate(&domain.WorkspaceUsagePeriod{}); err != nil {
		t.Fatalf("migrate workspace usage: %v", err)
	}

	owner := domain.User{Email: "usage-owner@example.test", Password: "hash", Role: "user"}
	if err := db.Create(&owner).Error; err != nil {
		t.Fatalf("create owner: %v", err)
	}
	workspace := createTestWorkspaceForUser(t, db, owner)
	proxy := domain.Proxy{IP: "192.0.2.88", Port: 8088, Country: "N/A", EstimatedType: "N/A"}
	inserted, err := InsertAndGetProxiesWithWorkspace([]domain.Proxy{proxy}, workspace.ID)
	if err != nil || len(inserted) != 1 {
		t.Fatalf("create managed route: count=%d error=%v", len(inserted), err)
	}

	checkedAt := time.Now().UTC()
	statistics := []domain.ProxyStatistic{
		{ProxyID: inserted[0].ID, Attempt: 0, WorkspaceIDs: []uint{workspace.ID}, CreatedAt: checkedAt},
		{ProxyID: inserted[0].ID, Attempt: 2, WorkspaceIDs: []uint{workspace.ID, workspace.ID}, CreatedAt: checkedAt},
	}
	if err := recordWorkspaceCheckUsage(db, statistics); err != nil {
		t.Fatalf("record workspace check usage: %v", err)
	}

	periodStart, _ := workspaceUsageMonth(checkedAt)
	var usage domain.WorkspaceUsagePeriod
	if err := db.Where("workspace_id = ? AND period_start = ?", workspace.ID, periodStart).First(&usage).Error; err != nil {
		t.Fatalf("load workspace usage: %v", err)
	}
	if usage.CheckAttempts != 4 {
		t.Fatalf("check attempts = %d, want 4", usage.CheckAttempts)
	}
	if usage.ActiveRoutes != 1 || usage.PeakActiveRoutes != 1 {
		t.Fatalf("active/peak routes = %d/%d, want 1/1", usage.ActiveRoutes, usage.PeakActiveRoutes)
	}
	if err := recordWorkspaceManagedTraffic(db, workspace.ID, 2, 1500, checkedAt); err != nil {
		t.Fatalf("record managed traffic: %v", err)
	}
	if err := recordWorkspaceManagedTraffic(db, workspace.ID, 1, 500, checkedAt); err != nil {
		t.Fatalf("record second managed traffic sample: %v", err)
	}
	if err := db.Where("workspace_id = ? AND period_start = ?", workspace.ID, periodStart).First(&usage).Error; err != nil {
		t.Fatalf("reload workspace traffic usage: %v", err)
	}
	if usage.ManagedRequests != 3 || usage.ManagedBytes != 2000 || usage.CheckAttempts != 4 {
		t.Fatalf("combined usage = %#v", usage)
	}

	if err := SetManagedProxyState(workspace.ID, inserted[0].ID, domain.ManagedProxyStatePaused); err != nil {
		t.Fatalf("pause managed route: %v", err)
	}
	if err := db.Where("workspace_id = ? AND period_start = ?", workspace.ID, periodStart).First(&usage).Error; err != nil {
		t.Fatalf("reload workspace usage: %v", err)
	}
	if usage.ActiveRoutes != 0 || usage.PeakActiveRoutes != 1 || usage.CheckAttempts != 4 || usage.ManagedRequests != 3 || usage.ManagedBytes != 2000 {
		t.Fatalf("usage after pause = %#v", usage)
	}
}

func TestDeleteUserAccountRequiresSharedWorkspaceOwnershipTransfer(t *testing.T) {
	db := setupRotatingProxyTestDB(t)
	if err := db.AutoMigrate(&domain.WorkspaceMemberPreference{}); err != nil {
		t.Fatalf("migrate workspace preferences: %v", err)
	}

	owner := domain.User{Email: "deleting-owner@example.test", Password: "hash", Role: "user"}
	successor := domain.User{Email: "successor@example.test", Password: "hash", Role: "user"}
	if err := db.Create(&owner).Error; err != nil {
		t.Fatalf("create owner: %v", err)
	}
	if err := db.Create(&successor).Error; err != nil {
		t.Fatalf("create successor: %v", err)
	}
	workspace := createTestWorkspaceForUser(t, db, owner)
	if _, err := AddWorkspaceMember(workspace.ID, successor.Email, domain.WorkspaceRoleOperator, false); err != nil {
		t.Fatalf("add successor: %v", err)
	}

	proxy := domain.Proxy{IP: "198.51.100.42", Port: 8042, Country: "N/A", EstimatedType: "N/A"}
	inserted, err := InsertAndGetProxiesWithWorkspace([]domain.Proxy{proxy}, workspace.ID)
	if err != nil || len(inserted) != 1 {
		t.Fatalf("create shared route: count=%d error=%v", len(inserted), err)
	}

	if err := ValidateUserAccountDeletion(context.Background(), owner.ID); !errors.Is(err, ErrWorkspaceOwnershipTransferRequired) {
		t.Fatalf("ownership preflight error = %v, want transfer required", err)
	}
	if _, err := DeleteUserAccountWithResult(context.Background(), owner.ID); !errors.Is(err, ErrWorkspaceOwnershipTransferRequired) {
		t.Fatalf("account deletion error = %v, want transfer required", err)
	}

	if err := UpdateWorkspaceMember(workspace.ID, successor.ID, domain.WorkspaceRoleOwner, true); err != nil {
		t.Fatalf("promote successor to owner: %v", err)
	}
	result, err := DeleteUserAccountWithResult(context.Background(), owner.ID)
	if err != nil {
		t.Fatalf("delete account after ownership transfer: %v", err)
	}
	if len(result.DeletedWorkspaceIDs) != 0 || len(result.InactiveProxies) != 0 {
		t.Fatalf("shared resources were scheduled for deletion: %#v", result)
	}

	if err := db.First(&domain.User{}, owner.ID).Error; !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("deleted user lookup error = %v, want not found", err)
	}
	var retainedWorkspace domain.Workspace
	if err := db.First(&retainedWorkspace, workspace.ID).Error; err != nil {
		t.Fatalf("shared workspace was deleted: %v", err)
	}
	if retainedWorkspace.Personal {
		t.Fatal("shared workspace remained marked personal")
	}
	active, stored, err := workspaceManagedProxyCounts(db, workspace.ID)
	if err != nil || active != 1 || stored != 1 {
		t.Fatalf("shared route capacity changed: active/stored=%d/%d error=%v", active, stored, err)
	}
	access, err := ResolveWorkspaceAccess(successor.ID, workspace.ID)
	if err != nil || !access.IsOwner() || !access.CanManageBilling() {
		t.Fatalf("successor ownership = %#v, error=%v", access, err)
	}
}

func TestDeleteUserAccountRemovesSolitaryPersonalWorkspaceWithoutDeletingRoute(t *testing.T) {
	db := setupRotatingProxyTestDB(t)

	user := domain.User{Email: "personal-delete@example.test", Password: "hash", Role: "user"}
	if err := db.Create(&user).Error; err != nil {
		t.Fatalf("create user: %v", err)
	}
	workspace := createTestWorkspaceForUser(t, db, user)
	proxy := domain.Proxy{IP: "203.0.113.77", Port: 8077, Country: "N/A", EstimatedType: "N/A"}
	inserted, err := InsertAndGetProxiesWithWorkspace([]domain.Proxy{proxy}, workspace.ID)
	if err != nil || len(inserted) != 1 {
		t.Fatalf("create personal route: count=%d error=%v", len(inserted), err)
	}

	result, err := DeleteUserAccountWithResult(context.Background(), user.ID)
	if err != nil {
		t.Fatalf("delete personal account: %v", err)
	}
	if len(result.DeletedWorkspaceIDs) != 1 || result.DeletedWorkspaceIDs[0] != workspace.ID {
		t.Fatalf("deleted workspace IDs = %#v", result.DeletedWorkspaceIDs)
	}
	if len(result.InactiveProxies) != 1 || result.InactiveProxies[0].ID != inserted[0].ID {
		t.Fatalf("inactive queue routes = %#v", result.InactiveProxies)
	}
	if len(result.RefreshProxies) != 0 {
		t.Fatalf("unexpected queue refresh routes: %#v", result.RefreshProxies)
	}

	if err := db.First(&domain.Workspace{}, workspace.ID).Error; !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("personal workspace lookup error = %v, want not found", err)
	}
	if err := db.First(&domain.Proxy{}, inserted[0].ID).Error; err != nil {
		t.Fatalf("route identity should remain available for deduplication: %v", err)
	}
	var managedCount int64
	if err := db.Model(&domain.ManagedProxy{}).Where("proxy_id = ?", inserted[0].ID).Count(&managedCount).Error; err != nil {
		t.Fatalf("count remaining management rows: %v", err)
	}
	if managedCount != 0 {
		t.Fatalf("remaining management rows = %d, want 0", managedCount)
	}
}
