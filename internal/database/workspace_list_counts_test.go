package database

import (
	"testing"

	"magpie/internal/domain"
)

func TestWorkspaceListCountsSharedRoutesAndEmptyMemberships(t *testing.T) {
	db := setupRotatingProxyTestDB(t)
	owner := domain.User{Email: "list-owner@example.test", Password: "hash", Role: "user"}
	other := domain.User{Email: "list-other@example.test", Password: "hash", Role: "user"}
	for _, user := range []*domain.User{&owner, &other} {
		if err := db.Create(user).Error; err != nil {
			t.Fatal(err)
		}
	}
	first := createTestWorkspaceForUser(t, db, owner)
	second := createTestWorkspaceForUser(t, db, other)
	empty := domain.Workspace{Name: "Empty shared workspace"}
	if err := db.Create(&empty).Error; err != nil {
		t.Fatal(err)
	}
	for _, workspace := range []domain.Workspace{second, empty} {
		if err := db.Create(&domain.WorkspaceMembership{WorkspaceID: workspace.ID, UserID: owner.ID, Role: domain.WorkspaceRoleViewer}).Error; err != nil {
			t.Fatal(err)
		}
	}
	for id, state := range []string{domain.ManagedProxyStateActive, domain.ManagedProxyStatePaused, domain.ManagedProxyStateArchived} {
		proxy := domain.Proxy{ID: uint64(id + 1), IP: "192.0.2.1", Port: uint16(8000 + id)}
		if err := db.Create(&proxy).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Create(&domain.ManagedProxy{WorkspaceID: first.ID, ProxyID: proxy.ID, State: state}).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Create(&domain.ManagedProxy{WorkspaceID: second.ID, ProxyID: proxy.ID, State: domain.ManagedProxyStateActive}).Error; err != nil {
			t.Fatal(err)
		}
	}
	rows, err := ListWorkspaces(owner.ID)
	if err != nil || len(rows) != 3 {
		t.Fatalf("workspace list length=%d error=%v", len(rows), err)
	}
	if rows[0].ID != first.ID || !rows[0].IsDefault {
		t.Fatalf("default workspace ordering changed: %+v", rows[0])
	}
	want := map[uint][2]uint64{first.ID: {1, 3}, second.ID: {3, 3}, empty.ID: {0, 0}}
	for _, row := range rows {
		if got := [2]uint64{row.Capacity.ActiveRoutes, row.Capacity.StoredRoutes}; got != want[row.ID] {
			t.Fatalf("workspace %d counts=%v want=%v", row.ID, got, want[row.ID])
		}
	}
	rows, err = ListWorkspaces(owner.ID + other.ID + 100)
	if err != nil || len(rows) != 0 {
		t.Fatalf("nonmember workspace list=%v error=%v", rows, err)
	}
}
