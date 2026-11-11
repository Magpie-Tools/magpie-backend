package testdb

import (
	"testing"

	"magpie/internal/domain"

	"gorm.io/gorm"
)

// CreateWorkspaceForUser creates the personal workspace of a user account with
// the user's ID, an owner membership that is its default, and an active
// subscription without limits. It copies the user's checker settings.
func CreateWorkspaceForUser(t testing.TB, db *gorm.DB, user domain.User) domain.Workspace {
	t.Helper()
	workspace := domain.Workspace{
		ID:                         user.ID,
		Name:                       user.Email + " workspace",
		Personal:                   true,
		HTTPProtocol:               user.HTTPProtocol,
		HTTPSProtocol:              user.HTTPSProtocol,
		SOCKS4Protocol:             user.SOCKS4Protocol,
		SOCKS5Protocol:             user.SOCKS5Protocol,
		Timeout:                    user.Timeout,
		Retries:                    user.Retries,
		UseHttpsForSocks:           user.UseHttpsForSocks,
		TransportProtocol:          user.TransportProtocol,
		AutoRemoveFailingProxies:   user.AutoRemoveFailingProxies,
		AutoRemoveFailureThreshold: user.AutoRemoveFailureThreshold,
	}
	if err := db.Create(&workspace).Error; err != nil {
		t.Fatalf("create test workspace: %v", err)
	}
	membership := domain.WorkspaceMembership{
		WorkspaceID:  workspace.ID,
		UserID:       user.ID,
		Role:         domain.WorkspaceRoleOwner,
		BillingAdmin: true,
		IsDefault:    true,
	}
	if err := db.Create(&membership).Error; err != nil {
		t.Fatalf("create test workspace membership: %v", err)
	}
	subscription := domain.WorkspaceSubscription{
		WorkspaceID: workspace.ID,
		PlanCode:    "test",
		Status:      domain.WorkspaceSubscriptionStatusActive,
		OverageMode: domain.WorkspaceOverageUnlimited,
	}
	if err := db.Create(&subscription).Error; err != nil {
		t.Fatalf("create test workspace subscription: %v", err)
	}
	return workspace
}
