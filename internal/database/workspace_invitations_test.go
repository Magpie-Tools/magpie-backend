package database

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"magpie/internal/domain"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestWorkspaceInvitationIsAccountBoundAndAcceptanceIsAtomic(t *testing.T) {
	db := setupWorkspaceInvitationTestDB(t)
	owner, workspace := createWorkspaceInvitationOwner(t, db, "owner@example.test")
	invitee := createWorkspaceInvitationUser(t, db, "invitee@example.test")
	outsider := createWorkspaceInvitationUser(t, db, "outsider@example.test")
	now := time.Now().UTC().Truncate(time.Second)

	if _, err := CreateWorkspaceInvitation(workspace.ID, owner.ID, "missing@example.test", domain.WorkspaceRoleOperator, false, now.Add(7*24*time.Hour)); !errors.Is(err, ErrWorkspaceInviteeNotFound) {
		t.Fatalf("unknown invitee error = %v, want ErrWorkspaceInviteeNotFound", err)
	}

	created, err := CreateWorkspaceInvitation(workspace.ID, owner.ID, "  INVITEE@example.test ", domain.WorkspaceRoleOperator, true, now.Add(7*24*time.Hour))
	if err != nil {
		t.Fatalf("create invitation: %v", err)
	}
	if created.InviteeUserID != invitee.ID || created.InviteeEmail != invitee.Email || created.InviterEmail != owner.Email {
		t.Fatalf("created invitation identity = %#v", created)
	}
	if created.Role != domain.WorkspaceRoleOperator || !created.BillingAdmin {
		t.Fatalf("created invitation access = role %q billing %v", created.Role, created.BillingAdmin)
	}
	if _, err := CreateWorkspaceInvitation(workspace.ID, owner.ID, invitee.Email, domain.WorkspaceRoleViewer, false, now.Add(7*24*time.Hour)); !errors.Is(err, ErrWorkspaceInvitationExists) {
		t.Fatalf("duplicate invitation error = %v, want ErrWorkspaceInvitationExists", err)
	}

	incoming, err := ListUserWorkspaceInvitations(invitee.ID, now)
	if err != nil || len(incoming) != 1 || incoming[0].ID != created.ID {
		t.Fatalf("invitee inbox = %#v, error %v", incoming, err)
	}
	outsiderIncoming, err := ListUserWorkspaceInvitations(outsider.ID, now)
	if err != nil || len(outsiderIncoming) != 0 {
		t.Fatalf("outsider inbox = %#v, error %v", outsiderIncoming, err)
	}
	if _, err := AcceptWorkspaceInvitation(created.ID, outsider.ID, now); !errors.Is(err, ErrWorkspaceInvitationNotFound) {
		t.Fatalf("outsider acceptance error = %v, want ErrWorkspaceInvitationNotFound", err)
	}

	accepted, err := AcceptWorkspaceInvitation(created.ID, invitee.ID, now)
	if err != nil {
		t.Fatalf("accept invitation: %v", err)
	}
	if accepted.WorkspaceID != workspace.ID || accepted.WorkspaceName != workspace.Name {
		t.Fatalf("acceptance = %#v", accepted)
	}
	var membership domain.WorkspaceMembership
	if err := db.Where("workspace_id = ? AND user_id = ?", workspace.ID, invitee.ID).First(&membership).Error; err != nil {
		t.Fatalf("load accepted membership: %v", err)
	}
	if membership.Role != domain.WorkspaceRoleOperator || !membership.BillingAdmin || membership.IsDefault {
		t.Fatalf("accepted membership = %#v", membership)
	}
	var preferenceCount, invitationCount int64
	if err := db.Model(&domain.WorkspaceMemberPreference{}).Where("workspace_id = ? AND user_id = ?", workspace.ID, invitee.ID).Count(&preferenceCount).Error; err != nil {
		t.Fatalf("count accepted preference: %v", err)
	}
	if err := db.Model(&domain.WorkspaceInvitation{}).Where("id = ?", created.ID).Count(&invitationCount).Error; err != nil {
		t.Fatalf("count accepted invitation: %v", err)
	}
	if preferenceCount != 1 || invitationCount != 0 {
		t.Fatalf("accepted preference/invitation counts = %d/%d, want 1/0", preferenceCount, invitationCount)
	}
	var updatedWorkspace domain.Workspace
	if err := db.First(&updatedWorkspace, workspace.ID).Error; err != nil {
		t.Fatalf("load accepted workspace: %v", err)
	}
	if updatedWorkspace.Personal {
		t.Fatal("accepted shared workspace must no longer be personal")
	}
}

func TestWorkspaceInvitationLifecyclePreservesExpiryAndDeletesTerminalRows(t *testing.T) {
	db := setupWorkspaceInvitationTestDB(t)
	owner, workspace := createWorkspaceInvitationOwner(t, db, "lifecycle-owner@example.test")
	invitee := createWorkspaceInvitationUser(t, db, "lifecycle-invitee@example.test")
	now := time.Now().UTC().Truncate(time.Second)
	expiresAt := now.Add(7 * 24 * time.Hour)

	created, err := CreateWorkspaceInvitation(workspace.ID, owner.ID, invitee.Email, domain.WorkspaceRoleViewer, false, expiresAt)
	if err != nil {
		t.Fatalf("create invitation: %v", err)
	}
	updated, err := UpdateWorkspaceInvitation(workspace.ID, created.ID, domain.WorkspaceRoleAdmin, true, now.Add(time.Hour))
	if err != nil {
		t.Fatalf("update invitation: %v", err)
	}
	if !updated.ExpiresAt.Equal(expiresAt) {
		t.Fatalf("updated expiry = %s, want unchanged %s", updated.ExpiresAt, expiresAt)
	}
	if updated.Role != domain.WorkspaceRoleAdmin || !updated.BillingAdmin {
		t.Fatalf("updated invitation = %#v", updated)
	}
	if err := RevokeWorkspaceInvitation(workspace.ID, created.ID, now); err != nil {
		t.Fatalf("revoke invitation: %v", err)
	}
	assertWorkspaceInvitationCount(t, db, created.ID, 0)

	declined, err := CreateWorkspaceInvitation(workspace.ID, owner.ID, invitee.Email, domain.WorkspaceRoleViewer, false, expiresAt)
	if err != nil {
		t.Fatalf("create invitation to decline: %v", err)
	}
	if err := DeclineWorkspaceInvitation(declined.ID, invitee.ID, now); err != nil {
		t.Fatalf("decline invitation: %v", err)
	}
	assertWorkspaceInvitationCount(t, db, declined.ID, 0)

	expired := domain.WorkspaceInvitation{
		WorkspaceID:   workspace.ID,
		InviteeUserID: invitee.ID,
		InviterUserID: &owner.ID,
		InviterEmail:  owner.Email,
		Role:          domain.WorkspaceRoleViewer,
		ExpiresAt:     now.Add(-time.Minute),
	}
	if err := expired.Normalize(); err != nil {
		t.Fatalf("normalize expired invitation: %v", err)
	}
	if err := db.Create(&expired).Error; err != nil {
		t.Fatalf("create expired invitation: %v", err)
	}
	replacement, err := CreateWorkspaceInvitation(workspace.ID, owner.ID, invitee.Email, domain.WorkspaceRoleOperator, false, expiresAt)
	if err != nil {
		t.Fatalf("replace expired invitation before cleanup: %v", err)
	}
	assertWorkspaceInvitationCount(t, db, expired.ID, 0)

	if err := db.Model(&domain.WorkspaceInvitation{}).Where("id = ?", replacement.ID).Update("expires_at", now.Add(-time.Second)).Error; err != nil {
		t.Fatalf("expire replacement: %v", err)
	}
	if listed, err := ListUserWorkspaceInvitations(invitee.ID, now); err != nil || len(listed) != 0 {
		t.Fatalf("expired inbox = %#v, error %v", listed, err)
	}
	removed, err := DeleteExpiredWorkspaceInvitations(context.Background(), now)
	if err != nil || removed != 1 {
		t.Fatalf("cleanup removed %d, error %v; want 1", removed, err)
	}
	assertWorkspaceInvitationCount(t, db, replacement.ID, 0)
}

func TestWorkspaceInvitationSurvivesInviterDeletion(t *testing.T) {
	db := setupWorkspaceInvitationTestDB(t)
	owner, workspace := createWorkspaceInvitationOwner(t, db, "departing-owner@example.test")
	invitee := createWorkspaceInvitationUser(t, db, "remaining-invitee@example.test")
	invitation, err := CreateWorkspaceInvitation(
		workspace.ID,
		owner.ID,
		invitee.Email,
		domain.WorkspaceRoleViewer,
		false,
		time.Now().UTC().Add(7*24*time.Hour),
	)
	if err != nil {
		t.Fatalf("create invitation: %v", err)
	}

	if err := db.Delete(&owner).Error; err != nil {
		t.Fatalf("delete inviter: %v", err)
	}

	var persisted domain.WorkspaceInvitation
	if err := db.First(&persisted, invitation.ID).Error; err != nil {
		t.Fatalf("load invitation after inviter deletion: %v", err)
	}
	if persisted.InviterUserID != nil {
		t.Fatalf("inviter user id = %v, want nil after deletion", *persisted.InviterUserID)
	}
	if persisted.InviterEmail != owner.Email {
		t.Fatalf("inviter email = %q, want preserved snapshot %q", persisted.InviterEmail, owner.Email)
	}
}

func TestConcurrentWorkspaceInvitationCreationKeepsOnePendingOffer(t *testing.T) {
	db := setupWorkspaceInvitationTestDB(t)
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("resolve sql db: %v", err)
	}
	sqlDB.SetMaxOpenConns(1)
	owner, workspace := createWorkspaceInvitationOwner(t, db, "concurrent-owner@example.test")
	invitee := createWorkspaceInvitationUser(t, db, "concurrent-invitee@example.test")
	expiresAt := time.Now().UTC().Add(7 * 24 * time.Hour)

	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, createErr := CreateWorkspaceInvitation(workspace.ID, owner.ID, invitee.Email, domain.WorkspaceRoleOperator, false, expiresAt)
			errs <- createErr
		}()
	}
	close(start)
	wg.Wait()
	close(errs)

	var succeeded, duplicate int
	for createErr := range errs {
		switch {
		case createErr == nil:
			succeeded++
		case errors.Is(createErr, ErrWorkspaceInvitationExists):
			duplicate++
		default:
			t.Fatalf("concurrent creation error = %v", createErr)
		}
	}
	if succeeded != 1 || duplicate != 1 {
		t.Fatalf("concurrent results success/duplicate = %d/%d, want 1/1", succeeded, duplicate)
	}
	var count int64
	if err := db.Model(&domain.WorkspaceInvitation{}).Where("workspace_id = ? AND invitee_user_id = ?", workspace.ID, invitee.ID).Count(&count).Error; err != nil {
		t.Fatalf("count concurrent invitations: %v", err)
	}
	if count != 1 {
		t.Fatalf("pending invitation count = %d, want 1", count)
	}
}

func TestConcurrentWorkspaceInvitationAcceptanceCreatesOneMembership(t *testing.T) {
	db := setupWorkspaceInvitationTestDB(t)
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("resolve sql db: %v", err)
	}
	sqlDB.SetMaxOpenConns(1)
	owner, workspace := createWorkspaceInvitationOwner(t, db, "accept-owner@example.test")
	invitee := createWorkspaceInvitationUser(t, db, "accept-invitee@example.test")
	now := time.Now().UTC()
	invitation, err := CreateWorkspaceInvitation(workspace.ID, owner.ID, invitee.Email, domain.WorkspaceRoleViewer, false, now.Add(7*24*time.Hour))
	if err != nil {
		t.Fatalf("create invitation: %v", err)
	}

	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, acceptErr := AcceptWorkspaceInvitation(invitation.ID, invitee.ID, now)
			errs <- acceptErr
		}()
	}
	close(start)
	wg.Wait()
	close(errs)

	var succeeded, alreadyGone int
	for acceptErr := range errs {
		switch {
		case acceptErr == nil:
			succeeded++
		case errors.Is(acceptErr, ErrWorkspaceInvitationNotFound):
			alreadyGone++
		default:
			t.Fatalf("concurrent acceptance error = %v", acceptErr)
		}
	}
	if succeeded != 1 || alreadyGone != 1 {
		t.Fatalf("concurrent acceptance success/not-found = %d/%d, want 1/1", succeeded, alreadyGone)
	}
	var membershipCount, invitationCount int64
	if err := db.Model(&domain.WorkspaceMembership{}).Where("workspace_id = ? AND user_id = ?", workspace.ID, invitee.ID).Count(&membershipCount).Error; err != nil {
		t.Fatalf("count accepted memberships: %v", err)
	}
	if err := db.Model(&domain.WorkspaceInvitation{}).Where("id = ?", invitation.ID).Count(&invitationCount).Error; err != nil {
		t.Fatalf("count accepted invitation: %v", err)
	}
	if membershipCount != 1 || invitationCount != 0 {
		t.Fatalf("membership/invitation counts = %d/%d, want 1/0", membershipCount, invitationCount)
	}
}

func setupWorkspaceInvitationTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	previous := DB
	t.Cleanup(func() { DB = previous })
	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared&_fk=1", t.Name())
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := db.AutoMigrate(
		&domain.User{},
		&domain.Workspace{},
		&domain.WorkspaceMembership{},
		&domain.WorkspaceMemberPreference{},
		&domain.WorkspaceInvitation{},
	); err != nil {
		t.Fatalf("migrate workspace invitation schema: %v", err)
	}
	DB = db
	return db
}

func createWorkspaceInvitationOwner(t *testing.T, db *gorm.DB, email string) (domain.User, domain.Workspace) {
	t.Helper()
	owner := createWorkspaceInvitationUser(t, db, email)
	workspace := domain.Workspace{Name: "Shared operations", Personal: true}
	if err := db.Create(&workspace).Error; err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := db.Create(&domain.WorkspaceMembership{
		WorkspaceID:  workspace.ID,
		UserID:       owner.ID,
		Role:         domain.WorkspaceRoleOwner,
		BillingAdmin: true,
		IsDefault:    true,
	}).Error; err != nil {
		t.Fatalf("create owner membership: %v", err)
	}
	return owner, workspace
}

func createWorkspaceInvitationUser(t *testing.T, db *gorm.DB, email string) domain.User {
	t.Helper()
	user := domain.User{Email: email, Password: "hash", Role: "user"}
	if err := db.Create(&user).Error; err != nil {
		t.Fatalf("create user %s: %v", email, err)
	}
	return user
}

func assertWorkspaceInvitationCount(t *testing.T, db *gorm.DB, invitationID uint, want int64) {
	t.Helper()
	var count int64
	if err := db.Model(&domain.WorkspaceInvitation{}).Where("id = ?", invitationID).Count(&count).Error; err != nil {
		t.Fatalf("count invitation %d: %v", invitationID, err)
	}
	if count != want {
		t.Fatalf("invitation %d count = %d, want %d", invitationID, count, want)
	}
}
