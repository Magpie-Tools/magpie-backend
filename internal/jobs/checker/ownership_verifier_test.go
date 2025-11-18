package checker

import (
	"context"
	"testing"
	"time"

	"magpie/internal/domain"
)

func TestOwnershipVerifierDetectsOwnershipChanges(t *testing.T) {
	db := setupCheckerTestDB(t)

	user := domain.User{
		Email:            "ownership@example.com",
		Password:         "password123",
		HTTPSProtocol:    true,
		UseHttpsForSocks: true,
	}
	if err := db.Create(&user).Error; err != nil {
		t.Fatalf("create user: %v", err)
	}

	proxy := domain.Proxy{
		IP:            "10.0.0.55",
		Port:          9090,
		Country:       "AA",
		EstimatedType: "residential",
	}
	if err := db.Create(&proxy).Error; err != nil {
		t.Fatalf("create proxy: %v", err)
	}

	link := domain.UserProxy{UserID: user.ID, ProxyID: proxy.ID}
	if err := db.Create(&link).Error; err != nil {
		t.Fatalf("link proxy: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	owned, err := verifyProxyOwnership(ctx, proxy.ID)
	if err != nil {
		t.Fatalf("verify owned proxy: %v", err)
	}
	if !owned {
		t.Fatal("expected proxy to be reported as owned")
	}

	if err := db.Where("user_id = ? AND proxy_id = ?", user.ID, proxy.ID).
		Delete(&domain.UserProxy{}).Error; err != nil {
		t.Fatalf("delete user proxy: %v", err)
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()

	owned, err = verifyProxyOwnership(ctx2, proxy.ID)
	if err != nil {
		t.Fatalf("verify orphaned proxy: %v", err)
	}
	if owned {
		t.Fatal("expected proxy to be reported as orphaned")
	}

	ctx3, cancel3 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel3()

	owned, err = verifyProxyOwnership(ctx3, proxy.ID+1000)
	if err != nil {
		t.Fatalf("verify unknown proxy: %v", err)
	}
	if owned {
		t.Fatal("expected unknown proxy to be reported as not owned")
	}
}
