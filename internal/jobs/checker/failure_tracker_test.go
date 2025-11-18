package checker

import (
	"context"
	"testing"
	"time"

	"magpie/internal/domain"
)

func TestFailureTrackerIncrementAndAutoRemove(t *testing.T) {
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

	proxy := domain.Proxy{
		IP:            "10.0.0.41",
		Port:          8080,
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

	event := failureEvent{
		UserID:            user.ID,
		Success:           false,
		AutoRemove:        true,
		FailureThreshold:  user.AutoRemoveFailureThreshold,
		HasEligibleChecks: true,
	}

	if _, _, err := processFailureEvents(ctx, proxy.ID, []failureEvent{event}); err != nil {
		t.Fatalf("first failure: %v", err)
	}

	var state domain.UserProxy
	if err := db.First(&state, "user_id = ? AND proxy_id = ?", user.ID, proxy.ID).Error; err != nil {
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
	if err := db.Model(&domain.UserProxy{}).
		Where("user_id = ? AND proxy_id = ?", user.ID, proxy.ID).
		Count(&remaining).Error; err != nil {
		t.Fatalf("count user proxy: %v", err)
	}
	if remaining != 0 {
		t.Fatalf("user proxy rows remaining = %d, want 0", remaining)
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
		UserID:              user.ID,
		ProxyID:             proxy.ID,
		ConsecutiveFailures: 3,
	}
	if err := db.Create(&link).Error; err != nil {
		t.Fatalf("create user proxy: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	event := failureEvent{
		UserID:            user.ID,
		Success:           true,
		AutoRemove:        true,
		FailureThreshold:  user.AutoRemoveFailureThreshold,
		HasEligibleChecks: true,
	}

	if _, _, err := processFailureEvents(ctx, proxy.ID, []failureEvent{event}); err != nil {
		t.Fatalf("reset event: %v", err)
	}

	var state domain.UserProxy
	if err := db.First(&state, "user_id = ? AND proxy_id = ?", user.ID, proxy.ID).Error; err != nil {
		t.Fatalf("reload user proxy: %v", err)
	}
	if state.ConsecutiveFailures != 0 {
		t.Fatalf("consecutive failures after reset = %d, want 0", state.ConsecutiveFailures)
	}
}
