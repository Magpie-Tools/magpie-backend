package database

import (
	"context"
	"fmt"
	"testing"
	"time"

	"magpie/internal/config"
	"magpie/internal/domain"
)

func TestNormalizeUserIDs(t *testing.T) {
	got := normalizeUserIDs([]uint{9, 4, 9, 2, 4, 7, 2})
	want := []uint{2, 4, 7, 9}
	if len(got) != len(want) {
		t.Fatalf("normalizeUserIDs length = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("normalizeUserIDs[%d] = %d, want %d", i, got[i], want[i])
		}
	}
}

func TestCleanupProxyLimitViolationsWithConfig_PausesNewestOverflow(t *testing.T) {
	db := setupRotatingProxyTestDB(t)

	user := domain.User{
		Email:    "limit-cleanup-user@example.com",
		Password: "password123",
	}
	if err := db.Create(&user).Error; err != nil {
		t.Fatalf("create user: %v", err)
	}
	createTestWorkspaceForUser(t, db, user)

	base := time.Now().Add(-10 * time.Minute)
	proxyIDs := make([]uint64, 0, 5)

	for i := 0; i < 5; i++ {
		proxy := domain.Proxy{
			IP:            fmt.Sprintf("10.11.0.%d", i+1),
			Port:          uint16(8000 + i),
			Country:       "US",
			EstimatedType: "datacenter",
		}
		if err := db.Create(&proxy).Error; err != nil {
			t.Fatalf("create proxy %d: %v", i, err)
		}
		link := domain.UserProxy{
			WorkspaceID: user.ID,
			ProxyID:     proxy.ID,
			CreatedAt:   base.Add(time.Duration(i) * time.Minute),
		}
		if err := db.Create(&link).Error; err != nil {
			t.Fatalf("create user proxy %d: %v", i, err)
		}
		proxyIDs = append(proxyIDs, proxy.ID)
	}

	paused, inactive, refresh, err := cleanupProxyLimitViolationsWithConfig(context.Background(), config.ProxyLimitConfig{
		Enabled:       true,
		MaxPerUser:    3,
		ExcludeAdmins: false,
	})
	if err != nil {
		t.Fatalf("cleanup limit violations: %v", err)
	}
	if paused != 2 {
		t.Fatalf("paused rows = %d, want 2", paused)
	}
	if len(inactive) != 2 {
		t.Fatalf("inactive proxies = %d, want 2", len(inactive))
	}
	if len(refresh) != 0 {
		t.Fatalf("refresh proxies = %d, want 0", len(refresh))
	}

	var links []domain.UserProxy
	if err := db.Where("workspace_id = ?", user.ID).Find(&links).Error; err != nil {
		t.Fatalf("load workspace managed proxies: %v", err)
	}
	if len(links) != 5 {
		t.Fatalf("stored managed proxies = %d, want 5", len(links))
	}

	stateByProxy := make(map[uint64]string, len(links))
	for _, link := range links {
		stateByProxy[link.ProxyID] = link.State
	}
	for _, expectedID := range proxyIDs[:3] {
		if stateByProxy[expectedID] != domain.ManagedProxyStateActive {
			t.Fatalf("expected older proxy %d to remain active", expectedID)
		}
	}
	for _, expectedID := range proxyIDs[3:] {
		if stateByProxy[expectedID] != domain.ManagedProxyStatePaused {
			t.Fatalf("expected newer proxy %d to be paused", expectedID)
		}
	}
}

func TestCleanupProxyLimitViolationsWithConfig_ExcludesAdmins(t *testing.T) {
	db := setupRotatingProxyTestDB(t)

	admin := domain.User{
		Email:    "limit-cleanup-admin@example.com",
		Password: "password123",
		Role:     "admin",
	}
	if err := db.Create(&admin).Error; err != nil {
		t.Fatalf("create admin: %v", err)
	}
	createTestWorkspaceForUser(t, db, admin)

	for i := 0; i < 4; i++ {
		proxy := domain.Proxy{
			IP:            fmt.Sprintf("10.12.0.%d", i+1),
			Port:          uint16(8100 + i),
			Country:       "US",
			EstimatedType: "datacenter",
		}
		if err := db.Create(&proxy).Error; err != nil {
			t.Fatalf("create proxy %d: %v", i, err)
		}
		if err := db.Create(&domain.UserProxy{
			WorkspaceID: admin.ID,
			ProxyID:     proxy.ID,
		}).Error; err != nil {
			t.Fatalf("create admin user proxy %d: %v", i, err)
		}
	}

	paused, inactive, refresh, err := cleanupProxyLimitViolationsWithConfig(context.Background(), config.ProxyLimitConfig{
		Enabled:       true,
		MaxPerUser:    2,
		ExcludeAdmins: true,
	})
	if err != nil {
		t.Fatalf("cleanup limit violations: %v", err)
	}
	if paused != 0 {
		t.Fatalf("paused rows = %d, want 0 for excluded admin", paused)
	}
	if len(inactive) != 0 {
		t.Fatalf("inactive proxies = %d, want 0", len(inactive))
	}
	if len(refresh) != 0 {
		t.Fatalf("refresh proxies = %d, want 0", len(refresh))
	}

	var remaining int64
	if err := db.Model(&domain.UserProxy{}).
		Where("workspace_id = ?", admin.ID).
		Count(&remaining).Error; err != nil {
		t.Fatalf("count admin proxies: %v", err)
	}
	if remaining != 4 {
		t.Fatalf("admin proxies remaining = %d, want 4", remaining)
	}
}
