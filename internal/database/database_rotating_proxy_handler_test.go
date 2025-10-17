package database

import (
	"fmt"
	"testing"
	"time"

	"magpie/internal/domain"
	"magpie/internal/security"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func setupRotatingProxyTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	t.Setenv("PROXY_ENCRYPTION_KEY", "rotating-proxy-test-key")
	security.ResetProxyCipherForTests()

	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared&_fk=1", t.Name())
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}

	if err := db.AutoMigrate(
		&domain.User{},
		&domain.Proxy{},
		&domain.UserProxy{},
		&domain.RotatingProxy{},
		&domain.ProxyStatistic{},
		&domain.Protocol{},
		&domain.Judge{},
	); err != nil {
		t.Fatalf("auto migrate: %v", err)
	}

	DB = db

	t.Cleanup(func() {
		DB = nil
	})

	return db
}

func TestGetNextRotatingProxy_RotatesAcrossAliveProxies(t *testing.T) {
	db := setupRotatingProxyTestDB(t)

	user := domain.User{
		Email:         "rotator@example.com",
		Password:      "password123",
		HTTPProtocol:  true,
		HTTPSProtocol: true,
	}
	if err := db.Create(&user).Error; err != nil {
		t.Fatalf("create user: %v", err)
	}

	protocol := domain.Protocol{Name: "http"}
	if err := db.Create(&protocol).Error; err != nil {
		t.Fatalf("create protocol: %v", err)
	}

	judge := domain.Judge{FullString: "http://judge.example.com"}
	if err := db.Create(&judge).Error; err != nil {
		t.Fatalf("create judge: %v", err)
	}

	proxies := []domain.Proxy{
		{
			IP:            "10.0.0.1",
			Port:          8080,
			Username:      "user-one",
			Password:      "pass-one",
			Country:       "AA",
			EstimatedType: "residential",
		},
		{
			IP:            "10.0.0.2",
			Port:          8081,
			Username:      "user-two",
			Password:      "pass-two",
			Country:       "AA",
			EstimatedType: "residential",
		},
	}

	for idx := range proxies {
		if err := db.Create(&proxies[idx]).Error; err != nil {
			t.Fatalf("create proxy %d: %v", idx, err)
		}
		if err := db.Create(&domain.UserProxy{
			UserID:  user.ID,
			ProxyID: proxies[idx].ID,
		}).Error; err != nil {
			t.Fatalf("link proxy %d: %v", idx, err)
		}
		stat := domain.ProxyStatistic{
			Alive:        true,
			Attempt:      1,
			ResponseTime: 150,
			ProtocolID:   protocol.ID,
			ProxyID:      proxies[idx].ID,
			JudgeID:      judge.ID,
			CreatedAt:    time.Unix(int64(idx+1), 0),
		}
		if err := db.Create(&stat).Error; err != nil {
			t.Fatalf("create proxy statistic %d: %v", idx, err)
		}
	}

	rotator := domain.RotatingProxy{
		UserID:     user.ID,
		Name:       "test-rotator",
		ProtocolID: protocol.ID,
		ListenPort: 10500,
	}
	if err := db.Create(&rotator).Error; err != nil {
		t.Fatalf("create rotating proxy: %v", err)
	}

	first, err := GetNextRotatingProxy(user.ID, rotator.ID)
	if err != nil {
		t.Fatalf("GetNextRotatingProxy first call: %v", err)
	}
	if first.ProxyID != proxies[0].ID {
		t.Fatalf("first proxy id = %d, want %d", first.ProxyID, proxies[0].ID)
	}
	if first.Protocol != protocol.Name {
		t.Fatalf("first protocol = %q, want %q", first.Protocol, protocol.Name)
	}

	var updated domain.RotatingProxy
	if err := db.First(&updated, rotator.ID).Error; err != nil {
		t.Fatalf("reload rotating proxy: %v", err)
	}
	if updated.LastProxyID == nil || *updated.LastProxyID != proxies[0].ID {
		t.Fatalf("last proxy id = %v, want %d", updated.LastProxyID, proxies[0].ID)
	}
	if updated.LastRotationAt == nil {
		t.Fatal("expected last rotation timestamp to be set")
	}

	second, err := GetNextRotatingProxy(user.ID, rotator.ID)
	if err != nil {
		t.Fatalf("GetNextRotatingProxy second call: %v", err)
	}
	if second.ProxyID != proxies[1].ID {
		t.Fatalf("second proxy id = %d, want %d", second.ProxyID, proxies[1].ID)
	}

	if err := db.First(&updated, rotator.ID).Error; err != nil {
		t.Fatalf("reload rotating proxy after second call: %v", err)
	}
	if updated.LastProxyID == nil || *updated.LastProxyID != proxies[1].ID {
		t.Fatalf("last proxy id after second call = %v, want %d", updated.LastProxyID, proxies[1].ID)
	}
}

func TestGetNextRotatingProxy_NoAliveProxies(t *testing.T) {
	db := setupRotatingProxyTestDB(t)

	user := domain.User{
		Email:         "noalive@example.com",
		Password:      "password123",
		HTTPProtocol:  true,
		HTTPSProtocol: true,
	}
	if err := db.Create(&user).Error; err != nil {
		t.Fatalf("create user: %v", err)
	}

	protocol := domain.Protocol{Name: "http"}
	if err := db.Create(&protocol).Error; err != nil {
		t.Fatalf("create protocol: %v", err)
	}

	judge := domain.Judge{FullString: "http://judge.example.com"}
	if err := db.Create(&judge).Error; err != nil {
		t.Fatalf("create judge: %v", err)
	}

	proxy := domain.Proxy{
		IP:            "10.0.0.3",
		Port:          8082,
		Username:      "user-three",
		Password:      "pass-three",
		Country:       "AA",
		EstimatedType: "residential",
	}
	if err := db.Create(&proxy).Error; err != nil {
		t.Fatalf("create proxy: %v", err)
	}
	if err := db.Create(&domain.UserProxy{
		UserID:  user.ID,
		ProxyID: proxy.ID,
	}).Error; err != nil {
		t.Fatalf("link proxy: %v", err)
	}

	stat := domain.ProxyStatistic{
		Alive:        false,
		Attempt:      1,
		ResponseTime: 200,
		ProtocolID:   protocol.ID,
		ProxyID:      proxy.ID,
		JudgeID:      judge.ID,
		CreatedAt:    time.Now(),
	}
	if err := db.Create(&stat).Error; err != nil {
		t.Fatalf("create proxy statistic: %v", err)
	}

	rotator := domain.RotatingProxy{
		UserID:     user.ID,
		Name:       "noalive-rotator",
		ProtocolID: protocol.ID,
		ListenPort: 10600,
	}
	if err := db.Create(&rotator).Error; err != nil {
		t.Fatalf("create rotating proxy: %v", err)
	}

	if _, err := GetNextRotatingProxy(user.ID, rotator.ID); err != ErrRotatingProxyNoAliveProxies {
		t.Fatalf("expected ErrRotatingProxyNoAliveProxies, got %v", err)
	}
}
