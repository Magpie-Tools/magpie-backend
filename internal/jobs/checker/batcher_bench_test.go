package checker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"magpie/internal/domain"
	"magpie/internal/platform/pg"
	"magpie/internal/security"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// BenchmarkCheckerBatchedCycle measures one checker cycle's batched database
// work as workers see it: a failure-tracking submit and an ownership check,
// with 64 concurrent workers sharing the batchers. Most of the time is the
// 50 ms batch window and SQLite; the benchmark exists to compare changes to
// how workers reach the batchers.
func BenchmarkCheckerBatchedCycle(b *testing.B) {
	b.Setenv("PROXY_ENCRYPTION_KEY", "checker-bench-key")
	security.ResetProxyCipherForTests()
	db, err := gorm.Open(sqlite.Open("file:checker-batched-cycle?mode=memory&cache=shared"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		b.Fatal(err)
	}
	sqlDB, _ := db.DB()
	b.Cleanup(func() { _ = sqlDB.Close() })
	if err := db.AutoMigrate(&domain.ManagedProxy{}); err != nil {
		b.Fatal(err)
	}
	if err := db.AutoMigrate(&domain.User{}, &domain.Workspace{}, &domain.Proxy{}); err != nil {
		b.Fatal(err)
	}
	workspace := domain.Workspace{Name: "bench"}
	if err := db.Create(&workspace).Error; err != nil {
		b.Fatal(err)
	}
	const routes = 1024
	for i := 0; i < routes; i++ {
		proxy := domain.Proxy{IP: fmt.Sprintf("10.1.%d.%d", i/256, i%256), Port: 8080}
		if err := db.Create(&proxy).Error; err != nil {
			b.Fatal(err)
		}
		if err := db.Create(&domain.ManagedProxy{WorkspaceID: workspace.ID, ProxyID: proxy.ID, State: domain.ManagedProxyStateActive}).Error; err != nil {
			b.Fatal(err)
		}
	}
	previous := pg.DB
	pg.DB = db
	b.Cleanup(func() { pg.DB = previous })

	r := NewRuntime()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.RunBatchers(ctx) }()
	b.Cleanup(func() { cancel(); <-done })

	events := []failureEvent{{WorkspaceID: workspace.ID, Success: true, HasEligibleChecks: true}}
	var next atomic.Uint64
	b.ResetTimer()
	var workers sync.WaitGroup
	per := (b.N + 63) / 64
	for w := 0; w < 64; w++ {
		workers.Go(func() {
			for i := 0; i < per; i++ {
				proxyID := next.Add(1)%routes + 1
				if _, _, err := r.processFailureEvents(context.Background(), proxyID, events); err != nil {
					b.Error(err)
					return
				}
				if owned, err := r.verifyProxyOwnership(context.Background(), proxyID); err != nil || !owned {
					b.Error(owned, err)
					return
				}
			}
		})
	}
	workers.Wait()
}
