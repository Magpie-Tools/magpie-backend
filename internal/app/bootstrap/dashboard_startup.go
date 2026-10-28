package bootstrap

import (
	"context"
	"time"

	"github.com/charmbracelet/log"

	"magpie/internal/database"
)

const dashboardCachePrewarmTimeout = 5 * time.Second

// Dashboard snapshots accelerate reads, but do not establish runtime readiness.
// Wait for the refresh to stop rather than abandon its queries in a goroutine.
func prewarmDashboardCaches(ctx context.Context) error {
	return prewarmDashboardCachesWithin(ctx, dashboardCachePrewarmTimeout, database.RefreshRequestedDashboardCaches)
}

func prewarmDashboardCachesWithin(ctx context.Context, timeout time.Duration, refresh func(context.Context) error) error {
	start := time.Now()
	err := refreshDashboardCachesWithin(ctx, timeout, refresh)
	if parentErr := ctx.Err(); parentErr != nil {
		return parentErr
	}
	if err != nil {
		log.Warn("Dashboard cache prewarm incomplete; continuing startup", "error", err, "duration", time.Since(start))
		return nil
	}
	log.Info("Dashboard caches prewarmed", "duration", time.Since(start))
	return nil
}

func refreshDashboardCachesWithin(ctx context.Context, timeout time.Duration, refresh func(context.Context) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	prewarmCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	err := refresh(prewarmCtx)
	if parentErr := ctx.Err(); parentErr != nil {
		return parentErr
	}
	if err != nil {
		return err
	}
	return prewarmCtx.Err()
}
