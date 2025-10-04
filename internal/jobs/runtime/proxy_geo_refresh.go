package runtime

import (
	"context"
	"errors"
	"time"

	"github.com/charmbracelet/log"
	"magpie/internal/database"
)

const proxyGeoRefreshInterval = 24 * time.Hour

func StartProxyGeoRefreshRoutine(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}

	ticker := time.NewTicker(proxyGeoRefreshInterval)
	defer ticker.Stop()

	refreshOnce(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			refreshOnce(ctx)
		}
	}
}

func refreshOnce(ctx context.Context) {
	start := time.Now()

	scanned, updated, err := database.RunProxyGeoRefresh(ctx, 0)
	if err != nil {
		switch {
		case errors.Is(err, database.ErrProxyGeoRefreshDatabaseNotInitialized):
			log.Warn("Proxy geo refresh skipped: database not initialized")
		case errors.Is(err, database.ErrProxyGeoRefreshGeoLiteUnavailable):
			log.Warn("Proxy geo refresh skipped: GeoLite databases unavailable")
		case errors.Is(err, context.Canceled):
			log.Info("Proxy geo refresh canceled", "duration", time.Since(start))
		default:
			log.Error("Proxy geo refresh failed", "error", err)
		}
		return
	}

	log.Info("Proxy geo refresh completed", "scanned", scanned, "updated", updated, "duration", time.Since(start))
}
