package runtime

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/charmbracelet/log"

	"magpie/internal/config"
	"magpie/internal/geolite"
)

func StartGeoLiteUpdateRoutine(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}

	intervalUpdates := config.GeoLiteUpdateIntervalUpdates()
	currentInterval := <-intervalUpdates
	ticker := time.NewTicker(currentInterval)
	defer ticker.Stop()

	triggerGeoLiteUpdate(ctx, "startup", true)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			triggerGeoLiteUpdate(ctx, "scheduled", false)
		case newInterval := <-intervalUpdates:
			if newInterval <= 0 || newInterval == currentInterval {
				continue
			}
			drainTicker(ticker)
			currentInterval = newInterval
			ticker.Reset(currentInterval)
		}
	}
}

// RunGeoLiteUpdate runs the updater on demand. When force is false the update
// is only executed if auto updates are enabled.
func RunGeoLiteUpdate(ctx context.Context, reason string, force bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	triggerGeoLiteUpdate(ctx, reason, force)
}

func triggerGeoLiteUpdate(ctx context.Context, reason string, force bool) {
	cfg := config.GetConfig()
	apiKey := strings.TrimSpace(cfg.GeoLite.APIKey)
	if apiKey == "" {
		log.Debug("GeoLite update skipped: API key missing", "reason", reason)
		return
	}

	if !force && !cfg.GeoLite.AutoUpdate {
		log.Debug("GeoLite update skipped: auto update disabled", "reason", reason)
		return
	}

	updated, err := geolite.UpdateDatabases(ctx)
	switch {
	case errors.Is(err, geolite.ErrNoAPIKey):
		log.Debug("GeoLite update skipped: API key missing", "reason", reason)
	case err != nil:
		log.Error("GeoLite update failed", "reason", reason, "error", err)
	case updated:
		log.Info("GeoLite databases updated", "reason", reason)
	default:
		log.Debug("GeoLite update skipped", "reason", reason)
	}
}
