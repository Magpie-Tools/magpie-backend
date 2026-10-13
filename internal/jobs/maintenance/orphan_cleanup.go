package maintenance

import (
	"context"
	"errors"
	"time"

	"github.com/charmbracelet/log"

	"magpie/internal/database"
	"magpie/internal/support"
)

const (
	envCleanupInterval        = "PROXY_ORPHAN_CLEAN_INTERVAL"
	envCleanupIntervalMinutes = "PROXY_ORPHAN_CLEAN_INTERVAL_MINUTES"
	envCleanupMaxRunDuration  = "PROXY_ORPHAN_CLEAN_MAX_RUN_DURATION"
	envCleanupRetryInterval   = "PROXY_ORPHAN_CLEAN_RETRY_INTERVAL"

	defaultCleanupMinutes = 60
	defaultCleanupBudget  = 45 * time.Second
	defaultCleanupRetry   = time.Minute
	orphanCleanupLockKey  = "magpie:leader:orphan_cleanup"
)

func StartOrphanCleanupRoutine(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}

	err := support.RunWithLeader(ctx, orphanCleanupLockKey, support.DefaultLeadershipTTL, func(leaderCtx context.Context) {
		runOrphanCleanupLoop(leaderCtx)
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Error("Orphan cleanup routine stopped", "error", err)
	}
}

func runOrphanCleanupLoop(ctx context.Context) {
	interval := resolveCleanupInterval()
	retry := resolveCleanupDuration(envCleanupRetryInterval, defaultCleanupRetry)
	runOrphanCleanupPasses(ctx, interval, retry, runOrphanCleanup)
}

func runOrphanCleanupPasses(ctx context.Context, interval, retry time.Duration, run func(context.Context) error) {
	for {
		if ctx.Err() != nil {
			return
		}
		delay := interval
		if err := run(ctx); err != nil {
			delay = min(interval, retry)
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func resolveCleanupInterval() time.Duration {
	if raw := support.GetEnv(envCleanupInterval, ""); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil && parsed > 0 {
			return parsed
		}
		log.Warn("Invalid PROXY_ORPHAN_CLEAN_INTERVAL value, falling back to minutes env", "value", raw)
	}

	minutes := support.GetEnvInt(envCleanupIntervalMinutes, defaultCleanupMinutes)
	if minutes <= 0 {
		minutes = defaultCleanupMinutes
	}

	return time.Duration(minutes) * time.Minute
}

func resolveCleanupDuration(env string, fallback time.Duration) time.Duration {
	raw := support.GetEnv(env, fallback.String())
	if duration, err := time.ParseDuration(raw); err == nil && duration > 0 {
		return duration
	}
	log.Warn("Invalid orphan cleanup duration; using default", "env", env, "value", raw, "default", fallback)
	return fallback
}

func runOrphanCleanup(ctx context.Context) error {
	start := time.Now()
	ctx, cancel := context.WithTimeout(ctx, resolveCleanupDuration(envCleanupMaxRunDuration, defaultCleanupBudget))
	defer cancel()
	var failures []error

	var proxyRemoved, sourceLinkRemoved, siteRemoved int64

	if removed, err := database.DeleteOrphanProxies(ctx); err != nil {
		log.Error("Failed to cleanup orphan proxies", "error", err)
		proxyRemoved = removed
		failures = append(failures, err)
	} else {
		proxyRemoved = removed
	}

	sourceLinkCleanupSucceeded := false
	if removed, err := database.DeleteOrphanProxyScrapeSiteRelations(ctx); err != nil {
		log.Error("Failed to cleanup orphan proxy scrape-source links", "error", err)
		failures = append(failures, err)
	} else {
		sourceLinkRemoved = removed
		sourceLinkCleanupSucceeded = true
	}

	if sourceLinkCleanupSucceeded {
		if err := database.ValidateProxyScrapeSiteCascadeConstraints(ctx); err != nil {
			log.Error("Failed to validate proxy scrape-source cascade constraints", "error", err)
			failures = append(failures, err)
		}
	}

	if removed, err := database.DeleteOrphanScrapeSites(ctx); err != nil {
		log.Error("Failed to cleanup orphan scrape sites", "error", err)
		failures = append(failures, err)
	} else {
		siteRemoved = removed
	}

	err := errors.Join(failures...)
	support.RecordMaintenanceRun("orphan_cleanup", err)
	if proxyRemoved == 0 && sourceLinkRemoved == 0 && siteRemoved == 0 {
		return err
	}

	log.Info(
		"Orphan cleanup completed",
		"proxies_removed", proxyRemoved,
		"proxy_scrape_source_links_removed", sourceLinkRemoved,
		"scrape_sites_removed", siteRemoved,
		"duration", time.Since(start),
	)
	return err
}
