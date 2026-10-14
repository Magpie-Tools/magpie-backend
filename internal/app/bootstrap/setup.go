package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/charmbracelet/log"
	"github.com/redis/go-redis/v9"

	"magpie/internal/blacklist"
	"magpie/internal/checkerconfig"
	"magpie/internal/config"
	"magpie/internal/database"
	"magpie/internal/domain"
	"magpie/internal/geolite"
	"magpie/internal/jobs/checker"
	"magpie/internal/jobs/checker/judges"
	"magpie/internal/jobs/maintenance"
	proxyqueue "magpie/internal/jobs/queue/proxy"
	sitequeue "magpie/internal/jobs/queue/sites"
	jobruntime "magpie/internal/jobs/runtime"
	"magpie/internal/jobs/scraper"
	"magpie/internal/rotatingproxy"
	"magpie/internal/support"
)

const startupProxyBatchSize = 2000
const startupQueueBootstrapLockKey = "magpie:leader:startup_queue_bootstrap"
const envStartupQueueBootstrapAsync = "STARTUP_QUEUE_BOOTSTRAP_ASYNC"
const startupQueueBootstrapRetryDelay = 10 * time.Second

var startupQueueBootstrapCompleted atomic.Bool
var startupQueueBootstrapRunning atomic.Bool

func Setup(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	fixtures, err := config.DevelopmentFixturesEnabled()
	if err != nil {
		return err
	}

	if err := config.ReadSettings(); err != nil {
		return fmt.Errorf("failed to load settings: %w", err)
	}

	if redisClient, err := support.GetRedisClient(); err != nil {
		log.Warn("Redis synchronization disabled", "error", err)
	} else {
		config.EnableRedisSynchronization(ctx, redisClient)
		judges.EnableRedisSynchronization(ctx, redisClient)
		geolite.EnableRedisDistribution(ctx, redisClient)
	}

	if _, err := database.SetupDB(); err != nil {
		return fmt.Errorf("failed to set up database: %w", err)
	}
	if fixtures {
		redisClient, _ := support.GetRedisClient()
		if err := checkerconfig.Initialize(ctx, redisClient, database.LoadCheckerWorkspace, database.ListCheckerWorkspaces); err != nil {
			return fmt.Errorf("load fixture checker settings: %w", err)
		}
		if err := database.RefreshDashboardCaches(ctx); err != nil {
			return fmt.Errorf("prewarm fixture dashboard: %w", err)
		}
		go database.StartReadModelRefreshRoutine(ctx)
		go jobruntime.StartDashboardCacheRoutine(ctx)
		startupQueueBootstrapCompleted.Store(true)
		log.Info("Development fixtures enabled; background jobs and rotating listeners are stopped")
		return nil
	}
	go maintainRuntimeCaches(ctx)
	config.SetBetweenTime()
	go proxyqueue.PublicProxyQueue.StartIntervalUpdates(ctx)
	go sitequeue.PublicScrapeSiteQueue.StartIntervalUpdates(ctx)

	if err := blacklist.Initialize(ctx); err != nil {
		log.Warn("Blacklist initialisation failed", "error", err)
	}

	if redisClient, err := support.GetRedisClient(); err != nil {
		log.Warn("Blacklist synchronization disabled", "error", err)
	} else {
		blacklist.EnableRedisSynchronization(ctx, redisClient)
	}

	judgeSetup()
	if err := checkerconfig.Initialize(ctx, nil, database.LoadCheckerWorkspace, database.ListCheckerWorkspaces); err != nil {
		return fmt.Errorf("load checker settings snapshots: %w", err)
	}

	go recoverRedisSynchronization(ctx)

	pausedForFailure, inactiveProxies, refreshProxies, cleanupErr := database.CleanupAutoRemovalViolations(ctx)
	if cleanupErr != nil {
		log.Error("auto-pause cleanup failed", "error", cleanupErr)
	} else if pausedForFailure > 0 {
		log.Info(
			"Auto-pause cleanup completed",
			"managed_proxies_paused", pausedForFailure,
			"inactive_routes", len(inactiveProxies),
			"refreshed_routes", len(refreshProxies),
		)
		if len(refreshProxies) > 0 {
			if err := proxyqueue.PublicProxyQueue.AddToQueue(refreshProxies); err != nil {
				log.Warn("failed to refresh auto-paused route payloads", "error", err)
			}
		}
		if len(inactiveProxies) > 0 {
			if err := proxyqueue.PublicProxyQueue.RemoveFromQueue(inactiveProxies); err != nil {
				log.Warn("failed to remove inactive routes from queue", "error", err)
			}
		}
	}

	capacityPaused, capacityInactiveProxies, capacityRefreshProxies, limitCleanupErr := database.CleanupProxyLimitViolations(ctx)
	if limitCleanupErr != nil {
		log.Error("workspace capacity cleanup failed", "error", limitCleanupErr)
	} else if capacityPaused > 0 {
		log.Info(
			"Workspace capacity cleanup completed",
			"managed_proxies_paused", capacityPaused,
			"inactive_routes", len(capacityInactiveProxies),
			"refreshed_routes", len(capacityRefreshProxies),
		)
		if len(capacityRefreshProxies) > 0 {
			if err := proxyqueue.PublicProxyQueue.AddToQueue(capacityRefreshProxies); err != nil {
				log.Warn("failed to refresh capacity-paused route payloads", "error", err)
			}
		}
		if len(capacityInactiveProxies) > 0 {
			if err := proxyqueue.PublicProxyQueue.RemoveFromQueue(capacityInactiveProxies); err != nil {
				log.Warn("failed to remove capacity-paused routes from queue", "error", err)
			}
		}
	}

	go refreshPublicIP(ctx)

	startupQueueBootstrapCompleted.Store(false)
	if support.GetEnvBool(envStartupQueueBootstrapAsync, true) {
		startBootstrapStartupQueuesAsync(ctx)
	} else {
		if err := waitForStartupQueues(ctx); err != nil {
			return err
		}
	}
	go monitorQueueState(ctx)

	rotatingproxy.GlobalManager.StartAll()
	go func() {
		<-ctx.Done()
		rotatingproxy.GlobalManager.StopAll()
	}()
	syncIntervalSeconds := support.GetEnvInt("ROTATING_PROXY_SYNC_INTERVAL_SECONDS", 10)
	if syncIntervalSeconds <= 0 {
		syncIntervalSeconds = 10
	}
	go rotatingproxy.GlobalManager.StartSyncLoop(ctx, time.Duration(syncIntervalSeconds)*time.Second)

	dashboardCacheStart := time.Now()
	if err := database.RefreshDashboardCaches(ctx); err != nil {
		log.Warn("Dashboard cache prewarm failed; first request may be slower", "error", err)
	} else {
		log.Info("Dashboard caches prewarmed", "duration", time.Since(dashboardCacheStart))
	}

	// Routines

	go judges.StartJudgeRoutine(ctx)
	go database.StartReadModelRefreshRoutine(ctx)
	go jobruntime.StartDashboardCacheRoutine(ctx)
	go jobruntime.StartProxyStatisticsRoutine(ctx)
	go jobruntime.StartWorkspaceUsageRoutine(ctx)
	go jobruntime.StartProxyStatisticsRetentionRoutine(ctx)
	go jobruntime.StartProxyTimelineRetentionRoutine(ctx)
	go jobruntime.StartProxyHistoryRoutine(ctx)
	go jobruntime.StartProxySnapshotRoutine(ctx)
	go jobruntime.StartProxyGeoRefreshRoutine(ctx)
	go maintenance.StartOrphanCleanupRoutine(ctx)
	go maintenance.StartPasswordResetCleanupRoutine(ctx)
	go maintenance.StartWorkspaceInvitationCleanupRoutine(ctx)
	go jobruntime.StartGeoLiteUpdateRoutine(ctx)
	go jobruntime.StartAbuseIPDBRoutine(ctx)
	go jobruntime.StartEmailDeliveryRoutine(ctx)
	go jobruntime.StartAlertEvaluationRoutine(ctx)
	go jobruntime.StartAlertDeliveryRoutine(ctx)
	go jobruntime.StartEmailDeliveryMaintenanceRoutine(ctx)
	go blacklist.StartRefreshRoutine(ctx)
	go checker.ThreadDispatcher(ctx)
	scraper.StartInfrastructure()
	go scraper.ThreadDispatcher(ctx)

	return nil
}

func StartupQueueBootstrapCompleted() bool {
	return startupQueueBootstrapCompleted.Load()
}

func judgeSetup() {
	addJudgeRelationsToCache()
	AddDefaultJudgesToWorkspaces()
}

func startBootstrapStartupQueuesAsync(ctx context.Context) {
	if !startupQueueBootstrapRunning.CompareAndSwap(false, true) {
		return
	}

	go func() {
		defer startupQueueBootstrapRunning.Store(false)
		for {
			err := initializeRuntimeQueues(ctx)
			if err == nil {
				log.Info("Startup queue bootstrap completed")
				return
			}
			if errors.Is(err, context.Canceled) {
				return
			}

			log.Error("Startup queue bootstrap failed; retrying", "error", err, "retry_in", startupQueueBootstrapRetryDelay)
			select {
			case <-ctx.Done():
				return
			case <-time.After(startupQueueBootstrapRetryDelay):
			}
		}
	}()
}

func waitForStartupQueues(ctx context.Context) error {
	for {
		err := initializeRuntimeQueues(ctx)
		if !errors.Is(err, support.ErrLeaderLockNotAcquired) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(startupQueueBootstrapRetryDelay):
		}
	}
}

func initializeRuntimeQueues(ctx context.Context) error {
	// Startup refreshes existing payloads as well as missing queues. The marker
	// can survive a deployment whose queue format or active ownership changed.
	startupQueueBootstrapCompleted.Store(false)
	err := bootstrapStartupQueues(ctx)
	if err != nil && !errors.Is(err, support.ErrLeaderLockNotAcquired) {
		return err
	}
	return recoverRuntimeQueueState(ctx)
}

func bootstrapStartupQueues(ctx context.Context) error {
	err := support.RunLeaderTaskOnce(ctx, startupQueueBootstrapLockKey, support.DefaultLeadershipTTL, func(leaderCtx context.Context) error {
		if err := queueStartupProxies(leaderCtx); err != nil {
			return err
		}
		if err := queueStartupScrapeSites(leaderCtx); err != nil {
			return err
		}
		client, err := support.GetRedisClientContext(leaderCtx)
		if err != nil {
			return err
		}
		return client.Set(leaderCtx, runtimeQueueBootstrapMarker, "ready", 0).Err()
	})
	if err == nil {
		return nil
	}
	if errors.Is(err, support.ErrLeaderLockNotAcquired) {
		log.Info("Skipped startup queue bootstrap on follower instance")
	}
	return err
}

func queueStartupProxies(ctx context.Context) error {
	queuedProxies := 0
	err := database.ForEachProxyBatch(startupProxyBatchSize, func(proxies []domain.Proxy) error {
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		missingGeo := database.FilterProxiesMissingGeo(proxies)
		if len(missingGeo) > 0 {
			database.AsyncEnrichProxyMetadata(missingGeo)
		}

		if err := proxyqueue.PublicProxyQueue.AddToQueue(proxies); err != nil {
			return err
		}
		queuedProxies += len(proxies)
		return nil
	})
	if err != nil {
		return fmt.Errorf("queue startup proxies: %w", err)
	}
	if queuedProxies > 0 {
		log.Infof("Added %d proxies to queue", queuedProxies)
	}
	return nil
}

func queueStartupScrapeSites(ctx context.Context) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	scrapeSites, err := database.GetAllScrapeSites()
	if err != nil {
		return fmt.Errorf("load startup scrape sites: %w", err)
	}

	filtered := make([]domain.ScrapeSite, 0, len(scrapeSites))
	blocked := 0
	for _, site := range scrapeSites {
		if config.IsWebsiteBlocked(site.URL) {
			blocked++
			continue
		}
		filtered = append(filtered, site)
	}

	if blocked > 0 {
		log.Info("Skipped blocked scrape sites", "count", blocked)
	}

	if len(filtered) == 0 {
		return nil
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}

	if err := sitequeue.PublicScrapeSiteQueue.AddToQueue(filtered); err != nil {
		return fmt.Errorf("queue startup scrape sites: %w", err)
	}
	log.Infof("Added %d scrape sites to queue", len(filtered))
	return nil
}

const runtimeQueueBootstrapMarker = "magpie:runtime:queues_bootstrapped"

func recoverRedisSynchronization(ctx context.Context) {
	for ctx.Err() == nil {
		client, err := support.GetRedisClient()
		if err == nil {
			config.EnableRedisSynchronization(ctx, client)
			judges.EnableRedisSynchronization(ctx, client)
			geolite.EnableRedisDistribution(ctx, client)
			blacklist.EnableRedisSynchronization(ctx, client)
			err = checkerconfig.EnableRedisSynchronization(ctx, client)
			if err == nil {
				return
			}
		}
		log.Warn("Redis synchronization unavailable; retrying", "error", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(startupQueueBootstrapRetryDelay):
		}
	}
}

func maintainRuntimeCaches(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		database.PruneDNSCache(time.Now())
		scraper.PruneRobotsCache(time.Now())
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func monitorQueueState(ctx context.Context) {
	ticker := time.NewTicker(startupQueueBootstrapRetryDelay)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		err := recoverRuntimeQueueState(ctx)
		support.RecordMaintenanceRun("queue_recovery", err)
		if err != nil && ctx.Err() == nil {
			log.Error("Queue state recovery failed", "error", err)
		}
	}
}

func recoverRuntimeQueueState(ctx context.Context) error {
	opCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	client, err := support.GetRedisClientContext(opCtx)
	cancel()
	if err != nil {
		startupQueueBootstrapCompleted.Store(false)
		return err
	}
	return recoverQueueState(ctx, client, bootstrapStartupQueues)
}

func queueBootstrapMarkerExists(ctx context.Context, client *redis.Client) (bool, error) {
	opCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	exists, err := client.Exists(opCtx, runtimeQueueBootstrapMarker).Result()
	return exists != 0, err
}

func recoverQueueState(ctx context.Context, client *redis.Client, restore func(context.Context) error) error {
	exists, err := queueBootstrapMarkerExists(ctx, client)
	if err != nil {
		startupQueueBootstrapCompleted.Store(false)
		return err
	}
	if exists {
		startupQueueBootstrapCompleted.Store(true)
		return nil
	}
	startupQueueBootstrapCompleted.Store(false)
	restoreErr := restore(ctx)
	if restoreErr != nil && !errors.Is(restoreErr, support.ErrLeaderLockNotAcquired) {
		return restoreErr
	}
	exists, err = queueBootstrapMarkerExists(ctx, client)
	if err != nil {
		return err
	}
	if !exists {
		if restoreErr != nil {
			return restoreErr
		}
		return errors.New("queue reconstruction did not restore bootstrap marker")
	}
	startupQueueBootstrapCompleted.Store(true)
	return nil
}

func refreshPublicIP(ctx context.Context) {
	for ctx.Err() == nil {
		delay := 15 * time.Minute
		lookup := config.GetConfig().Checker.IpLookup
		if lookup != "" {
			body, err := checker.DefaultRequestWithContext(ctx, lookup)
			if err != nil {
				log.Warn("Public IP lookup failed", "error", err)
				delay = 3 * time.Second
			} else if ip := support.FindIP(body); ip != "" {
				if ip != config.GetCurrentIp() {
					config.SetCurrentIp(ip)
					log.Info("Public IP updated", "ip", ip)
				}
			} else {
				delay = 3 * time.Second
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
}
