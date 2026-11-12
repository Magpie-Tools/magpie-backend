package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/charmbracelet/log"
	"github.com/redis/go-redis/v9"

	"magpie/internal/alerts"
	"magpie/internal/blacklist"
	"magpie/internal/checkerconfig"
	"magpie/internal/checkpace"
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
	"magpie/internal/mail"
	"magpie/internal/rotatingproxy"
	"magpie/internal/scrapestore"
	"magpie/internal/support"
)

const startupProxyBatchSize = 2000
const startupQueueBootstrapLockKey = "magpie:leader:startup_queue_bootstrap"
const envStartupQueueBootstrapAsync = "STARTUP_QUEUE_BOOTSTRAP_ASYNC"
const startupQueueBootstrapRetryDelay = 10 * time.Second

var startupQueueBootstrapCompleted atomic.Bool
var startupQueueBootstrapRunning atomic.Bool

// Setup starts the process's background routines. pace is the process's
// Pacer: Setup configures and runs it and gives it to every routine that paces
// work. The caller passes the same Pacer to the queues and the API.
func Setup(ctx context.Context, pace *checkpace.Pacer) (*Runtime, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	running := newRuntime(ctx)
	if pace == nil {
		return running, errors.New("bootstrap: Setup needs the process's Pacer")
	}
	running.pace = pace
	return running, running.setup(ctx)
}

func (running *Runtime) setup(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	fixtures, err := config.DevelopmentFixturesEnabled()
	if err != nil {
		return err
	}

	if err := config.ReadSettings(); err != nil {
		return fmt.Errorf("failed to load settings: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
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
	if err := ctx.Err(); err != nil {
		return err
	}
	if fixtures {
		redisClient, _ := support.GetRedisClient()
		if err := checkerconfig.Initialize(ctx, redisClient, database.LoadCheckerWorkspace, database.ListCheckerWorkspaces); err != nil {
			return fmt.Errorf("load fixture checker settings: %w", err)
		}
		if err := refreshDashboardCachesWithin(ctx, dashboardCachePrewarmTimeout, database.RefreshDashboardCaches); err != nil {
			if parentErr := ctx.Err(); parentErr != nil {
				return parentErr
			}
			return fmt.Errorf("prewarm fixture dashboard: %w", err)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		running.readModels.start(database.StartReadModelRefreshRoutine)
		running.producers.start(jobruntime.StartDashboardCacheRoutine)
		startupQueueBootstrapCompleted.Store(true)
		log.Info("Development fixtures enabled; background jobs and rotating listeners are stopped")
		return nil
	}
	running.producers.start(maintainRuntimeCaches)
	config.SetBetweenTime()

	if err := blacklist.Initialize(ctx); err != nil {
		log.Warn("Blacklist initialisation failed", "error", err)
	}
	if err := ctx.Err(); err != nil {
		return err
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
	if err := ctx.Err(); err != nil {
		return err
	}
	startCheckPacing(ctx, running)

	running.producers.start(recoverRedisSynchronization)

	pausedForFailure, inactiveProxies, refreshProxies, cleanupErr := database.CleanupAutoRemovalViolations(ctx)
	if err := ctx.Err(); err != nil {
		return err
	}
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
	if err := ctx.Err(); err != nil {
		return err
	}
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

	if err := ctx.Err(); err != nil {
		return err
	}
	running.producers.start(refreshPublicIP)

	startupQueueBootstrapCompleted.Store(false)
	if support.GetEnvBool(envStartupQueueBootstrapAsync, true) {
		running.producers.start(startBootstrapStartupQueuesAsync)
	} else {
		if err := waitForStartupQueues(ctx); err != nil {
			return err
		}
	}
	running.producers.start(monitorQueueState)

	if err := prewarmDashboardCaches(ctx); err != nil {
		return err
	}

	// Routines

	if err := ctx.Err(); err != nil {
		return err
	}
	// Register final drains before opening listeners or starting check producers.
	running.readModels.start(database.StartReadModelRefreshRoutine)
	running.consumers.start(jobruntime.StartProxyStatisticsRoutine)
	running.consumers.start(jobruntime.StartWorkspaceUsageRoutine)
	if err := ctx.Err(); err != nil {
		return err
	}
	rotatingproxy.GlobalManager.StartAll()
	syncIntervalSeconds := support.GetEnvInt("ROTATING_PROXY_SYNC_INTERVAL_SECONDS", 10)
	if syncIntervalSeconds <= 0 {
		syncIntervalSeconds = 10
	}
	running.producers.start(func(ctx context.Context) {
		rotatingproxy.GlobalManager.StartSyncLoop(ctx, time.Duration(syncIntervalSeconds)*time.Second)
		rotatingproxy.GlobalManager.StopAll()
	})

	// One checker runtime and one scraper dispatcher per process, both paced
	// by the process's Pacer. The checker's batchers start before its
	// dispatcher and stop after it has joined its workers.
	checkers := checker.NewRuntime(running.pace)
	scrapers := scraper.NewDispatcher(running.pace)
	running.batchers.start(checkers.RunBatchers)
	for _, routine := range []func(context.Context){
		judges.StartJudgeRoutine,
		jobruntime.StartDashboardCacheRoutine,
		jobruntime.StartProxyStatisticsRetentionRoutine,
		jobruntime.StartProxyTimelineRetentionRoutine,
		jobruntime.StartProxyHistoryRoutine,
		jobruntime.StartProxySnapshotRoutine,
		geolite.StartProxyGeoRefreshRoutine,
		maintenance.StartOrphanCleanupRoutine,
		maintenance.StartPasswordResetCleanupRoutine,
		maintenance.StartWorkspaceInvitationCleanupRoutine,
		geolite.StartGeoLiteUpdateRoutine,
		jobruntime.StartAbuseIPDBRoutine,
		jobruntime.StartIPGeolocationRoutine,
		mail.StartEmailDeliveryRoutine,
		alerts.StartAlertEvaluationRoutine,
		alerts.StartAlertDeliveryRoutine,
		mail.StartEmailDeliveryMaintenanceRoutine,
		blacklist.StartRefreshRoutine,
		checkers.ThreadDispatcher,
		scrapers.ThreadDispatcher,
	} {
		if err := ctx.Err(); err != nil {
			return err
		}
		running.producers.start(routine)
	}

	return nil
}

// startCheckPacing publishes workspace cadences before any route is queued,
// so startup imports already spread routes over their workspaces' intervals.
func startCheckPacing(ctx context.Context, running *Runtime) {
	pace := running.pace
	pace.SetPauseAfter(checkUsagePauseAfter())
	pace.Configure(checkpace.Store{
		Load:       database.LoadWorkspaceAllowances,
		FlushUsage: database.FlushWorkspaceCheckUsage,
	})
	convertLegacyCadence(ctx)
	if err := database.PruneWorkspaceUsageFlushes(ctx); err != nil {
		log.Warn("prune workspace check usage batch marks", "error", err)
	}
	if err := pace.Refresh(ctx); err != nil {
		log.Warn("initial workspace check allowance load failed; using fallback cadence", "error", err)
	}
	config.OnConfigApplied(pace.RequestRefresh)
	running.startCheckUsageMeter()
}

// startCheckUsageMeter runs the process's Pacer with the consumers. Checker
// workers charge requests until the producers have stopped, and the Pacer's
// last usage flush must follow the last charge.
func (r *Runtime) startCheckUsageMeter() { r.consumers.start(r.pace.Run) }

// envCheckUsagePauseAfter sets how long check usage may stay unwritten before
// this instance pauses checking. Zero never pauses.
const envCheckUsagePauseAfter = "CHECK_USAGE_PAUSE_AFTER"

func checkUsagePauseAfter() time.Duration {
	raw := strings.TrimSpace(os.Getenv(envCheckUsagePauseAfter))
	if raw == "" {
		return checkpace.DefaultPauseAfter
	}
	if raw == "0" {
		return 0
	}
	value, err := time.ParseDuration(raw)
	if err != nil || value < 0 {
		log.Warn("invalid "+envCheckUsagePauseAfter+"; using the default", "value", raw, "default", checkpace.DefaultPauseAfter)
		return checkpace.DefaultPauseAfter
	}
	return value
}

// convertLegacyCadence keeps an upgraded installation at the cadence of its
// older settings file. Old timers had no monthly limit, so the converted
// instance default is unlimited, and every workspace receives the old timer as
// its fixed interval. Nothing becomes faster or slower, and nothing stops.
func convertLegacyCadence(ctx context.Context) {
	legacy := config.PendingLegacyCadence()
	if legacy == nil {
		return
	}
	period := func(timer *config.Timer) time.Duration {
		if timer == nil {
			return 0
		}
		return config.CalculateBetweenTime(*timer)
	}
	checkPeriod, scrapePeriod := period(legacy.CheckerTimer), period(legacy.ScraperTimer)
	checks, scrapes, err := database.PreserveLegacyCadence(ctx, checkPeriod, scrapePeriod)
	if err != nil {
		log.Warn("convert legacy checker and scraper timers", "error", err)
		return
	}
	cfg := config.GetConfig()
	if checkPeriod > 0 {
		cfg.Checker.UnlimitedMonthlyChecks = true
	}
	if scrapePeriod > 0 {
		cfg.Scraper.UnlimitedMonthlyScrapes = true
	}
	if err := config.SetConfig(cfg); err != nil {
		log.Warn("save converted check allowances", "error", err)
		return
	}
	config.ClearLegacyCadence()
	log.Info("Converted legacy checker and scraper timers to fixed workspace intervals without a monthly limit",
		"check_interval", checkPeriod, "workspaces_with_check_interval", checks,
		"scrape_interval", scrapePeriod, "workspaces_with_scrape_interval", scrapes)
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
		missingGeo := geolite.FilterProxiesMissingGeo(proxies)
		if len(missingGeo) > 0 {
			geolite.AsyncEnrichProxyMetadata(missingGeo)
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
	scrapeSites, err := scrapestore.GetAllScrapeSites()
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
		geolite.PruneDNSCache(time.Now())
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
