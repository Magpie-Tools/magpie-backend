package scraper

import (
	"context"
	"errors"
	"fmt"
	"magpie/internal/blacklist"
	"magpie/internal/config"
	"magpie/internal/database"
	"magpie/internal/domain"
	proxyqueue "magpie/internal/jobs/queue/proxy"
	sitequeue "magpie/internal/jobs/queue/sites"
	"magpie/internal/support"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/charmbracelet/log"
)

/* ─────────────────────────────  thread control  ─────────────────────────── */

var (
	currentThreads atomic.Uint32
	stopThread     = make(chan struct{}) // signals a worker to exit
	scraperNow     = time.Now
)

const (
	scrapePopErrorLogInterval   = 30 * time.Second
	defaultPostProcessWorkers   = 8
	defaultPostProcessQueueSize = 16
	maxPostProcessWorkers       = 64
	maxPostProcessQueueSize     = 4096
	envPostProcessWorkers       = "SCRAPER_POST_PROCESS_WORKERS"
	envPostProcessQueueSize     = "SCRAPER_POST_PROCESS_QUEUE_CAPACITY"
)

var (
	postProcessQueue       chan scrapedHTMLJob
	startOnce              sync.Once
	postProcessWorkers     sync.WaitGroup
	scrapePopErrorLogState struct {
		mu         sync.Mutex
		lastLogAt  time.Time
		suppressed int
	}
)

type scrapedHTMLJob struct {
	site        domain.ScrapeSite
	html        string
	mode        string
	attemptedAt time.Time
}

/* ─────────────────────────────  startup  ─────────────────────────────────── */

func StartInfrastructure() {
	startOnce.Do(func() {
		postProcessQueue = make(chan scrapedHTMLJob, resolvePostProcessQueueSize())
		startScrapedHTMLWorkers(resolvePostProcessWorkers())
	})
}

/* ─────────────────────────────  dispatcher  ─────────────────────────────── */

func ThreadDispatcher(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	StartInfrastructure()
	var workers sync.WaitGroup
	defer func() {
		workers.Wait()
		close(postProcessQueue)
		postProcessWorkers.Wait()
		sharedBrowser().close()
	}()

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		cfg := config.GetConfig()

		var target uint32
		if cfg.Scraper.DynamicThreads {
			target = autoThreadCount(cfg)
		} else {
			target = cfg.Scraper.Threads
		}

		activeThreads := currentThreads.Load()
		for activeThreads < target {
			launchScraperWorker(ctx, &workers)
			activeThreads = currentThreads.Load()
		}
		// Non-blocking stop requests avoid dispatcher stalls when counts drift.
		for activeThreads > target {
			if !requestScraperWorkerStop() {
				break
			}
			activeThreads--
		}

		log.Debug("Scraper threads", "active", currentThreads.Load())
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func launchScraperWorker(ctx context.Context, workers *sync.WaitGroup) {
	currentThreads.Add(1)
	workers.Add(1)
	go func() {
		defer workers.Done()
		defer func() {
			if recovered := recover(); recovered != nil {
				log.Error("scraper worker panicked", "panic", recovered)
			}
			currentThreads.Add(^uint32(0))
		}()

		scrapeWorker(ctx)
	}()
}

func requestScraperWorkerStop() bool {
	select {
	case stopThread <- struct{}{}:
		return true
	default:
		return false
	}
}

// Each URL is fetched once for each mode requested by its current workspaces.
// HTTP groups run first, and browser capacity failures do not wait for a slot.
func scrapeWorker(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-stopThread:
			cancel()
		case <-ctx.Done():
		}
	}()
	for ctx.Err() == nil {
		job, err := sitequeue.PublicScrapeSiteQueue.GetNextScrapeSiteContext(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			logScrapeQueuePopError(err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
			continue
		}
		processScrapeSiteJob(ctx, job)
	}
}

func processScrapeSiteJob(ctx context.Context, job sitequeue.ScrapeSiteJob) {
	workCtx, stopRenewal := sitequeue.PublicScrapeSiteQueue.MaintainScrapeSiteLease(ctx, job)
	defer stopRenewal() // Also join the renewal goroutine if a worker panics.
	queued := scrapeSite(workCtx, &job)
	renewalErr := stopRenewal()
	if !queued {
		return
	}
	if renewalErr != nil {
		log.Warn("scrape source lease renewal failed", "site_id", job.Site.ID, "err", renewalErr)
	}
	// Save completed modes even during shutdown. Provider deadlines remain useful
	// observations when renewal failed or another worker took over.
	completed, err := sitequeue.PublicScrapeSiteQueue.CompleteScrapeSite(job)
	if err != nil {
		log.Error("requeue scrape source", "site_id", job.Site.ID, "err", err)
	} else if !completed && renewalErr == nil {
		log.Warn("scrape source lease lost before schedule completion", "site_id", job.Site.ID)
	}
}

func scrapeSite(ctx context.Context, job *sitequeue.ScrapeSiteJob) (queued bool) {
	site := job.Site
	if job.Schedule.RetryAfterMS > time.Now().UnixMilli() {
		return true
	}
	lookupCtx, cancelLookup := context.WithTimeout(ctx, 5*time.Second)
	subscriptions, err := database.GetScrapeSiteSubscriptions(lookupCtx, site.ID)
	cancelLookup()
	if err != nil {
		log.Error("load scrape subscriptions", "site_id", site.ID, "err", err)
		job.Schedule.DeferredUntilMS = time.Now().Add(min(browserRetryDelay, job.Interval)).UnixMilli()
		job.Schedule.DeferredReason = "Waiting for source settings to become available"
		return true
	}
	if len(subscriptions) == 0 || config.IsWebsiteBlocked(site.URL) {
		if err := sitequeue.PublicScrapeSiteQueue.RemoveFromQueue([]domain.ScrapeSite{site}); err != nil {
			log.Error("remove unused scrape source", "err", err)
		}
		// Caller must not recreate the removed queue member.
		return false
	}
	job.Schedule.DeferredUntilMS = 0
	job.Schedule.DeferredReason = ""
	if job.Schedule.Modes == nil {
		job.Schedule.Modes = make(map[string]sitequeue.ScrapeModeSchedule)
	}
	requestedModes := make(map[string]bool)
	for _, subscription := range subscriptions {
		requestedModes[subscription.FetchMode] = true
	}
	for mode := range job.Schedule.Modes {
		if !requestedModes[mode] {
			delete(job.Schedule.Modes, mode)
		}
	}
	cfg := config.GetConfig()
	timeout := effectiveScrapeTimeout(time.Duration(cfg.Scraper.Timeout) * time.Millisecond)
	var robotsBlocked bool
	if cfg.Scraper.RespectRobots {
		result, err := CheckRobotsAllowance(site.URL, timeout)
		if err != nil {
			log.Warn("robots.txt check failed", "url", site.URL, "err", err)
		}
		robotsBlocked = result.RobotsFound && !result.Allowed
	}
	for _, mode := range []string{domain.ScrapeFetchHTTP, domain.ScrapeFetchBrowser} {
		if job.Schedule.Modes[mode].NextAttemptMS > time.Now().UnixMilli() {
			continue
		}
		group := site
		group.Workspaces = nil
		for _, subscription := range subscriptions {
			if subscription.FetchMode == mode {
				group.Workspaces = append(group.Workspaces, domain.Workspace{ID: subscription.WorkspaceID})
			}
		}
		if len(group.Workspaces) == 0 {
			continue
		}
		attemptedAt := time.Now()
		if robotsBlocked {
			recordScrapeResult(group, mode, "blocked", 0, nil, attemptedAt)
			job.Schedule.Modes[mode] = sitequeue.ScrapeModeSchedule{NextAttemptMS: time.Now().Add(job.Interval).UnixMilli()}
			continue
		}
		html, err := ScraperRequestContext(ctx, site.URL, mode, timeout)
		if err != nil {
			// Retain an observed provider deadline even if cancellation or lease
			// loss prevents this worker from committing its normal mode progress.
			var status *scrapeHTTPStatusError
			if errors.As(err, &status) && !status.retryAfter.IsZero() {
				job.Schedule.RetryAfterMS = max(job.Schedule.RetryAfterMS, status.retryAfter.UnixMilli())
			}
			if isScrapeDeferred(err) {
				job.Schedule.Modes[mode] = sitequeue.ScrapeModeSchedule{
					NextAttemptMS:  time.Now().Add(min(browserRetryDelay, job.Interval)).UnixMilli(),
					DeferredReason: "Waiting for browser capacity or availability",
				}
				continue
			}
			if ctx.Err() != nil {
				break
			}
			log.Warn("scrape failed", "url", site.URL, "mode", mode, "err", err)
			recordScrapeResult(group, mode, "error", 0, err, attemptedAt)
			job.Schedule.Modes[mode] = sitequeue.ScrapeModeSchedule{NextAttemptMS: time.Now().Add(job.Interval).UnixMilli()}
			if job.Schedule.RetryAfterMS > time.Now().UnixMilli() {
				// Both modes fetch the same URL and must honor this deadline.
				break
			}
			continue
		}
		if err := enqueueScrapedHTML(ctx, group, html, mode, attemptedAt); err != nil {
			break
		}
		job.Schedule.Modes[mode] = sitequeue.ScrapeModeSchedule{NextAttemptMS: time.Now().Add(job.Interval).UnixMilli()}
	}
	// Modes deferred by Retry-After remain pending even if no fetch ran yet.
	for mode := range requestedModes {
		if _, ok := job.Schedule.Modes[mode]; !ok {
			job.Schedule.Modes[mode] = sitequeue.ScrapeModeSchedule{}
		}
	}
	return true
}

func recordScrapeResult(site domain.ScrapeSite, mode, status string, count int, scrapeErr error, attemptedAt time.Time) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := database.RecordScrapeResult(ctx, site.ID, support.GetWorkspaceIDsFromList(site.Workspaces), mode, status, count, scrapeErr, attemptedAt); err != nil {
		log.Error("record scrape result", "site_id", site.ID, "err", err)
	}
}

func logScrapeQueuePopError(err error) {
	now := scraperNow()
	emit, suppressed := shouldEmitScrapePopErrorLog(now)
	if !emit {
		return
	}

	if suppressed > 0 {
		log.Error("pop scrape site", "err", err, "suppressed_repeats", suppressed)
		return
	}
	log.Error("pop scrape site", "err", err)
}

func shouldEmitScrapePopErrorLog(now time.Time) (bool, int) {
	scrapePopErrorLogState.mu.Lock()
	defer scrapePopErrorLogState.mu.Unlock()

	if scrapePopErrorLogState.lastLogAt.IsZero() || now.Sub(scrapePopErrorLogState.lastLogAt) >= scrapePopErrorLogInterval {
		suppressed := scrapePopErrorLogState.suppressed
		scrapePopErrorLogState.lastLogAt = now
		scrapePopErrorLogState.suppressed = 0
		return true, suppressed
	}

	scrapePopErrorLogState.suppressed++
	return false, 0
}

/* ─────────────────────────────  auto-sizing  ────────────────────────────── */

func autoThreadCount(cfg config.Config) uint32 {
	totalSites, err := sitequeue.PublicScrapeSiteQueue.GetScrapeSiteCount()
	if err != nil {
		log.Error("count sites", "err", err)
		return 1
	}

	instances, err := sitequeue.PublicScrapeSiteQueue.GetActiveInstances()
	if err != nil || instances == 0 {
		instances = 1
	}

	perInstance := (totalSites + int64(instances) - 1) / int64(instances)

	period := config.CalculateMillisecondsOfCheckingPeriod(cfg.Scraper.ScraperTimer)
	if period == 0 {
		log.Warn("scraper period 0 → forcing 1 day")
		period = 86_400_000
	}

	numerator := uint64(perInstance) * uint64(cfg.Scraper.Timeout) * uint64(cfg.Scraper.Retries+1)
	threads := (numerator + period - 1) / period
	maxThreads := cfg.Scraper.MaxThreads
	if maxThreads == 0 {
		if cfg.Scraper.Threads > 0 {
			maxThreads = cfg.Scraper.Threads
		} else {
			maxThreads = 250
		}
	}

	if threads == 0 && perInstance > 0 {
		threads = 1
	}
	if threads > uint64(maxThreads) {
		threads = uint64(maxThreads)
	}
	if threads > math.MaxUint32 {
		threads = math.MaxUint32
	}
	return uint32(threads)
}

func startScrapedHTMLWorkers(count int) {
	for i := 0; i < count; i++ {
		postProcessWorkers.Add(1)
		go func() {
			defer postProcessWorkers.Done()
			for job := range postProcessQueue {
				processScrapedHTMLJob(job)
			}
		}()
	}
}

func processScrapedHTMLJob(job scrapedHTMLJob) {
	count := 0
	var resultErr error
	defer func() {
		if recovered := recover(); recovered != nil {
			resultErr = fmt.Errorf("process scrape result: %v", recovered)
		}
		status := "success"
		if count == 0 {
			status = "empty"
		}
		if resultErr != nil {
			status = "error"
			log.Error("process scrape result", "site_id", job.site.ID, "err", resultErr)
		}
		recordScrapeResult(job.site, job.mode, status, count, resultErr, job.attemptedAt)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	subscriptions, err := database.GetScrapeSiteSubscriptions(ctx, job.site.ID)
	if err != nil {
		resultErr = err
		return
	}
	// A queued HTML job must not reattach a removed workspace or deliver results
	// from the previous mode after its setting changed.
	allowed := make(map[uint]bool, len(subscriptions))
	for _, subscription := range subscriptions {
		if subscription.FetchMode == job.mode {
			allowed[subscription.WorkspaceID] = true
		}
	}
	workspaces := job.site.Workspaces
	job.site.Workspaces = nil
	for _, workspace := range workspaces {
		if allowed[workspace.ID] {
			job.site.Workspaces = append(job.site.Workspaces, workspace)
		}
	}
	if len(job.site.Workspaces) == 0 {
		return
	}
	count, resultErr = handleScrapedHTML(job.site, job.html, job.mode)
}

func enqueueScrapedHTML(ctx context.Context, site domain.ScrapeSite, html, mode string, attemptedAt time.Time) error {
	job := scrapedHTMLJob{
		site: site,
		html: html, mode: mode, attemptedAt: attemptedAt,
	}

	select {
	case postProcessQueue <- job:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func resolvePostProcessWorkers() int {
	workers := support.GetEnvInt(envPostProcessWorkers, defaultPostProcessWorkers)
	if workers < 1 {
		return 1
	}
	if workers > maxPostProcessWorkers {
		return maxPostProcessWorkers
	}
	return workers
}

func resolvePostProcessQueueSize() int {
	size := support.GetEnvInt(envPostProcessQueueSize, defaultPostProcessQueueSize)
	if size < 1 {
		return 1
	}
	if size > maxPostProcessQueueSize {
		return maxPostProcessQueueSize
	}
	return size
}

/* ─────────────────────────────  downstream handlers  ────────────────────── */

func handleScrapedHTML(site domain.ScrapeSite, rawHTML, mode string) (int, error) {
	proxyList := support.GetProxiesOfHTML(rawHTML)
	parsedProxies := support.ParseScrapedTextToIPv4Proxies(strings.Join(proxyList, "\n"))

	parsedProxies, blocked := blacklist.FilterProxies(parsedProxies)
	if len(blocked) > 0 {
		log.Info("Skipped blacklisted scraped proxies", "count", len(blocked), "url", site.URL)
	}

	proxies, err := database.InsertAndGetProxiesWithWorkspace(parsedProxies, support.GetWorkspaceIDsFromList(site.Workspaces)...)
	if err != nil {
		return 0, fmt.Errorf("save %d scraped proxies: %w", len(parsedProxies), err)
	} else {
		proxiesToEnrich := database.FilterProxiesMissingGeo(proxies)
		if len(proxiesToEnrich) > 0 {
			database.AsyncEnrichProxyMetadata(proxiesToEnrich)
		}
	}

	err = database.AssociateProxiesToScrapeSite(site.ID, proxies)
	if err != nil {
		return 0, fmt.Errorf("associate proxies with source: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := database.ApplyScrapeSourceTags(ctx, site.ID, mode, support.GetWorkspaceIDsFromList(site.Workspaces), proxies); err != nil {
		return 0, fmt.Errorf("assign source tags: %w", err)
	}

	err = proxyqueue.PublicProxyQueue.AddToQueue(proxies)
	if err != nil {
		return 0, err
	}

	log.Info("scrape completed", "proxy_count", len(parsedProxies), "url", site.URL)
	return len(parsedProxies), nil
}
