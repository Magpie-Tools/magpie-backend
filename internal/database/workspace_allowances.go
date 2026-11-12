package database

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"magpie/internal/api/dto"
	"magpie/internal/checkerconfig"
	"magpie/internal/checkpace"
	"magpie/internal/config"
	"magpie/internal/domain"
	"magpie/internal/platform/pg"

	"gorm.io/gorm"
)

// LoadWorkspaceAllowances reads every workspace's monthly allowances and the
// current period's metered usage for the check pacer. It runs once per
// checkpace.RefreshInterval on each instance, never per check. Active route
// counts come from the usage meter maintained by lifecycle writes, so the
// managed-proxy table is scanned only for workspaces without any usage row.
func LoadWorkspaceAllowances(ctx context.Context, now time.Time) ([]checkpace.Allowance, error) {
	if pg.DB == nil {
		return nil, fmt.Errorf("database not initialised")
	}
	db := pg.DB
	if ctx != nil {
		db = db.WithContext(ctx)
	}
	return loadWorkspaceAllowances(db, now)
}

type workspaceAllowanceUsage struct {
	activeRoutes   uint64
	checks         uint64
	protocolChecks uint64
	scrapes        uint64
	periodStart    time.Time
	current        bool
}

// requestsPerCheck is the observed average of requests per protocol check,
// retries included, from the latest usage period. It needs enough checks to
// be meaningful; until then every check is assumed to need one request, and
// recomputing from the remaining allowance corrects the difference.
func (usage workspaceAllowanceUsage) requestsPerCheck() float64 {
	const minimumSample = 100
	if usage.protocolChecks < minimumSample || usage.checks < usage.protocolChecks {
		return 1
	}
	return float64(usage.checks) / float64(usage.protocolChecks)
}

func loadWorkspaceAllowances(db *gorm.DB, now time.Time) ([]checkpace.Allowance, error) {
	cfg := config.GetConfig()
	defaultChecks := cfg.Checker.MonthlyChecksPerWorkspace
	defaultScrapes := cfg.Scraper.MonthlyScrapesPerWorkspace

	var workspaces []domain.Workspace
	if err := db.Model(&domain.Workspace{}).Select("id", "check_interval_seconds", "scrape_interval_seconds").
		Order("id").Find(&workspaces).Error; err != nil {
		return nil, fmt.Errorf("list workspaces: %w", err)
	}
	if len(workspaces) == 0 {
		return nil, nil
	}
	workspaceIDs := make([]uint, 0, len(workspaces))
	requested := make(map[uint]domain.Workspace, len(workspaces))
	for _, workspace := range workspaces {
		workspaceIDs = append(workspaceIDs, workspace.ID)
		requested[workspace.ID] = workspace
	}

	var subscriptions []domain.WorkspaceSubscription
	if err := db.Select("workspace_id", "monthly_checks", "monthly_scrapes", "unlimited_checks", "unlimited_scrapes", "minimum_check_interval_seconds").
		Find(&subscriptions).Error; err != nil {
		return nil, fmt.Errorf("load workspace subscriptions: %w", err)
	}
	subscriptionByWorkspace := make(map[uint]domain.WorkspaceSubscription, len(subscriptions))
	for _, subscription := range subscriptions {
		subscriptionByWorkspace[subscription.WorkspaceID] = subscription
	}

	periodStart, _ := checkpace.UsagePeriod(now)
	usage, err := loadAllowanceUsage(db, periodStart)
	if err != nil {
		return nil, err
	}
	missing := make([]uint, 0)
	for _, workspaceID := range workspaceIDs {
		if _, ok := usage[workspaceID]; !ok {
			missing = append(missing, workspaceID)
		}
	}
	if err := countMissingActiveRoutes(db, missing, usage); err != nil {
		return nil, err
	}

	var sourceRows []struct {
		WorkspaceID uint
		Sources     uint64
	}
	if err := db.Model(&domain.WorkspaceScrapeSite{}).
		Select("workspace_id, COUNT(*) AS sources").
		Group("workspace_id").
		Scan(&sourceRows).Error; err != nil {
		return nil, fmt.Errorf("count scrape source subscriptions: %w", err)
	}
	sources := make(map[uint]uint64, len(sourceRows))
	for _, row := range sourceRows {
		sources[row.WorkspaceID] = row.Sources
	}

	allowances := make([]checkpace.Allowance, 0, len(workspaceIDs))
	for _, workspaceID := range workspaceIDs {
		subscription := subscriptionByWorkspace[workspaceID]
		used := usage[workspaceID]
		protocolChecks, err := protocolChecksPerCycle(db, workspaceID, used.activeRoutes, now)
		if err != nil {
			return nil, err
		}
		allowance := checkpace.Allowance{
			WorkspaceID:          workspaceID,
			UnlimitedChecks:      subscription.ChecksUnlimited(cfg.Checker.UnlimitedMonthlyChecks),
			UnlimitedScrapes:     subscription.ScrapesUnlimited(cfg.Scraper.UnlimitedMonthlyScrapes),
			MonthlyChecks:        subscription.EffectiveMonthlyChecks(defaultChecks),
			ChecksPerCycle:       uint64(math.Ceil(float64(protocolChecks) * used.requestsPerCheck())),
			MinimumCheckInterval: time.Duration(subscription.MinimumCheckIntervalSeconds) * time.Second,
			MonthlyScrapes:       subscription.EffectiveMonthlyScrapes(defaultScrapes),
			ScrapesPerCycle:      sources[workspaceID],

			RequestedCheckInterval:  time.Duration(requested[workspaceID].CheckIntervalSeconds) * time.Second,
			RequestedScrapeInterval: time.Duration(requested[workspaceID].ScrapeIntervalSeconds) * time.Second,
		}
		if used.current {
			allowance.UsedChecks = used.checks
			allowance.UsedScrapes = used.scrapes
		}
		allowances = append(allowances, allowance)
	}
	return allowances, nil
}

// The current and previous months are enough to find the latest active route
// count of every workspace that changed or was checked recently.
func loadAllowanceUsage(db *gorm.DB, periodStart time.Time) (map[uint]workspaceAllowanceUsage, error) {
	var rows []domain.WorkspaceUsagePeriod
	if err := db.Select("workspace_id", "period_start", "active_routes", "checks", "protocol_checks", "scrapes").
		Where("period_start >= ? AND period_start <= ?", periodStart.AddDate(0, -1, 0), periodStart).
		Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("load workspace usage periods: %w", err)
	}
	usage := make(map[uint]workspaceAllowanceUsage, len(rows))
	for _, row := range rows {
		start := row.PeriodStart.UTC()
		previous, seen := usage[row.WorkspaceID]
		if seen && !start.After(previous.periodStart) {
			continue
		}
		usage[row.WorkspaceID] = workspaceAllowanceUsage{
			activeRoutes: row.ActiveRoutes, checks: row.Checks, protocolChecks: row.ProtocolChecks, scrapes: row.Scrapes,
			periodStart: start, current: start.Equal(periodStart),
		}
	}
	return usage, nil
}

func countMissingActiveRoutes(db *gorm.DB, workspaceIDs []uint, usage map[uint]workspaceAllowanceUsage) error {
	const chunk = 500
	for start := 0; start < len(workspaceIDs); start += chunk {
		ids := workspaceIDs[start:min(start+chunk, len(workspaceIDs))]
		var rows []struct {
			WorkspaceID uint
			Active      uint64
		}
		if err := db.Model(&domain.ManagedProxy{}).
			Select("workspace_id, COUNT(*) AS active").
			Where("workspace_id IN ? AND state = ?", ids, domain.ManagedProxyStateActive).
			Group("workspace_id").
			Scan(&rows).Error; err != nil {
			return fmt.Errorf("count active managed proxies: %w", err)
		}
		for _, row := range rows {
			usage[row.WorkspaceID] = workspaceAllowanceUsage{activeRoutes: row.Active}
		}
	}
	return nil
}

// Tag rules can enable more or fewer protocols for some routes. Their effect on
// checks per cycle depends on which of those routes are active, which needs a
// database count. The result is cached per workspace until the checker
// snapshot or the active route count changes, a lifecycle write on this
// instance invalidates it, or planDeltaTTL passes (which also catches a pause
// and an activation on another instance that leave the count unchanged).
const planDeltaTTL = 10 * time.Minute

type planDeltaEntry struct {
	snapshot     *checkerconfig.Workspace
	activeRoutes uint64
	delta        int64
	computedAt   time.Time
}

var planDeltas sync.Map // uint -> planDeltaEntry

// invalidatePlanDeltas forgets a workspace's cached tag-rule effect after one
// of its routes changed state.
func invalidatePlanDeltas(workspaceID uint) { planDeltas.Delete(workspaceID) }

// protocolChecksPerCycle is how many protocol checks one cycle over the
// workspace's active routes makes: active routes × default protocols,
// adjusted by tag rules that apply to active routes. Paused and archived
// routes are not checked and do not count.
func protocolChecksPerCycle(db *gorm.DB, workspaceID uint, activeRoutes uint64, now time.Time) (uint64, error) {
	if activeRoutes == 0 {
		return 0, nil
	}
	snapshot, _ := checkerconfig.Lookup(workspaceID)
	if snapshot == nil || snapshot.Default == nil {
		return activeRoutes, nil
	}
	defaultProtocols := enabledProtocols(snapshot.Default)
	delta, err := cachedPlanDelta(db, workspaceID, snapshot, defaultProtocols, activeRoutes, now)
	if err != nil {
		return 0, err
	}
	total := int64(min(activeRoutes, math.MaxInt64/4))*defaultProtocols + delta
	if total < 0 {
		return 0, nil
	}
	return uint64(total), nil
}

func cachedPlanDelta(db *gorm.DB, workspaceID uint, snapshot *checkerconfig.Workspace, defaultProtocols int64, activeRoutes uint64, now time.Time) (int64, error) {
	if cached, ok := planDeltas.Load(workspaceID); ok {
		entry := cached.(planDeltaEntry)
		if entry.snapshot == snapshot && entry.activeRoutes == activeRoutes && now.Sub(entry.computedAt) < planDeltaTTL {
			return entry.delta, nil
		}
	}
	if snapshot.Proxies.Len() == 0 {
		return 0, nil
	}
	delta, err := activePlanDelta(db, workspaceID, snapshot, defaultProtocols)
	if err != nil {
		return 0, err
	}
	planDeltas.Store(workspaceID, planDeltaEntry{snapshot: snapshot, activeRoutes: activeRoutes, delta: delta, computedAt: now})
	return delta, nil
}

// activePlanDelta sums, over active routes with a tag-rule plan, how many
// protocols the plan enables beyond the default. Routes whose plan enables
// the same number as the default need no lookup.
func activePlanDelta(db *gorm.DB, workspaceID uint, snapshot *checkerconfig.Workspace, defaultProtocols int64) (int64, error) {
	byDelta := map[int64][]uint64{}
	snapshot.Proxies.Range(func(proxyID uint64, plan *checkerconfig.Plan) {
		if difference := enabledProtocols(plan) - defaultProtocols; difference != 0 {
			byDelta[difference] = append(byDelta[difference], proxyID)
		}
	})
	const chunk = 1000
	var delta int64
	for difference, proxyIDs := range byDelta {
		for start := 0; start < len(proxyIDs); start += chunk {
			var active int64
			if err := db.Model(&domain.ManagedProxy{}).
				Where("workspace_id = ? AND state = ? AND proxy_id IN ?", workspaceID, domain.ManagedProxyStateActive, proxyIDs[start:min(start+chunk, len(proxyIDs))]).
				Count(&active).Error; err != nil {
				return 0, fmt.Errorf("count active routes with tag-rule protocols: %w", err)
			}
			delta += difference * active
		}
	}
	return delta, nil
}

func enabledProtocols(plan *checkerconfig.Plan) int64 {
	if plan == nil {
		return 0
	}
	var count int64
	for _, setting := range plan.Settings {
		if setting.Enabled {
			count++
		}
	}
	return count
}

var (
	ErrCheckIntervalTooShort  = fmt.Errorf("check interval is shorter than this workspace's minimum")
	ErrScrapeIntervalTooShort = fmt.Errorf("scrape interval must be at least %d minutes", int(checkpace.MinimumScrapeInterval/time.Minute))
	ErrIntervalTooLong        = fmt.Errorf("interval must be at most %d days", int(maxRequestedInterval/(24*time.Hour)))
	// ErrUnlimitedNeedsInterval rejects automatic spreading for a workspace
	// without a monthly limit: there is no allowance to spread.
	ErrUnlimitedNeedsInterval = fmt.Errorf("this workspace has no monthly limit, so it needs a fixed interval")
)

const maxRequestedInterval = 31 * 24 * time.Hour

// UpdateWorkspaceCadence stores a workspace's requested check and scrape
// intervals. Zero selects automatic spreading, which an unlimited workspace
// cannot use. Intervals below the plan's minimum are rejected rather than
// silently raised.
func UpdateWorkspaceCadence(ctx context.Context, workspaceID uint, request dto.WorkspaceCadenceUpdateRequest) error {
	if pg.DB == nil {
		return fmt.Errorf("database not initialised")
	}
	db := pg.DB.WithContext(ctx)
	var workspace domain.Workspace
	if err := db.Select("id").Preload("Subscription").First(&workspace, workspaceID).Error; err != nil {
		return err
	}
	floor := checkpace.CheckFloor(time.Duration(workspace.Subscription.MinimumCheckIntervalSeconds) * time.Second)
	check := time.Duration(request.CheckIntervalSeconds) * time.Second
	scrape := time.Duration(request.ScrapeIntervalSeconds) * time.Second
	cfg := config.GetConfig()
	switch {
	case check == 0 && workspace.Subscription.ChecksUnlimited(cfg.Checker.UnlimitedMonthlyChecks):
		return fmt.Errorf("%w: choose a check interval", ErrUnlimitedNeedsInterval)
	case scrape == 0 && workspace.Subscription.ScrapesUnlimited(cfg.Scraper.UnlimitedMonthlyScrapes):
		return fmt.Errorf("%w: choose a scrape interval", ErrUnlimitedNeedsInterval)
	case check > maxRequestedInterval || scrape > maxRequestedInterval:
		return ErrIntervalTooLong
	case check > 0 && check < floor:
		return fmt.Errorf("%w (%d seconds)", ErrCheckIntervalTooShort, int(floor/time.Second))
	case scrape > 0 && scrape < checkpace.MinimumScrapeInterval:
		return ErrScrapeIntervalTooShort
	}
	return db.Model(&domain.Workspace{}).Where("id = ?", workspaceID).UpdateColumns(map[string]any{
		"check_interval_seconds":  request.CheckIntervalSeconds,
		"scrape_interval_seconds": request.ScrapeIntervalSeconds,
	}).Error
}

// CadenceWarnings explains when a requested interval needs more than the
// monthly allowance, using snapshot's route and source counts (none if nil).
func CadenceWarnings(snapshot *checkpace.Snapshot, workspaceID uint, subscription domain.WorkspaceSubscription, request dto.WorkspaceCadenceUpdateRequest) []string {
	cfg := config.GetConfig()
	warnings := []string{}
	pace, ok := snapshot.Pace(workspaceID)
	if !ok {
		return warnings
	}
	start, end := checkpace.UsagePeriod(time.Now())
	period := end.Sub(start)
	if request.CheckIntervalSeconds > 0 && !subscription.ChecksUnlimited(cfg.Checker.UnlimitedMonthlyChecks) {
		cadence := max(checkpace.CheckFloor(time.Duration(subscription.MinimumCheckIntervalSeconds)*time.Second), time.Duration(request.CheckIntervalSeconds)*time.Second)
		allowance := subscription.EffectiveMonthlyChecks(cfg.Checker.MonthlyChecksPerWorkspace)
		if projected := checkpace.ProjectMonthly(pace.Allowance.ChecksPerCycle, cadence, period); projected > allowance {
			warnings = append(warnings, fmt.Sprintf("This interval needs about %d checks a month, more than the allowance of %d. Checks stop when the allowance is used up.", projected, allowance))
		}
	}
	if request.ScrapeIntervalSeconds > 0 && !subscription.ScrapesUnlimited(cfg.Scraper.UnlimitedMonthlyScrapes) {
		cadence := max(checkpace.MinimumScrapeInterval, time.Duration(request.ScrapeIntervalSeconds)*time.Second)
		allowance := subscription.EffectiveMonthlyScrapes(cfg.Scraper.MonthlyScrapesPerWorkspace)
		if projected := checkpace.ProjectMonthly(pace.Allowance.ScrapesPerCycle, cadence, period); projected > allowance {
			warnings = append(warnings, fmt.Sprintf("This interval needs about %d scrapes a month, more than the allowance of %d. Scraping stops when the allowance is used up.", projected, allowance))
		}
	}
	return warnings
}

// PreserveLegacyCadence gives every workspace that still uses automatic
// spreading a fixed interval equal to an older settings file's timer, so an
// upgrade keeps each workspace's cadence. A zero period leaves that kind
// unchanged. Intervals are raised to the workspace's minimum and lowered to
// the 31-day maximum. It returns how many workspaces received each interval.
func PreserveLegacyCadence(ctx context.Context, checkPeriod, scrapePeriod time.Duration) (int, int, error) {
	if pg.DB == nil {
		return 0, 0, fmt.Errorf("database not initialised")
	}
	db := pg.DB
	if ctx != nil {
		db = db.WithContext(ctx)
	}
	return preserveLegacyCadence(db, checkPeriod, scrapePeriod)
}

func preserveLegacyCadence(db *gorm.DB, checkPeriod, scrapePeriod time.Duration) (int, int, error) {
	var workspaces []domain.Workspace
	if err := db.Select("id", "check_interval_seconds", "scrape_interval_seconds").Preload("Subscription").
		Find(&workspaces).Error; err != nil {
		return 0, 0, fmt.Errorf("list workspaces: %w", err)
	}
	seconds := func(value time.Duration) uint32 { return uint32(value / time.Second) }
	var checks, scrapes int
	err := db.Transaction(func(tx *gorm.DB) error {
		for _, workspace := range workspaces {
			updates := map[string]any{}
			if checkPeriod > 0 && workspace.CheckIntervalSeconds == 0 {
				floor := checkpace.CheckFloor(time.Duration(workspace.Subscription.MinimumCheckIntervalSeconds) * time.Second)
				updates["check_interval_seconds"] = seconds(min(max(checkPeriod, floor), maxRequestedInterval))
				checks++
			}
			if scrapePeriod > 0 && workspace.ScrapeIntervalSeconds == 0 {
				updates["scrape_interval_seconds"] = seconds(min(max(scrapePeriod, checkpace.MinimumScrapeInterval), maxRequestedInterval))
				scrapes++
			}
			if len(updates) == 0 {
				continue
			}
			if err := tx.Model(&domain.Workspace{}).Where("id = ?", workspace.ID).UpdateColumns(updates).Error; err != nil {
				return fmt.Errorf("preserve workspace %d cadence: %w", workspace.ID, err)
			}
		}
		return nil
	})
	if err != nil {
		return 0, 0, err
	}
	return checks, scrapes, nil
}
