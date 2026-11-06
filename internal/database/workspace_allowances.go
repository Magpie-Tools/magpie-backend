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

	"gorm.io/gorm"
)

// LoadWorkspaceAllowances reads every workspace's monthly allowances and the
// current period's metered usage for the check pacer. It runs once per
// checkpace.RefreshInterval on each instance, never per check. Active route
// counts come from the usage meter maintained by lifecycle writes, so the
// managed-proxy table is scanned only for workspaces without any usage row.
func LoadWorkspaceAllowances(ctx context.Context, now time.Time) ([]checkpace.Allowance, error) {
	if DB == nil {
		return nil, fmt.Errorf("database not initialised")
	}
	db := DB
	if ctx != nil {
		db = db.WithContext(ctx)
	}
	return loadWorkspaceAllowances(db, now)
}

type workspaceAllowanceUsage struct {
	activeRoutes uint64
	checks       uint64
	scrapes      uint64
	periodStart  time.Time
	current      bool
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
		allowance := checkpace.Allowance{
			WorkspaceID:          workspaceID,
			UnlimitedChecks:      subscription.ChecksUnlimited(cfg.Checker.UnlimitedMonthlyChecks),
			UnlimitedScrapes:     subscription.ScrapesUnlimited(cfg.Scraper.UnlimitedMonthlyScrapes),
			MonthlyChecks:        subscription.EffectiveMonthlyChecks(defaultChecks),
			ChecksPerCycle:       checksPerCycle(workspaceID, used.activeRoutes),
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
	if err := db.Select("workspace_id", "period_start", "active_routes", "checks", "scrapes").
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
			activeRoutes: row.ActiveRoutes, checks: row.Checks, scrapes: row.Scrapes,
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

// planProtocolDeltas caches, per published checker snapshot, how many protocol
// checks its tag-rule plans add to or remove from the default plan. Snapshots
// are immutable, so a pointer identifies the result.
var planProtocolDeltas sync.Map // *checkerconfig.Workspace -> int64

func checksPerCycle(workspaceID uint, activeRoutes uint64) uint64 {
	if activeRoutes == 0 {
		return 0
	}
	snapshot, _ := checkerconfig.Lookup(workspaceID)
	if snapshot == nil || snapshot.Default == nil {
		return activeRoutes
	}
	defaultProtocols := enabledProtocols(snapshot.Default)
	var delta int64
	if cached, ok := planProtocolDeltas.Load(snapshot); ok {
		delta = cached.(int64)
	} else {
		snapshot.Proxies.Range(func(_ uint64, plan *checkerconfig.Plan) {
			delta += enabledProtocols(plan) - defaultProtocols
		})
		planProtocolDeltas.Range(func(key, _ any) bool {
			if previous := key.(*checkerconfig.Workspace); previous.Value.ID == workspaceID {
				planProtocolDeltas.Delete(key)
			}
			return true
		})
		planProtocolDeltas.Store(snapshot, delta)
	}
	total := int64(min(activeRoutes, math.MaxInt64/4))*defaultProtocols + delta
	if total < 0 {
		return 0
	}
	return uint64(total)
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
	if DB == nil {
		return fmt.Errorf("database not initialised")
	}
	db := DB.WithContext(ctx)
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
// monthly allowance, using the current snapshot's route and source counts.
func CadenceWarnings(workspaceID uint, subscription domain.WorkspaceSubscription, request dto.WorkspaceCadenceUpdateRequest) []string {
	cfg := config.GetConfig()
	warnings := []string{}
	pace, ok := checkpace.Current().Pace(workspaceID)
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

// LegacyCadenceAllowances converts the fixed timers of an older settings file
// into instance allowances. The result is the larger of the shipped default and
// what the busiest workspace needed for its previous cadence over a 31-day
// month, so no existing workspace becomes slower after the upgrade.
func LegacyCadenceAllowances(ctx context.Context, checkPeriod, scrapePeriod time.Duration) (uint64, uint64, error) {
	allowances, err := LoadWorkspaceAllowances(ctx, time.Now())
	if err != nil {
		return 0, 0, err
	}
	var checksPerCycle, scrapesPerCycle uint64
	for _, allowance := range allowances {
		checksPerCycle = max(checksPerCycle, allowance.ChecksPerCycle)
		scrapesPerCycle = max(scrapesPerCycle, allowance.ScrapesPerCycle)
	}
	const month = 31 * 24 * time.Hour
	convert := func(perCycle uint64, period time.Duration, fallback uint64) uint64 {
		if period <= 0 || perCycle == 0 {
			return fallback
		}
		cycles := math.Ceil(float64(month) / float64(period))
		needed := math.Ceil(float64(perCycle) * cycles)
		if needed >= math.MaxUint64/2 {
			return math.MaxUint64 / 2
		}
		return max(fallback, uint64(needed))
	}
	return convert(checksPerCycle, checkPeriod, config.DefaultMonthlyChecksPerWorkspace),
		convert(scrapesPerCycle, scrapePeriod, config.DefaultMonthlyScrapesPerWorkspace), nil
}
