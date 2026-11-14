package database

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"magpie/internal/api/dto"
	"magpie/internal/checkerconfig"
	"magpie/internal/checkpace"
	"magpie/internal/config"
	"magpie/internal/domain"
	"magpie/internal/platform/pg"

	"github.com/charmbracelet/log"
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
	refreshPlanDeltas(db, workspaceIDs, usage, now, time.Now().Add(planDeltaLoadBudget))
	for _, workspaceID := range workspaceIDs {
		subscription := subscriptionByWorkspace[workspaceID]
		used := usage[workspaceID]
		protocolChecks := protocolChecksPerCycle(workspaceID, used.activeRoutes)
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
// database count over the routes that have a tag-rule plan.
//
// While a workspace has at most planDeltaExactRoutes such routes, the count is
// cheap and kept exact: it is cached until the checker snapshot or the active
// route count changes, a lifecycle write on this instance invalidates it, or
// planDeltaTTL passes (which also catches a pause and an activation on another
// instance that leave the count unchanged).
//
// Beyond that the count takes one query per thousand routes, and a workspace
// that imports or scrapes changes its route count before every refresh. Such a
// workspace is counted once per planDeltaTTL. In between, the last count
// follows the active route count in proportion, which is an estimate that the
// recomputed cadence corrects.
//
// A refresh loads every workspace, so it must not depend on one workspace's
// count. Counts run before the allowances are built, the one that has waited
// longest first, and only while the load's budget lasts; each has its own
// time limit. A workspace whose count did not run or failed keeps its last
// count while its rules and defaults are unchanged, and is otherwise estimated
// from the checker snapshot alone. A failed count is repeated after
// planDeltaTTL. One large or slow workspace therefore cannot keep every
// workspace's cadence from being refreshed, and no workspace waits behind the
// same ones at every refresh.
const (
	planDeltaTTL          = 10 * time.Minute
	planDeltaExactRoutes  = 5_000
	planDeltaCountTimeout = 10 * time.Second
	planDeltaLoadBudget   = 10 * time.Second
)

type planDeltaEntry struct {
	snapshot     *checkerconfig.Workspace
	signature    string // the snapshot's rules and settings, without its tag assignments
	activeRoutes uint64
	delta        int64
	// computedAt is when the workspace was last counted, or when a count last
	// failed. The workspace whose time is oldest is counted first.
	computedAt time.Time
	known      bool
	failed     bool
	// changed is set by a lifecycle write: the count is out of date.
	changed bool
	// throttled marks a workspace beyond planDeltaExactRoutes, whose entry
	// lifecycle writes keep.
	throttled bool
}

func loadPlanDelta(workspaceID uint) planDeltaEntry {
	cached, _ := planDeltas.Load(workspaceID)
	entry, _ := cached.(planDeltaEntry)
	return entry
}

// due reports whether the workspace should be counted now.
func (e planDeltaEntry) due(snapshot *checkerconfig.Workspace, activeRoutes uint64, now time.Time) bool {
	switch {
	case !e.known || now.Sub(e.computedAt) >= planDeltaTTL || e.signature != snapshot.InputSignature:
		return true
	case e.failed || snapshot.Proxies.Len() > planDeltaExactRoutes:
		return false
	default:
		return e.changed || e.snapshot != snapshot || e.activeRoutes != activeRoutes
	}
}

// value is the tag rules' effect on the checks of one cycle over activeRoutes
// routes: the last count, in proportion to the route count, while the rules
// and defaults are the ones that were counted. A count made under other rules
// says nothing about these, so the effect is then estimated without one.
func (e planDeltaEntry) value(snapshot *checkerconfig.Workspace, defaultProtocols int64, activeRoutes uint64) int64 {
	switch {
	case !e.known || e.signature != snapshot.InputSignature:
		return uncountedPlanDelta(snapshot, defaultProtocols, activeRoutes)
	case e.activeRoutes == 0 || e.activeRoutes == activeRoutes:
		return e.delta
	default:
		return int64(math.Round(float64(e.delta) * float64(activeRoutes) / float64(e.activeRoutes)))
	}
}

// uncountedPlanDelta estimates the tag rules' effect from the checker snapshot
// alone, as if every route whose plan adds protocols were active. If there are
// more routes with a plan than active ones, the active routes are taken to
// have the same mix of plans. Plans that remove protocols are left out: they
// may belong to paused routes, and counting them would plan fewer checks per
// cycle than the workspace makes, which makes an automatic cadence too fast.
// The estimate errs on the side of a slower cadence until the count has run.
func uncountedPlanDelta(snapshot *checkerconfig.Workspace, defaultProtocols int64, activeRoutes uint64) int64 {
	var delta int64
	snapshot.Proxies.Range(func(_ uint64, plan *checkerconfig.Plan) {
		delta += max(enabledProtocols(plan)-defaultProtocols, 0)
	})
	if planned := uint64(snapshot.Proxies.Len()); planned > activeRoutes {
		delta = int64(math.Round(float64(delta) * float64(activeRoutes) / float64(planned)))
	}
	return delta
}

var planDeltas sync.Map // uint -> planDeltaEntry

// invalidatePlanDeltas asks for a new count after one of the workspace's
// routes changed state. Until it ran, the last count follows the route count
// in proportion. The workspace keeps its place among the counts that are due
// and the waiting time after a failed count, so one that changes before every
// load neither goes first every time nor repeats a failing count. A throttled
// workspace is counted once per planDeltaTTL.
func invalidatePlanDeltas(workspaceID uint) {
	if entry := loadPlanDelta(workspaceID); entry.known && !entry.throttled && !entry.changed {
		entry.changed = true
		planDeltas.Store(workspaceID, entry)
	}
}

// refreshPlanDeltas repeats the counts that are due, the one that has waited
// longest first, until countsUntil, when the load's budget for them ends. A
// workspace that was never counted has waited longest.
func refreshPlanDeltas(db *gorm.DB, workspaceIDs []uint, usage map[uint]workspaceAllowanceUsage, now, countsUntil time.Time) {
	type dueCount struct {
		workspaceID  uint
		snapshot     *checkerconfig.Workspace
		activeRoutes uint64
		last         time.Time
	}
	var due []dueCount
	for _, workspaceID := range workspaceIDs {
		activeRoutes := usage[workspaceID].activeRoutes
		snapshot, _ := checkerconfig.Lookup(workspaceID)
		if activeRoutes == 0 || snapshot == nil || snapshot.Default == nil || snapshot.Proxies.Len() == 0 {
			continue
		}
		if entry := loadPlanDelta(workspaceID); entry.due(snapshot, activeRoutes, now) {
			due = append(due, dueCount{workspaceID, snapshot, activeRoutes, entry.computedAt})
		}
	}
	sort.SliceStable(due, func(i, j int) bool { return due[i].last.Before(due[j].last) })
	for _, count := range due {
		if time.Now().After(countsUntil) {
			return
		}
		countPlanDelta(db, count.workspaceID, count.snapshot, count.activeRoutes, now)
	}
}

// countPlanDelta counts one workspace and stores the result. A count that
// fails keeps the workspace's current value and is repeated after
// planDeltaTTL, so it does not use up the budget of every load.
func countPlanDelta(db *gorm.DB, workspaceID uint, snapshot *checkerconfig.Workspace, activeRoutes uint64, now time.Time) {
	defaultProtocols := enabledProtocols(snapshot.Default)
	entry := planDeltaEntry{
		snapshot: snapshot, signature: snapshot.InputSignature, activeRoutes: activeRoutes,
		computedAt: now, known: true, throttled: snapshot.Proxies.Len() > planDeltaExactRoutes,
	}
	delta, err := activePlanDelta(db, workspaceID, snapshot, defaultProtocols)
	if err != nil {
		log.Warn("count active routes with tag-rule protocols; keeping the current estimate", "workspace_id", workspaceID, "error", err)
		delta, entry.failed = loadPlanDelta(workspaceID).value(snapshot, defaultProtocols, activeRoutes), true
	}
	entry.delta = delta
	planDeltas.Store(workspaceID, entry)
}

// protocolChecksPerCycle is how many protocol checks one cycle over the
// workspace's active routes makes: active routes × default protocols,
// adjusted by tag rules that apply to active routes. Paused and archived
// routes are not checked and do not count. It reads what refreshPlanDeltas
// counted and issues no query.
func protocolChecksPerCycle(workspaceID uint, activeRoutes uint64) uint64 {
	if activeRoutes == 0 {
		return 0
	}
	snapshot, _ := checkerconfig.Lookup(workspaceID)
	if snapshot == nil || snapshot.Default == nil {
		return activeRoutes
	}
	defaultProtocols := enabledProtocols(snapshot.Default)
	total := int64(min(activeRoutes, math.MaxInt64/4)) * defaultProtocols
	if snapshot.Proxies.Len() > 0 {
		total += loadPlanDelta(workspaceID).value(snapshot, defaultProtocols, activeRoutes)
	}
	if total < 0 {
		return 0
	}
	return uint64(total)
}

// activePlanDelta sums, over active routes with a tag-rule plan, how many
// protocols the plan enables beyond the default. Routes whose plan enables
// the same number as the default need no lookup. The count is limited to
// planDeltaCountTimeout, so a slow one fails alone and leaves the caller's
// context usable.
func activePlanDelta(db *gorm.DB, workspaceID uint, snapshot *checkerconfig.Workspace, defaultProtocols int64) (int64, error) {
	parent := db.Statement.Context
	if parent == nil {
		parent = context.Background()
	}
	limited, cancel := context.WithTimeout(parent, planDeltaCountTimeout)
	defer cancel()
	db = db.WithContext(limited)
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
