package dashboard

import (
	"context"
	"errors"
	"sync"
	"time"

	"magpie/internal/api/dto"

	"github.com/charmbracelet/log"
	"gorm.io/gorm"
)

// independentTimeout bounds a publish, failure or release update, which runs
// on a context independent of the build so a canceled build still records
// its outcome.
const independentTimeout = 5 * time.Second

var errLeaseLost = errors.New("dashboard: lease expired or was taken before publish")

// Service serves dashboard views from shared snapshot rows and builds them on
// demand. Each backend instance owns one; app/bootstrap constructs it, runs it
// and stops it.
type Service struct {
	store *Store
	cfg   Config
	// slots bounds the builds this instance runs at once.
	slots chan struct{}

	mu      sync.Mutex
	flights map[uint]*flight

	// buildCtx is the parent of every build; Run's end cancels it.
	buildCtx    context.Context
	cancelBuild context.CancelFunc
	builds      sync.WaitGroup
}

// flight is this instance's claim attempt or build for one workspace. Local
// requests for the same workspace follow it instead of claiming again.
type flight struct {
	done chan struct{}
}

// NewService returns a Service on the pool handle db.
func NewService(db *gorm.DB, cfg Config) *Service {
	cfg = cfg.withDefaults()
	buildCtx, cancel := context.WithCancel(context.Background())
	return &Service{
		store:       NewStore(db),
		cfg:         cfg,
		slots:       make(chan struct{}, cfg.InstanceLimit),
		flights:     make(map[uint]*flight),
		buildCtx:    buildCtx,
		cancelBuild: cancel,
	}
}

// Run blocks until ctx ends, then cancels this instance's builds, clears
// their leases and waits for them.
func (s *Service) Run(ctx context.Context) {
	<-ctx.Done()
	s.Stop()
}

// Stop cancels this instance's builds and waits for them. A canceled build
// clears its lease, fenced by its epoch, without counting a failure. The
// cancellation happens under the mutex that claims check and register
// builds under, so no build is registered after Stop starts waiting.
func (s *Service) Stop() {
	s.mu.Lock()
	s.cancelBuild()
	s.mu.Unlock()
	s.builds.Wait()
}

// DeleteExpiredSnapshots is the retention step of the maintenance routine.
func (s *Service) DeleteExpiredSnapshots(ctx context.Context) (int64, error) {
	return s.store.DeleteExpired(ctx)
}

// Status says how current a view is.
type Status string

const (
	// StatusFresh: published less than 30 s ago, or during this request.
	StatusFresh Status = "FRESH"
	// StatusStale: an older snapshot, up to ten minutes, or the last build
	// failed and some part is still valid.
	StatusStale Status = "STALE"
	// StatusPending: no valid snapshot yet; a build is pending.
	StatusPending Status = "PENDING"
	// StatusFailed: the last build failed and no part is valid.
	StatusFailed Status = "FAILED"
)

// View is one dashboard response. The check figures are live; every other
// part comes from the snapshot and is nil when the snapshot has no valid
// value for it. Absent parts are never reported as zero.
type View struct {
	Status       Status
	Rebuilding   bool
	HealthMasked bool
	// CollectedAt is the published build's claim time: the snapshot's data
	// is at least that old. It does not describe the live check figures.
	CollectedAt *time.Time
	ComputedAt  *time.Time

	TotalChecks     int64
	ChecksThisMonth int64

	inventory *inventoryPart
	health    *healthPart
}

// TotalScraped is the workspace's scraped route count.
func (v View) TotalScraped() *int64 {
	if v.inventory == nil {
		return nil
	}
	return &v.inventory.TotalScraped
}

// ProxyCount is the number of routes the workspace owns, in any state.
func (v View) ProxyCount() *int64 {
	if v.inventory == nil {
		return nil
	}
	return &v.inventory.ProxyCount
}

// ReputationBreakdown counts the workspace's active routes per reputation label.
func (v View) ReputationBreakdown() *dto.DashboardReputationBreakdown {
	if v.inventory == nil {
		return nil
	}
	r := v.inventory.ReputationBreakdown
	return &dto.DashboardReputationBreakdown{Good: r.Good, Neutral: r.Neutral, Poor: r.Poor, Unknown: r.Unknown}
}

// CountryBreakdown counts the workspace's active routes per country.
func (v View) CountryBreakdown() []dto.DashboardCountryCount {
	if v.inventory == nil {
		return nil
	}
	countries := make([]dto.DashboardCountryCount, 0, len(v.inventory.CountryBreakdown))
	for _, row := range v.inventory.CountryBreakdown {
		countries = append(countries, dto.DashboardCountryCount{Country: row.Country, Count: row.Count})
	}
	return countries
}

// TopReputationProxy is the active route with the best reputation score.
func (v View) TopReputationProxy() *dto.DashboardTopReputationProxy {
	if v.inventory == nil || v.inventory.TopReputationProxy == nil {
		return nil
	}
	top := v.inventory.TopReputationProxy
	return &dto.DashboardTopReputationProxy{ProxyID: top.ProxyID, IP: top.IP, Port: top.Port, Score: top.Score, Label: top.Label}
}

// AliveCount is the workspace's alive route count under its current
// settings; nil while health is masked or not yet built.
func (v View) AliveCount() *int64 {
	if v.health == nil {
		return nil
	}
	return &v.health.AliveCount
}

// JudgeValidProxies counts alive routes per judge and anonymity level: empty
// while health is masked, nil while it is not yet built.
func (v View) JudgeValidProxies() []dto.DashboardJudgeCount {
	if v.HealthMasked {
		return []dto.DashboardJudgeCount{}
	}
	if v.health == nil {
		return nil
	}
	judges := make([]dto.DashboardJudgeCount, 0, len(v.health.JudgeValidProxies))
	for _, row := range v.health.JudgeValidProxies {
		judges = append(judges, dto.DashboardJudgeCount{JudgeURL: row.JudgeURL, EliteProxies: row.EliteProxies, AnonymousProxies: row.AnonymousProxies, TransparentProxies: row.TransparentProxies})
	}
	return judges
}

// RecentProxyChecks returns what the recent-checks query returns for limit
// (1 to 50): empty while health is masked, nil while it is not yet built.
func (v View) RecentProxyChecks(limit int) []dto.ProxyRecentCheck {
	if v.HealthMasked {
		return []dto.ProxyRecentCheck{}
	}
	if v.health == nil {
		return nil
	}
	return v.health.recentChecks(min(max(limit, 1), recentCandidateLimit))
}

// FastestAliveProxies returns the first limit (1 to 100) fastest alive routes:
// empty while health is masked, nil while it is not yet built.
func (v View) FastestAliveProxies(limit int) []dto.ProxyFastestAlive {
	if v.HealthMasked {
		return []dto.ProxyFastestAlive{}
	}
	if v.health == nil {
		return nil
	}
	return v.health.fastestAlive(min(max(limit, 1), fastestLimit))
}

// View returns the workspace's dashboard. A fresh snapshot is served at once.
// Otherwise the request tries to claim a build when the row is eligible,
// waits at most two seconds for this instance's build or another instance's
// lease, and serves the valid parts of the newest row with an explicit
// status. A missing workspace is an error.
func (s *Service) View(ctx context.Context, workspaceID uint) (View, error) {
	state, err := s.store.read(ctx, workspaceID, true, time.Now())
	if err != nil {
		return View{}, err
	}
	if fresh(state) {
		return assemble(state)
	}
	changed, err := s.refresh(ctx, workspaceID, state)
	if err != nil {
		return View{}, err
	}
	if changed {
		// Serve-time validation: the response comes from one read taken after
		// the wait or the admission attempt, so a publish, generation change or
		// dirty flag committed meanwhile is never missed, and a newly observed
		// generation is never combined with an older payload.
		if state, err = s.store.read(ctx, workspaceID, true, time.Now()); err != nil {
			return View{}, err
		}
	}
	return assemble(state)
}

// refresh claims or follows a build of a snapshot that is not fresh and
// waits for it, for at most maxWait in total. It reports whether the row may
// have changed since state was read: after any wait, and after any admission
// attempt, admitted or not. Running out of the wait is not an error; the
// request's own cancellation is.
func (s *Service) refresh(ctx context.Context, workspaceID uint, state snapshotState) (bool, error) {
	waitCtx, cancel := context.WithTimeout(ctx, maxWait)
	defer cancel()
	if f := s.follow(workspaceID); f != nil {
		return true, s.wait(ctx, waitCtx, f)
	}
	if state.Rebuilding {
		return true, s.poll(ctx, waitCtx, workspaceID, state.ComputedAt)
	}
	if !state.Eligible {
		return false, nil
	}
	// A flight another local request started first is followed like this
	// request's own.
	f, attempted := s.claim(waitCtx, workspaceID, state.RowExists)
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if f == nil {
		return attempted, nil
	}
	return true, s.wait(ctx, waitCtx, f)
}

// follow returns this instance's flight for the workspace, if any.
func (s *Service) follow(workspaceID uint) *flight {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flights[workspaceID]
}

// claim tries to admit and start a build and returns the flight to wait on;
// nil means no build runs. attempted reports whether the database admission
// ran. Claims need a free local slot.
func (s *Service) claim(ctx context.Context, workspaceID uint, rowExists bool) (*flight, bool) {
	s.mu.Lock()
	if f := s.flights[workspaceID]; f != nil {
		s.mu.Unlock()
		return f, false
	}
	if s.buildCtx.Err() != nil {
		s.mu.Unlock()
		return nil, false
	}
	select {
	case s.slots <- struct{}{}:
	default:
		s.mu.Unlock()
		return nil, false
	}
	f := &flight{done: make(chan struct{})}
	s.flights[workspaceID] = f
	s.builds.Add(1)
	s.mu.Unlock()

	finish := func() {
		s.mu.Lock()
		delete(s.flights, workspaceID)
		s.mu.Unlock()
		close(f.done)
		<-s.slots
		s.builds.Done()
	}
	admitted, granted, err := s.admit(ctx, workspaceID, rowExists)
	if err != nil {
		log.Warn("Dashboard snapshot claim failed", "workspace_id", workspaceID, "error", err)
	}
	if !admitted {
		finish()
		return nil, true
	}
	go func() {
		defer finish()
		s.runBuild(workspaceID, granted)
	}()
	return f, true
}

// admit inserts the row when the read found none, then claims it.
func (s *Service) admit(ctx context.Context, workspaceID uint, rowExists bool) (bool, lease, error) {
	if !rowExists {
		if inserted, err := s.store.ensureRow(ctx, workspaceID); err != nil || !inserted {
			return false, lease{}, err
		}
	}
	granted, admitted, err := s.store.claim(ctx, workspaceID, s.cfg.ClusterLimit, s.cfg.BuildDeadline+leaseGrace)
	return admitted, granted, err
}

// runBuild builds and publishes one admitted lease. A failed build or a
// publish that updates no row records a failure; a build canceled by Stop
// only clears its lease.
func (s *Service) runBuild(workspaceID uint, granted lease) {
	buildCtx, cancel := context.WithTimeout(s.buildCtx, s.cfg.BuildDeadline)
	result, err := s.store.build(buildCtx, workspaceID, s.cfg.StatementConcurrency)
	cancel()
	outcome, done := context.WithTimeout(context.Background(), independentTimeout)
	defer done()
	if err != nil && s.buildCtx.Err() != nil {
		if releaseErr := s.store.release(outcome, workspaceID, granted.Epoch); releaseErr != nil {
			log.Warn("Dashboard snapshot lease release failed", "workspace_id", workspaceID, "error", releaseErr)
		}
		return
	}
	var encoded []byte
	if err == nil {
		encoded, err = encodePayload(result.payload)
	}
	if err == nil {
		var published bool
		if published, err = s.store.publish(outcome, workspaceID, granted.Epoch, encoded, result.generation, result.healthValid); err == nil && published {
			return
		} else if err == nil {
			err = errLeaseLost
		}
	}
	log.Warn("Dashboard snapshot build failed", "workspace_id", workspaceID, "error", err)
	if failErr := s.store.fail(outcome, workspaceID, granted.Epoch); failErr != nil {
		log.Warn("Dashboard snapshot failure was not recorded", "workspace_id", workspaceID, "error", failErr)
	}
}

// wait follows a local flight until it ends or waitCtx does.
func (s *Service) wait(ctx, waitCtx context.Context, f *flight) error {
	select {
	case <-f.done:
		return nil
	case <-waitCtx.Done():
		return ctx.Err()
	}
}

// poll re-reads, without payload or usage, a row whose lease another
// instance holds, until the lease ends, a build publishes, or waitCtx ends.
// The re-reads run on waitCtx, so a blocked read cannot extend the wait.
func (s *Service) poll(ctx, waitCtx context.Context, workspaceID uint, computedAt *time.Time) error {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-waitCtx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
		state, err := s.store.read(waitCtx, workspaceID, false, time.Now())
		if err != nil {
			if waitCtx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		if !state.Rebuilding || !sameTime(state.ComputedAt, computedAt) {
			return nil
		}
	}
}

func sameTime(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Equal(*b)
}

// fresh is row 1 of the read table: a payload younger than 30 s whose health
// parts are current, or masked by a dirty workspace.
func fresh(state snapshotState) bool {
	age, published := state.age()
	return state.hasPayload() && published && age < freshFor && !state.healthMissing()
}

// statusOf applies the read table to the row a response is built from.
func statusOf(state snapshotState) Status {
	if fresh(state) {
		return StatusFresh
	}
	if state.FailedAt != nil {
		if state.hasPayload() {
			return StatusStale
		}
		return StatusFailed
	}
	if age, published := state.age(); state.hasPayload() && published && age < staleFor && !state.healthMissing() {
		return StatusStale
	}
	return StatusPending
}

// assemble builds the response from one read. Age never hides a part; only
// the generation and the dirty flag hide health.
func assemble(state snapshotState) (View, error) {
	view := View{
		Status:       statusOf(state),
		Rebuilding:   state.Rebuilding,
		HealthMasked: state.WorkspaceDirty,
		CollectedAt:  state.CollectedAt,
		ComputedAt:   state.ComputedAt,
	}
	if state.TotalChecks != nil {
		view.TotalChecks = *state.TotalChecks
	}
	if state.ChecksThisMonth != nil {
		view.ChecksThisMonth = *state.ChecksThisMonth
	}
	stored, err := decodePayload(state.Payload)
	if err != nil {
		return View{}, err
	}
	if stored != nil {
		view.inventory = &stored.Inventory
		if state.healthCurrent() && stored.Health != nil {
			view.health = stored.Health
		}
	}
	return view, nil
}
