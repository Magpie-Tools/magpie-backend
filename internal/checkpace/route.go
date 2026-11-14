package checkpace

import (
	"math"
	"time"

	"magpie/internal/domain"
)

// A route is scheduled at the earliest grid point any of its owners waits for,
// and an owner is checked at the dequeues that reach one of its own points.
// Each owner therefore keeps its own cadence on a shared route, whichever
// owner is fastest, and a workspace's routes stay spread across its cadence
// when a cadence changes or an owner leaves. See grid for the points.
//
// The queue score is the route's only scheduling state. It is a watermark:
// every owner's points before it are settled (checked or skipped), and every
// point from it on is pending. A cycle keeps that true for the score it
// returns.
//
// Owners of one route share its checks. Their grids are laid over each other
// (see phaseKey), so owners with equal or nested cadences are due at the same
// dequeues. For the others, a cycle that checks the route anyway also serves
// every owner whose next point comes before the route's next visit, and that
// point is then settled: see Select.

const (
	revisitSpan  = int64(MaxRouteInterval / time.Millisecond)
	fallbackSpan = int64(FallbackInterval / time.Millisecond)
)

// RouteCycle is one dequeue of a route: which owners it serves and when the
// route is due next. A worker begins one per dequeue, calls Select with the
// route's owners, calls Reschedule for the owners it served once their checks
// are done, and requeues the route at Next. It is in-memory arithmetic over
// the snapshot: no Redis, database, JSON or cryptographic work, and no
// allocation.
type RouteCycle struct {
	pacer    *Pacer
	snapshot *Snapshot
	routeID  uint64
	key      uint64
	// Owners with a grid point in (from, at] are due. Unix milliseconds.
	from, at int64
	// next is the earliest point an owner waits for, or zero.
	next int64
	// settled is the last moment whose points this cycle's checks settle for
	// the owners it serves ahead of their point, or zero.
	settled int64
	month   int32
	paused  bool
	owners  bool
	// The owner asked about last. A single-owner route asks Due and then
	// Reschedule for the same owner, which then reads the snapshot once.
	lastID    uint
	lastPace  *Pace
	lastKnown bool
}

// BeginRouteCycle starts the decisions for a route that was due at scheduled
// and was dequeued at now. s is the snapshot the caller holds (nil before the
// first load); the cycle keeps it, so one cycle decides on one snapshot.
//
// The cycle serves the points between the route's due time and its dequeue.
// The requeue placed the due time at the earliest point any owner waited for,
// so no point lies before it, and time spent waiting in the queue is covered.
func (p *Pacer) BeginRouteCycle(s *Snapshot, routeID uint64, scheduled, now time.Time) RouteCycle {
	due := scheduled.UnixMilli()
	return RouteCycle{
		pacer: p, snapshot: s, routeID: routeID, key: phaseKey(routeID),
		// The due time is itself a point, so the window starts just before it.
		from: due - 1, at: max(now.UnixMilli(), due),
		month:  p.monthOf(now),
		paused: p.checksPaused(),
	}
}

// ownerGrid returns the grid an owner follows on this route. waits is false
// for an owner that is out of checks, which neither is due nor holds the route
// in the queue. always is true for an owner without a published cadence:
// before the first load, or a workspace created since the last one. Such an
// owner is served by every cycle, as before it had a cadence, and holds the
// route at the fallback cadence.
func (c *RouteCycle) ownerGrid(workspaceID uint) (g grid, waits, always bool) {
	var pace *Pace
	if c.lastKnown && c.lastID == workspaceID {
		pace = c.lastPace
	} else {
		if c.snapshot != nil {
			pace = c.snapshot.lookup(workspaceID)
		}
		c.lastID, c.lastPace, c.lastKnown = workspaceID, pace, true
	}
	// Whether the allowance is used up is read every time: the checks of
	// this very cycle may have used it up.
	switch {
	case pace == nil:
		return grid{span: fallbackSpan}, true, true
	case c.pacer.exhaustedIn(pace, c.month), pace.checkGrid.span <= 0:
		// The second case is an allowance that was used up when the snapshot
		// was taken and is judged for another month: it has no cadence until
		// the next load.
		return grid{}, false, false
	default:
		return pace.checkGrid, true, false
	}
}

// Due reports whether this cycle serves the owner, for a route with one owner.
// An owner that is not served keeps waiting for its next point after the
// dequeue, which Next takes into account. Nothing is due while checking is
// paused because usage could not be written. An owner the snapshot lacks asks
// the Pacer for a refresh.
func (c *RouteCycle) Due(workspaceID uint) bool {
	g, due := c.reached(workspaceID)
	if !due && g.span > 0 {
		c.wait(g.next(c.key, c.at))
	}
	return due
}

// reached reports whether one of the owner's points lies between the route's
// due time and the dequeue. The grid has no span for an owner that does not
// hold the route in the queue.
func (c *RouteCycle) reached(workspaceID uint) (grid, bool) {
	c.owners = true
	g, waits, always := c.ownerGrid(workspaceID)
	if !waits {
		return grid{}, false
	}
	if always && c.snapshot != nil {
		c.pacer.RequestRefresh()
	}
	return g, !c.paused && (always || g.due(c.key, c.from, c.at))
}

// Selection is which of a route's owners a cycle serves, by position in the
// owner list. A worker keeps one and reuses it for every cycle, so a cycle
// allocates nothing once the selection has grown to the longest owner list
// the worker has seen. Owners stay where they are: copying them would cost one
// full workspace value per owner at every dequeue of a shared route.
type Selection struct {
	// All and Any report whether every owner, or at least one, is served.
	All, Any bool
	// mask is used only when some owners are served and some are not.
	mask []bool
	// points is the cycle's working memory: one entry per owner.
	points []ownerPoint
}

// ownerPoint is an owner's grid and, for an owner that has not reached a
// point, its first point after the dequeue.
type ownerPoint struct {
	g    grid
	next int64
}

// Has reports whether the owner at index is served. A nil selection serves
// every owner.
func (s *Selection) Has(index int) bool { return s == nil || s.All || s.mask[index] }

// exclude records that the owner at index is not served.
func (s *Selection) exclude(index, owners int) {
	if !s.All {
		return
	}
	// The first owner that is not served: the owners before it were.
	s.All = false
	if cap(s.mask) < owners {
		s.mask = make([]bool, owners)
	} else {
		s.mask = s.mask[:owners]
		clear(s.mask)
	}
	for served := 0; served < index; served++ {
		s.mask[served] = true
	}
}

// Select decides which owners this cycle serves, and registers when the
// others are due. It is called once per cycle, with every owner of the route.
//
// An owner is served when one of its points lies between the route's due time
// and the dequeue. If that holds for at least one owner, the route is checked
// anyway, and Select adds every owner whose next point comes before the
// horizon. Such an owner is checked ahead of its point and the point is
// settled. It would otherwise bring the route back for a check of its own
// between two checks that the route gets anyway.
//
// The horizon is the route's next visit: the earliest next point of the
// owners that reached theirs. An owner is served ahead for one point only, or
// it would lose checks. Its second point therefore stays pending and brings
// the route back, so the horizon ends at the earliest such second point as
// well: the score the route returns with must still be a watermark, and no
// point settled ahead may lie at or after it.
//
// Owners with equal or nested cadences are due together and need none of
// this. For the others it merges most checks: see the measurements in ADR
// 0016 for how close a route with mixed cadences comes to one check per point
// of its fastest owner.
func (c *RouteCycle) Select(owners []domain.Workspace, s *Selection) {
	s.All, s.Any = true, false
	count := len(owners)
	if count == 1 {
		// Most routes have one owner, which needs no working memory.
		if s.Any = c.Due(owners[0].ID); !s.Any {
			s.exclude(0, 1)
		}
		return
	}
	if cap(s.points) < count {
		s.points = make([]ownerPoint, count)
	}
	points := s.points[:count]
	reached, waiting := 0, 0
	for index := range owners {
		g, due := c.reached(owners[index].ID)
		points[index] = ownerPoint{g: g}
		switch {
		case due:
			reached++
			if !s.All {
				s.mask[index] = true
			}
		case g.span > 0:
			waiting++
			points[index].next = g.next(c.key, c.at)
			s.exclude(index, count)
		default:
			s.exclude(index, count)
		}
	}
	s.Any = reached > 0
	if waiting == 0 {
		return
	}
	// The horizon is the moment before which waiting owners are served ahead.
	// Without a served owner there is no check to share, and it stays zero.
	var horizon int64
	if reached > 0 {
		horizon = math.MaxInt64
		for index := range points {
			if s.mask[index] {
				horizon = min(horizon, points[index].g.next(c.key, c.at))
			}
		}
		// An owner is served ahead for one point. Its second point ends the
		// horizon: it stays pending, so the score cannot pass it.
		for index := range points {
			point := points[index]
			if !s.mask[index] && point.g.span > 0 && point.next < horizon {
				horizon = min(horizon, point.g.next(c.key, point.next))
			}
		}
		c.settled = horizon - 1
	}
	served := reached
	for index := range points {
		point := points[index]
		switch {
		case s.mask[index] || point.g.span <= 0:
		case point.next < horizon:
			s.mask[index] = true
			served++
		default:
			c.wait(point.next)
		}
	}
	s.All = served == count
}

// Reschedule makes the owner wait for its first point after a time: the end
// of the cycle for an owner the cycle served, the import for a new route.
// Points that passed while an owner was being checked are skipped, so a check
// that takes longer than the owner's cadence does not run back to back. An
// owner that was served ahead of its point waits for the one after it.
func (c *RouteCycle) Reschedule(workspaceID uint, after time.Time) {
	c.owners = true
	if g, waits, _ := c.ownerGrid(workspaceID); waits {
		c.wait(g.next(c.key, max(after.UnixMilli(), c.at, c.settled)))
	}
}

func (c *RouteCycle) wait(point int64) {
	if c.next == 0 || point < c.next {
		c.next = point
	}
}

// Next returns when the route is due again, for a cycle that ended at
// completed: the earliest point an owner waits for. If no owner has a point
// within MaxRouteInterval, the route is revisited at its own revisit point
// instead. Revisit points are MaxRouteInterval apart and spread by route, so
// an allowance refill, an upgrade or a shorter interval reaches every route
// within that bound without all routes returning at once.
//
// The result lies in the past when an owner the cycle did not serve reached a
// point while the cycle ran. The route is then due at once, for that owner.
func (c *RouteCycle) Next(completed time.Time) time.Time {
	done := max(completed.UnixMilli(), c.at)
	if !c.owners {
		// Only a race leaves a queued route without owners in the worker's
		// view: an import may have just added one. Do not starve it.
		delay := FallbackInterval
		if c.snapshot != nil && c.snapshot.FastestCheckInterval > 0 {
			delay = min(delay, c.snapshot.FastestCheckInterval)
		}
		return time.UnixMilli(done).Add(delay)
	}
	if c.next != 0 && c.next-done <= revisitSpan {
		return time.UnixMilli(c.next)
	}
	return time.UnixMilli(grid{span: revisitSpan}.next(phaseKey(c.routeID), done))
}

// FirstRouteCheck returns when a route that is being added to the queue is
// first due: the earliest point any owner reaches after now. A route that is
// already queued for other owners is pulled forward to this time if it is
// earlier, so a new owner does not wait for their cadence.
func (p *Pacer) FirstRouteCheck(s *Snapshot, routeID uint64, workspaces []domain.Workspace, now time.Time) time.Time {
	cycle := p.BeginRouteCycle(s, routeID, now, now)
	for index := range workspaces {
		if id := workspaces[index].ID; id != 0 {
			cycle.Reschedule(id, now)
		}
	}
	return cycle.Next(now)
}

// SourceScrapeInterval is the delay before a scrape source's fetch mode is
// revisited for its subscribed workspaces, read from the snapshot the caller
// holds (nil before the first load).
func (p *Pacer) SourceScrapeInterval(s *Snapshot, workspaceIDs []uint) time.Duration {
	if s == nil {
		return FallbackInterval
	}
	interval := MaxRouteInterval
	for _, workspaceID := range workspaceIDs {
		pace := s.lookup(workspaceID)
		switch {
		case pace == nil:
			p.RequestRefresh()
			interval = min(interval, FallbackInterval)
		case !pace.ScrapesExhausted:
			interval = min(interval, pace.ScrapeInterval)
		}
	}
	return interval
}

// ScrapeDue reports whether a workspace should receive this fetch of a source
// whose fetches are sourceInterval apart and which was due at scheduled. The
// source's fastest subscriber is due on every fetch. A slower subscriber is
// due when one of its own grid points fell inside the fetch's window, so it is
// served about once per its own cadence without per-workspace state.
func (s *Snapshot) ScrapeDue(workspaceID uint, sourceID uint64, scheduled time.Time, sourceInterval time.Duration) bool {
	if s == nil {
		return true
	}
	pace := s.lookup(workspaceID)
	if pace == nil {
		return true
	}
	if pace.ScrapesExhausted {
		return false
	}
	if pace.ScrapeInterval <= sourceInterval || sourceInterval <= 0 {
		return true
	}
	at := scheduled.UnixMilli()
	return pace.scrapeGrid.due(phaseKey(sourceID), at-sourceInterval.Milliseconds(), at)
}
