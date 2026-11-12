package checkpace

import (
	"time"

	"magpie/internal/domain"
)

// RouteCheckInterval is the requeue delay for a route owned by workspaces,
// read from the snapshot the caller holds (nil before the first load). The
// fastest owner sets it, capped at MaxRouteInterval. Exhausted owners never
// shorten it, and an owner the snapshot lacks asks the Pacer for a refresh.
// This is in-memory arithmetic: no Redis, database, JSON or cryptographic work
// is added to a requeue.
func (p *Pacer) RouteCheckInterval(s *Snapshot, workspaces []domain.Workspace) time.Duration {
	if s == nil {
		return FallbackInterval
	}
	if len(workspaces) == 0 {
		// Only a race leaves a queued route without owners in the worker's
		// view: an import may have just added one. Do not starve it.
		if s.FastestCheckInterval > 0 {
			return min(s.FastestCheckInterval, FallbackInterval)
		}
		return FallbackInterval
	}
	route := MaxRouteInterval
	for _, workspace := range workspaces {
		pace := s.lookup(workspace.ID)
		switch {
		case pace == nil:
			p.RequestRefresh()
			route = min(route, FallbackInterval)
		case !pace.checksExhausted():
			route = min(route, pace.CheckInterval)
		}
	}
	return route
}

// CheckDue reports whether a workspace should receive this cycle of a route
// whose cycles are routeInterval apart and which was due at scheduled. A route's
// fastest owner is due on every cycle. A slower owner is due when one of its
// own cadence boundaries fell inside the cycle's window, so it is checked about
// once per its own interval without per-workspace state. Each route has its
// own phase, which spreads a workspace's routes across its interval. Nothing
// is due while checking is paused because usage could not be written. s is
// the snapshot the caller holds; nil before the first load.
func (p *Pacer) CheckDue(s *Snapshot, workspaceID uint, routeID uint64, scheduled time.Time, routeInterval time.Duration) bool {
	if p.checksPaused() {
		return false
	}
	if s == nil {
		return true
	}
	pace := s.lookup(workspaceID)
	if pace == nil {
		return true
	}
	if p.exhaustedIn(pace, p.monthOf(scheduled)) {
		return false
	}
	return due(pace.CheckInterval, routeInterval, scheduled, routeID, workspaceID)
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
		pace, ok := s.Pace(workspaceID)
		switch {
		case !ok:
			p.RequestRefresh()
			interval = min(interval, FallbackInterval)
		case !pace.ScrapesExhausted:
			interval = min(interval, pace.ScrapeInterval)
		}
	}
	return interval
}

// ScrapeDue is CheckDue for a workspace's subscription to one scrape source.
func (s *Snapshot) ScrapeDue(workspaceID uint, sourceID uint64, scheduled time.Time, sourceInterval time.Duration) bool {
	if s == nil {
		return true
	}
	pace, ok := s.Pace(workspaceID)
	if !ok {
		return true
	}
	if pace.ScrapesExhausted {
		return false
	}
	return due(pace.ScrapeInterval, sourceInterval, scheduled, sourceID, workspaceID)
}

func due(interval, window time.Duration, at time.Time, itemID uint64, workspaceID uint) bool {
	if interval <= window || window <= 0 {
		return true
	}
	intervalMs := interval.Milliseconds()
	phase := int64(mix(itemID^uint64(workspaceID)<<40) % uint64(intervalMs))
	end := at.UnixMilli() + phase
	start := end - window.Milliseconds()
	return floorDiv(end, intervalMs) != floorDiv(start, intervalMs)
}

func floorDiv(value, divisor int64) int64 {
	quotient := value / divisor
	if value%divisor != 0 && (value < 0) != (divisor < 0) {
		quotient--
	}
	return quotient
}

// mix is the SplitMix64 finalizer, used only to spread phases.
func mix(value uint64) uint64 {
	value += 0x9e3779b97f4a7c15
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return value ^ (value >> 31)
}
