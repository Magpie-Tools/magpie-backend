// Package checkpace turns each workspace's monthly check and scrape allowances
// into cadences. Snapshots are computed off the hot path from aggregated usage
// rows; checker and scraper workers only read an immutable published map.
//
// A Pacer owns all of the package's state: the published snapshot, the check
// usage meter, its flush state and its health. app constructs one per process
// and hands it to both queues, app/bootstrap (which configures and runs it and
// passes it to the checker and the scraper) and the API.
package checkpace

import (
	"context"
	"math"
	"time"
)

const (
	// MinimumCheckInterval is the fastest cadence any workspace can reach. A
	// subscription's minimum check interval can only make its cadence slower.
	MinimumCheckInterval = time.Minute
	// MinimumScrapeInterval is the fastest cadence for one subscribed source,
	// which keeps small allowances from refetching a list every few minutes.
	MinimumScrapeInterval = 15 * time.Minute
	// MaxRouteInterval bounds how long a queued route or source waits before it
	// is revisited. Slower workspace cadences are enforced by the due test, so
	// an allowance refill or upgrade takes effect within this bound.
	MaxRouteInterval = 6 * time.Hour
	// FallbackInterval applies before the first snapshot is published and to
	// workspaces created since the last refresh.
	FallbackInterval = time.Hour
	// UnlimitedDefaultInterval applies to a workspace without a monthly limit
	// until it chooses a fixed interval. Automatic spreading needs a limit.
	UnlimitedDefaultInterval = time.Hour
	// RefreshInterval is how often allowances and usage are re-read.
	RefreshInterval = time.Minute

	maxComputedInterval = 400 * 24 * time.Hour
)

// Allowance is one workspace's monthly entitlement and current-month usage.
// A zero monthly allowance means the workspace may not check or scrape, unless
// the matching Unlimited flag is set.
type Allowance struct {
	WorkspaceID uint

	// UnlimitedChecks removes the monthly check limit. The workspace runs at
	// its requested interval, or UnlimitedDefaultInterval if it has none.
	UnlimitedChecks      bool
	MonthlyChecks        uint64
	UsedChecks           uint64
	ChecksPerCycle       uint64 // active managed routes × enabled protocols
	MinimumCheckInterval time.Duration
	// RequestedCheckInterval is the workspace's own choice. Zero spreads the
	// remaining allowance automatically. A fixed interval runs until the
	// allowance is used up.
	RequestedCheckInterval time.Duration

	UnlimitedScrapes        bool
	MonthlyScrapes          uint64
	UsedScrapes             uint64
	ScrapesPerCycle         uint64 // subscribed scrape sources
	RequestedScrapeInterval time.Duration
}

// Pace is the cadence derived from one workspace's allowance, together with
// the allowance and usage it was derived from.
type Pace struct {
	CheckInterval    time.Duration
	ChecksExhausted  bool
	ScrapeInterval   time.Duration
	ScrapesExhausted bool

	// Projections at the current cadence, used to warn that a fixed interval
	// needs more than the allowance. RunOut is zero when the allowance lasts
	// until the period ends.
	ProjectedMonthlyChecks  uint64
	ChecksRunOutAt          time.Time
	ProjectedMonthlyScrapes uint64
	ScrapesRunOutAt         time.Time

	Allowance Allowance

	// counter and flushedMark connect a published snapshot to this
	// instance's check meter, so checks made since the load count at once.
	counter     *monthCounter
	flushedMark uint64
}

// Snapshot is immutable after Compute returns.
type Snapshot struct {
	// paces holds pointers so hot-path lookups do not copy a Pace.
	paces map[uint]*Pace
	// uniform, set only by Uniform, applies to every workspace.
	uniform *Pace

	PeriodStart time.Time
	PeriodEnd   time.Time
	ComputedAt  time.Time

	// Planned rates are upper bounds: shared routes and sources serve several
	// workspaces with one outbound request.
	ChecksPerSecond  float64
	ScrapesPerSecond float64

	FastestCheckInterval  time.Duration
	FastestScrapeInterval time.Duration
}

// UsagePeriod returns the calendar month (UTC) that allowances apply to. It
// matches the workspace usage meter's periods.
func UsagePeriod(now time.Time) (time.Time, time.Time) {
	utc := now.UTC()
	start := time.Date(utc.Year(), utc.Month(), 1, 0, 0, 0, 0, time.UTC)
	return start, start.AddDate(0, 1, 0)
}

// Compute spreads each workspace's remaining allowance evenly over the rest of
// the period. Re-running it every RefreshInterval corrects estimation errors:
// a workspace that consumed faster than planned slows down, one that consumed
// less speeds up, and remaining allowance can only reach zero at period end.
func Compute(now time.Time, allowances []Allowance) *Snapshot {
	start, end := UsagePeriod(now)
	remaining := end.Sub(now)
	if remaining < time.Minute {
		remaining = time.Minute
	}
	snapshot := &Snapshot{
		paces:       make(map[uint]*Pace, len(allowances)),
		PeriodStart: start,
		PeriodEnd:   end,
		ComputedAt:  now,
	}
	for _, allowance := range allowances {
		if allowance.WorkspaceID == 0 {
			continue
		}
		checkFloor := CheckFloor(allowance.MinimumCheckInterval)
		checkInterval, checksExhausted := interval(remaining, allowance.UnlimitedChecks, allowance.MonthlyChecks, allowance.UsedChecks, allowance.ChecksPerCycle, checkFloor, allowance.RequestedCheckInterval)
		scrapeInterval, scrapesExhausted := interval(remaining, allowance.UnlimitedScrapes, allowance.MonthlyScrapes, allowance.UsedScrapes, allowance.ScrapesPerCycle, MinimumScrapeInterval, allowance.RequestedScrapeInterval)
		pace := Pace{
			CheckInterval: checkInterval, ChecksExhausted: checksExhausted,
			ScrapeInterval: scrapeInterval, ScrapesExhausted: scrapesExhausted,
			Allowance: allowance,
		}
		periodLength := end.Sub(start)
		if !checksExhausted {
			pace.ProjectedMonthlyChecks = ProjectMonthly(allowance.ChecksPerCycle, checkInterval, periodLength)
			// Automatic cadence is sized to last the period by construction,
			// and an unlimited workspace never runs out.
			if allowance.RequestedCheckInterval > 0 && !allowance.UnlimitedChecks {
				pace.ChecksRunOutAt = runOutAt(now, end, allowance.MonthlyChecks-allowance.UsedChecks, allowance.ChecksPerCycle, checkInterval)
			}
		}
		if !scrapesExhausted {
			pace.ProjectedMonthlyScrapes = ProjectMonthly(allowance.ScrapesPerCycle, scrapeInterval, periodLength)
			if allowance.RequestedScrapeInterval > 0 && !allowance.UnlimitedScrapes {
				pace.ScrapesRunOutAt = runOutAt(now, end, allowance.MonthlyScrapes-allowance.UsedScrapes, allowance.ScrapesPerCycle, scrapeInterval)
			}
		}
		snapshot.paces[allowance.WorkspaceID] = &pace
		if !checksExhausted && allowance.ChecksPerCycle > 0 {
			snapshot.ChecksPerSecond += float64(allowance.ChecksPerCycle) / checkInterval.Seconds()
			if snapshot.FastestCheckInterval == 0 || checkInterval < snapshot.FastestCheckInterval {
				snapshot.FastestCheckInterval = checkInterval
			}
		}
		if !scrapesExhausted && allowance.ScrapesPerCycle > 0 {
			snapshot.ScrapesPerSecond += float64(allowance.ScrapesPerCycle) / scrapeInterval.Seconds()
			if snapshot.FastestScrapeInterval == 0 || scrapeInterval < snapshot.FastestScrapeInterval {
				snapshot.FastestScrapeInterval = scrapeInterval
			}
		}
	}
	return snapshot
}

func interval(remaining time.Duration, unlimited bool, allowance, used, perCycle uint64, floor, requested time.Duration) (time.Duration, bool) {
	if unlimited {
		if requested > 0 {
			return max(floor, requested), false
		}
		return max(floor, UnlimitedDefaultInterval), false
	}
	if used >= allowance {
		return 0, true
	}
	if requested > 0 {
		return max(floor, requested), false
	}
	if perCycle == 0 {
		return floor, false
	}
	cycles := float64(allowance-used) / float64(perCycle)
	value := float64(remaining) / cycles
	if value > float64(maxComputedInterval) || math.IsInf(value, 0) || math.IsNaN(value) {
		value = float64(maxComputedInterval)
	}
	return max(floor, time.Duration(value)), false
}

// CheckFloor is the fastest check cadence a subscription permits.
func CheckFloor(subscriptionMinimum time.Duration) time.Duration {
	return max(MinimumCheckInterval, subscriptionMinimum)
}

// ProjectMonthly estimates how many checks or scrapes a full period uses at a
// cadence. It is the basis of the over-allowance warning.
func ProjectMonthly(perCycle uint64, cadence, period time.Duration) uint64 {
	if perCycle == 0 || cadence <= 0 {
		return 0
	}
	projected := float64(perCycle) * float64(period) / float64(cadence)
	if projected >= math.MaxUint64/2 {
		return math.MaxUint64 / 2
	}
	return uint64(math.Ceil(projected))
}

// runOutAt is when the remaining allowance is used up at this cadence, or zero
// if it lasts until the period ends.
func runOutAt(now, end time.Time, remaining, perCycle uint64, cadence time.Duration) time.Time {
	if perCycle == 0 || cadence <= 0 {
		return time.Time{}
	}
	cycles := float64(remaining) / float64(perCycle)
	span := cycles * float64(cadence)
	if span >= float64(end.Sub(now)) {
		return time.Time{}
	}
	return now.Add(time.Duration(span))
}

// Pace returns one workspace's cadence. Unknown workspaces were created after
// the snapshot and are reported as not found.
func (s *Snapshot) Pace(workspaceID uint) (Pace, bool) {
	if s == nil {
		return Pace{}, false
	}
	if pace := s.lookup(workspaceID); pace != nil {
		return *pace, true
	}
	return Pace{}, false
}

// lookup is Pace without copying, for the checker's per-dequeue decisions.
func (s *Snapshot) lookup(workspaceID uint) *Pace {
	if s == nil {
		return nil
	}
	if pace, ok := s.paces[workspaceID]; ok {
		return pace
	}
	return s.uniform
}

// Uniform returns a snapshot that gives every workspace the same cadence,
// without allowance floors. It exists for tests and benchmarks that need
// short, predictable intervals; production publishes only Compute results.
func Uniform(checkInterval, scrapeInterval time.Duration) *Snapshot {
	start, end := UsagePeriod(time.Now())
	return &Snapshot{
		paces:                 map[uint]*Pace{},
		uniform:               &Pace{CheckInterval: checkInterval, ScrapeInterval: scrapeInterval},
		PeriodStart:           start,
		PeriodEnd:             end,
		ComputedAt:            time.Now(),
		FastestCheckInterval:  checkInterval,
		FastestScrapeInterval: scrapeInterval,
	}
}

// Len reports the number of workspaces in the snapshot.
func (s *Snapshot) Len() int {
	if s == nil {
		return 0
	}
	return len(s.paces)
}

// Loader reads every workspace's allowance and current-period usage.
type Loader func(ctx context.Context, now time.Time) ([]Allowance, error)

// Store connects the pacer to persistent allowances and the usage meter.
type Store struct {
	Load       Loader
	FlushUsage Flusher
}
