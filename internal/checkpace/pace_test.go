package checkpace

import (
	"math"
	"testing"
	"time"

	"magpie/internal/domain"
)

// Mid-month keeps the remaining period a round, known duration.
var testNow = time.Date(2026, time.November, 16, 0, 0, 0, 0, time.UTC)

const testRemaining = 15 * 24 * time.Hour

// noPacer reads snapshots without a meter: it never pauses and counts nothing,
// which is what the pure cadence tests need.
var noPacer *Pacer

func TestComputeSpreadsRemainingAllowanceOverRemainingPeriod(t *testing.T) {
	snapshot := Compute(testNow, []Allowance{{
		WorkspaceID: 1, MonthlyChecks: 1_000_000, UsedChecks: 280_000, ChecksPerCycle: 1_000,
		MonthlyScrapes: 1_000, ScrapesPerCycle: 2,
	}})
	pace, ok := snapshot.Pace(1)
	if !ok {
		t.Fatal("workspace missing from snapshot")
	}
	// 720,000 remaining checks / 1,000 per cycle = 720 cycles in 15 days.
	if want := testRemaining / 720; pace.CheckInterval != want {
		t.Fatalf("check interval = %s, want %s", pace.CheckInterval, want)
	}
	// 1,000 scrapes / 2 sources = 500 cycles in 15 days.
	if want := testRemaining / 500; pace.ScrapeInterval != want {
		t.Fatalf("scrape interval = %s, want %s", pace.ScrapeInterval, want)
	}
	if pace.ChecksExhausted || pace.ScrapesExhausted {
		t.Fatalf("allowances should not be exhausted: %+v", pace)
	}
	if want := 1_000 / (testRemaining / 720).Seconds(); math.Abs(snapshot.ChecksPerSecond-want) > 1e-9 {
		t.Fatalf("planned checks/s = %f, want %f", snapshot.ChecksPerSecond, want)
	}
}

func TestComputeAppliesCadenceFloors(t *testing.T) {
	snapshot := Compute(testNow, []Allowance{
		{WorkspaceID: 1, MonthlyChecks: math.MaxUint32, ChecksPerCycle: 1, MonthlyScrapes: math.MaxUint32, ScrapesPerCycle: 1},
		{WorkspaceID: 2, MonthlyChecks: math.MaxUint32, ChecksPerCycle: 1, MinimumCheckInterval: 30 * time.Minute, MonthlyScrapes: 1, ScrapesPerCycle: 1},
	})
	fast, _ := snapshot.Pace(1)
	if fast.CheckInterval != MinimumCheckInterval || fast.ScrapeInterval != MinimumScrapeInterval {
		t.Fatalf("instance floors not applied: %+v", fast)
	}
	plan, _ := snapshot.Pace(2)
	if plan.CheckInterval != 30*time.Minute {
		t.Fatalf("subscription minimum interval not applied: %s", plan.CheckInterval)
	}
	if snapshot.FastestCheckInterval != MinimumCheckInterval {
		t.Fatalf("fastest check interval = %s", snapshot.FastestCheckInterval)
	}
}

func TestComputeStopsExhaustedAllowances(t *testing.T) {
	snapshot := Compute(testNow, []Allowance{
		{WorkspaceID: 1, MonthlyChecks: 100, UsedChecks: 100, ChecksPerCycle: 10, MonthlyScrapes: 5, UsedScrapes: 9, ScrapesPerCycle: 1},
		{WorkspaceID: 2, MonthlyChecks: 0, ChecksPerCycle: 10},
	})
	for _, id := range []uint{1, 2} {
		pace, _ := snapshot.Pace(id)
		if !pace.ChecksExhausted {
			t.Fatalf("workspace %d should be out of checks: %+v", id, pace)
		}
	}
	if pace, _ := snapshot.Pace(1); !pace.ScrapesExhausted {
		t.Fatalf("overused scrape allowance should be exhausted: %+v", pace)
	}
	if snapshot.ChecksPerSecond != 0 {
		t.Fatalf("exhausted workspaces must not plan checks: %f", snapshot.ChecksPerSecond)
	}
}

func TestComputeWithoutWorkUsesFloor(t *testing.T) {
	snapshot := Compute(testNow, []Allowance{{WorkspaceID: 1, MonthlyChecks: 10, MonthlyScrapes: 10}})
	pace, _ := snapshot.Pace(1)
	if pace.CheckInterval != MinimumCheckInterval || pace.ChecksExhausted {
		t.Fatalf("empty workspace pace = %+v", pace)
	}
}

func TestRecomputingAfterOverspendSlowsDown(t *testing.T) {
	base := Allowance{WorkspaceID: 1, MonthlyChecks: 1_000_000, ChecksPerCycle: 1_000}
	onPlan := base
	onPlan.UsedChecks = 500_000
	overspent := base
	overspent.UsedChecks = 750_000
	planned, _ := Compute(testNow, []Allowance{onPlan}).Pace(1)
	slowed, _ := Compute(testNow, []Allowance{overspent}).Pace(1)
	if slowed.CheckInterval <= planned.CheckInterval {
		t.Fatalf("overspent cadence %s should be slower than %s", slowed.CheckInterval, planned.CheckInterval)
	}
}

func TestUsagePeriodIsUTCMonth(t *testing.T) {
	start, end := UsagePeriod(time.Date(2026, time.December, 31, 23, 30, 0, 0, time.FixedZone("UTC-5", -5*3600)))
	if !start.Equal(time.Date(2027, time.January, 1, 0, 0, 0, 0, time.UTC)) || !end.Equal(time.Date(2027, time.February, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("period = %s – %s", start, end)
	}
}

func TestRouteCheckIntervalUsesFastestActiveOwner(t *testing.T) {
	snapshot := Compute(testNow, []Allowance{
		{WorkspaceID: 1, MonthlyChecks: uint64(testRemaining / (10 * time.Minute)), ChecksPerCycle: 1},
		{WorkspaceID: 2, MonthlyChecks: uint64(testRemaining / (2 * time.Minute)), ChecksPerCycle: 1},
		{WorkspaceID: 3, MonthlyChecks: 1, UsedChecks: 1, ChecksPerCycle: 1},
		{WorkspaceID: 4, MonthlyChecks: 1, ChecksPerCycle: 1},
	})
	owners := func(ids ...uint) []domain.Workspace {
		out := make([]domain.Workspace, 0, len(ids))
		for _, id := range ids {
			out = append(out, domain.Workspace{ID: id})
		}
		return out
	}
	if got := noPacer.RouteCheckInterval(snapshot, owners(1, 2)); got != 2*time.Minute {
		t.Fatalf("shared route interval = %s, want 2m", got)
	}
	if got := noPacer.RouteCheckInterval(snapshot, owners(3)); got != MaxRouteInterval {
		t.Fatalf("exhausted route interval = %s, want the revisit cap", got)
	}
	if got := noPacer.RouteCheckInterval(snapshot, owners(4)); got != MaxRouteInterval {
		t.Fatalf("slow route interval = %s, want the revisit cap", got)
	}
	if got := noPacer.RouteCheckInterval(snapshot, owners(4, 99)); got != FallbackInterval {
		t.Fatalf("unknown owner interval = %s, want fallback", got)
	}
	if got := noPacer.RouteCheckInterval(snapshot, owners(1, 99)); got != 10*time.Minute {
		t.Fatalf("a faster known owner must win over the fallback: %s", got)
	}
	if got := (*Pacer)(nil).RouteCheckInterval(nil, owners(1)); got != FallbackInterval {
		t.Fatalf("startup interval = %s, want fallback", got)
	}
}

func TestCheckDueMatchesEachOwnersCadenceOnSharedRoute(t *testing.T) {
	snapshot := Compute(testNow, []Allowance{
		{WorkspaceID: 1, MonthlyChecks: uint64(testRemaining / time.Minute), ChecksPerCycle: 1},
		{WorkspaceID: 2, MonthlyChecks: uint64(testRemaining / (10 * time.Minute)), ChecksPerCycle: 1},
		{WorkspaceID: 3, MonthlyChecks: 1, UsedChecks: 1, ChecksPerCycle: 1},
	})
	owners := []domain.Workspace{{ID: 1}, {ID: 2}, {ID: 3}}
	route := noPacer.RouteCheckInterval(snapshot, owners)
	if route != time.Minute {
		t.Fatalf("route interval = %s", route)
	}
	const cycles = 6_000
	counts := map[uint]int{}
	for routeID := uint64(1); routeID <= 5; routeID++ {
		scheduled := testNow.Add(time.Duration(routeID) * 7 * time.Second)
		for cycle := 0; cycle < cycles; cycle++ {
			for _, owner := range owners {
				if noPacer.CheckDue(snapshot, owner.ID, routeID, scheduled, route) {
					counts[owner.ID]++
				}
			}
			scheduled = scheduled.Add(route)
		}
	}
	if counts[1] != 5*cycles {
		t.Fatalf("fastest owner checks = %d, want every cycle", counts[1])
	}
	// The 10-minute owner sees one boundary per ten one-minute windows.
	if want := 5 * cycles / 10; counts[2] < want-5 || counts[2] > want+5 {
		t.Fatalf("slower owner checks = %d, want about %d", counts[2], want)
	}
	if counts[3] != 0 {
		t.Fatalf("exhausted owner was checked %d times", counts[3])
	}
}

func TestCheckDueSpreadsSlowSingleOwnerAcrossRevisits(t *testing.T) {
	// 2-day cadence is slower than the revisit cap, so one owner is due on
	// roughly one revisit in eight.
	snapshot := Compute(testNow, []Allowance{{WorkspaceID: 1, MonthlyChecks: 7, ChecksPerCycle: 1}})
	pace, _ := snapshot.Pace(1)
	route := noPacer.RouteCheckInterval(snapshot, []domain.Workspace{{ID: 1}})
	if route != MaxRouteInterval || pace.CheckInterval <= route {
		t.Fatalf("route=%s cadence=%s", route, pace.CheckInterval)
	}
	due := 0
	const routes, revisits = 400, 40
	for routeID := uint64(1); routeID <= routes; routeID++ {
		scheduled := testNow
		for i := 0; i < revisits; i++ {
			if noPacer.CheckDue(snapshot, 1, routeID, scheduled, route) {
				due++
			}
			scheduled = scheduled.Add(route)
		}
	}
	want := float64(routes*revisits) * route.Seconds() / pace.CheckInterval.Seconds()
	if math.Abs(float64(due)-want) > want*0.05 {
		t.Fatalf("due checks = %d, want about %.0f", due, want)
	}
}

func TestUnknownWorkspaceStaysDueUntilRefresh(t *testing.T) {
	snapshot := Compute(testNow, nil)
	if !noPacer.CheckDue(snapshot, 7, 1, testNow, time.Minute) || !snapshot.ScrapeDue(7, 1, testNow, time.Minute) {
		t.Fatal("new workspaces must not be skipped before the next refresh")
	}
	if !(*Pacer)(nil).CheckDue(nil, 7, 1, testNow, time.Minute) {
		t.Fatal("checks must run before the first snapshot")
	}
}

func TestSourceScrapeIntervalAndDue(t *testing.T) {
	snapshot := Compute(testNow, []Allowance{
		{WorkspaceID: 1, MonthlyScrapes: uint64(testRemaining / (30 * time.Minute)), ScrapesPerCycle: 1},
		{WorkspaceID: 2, MonthlyScrapes: uint64(testRemaining / (2 * time.Hour)), ScrapesPerCycle: 1},
		{WorkspaceID: 3, MonthlyScrapes: 2, UsedScrapes: 2, ScrapesPerCycle: 1},
	})
	interval := noPacer.SourceScrapeInterval(snapshot, []uint{1, 2, 3})
	if interval != 30*time.Minute {
		t.Fatalf("source interval = %s", interval)
	}
	counts := map[uint]int{}
	scheduled := testNow
	for i := 0; i < 400; i++ {
		for _, id := range []uint{1, 2, 3} {
			if snapshot.ScrapeDue(id, 11, scheduled, interval) {
				counts[id]++
			}
		}
		scheduled = scheduled.Add(interval)
	}
	if counts[1] != 400 || counts[2] != 100 || counts[3] != 0 {
		t.Fatalf("scrape counts = %v, want 400/100/0", counts)
	}
}

func TestRequestRefreshNeverBlocks(t *testing.T) {
	p := NewPacer()
	for i := 0; i < 10; i++ {
		p.RequestRefresh()
	}
	select {
	case <-p.refreshCh:
	default:
		t.Fatal("refresh request was dropped")
	}
}

func TestRequestedIntervalOverridesSpreadUntilAllowanceIsUsed(t *testing.T) {
	snapshot := Compute(testNow, []Allowance{
		// 1,000 routes every 10 minutes for the remaining 15 days needs
		// 2,160,000 checks, but only 1,000,000 remain.
		{WorkspaceID: 1, MonthlyChecks: 1_000_000, ChecksPerCycle: 1_000, RequestedCheckInterval: 10 * time.Minute},
		// Below the plan minimum, the request is raised to the floor.
		{WorkspaceID: 2, MonthlyChecks: 1_000_000, ChecksPerCycle: 1, MinimumCheckInterval: 30 * time.Minute, RequestedCheckInterval: 5 * time.Minute},
		// Fits the allowance: no run-out.
		{WorkspaceID: 3, MonthlyChecks: 1_000_000, ChecksPerCycle: 10, RequestedCheckInterval: time.Hour},
		{WorkspaceID: 4, MonthlyChecks: 10, UsedChecks: 10, ChecksPerCycle: 10, RequestedCheckInterval: time.Hour},
	})
	tooFast, _ := snapshot.Pace(1)
	if tooFast.CheckInterval != 10*time.Minute || tooFast.ChecksExhausted {
		t.Fatalf("requested interval not used: %+v", tooFast)
	}
	if want := uint64(1_000 * 30 * 24 * 6); tooFast.ProjectedMonthlyChecks != want {
		t.Fatalf("projected checks = %d, want %d", tooFast.ProjectedMonthlyChecks, want)
	}
	// 1,000 cycles of 10 minutes is about 6.9 days from now.
	if want := testNow.Add(1_000 * 10 * time.Minute); !tooFast.ChecksRunOutAt.Equal(want) {
		t.Fatalf("run-out = %v, want %v", tooFast.ChecksRunOutAt, want)
	}
	floored, _ := snapshot.Pace(2)
	if floored.CheckInterval != 30*time.Minute {
		t.Fatalf("requested interval below the plan minimum = %s", floored.CheckInterval)
	}
	fits, _ := snapshot.Pace(3)
	if !fits.ChecksRunOutAt.IsZero() || fits.ProjectedMonthlyChecks > 1_000_000 {
		t.Fatalf("fitting interval should not run out: %+v", fits)
	}
	if stopped, _ := snapshot.Pace(4); !stopped.ChecksExhausted {
		t.Fatalf("a fixed interval must stop once the allowance is used: %+v", stopped)
	}
}

func TestAutomaticCadenceNeverReportsRunOut(t *testing.T) {
	snapshot := Compute(testNow, []Allowance{{WorkspaceID: 1, MonthlyChecks: 720_000, ChecksPerCycle: 1_000}})
	pace, _ := snapshot.Pace(1)
	if !pace.ChecksRunOutAt.IsZero() {
		t.Fatalf("automatic cadence reported a run-out at %v", pace.ChecksRunOutAt)
	}
}

func TestUnlimitedAllowanceRunsAtFixedIntervalAndNeverStops(t *testing.T) {
	snapshot := Compute(testNow, []Allowance{
		// Far past any monthly value: still running at the chosen interval.
		{WorkspaceID: 1, UnlimitedChecks: true, UsedChecks: 50_000_000, ChecksPerCycle: 1_000, RequestedCheckInterval: 5 * time.Minute,
			UnlimitedScrapes: true, UsedScrapes: 10_000, ScrapesPerCycle: 3, RequestedScrapeInterval: 30 * time.Minute},
		// No interval chosen yet: the hourly default, never automatic spreading.
		{WorkspaceID: 2, UnlimitedChecks: true, ChecksPerCycle: 10, UnlimitedScrapes: true, ScrapesPerCycle: 1},
		// The plan minimum still applies.
		{WorkspaceID: 3, UnlimitedChecks: true, ChecksPerCycle: 1, MinimumCheckInterval: 10 * time.Minute, RequestedCheckInterval: 2 * time.Minute},
	})
	fixed, _ := snapshot.Pace(1)
	if fixed.ChecksExhausted || fixed.CheckInterval != 5*time.Minute || fixed.ScrapesExhausted || fixed.ScrapeInterval != 30*time.Minute {
		t.Fatalf("unlimited workspace with fixed intervals = %+v", fixed)
	}
	if !fixed.ChecksRunOutAt.IsZero() || !fixed.ScrapesRunOutAt.IsZero() {
		t.Fatalf("unlimited workspace reported a run-out: %+v", fixed)
	}
	if want := uint64(1_000 * 30 * 24 * 12); fixed.ProjectedMonthlyChecks != want {
		t.Fatalf("projected checks = %d, want %d", fixed.ProjectedMonthlyChecks, want)
	}
	unset, _ := snapshot.Pace(2)
	if unset.ChecksExhausted || unset.CheckInterval != UnlimitedDefaultInterval || unset.ScrapeInterval != UnlimitedDefaultInterval {
		t.Fatalf("unlimited workspace without an interval = %+v", unset)
	}
	if floored, _ := snapshot.Pace(3); floored.CheckInterval != 10*time.Minute {
		t.Fatalf("unlimited workspace ignored the plan minimum: %s", floored.CheckInterval)
	}
}
