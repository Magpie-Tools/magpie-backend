package checkpace

import (
	"context"
	"math"
	"testing"
	"time"

	"magpie/internal/domain"
)

// every is a workspace on a fixed check interval without a monthly limit.
func every(workspaceID uint, interval time.Duration) Allowance {
	return Allowance{WorkspaceID: workspaceID, UnlimitedChecks: true, RequestedCheckInterval: interval, ChecksPerCycle: 1}
}

func owners(ids ...uint) []domain.Workspace {
	out := make([]domain.Workspace, 0, len(ids))
	for _, id := range ids {
		out = append(out, domain.Workspace{ID: id})
	}
	return out
}

// ownPoint returns an owner's first cadence point on a route after a time. It
// reports false for an owner that does not hold the route in the queue.
func ownPoint(p *Pacer, s *Snapshot, workspaceID uint, routeID uint64, after time.Time) (time.Time, bool) {
	cycle := p.BeginRouteCycle(s, routeID, after, after)
	cycle.Reschedule(workspaceID, after)
	return time.UnixMilli(cycle.next), cycle.next != 0
}

// dueAt reports whether a route that was due at a time, and is dequeued at
// now, is checked for the owner.
func dueAt(p *Pacer, s *Snapshot, workspaceID uint, routeID uint64, due, now time.Time) bool {
	cycle := p.BeginRouteCycle(s, routeID, due, now)
	return cycle.Due(workspaceID)
}

// queuedRoute replays what the queue and one checker worker do with a route:
// dequeue it when it is due and a worker is free, check it for the owners the
// cycle serves, and requeue it at the cycle's next due time.
type queuedRoute struct {
	id       uint64
	owners   []uint
	due      time.Time // the queue score
	free     time.Time // when the previous cycle ended
	checks   map[uint][]time.Time
	dequeues []time.Time
	// visits counts the cycles that checked the route for at least one owner.
	visits    int
	list      []domain.Workspace
	selection Selection
}

func queueRoute(p *Pacer, s *Snapshot, id uint64, at time.Time, ownerIDs ...uint) *queuedRoute {
	return &queuedRoute{
		id: id, owners: ownerIDs, list: owners(ownerIDs...), free: at, checks: map[uint][]time.Time{},
		due: p.FirstRouteCheck(s, id, owners(ownerIDs...), at),
	}
}

// step runs one cycle and returns when it was dequeued. A cycle that checks
// at least one owner takes checkTime.
func (r *queuedRoute) step(p *Pacer, snapshotAt func(time.Time) *Snapshot, checkTime time.Duration) time.Time {
	now := r.due
	if r.free.After(now) {
		now = r.free
	}
	cycle := p.BeginRouteCycle(snapshotAt(now), r.id, r.due, now)
	cycle.Select(r.list, &r.selection)
	var served []uint
	for index, owner := range r.owners {
		if r.selection.Has(index) {
			served = append(served, owner)
			r.checks[owner] = append(r.checks[owner], now)
		}
	}
	if len(served) > 0 {
		r.visits++
	}
	completed := now
	if len(served) > 0 {
		completed = now.Add(checkTime)
	}
	for _, owner := range served {
		cycle.Reschedule(owner, completed)
	}
	r.due, r.free = cycle.Next(completed), completed
	r.dequeues = append(r.dequeues, now)
	return now
}

func (r *queuedRoute) runUntil(end time.Time, p *Pacer, snapshotAt func(time.Time) *Snapshot, checkTime time.Duration) {
	for r.due.Before(end) && r.free.Before(end) {
		r.step(p, snapshotAt, checkTime)
	}
}

func always(s *Snapshot) func(time.Time) *Snapshot {
	return func(time.Time) *Snapshot { return s }
}

func gaps(times []time.Time) []time.Duration {
	out := make([]time.Duration, 0, len(times))
	for index := 1; index < len(times); index++ {
		out = append(out, times[index].Sub(times[index-1]))
	}
	return out
}

func TestSharedRouteChecksEachOwnerAtItsOwnCadence(t *testing.T) {
	snapshot := Compute(testNow, []Allowance{
		every(1, time.Minute),
		every(2, 10*time.Minute),
		{WorkspaceID: 3, MonthlyChecks: 1, UsedChecks: 1, ChecksPerCycle: 1},
	})
	const minutes = 6_000
	for routeID := uint64(1); routeID <= 5; routeID++ {
		route := queueRoute(noPacer, snapshot, routeID, testNow, 1, 2, 3)
		if wait := route.due.Sub(testNow); wait <= 0 || wait > time.Minute {
			t.Fatalf("route %d is first due after %s, want within its fastest owner's minute", routeID, wait)
		}
		route.runUntil(testNow.Add(minutes*time.Minute), noPacer, always(snapshot), 0)
		if got := len(route.checks[1]); got < minutes-1 || got > minutes {
			t.Fatalf("route %d: one-minute owner checked %d times in %d minutes", routeID, got, minutes)
		}
		if got := len(route.checks[2]); got < minutes/10-1 || got > minutes/10 {
			t.Fatalf("route %d: ten-minute owner checked %d times in %d minutes", routeID, got, minutes)
		}
		for _, gap := range gaps(route.checks[2]) {
			if gap != 10*time.Minute {
				t.Fatalf("route %d: ten-minute owner checked %s apart", routeID, gap)
			}
		}
		if len(route.checks[3]) != 0 {
			t.Fatalf("route %d: an owner without allowance was checked %d times", routeID, len(route.checks[3]))
		}
		// The ten-minute owner's points are points of the one-minute owner,
		// so both share the route's one check per minute.
		if len(route.dequeues) > minutes || route.visits != len(route.checks[1]) {
			t.Fatalf("route %d was dequeued %d times and checked %d times for %d one-minute checks", routeID, len(route.dequeues), route.visits, len(route.checks[1]))
		}
	}
}

// The queue score is the route's only scheduling state, so it must stay a
// watermark through every cycle: no owner has a pending point before it, and
// the route is not due before the earliest pending point. Every check must
// settle at least one pending point of the owner it is charged to, or an owner
// would be checked twice for one point. Checked for random owner sets, check
// durations and queue delays, including owners served ahead of their point.
func TestRouteScoreStaysAWatermarkOfItsOwnersPoints(t *testing.T) {
	fixed := []time.Duration{time.Minute, 61 * time.Second, 2 * time.Minute, 5 * time.Minute, 7 * time.Minute, 9 * time.Minute, 10 * time.Minute, 11 * time.Minute, 30 * time.Minute, time.Hour, 6 * time.Hour}
	spread := []time.Duration{3 * time.Minute, 9 * time.Minute, 47 * time.Minute, 50 * time.Minute, 3 * time.Hour, 30 * time.Hour}
	checkTimes := []time.Duration{0, 5 * time.Second, 40 * time.Second, 3 * time.Minute, 20 * time.Minute}
	delays := []time.Duration{0, time.Second, 30 * time.Second, 10 * time.Minute}
	for scenario := uint64(1); scenario <= 300; scenario++ {
		state := scenario
		random := func(limit int) int {
			state = mix(state)
			return int(state % uint64(limit))
		}
		var allowances []Allowance
		var ids []uint
		for id := uint(1); id <= uint(2+random(6)); id++ {
			switch random(8) {
			case 0:
				allowances = append(allowances, Allowance{WorkspaceID: id, MonthlyChecks: 5, UsedChecks: 5, ChecksPerCycle: 1})
			case 1, 2, 3:
				const perCycle = 1_000
				cadence := spread[random(len(spread))]
				allowances = append(allowances, Allowance{WorkspaceID: id, ChecksPerCycle: perCycle, MonthlyChecks: uint64(float64(testRemaining) / float64(cadence) * perCycle)})
			default:
				allowances = append(allowances, every(id, fixed[random(len(fixed))]))
			}
			ids = append(ids, id)
		}
		snapshot := Compute(testNow, allowances)
		checkTime, delay := checkTimes[random(len(checkTimes))], delays[random(len(delays))]
		route := queueRoute(noPacer, snapshot, scenario, testNow, ids...)
		// settled[i] is the moment up to which owner i's points are settled.
		settled := make([]time.Time, len(ids))
		for index := range settled {
			settled[index] = testNow
		}
		for step := 0; step < 4_000; step++ {
			now := route.due
			if route.free.After(now) {
				now = route.free
			}
			if delay > 0 {
				now = now.Add(time.Duration(random(int(delay/time.Millisecond)+1)) * time.Millisecond)
			}
			cycle := noPacer.BeginRouteCycle(snapshot, route.id, route.due, now)
			cycle.Select(route.list, &route.selection)
			completed := now
			if route.selection.Any {
				completed = now.Add(checkTime)
			}
			for index, id := range ids {
				if !route.selection.Has(index) {
					continue
				}
				pending, _ := ownPoint(noPacer, snapshot, id, route.id, settled[index])
				settled[index] = time.UnixMilli(max(completed.UnixMilli(), cycle.settled))
				if pending.After(settled[index]) {
					t.Fatalf("scenario %d step %d: owner %d was checked at %s without a pending point; its next one is %s", scenario, step, id, now, pending)
				}
				if pending.After(now) && checkTime == 0 && pending.Sub(now) >= time.Duration(snapshot.lookup(id).checkGrid.span)*time.Millisecond {
					t.Fatalf("scenario %d step %d: owner %d was checked %s ahead of its point", scenario, step, id, pending.Sub(now))
				}
				cycle.Reschedule(id, completed)
			}
			next := cycle.Next(completed)
			earliest := time.Time{}
			for index, id := range ids {
				// A point that an owner reached during its own check is
				// skipped, unless the route is due again at once for another
				// owner: the owner then shares that check, so the point is
				// still pending. The points up to the dequeue, and the one
				// the owner was served ahead for, stay settled.
				if last := next.Add(-time.Millisecond); route.selection.Has(index) && settled[index].After(last) {
					settled[index] = time.UnixMilli(max(now.UnixMilli(), cycle.settled, last.UnixMilli()))
				}
				pending, holds := ownPoint(noPacer, snapshot, id, route.id, settled[index])
				if !holds {
					continue
				}
				if pending.Before(next) {
					t.Fatalf("scenario %d step %d: the route is due at %s, after owner %d's pending point %s", scenario, step, next, id, pending)
				}
				if earliest.IsZero() || pending.Before(earliest) {
					earliest = pending
				}
			}
			if !earliest.IsZero() && !next.Equal(earliest) && earliest.Sub(completed) <= MaxRouteInterval {
				t.Fatalf("scenario %d step %d: the route is due at %s, before its earliest pending point %s", scenario, step, next, earliest)
			}
			route.due, route.free = next, completed
		}
	}
}

// pointsOf counts an owner's cadence points on a route in (from, to].
func pointsOf(s *Snapshot, workspaceID uint, routeID uint64, from, to time.Time) int {
	count := 0
	for at := from; ; count++ {
		point, holds := ownPoint(noPacer, s, workspaceID, routeID, at)
		if !holds || point.After(to) {
			return count
		}
		at = point
	}
}

// Owners of one route share its checks: the route is checked at the rate of
// its fastest owner, however many owners it has and whatever their cadences
// are, and every owner still gets one check per point of its own cadence.
func TestOwnersOfARouteShareItsChecks(t *testing.T) {
	automatic := func(workspaceID uint, cadence time.Duration) Allowance {
		const perCycle = 1_000
		return Allowance{WorkspaceID: workspaceID, ChecksPerCycle: perCycle, MonthlyChecks: uint64(float64(testRemaining) / float64(cadence) * perCycle)}
	}
	many := make([]Allowance, 0, 300)
	for id := uint(1); id <= 300; id++ {
		many = append(many, every(id, time.Minute))
	}
	for _, test := range []struct {
		name       string
		allowances []Allowance
		fastest    uint
		// ahead is how far before its point an owner may be checked: the
		// longest gap of the fastest owner.
		ahead time.Duration
		// spare is how many percent more checks than the fastest owner has
		// points the route may get. Grids that are not nested cannot share
		// every check.
		spare     int
		checkTime time.Duration
		hours     int
	}{
		{name: "300 owners on the same interval", allowances: many, fastest: 1, hours: 4},
		{name: "300 owners on the same interval, slow checks", allowances: many, fastest: 1, checkTime: 20 * time.Second, hours: 4},
		{
			name: "cadences that do not divide each other", fastest: 1, ahead: 5 * time.Minute, spare: 2, hours: 21 * 24,
			allowances: []Allowance{
				every(1, 5*time.Minute), every(2, 7*time.Minute), every(3, 11*time.Minute), every(4, time.Hour),
				automatic(5, 47*time.Minute), automatic(6, 3*time.Hour), automatic(7, 96*time.Hour),
			},
		},
		{
			name: "automatic cadences on one floor", fastest: 1, hours: 21 * 24,
			allowances: []Allowance{automatic(1, 9*time.Minute), automatic(2, 47*time.Minute), automatic(3, 50*time.Minute), automatic(4, 30*time.Hour)},
		},
		{
			name: "no owner much faster than the others", fastest: 1, ahead: 9 * time.Minute, hours: 21 * 24,
			allowances: []Allowance{every(1, 9*time.Minute), every(2, 10*time.Minute), every(3, 11*time.Minute)},
		},
		{
			// The automatic owner's points are 64 or 128 minutes apart. Where
			// two of them fall between two of the fixed owner's, one check
			// cannot serve both owners.
			name: "a fixed and an automatic cadence at the same rate", fastest: 1, ahead: 128 * time.Minute, spare: 10, hours: 21 * 24,
			allowances: []Allowance{every(1, 90*time.Minute), automatic(2, 91*time.Minute)},
		},
	} {
		snapshot := Compute(testNow, test.allowances)
		ids := make([]uint, 0, len(test.allowances))
		for _, allowance := range test.allowances {
			ids = append(ids, allowance.WorkspaceID)
		}
		end := testNow.Add(time.Duration(test.hours) * time.Hour)
		for routeID := uint64(1); routeID <= 3; routeID++ {
			route := queueRoute(noPacer, snapshot, routeID, testNow, ids...)
			route.runUntil(end, noPacer, always(snapshot), test.checkTime)
			fastest := pointsOf(snapshot, test.fastest, routeID, testNow, end)
			// Without sharing the route would be checked once per point of
			// every owner.
			if route.visits > fastest+fastest*test.spare/100+1 {
				t.Fatalf("%s: route %d was checked %d times; its fastest owner has %d points", test.name, routeID, route.visits, fastest)
			}
			for _, id := range ids {
				pace, _ := snapshot.Pace(id)
				want, got := pointsOf(snapshot, id, routeID, testNow, end), len(route.checks[id])
				// The last check may be ahead of a point just after the end.
				if test.checkTime == 0 && (got > want+1 || got < want-1) {
					t.Fatalf("%s: route %d: owner %d was checked %d times for %d points", test.name, routeID, id, got, want)
				}
				// A check that takes longer than the time to the next point
				// skips that point.
				if got > want+1 || got < want/2 {
					t.Fatalf("%s: route %d: owner %d was checked %d times for %d points", test.name, routeID, id, got, want)
				}
				shortest, longest := time.Duration(pace.checkGrid.span)*time.Millisecond, time.Duration(pace.checkGrid.span)*time.Millisecond
				if pace.checkGrid.extra != 0 {
					shortest /= 2
				}
				for _, gap := range gaps(route.checks[id]) {
					if gap < shortest-test.ahead-test.checkTime || gap > longest+test.ahead+test.checkTime {
						t.Fatalf("%s: route %d: owner %d was checked %s apart; its points are %s to %s apart", test.name, routeID, id, gap, shortest, longest)
					}
				}
			}
		}
	}
}

// A check takes time, and a route can wait in the queue. Neither may cost a
// slower owner its checks: a point it reaches while the route is being checked
// for someone else makes the route due again at once.
func TestTimeSpentCheckingDoesNotCostSlowerOwnersTheirCadence(t *testing.T) {
	snapshot := Compute(testNow, []Allowance{every(1, time.Minute), every(2, time.Hour)})
	const hours = 200
	for _, checkTime := range []time.Duration{10 * time.Second, 30 * time.Second, 90 * time.Second, 5 * time.Minute} {
		slow := 0
		const routes = 20
		for routeID := uint64(1); routeID <= routes; routeID++ {
			route := queueRoute(noPacer, snapshot, routeID, testNow, 1, 2)
			route.runUntil(testNow.Add(hours*time.Hour), noPacer, always(snapshot), checkTime)
			slow += len(route.checks[2])
			for _, gap := range gaps(route.checks[2]) {
				// Its check starts at its point, or when the cycle that
				// overlapped the point ends.
				if gap < time.Hour-checkTime || gap > time.Hour+checkTime {
					t.Fatalf("check time %s: hourly owner checked %s apart on route %d", checkTime, gap, routeID)
				}
			}
			// The fast owner cannot be checked more often than the checks
			// take, and skips the points it reached while being checked.
			fast, most := len(route.checks[1]), int(hours*time.Hour/max(checkTime, time.Minute))
			if fast > most+1 || fast < most/3 {
				t.Fatalf("check time %s: one-minute owner checked %d times on route %d, want at most %d", checkTime, fast, routeID, most)
			}
		}
		if want := routes * hours; slow < want-routes || slow > want {
			t.Fatalf("check time %s: hourly owner got %d checks, want %d", checkTime, slow, want)
		}
	}
}

// An automatic cadence changes a little at every refresh. One route's checks
// must stay a span or half a span apart, and add up to the cadence.
func TestAutomaticCadenceStaysRegularWhileItIsRecomputed(t *testing.T) {
	for _, test := range []struct {
		name          string
		cadence       time.Duration
		span          time.Duration
		fastCoOwner   bool
		routes, hours int
	}{
		{name: "slower owner of a shared route", cadence: 47 * time.Minute, span: 64 * time.Minute, fastCoOwner: true, routes: 40, hours: 14 * 24},
		{name: "single owner slower than the revisit bound", cadence: 96 * time.Hour, span: 8192 * time.Minute, routes: 400, hours: 14 * 24},
	} {
		// On plan, the remaining allowance always spreads to the same
		// cadence. Usage that runs a little ahead or behind moves it.
		const perCycle = 1_000
		monthly := uint64(float64(testRemaining) / float64(test.cadence) * perCycle)
		seen := map[time.Duration]bool{}
		snapshotAt := func(now time.Time) *Snapshot {
			minute := now.Truncate(time.Minute)
			elapsed := minute.Sub(testNow)
			planned := float64(monthly) * float64(elapsed) / float64(testRemaining)
			noise := (float64(mix(uint64(minute.Unix()))%2001) - 1000) / 1000 // -1 .. 1
			remaining := float64(monthly) - planned
			allowances := []Allowance{{
				WorkspaceID: 2, MonthlyChecks: monthly, ChecksPerCycle: perCycle,
				UsedChecks: uint64(max(planned+noise*0.004*remaining, 0)),
			}}
			if test.fastCoOwner {
				allowances = append(allowances, every(1, 5*time.Minute))
			}
			snapshot := Compute(minute, allowances)
			pace, _ := snapshot.Pace(2)
			seen[pace.CheckInterval] = true
			return snapshot
		}
		end := testNow.Add(time.Duration(test.hours) * time.Hour)
		total := 0
		for routeID := uint64(1); routeID <= uint64(test.routes); routeID++ {
			ids := []uint{2}
			if test.fastCoOwner {
				ids = []uint{1, 2}
			}
			route := queueRoute(noPacer, snapshotAt(testNow), routeID, testNow, ids...)
			route.runUntil(end, noPacer, snapshotAt, 0)
			total += len(route.checks[2])
			// On a shared route a check may come ahead of its point, by less
			// than the fastest owner's interval: it shares that owner's check.
			var ahead time.Duration
			if test.fastCoOwner {
				ahead = 5 * time.Minute
			}
			for _, gap := range gaps(route.checks[2]) {
				if (gap < test.span/2-ahead || gap > test.span/2+ahead) && (gap < test.span-ahead || gap > test.span+ahead) {
					t.Fatalf("%s: route %d checked %s apart, want %s or %s", test.name, routeID, gap, test.span/2, test.span)
				}
			}
			if test.fastCoOwner && route.visits != len(route.checks[1]) {
				t.Fatalf("%s: route %d was checked %d times for the fast owner's %d checks", test.name, routeID, route.visits, len(route.checks[1]))
			}
			for _, gap := range gaps(route.dequeues) {
				if gap > MaxRouteInterval {
					t.Fatalf("%s: route %d was left alone for %s, longer than the revisit bound", test.name, routeID, gap)
				}
			}
		}
		if len(seen) < 100 {
			t.Fatalf("%s: the cadence took only %d values; the test needs it to keep changing", test.name, len(seen))
		}
		want := float64(test.routes) * float64(end.Sub(testNow)) / float64(test.cadence)
		if math.Abs(float64(total)-want) > want*0.06 {
			t.Fatalf("%s: %d checks, want about %.0f", test.name, total, want)
		}
	}
}

// busiest returns the largest number of times inside any window of the length.
func busiest(times []time.Time, start time.Time, window time.Duration) int {
	buckets := map[int64]int{}
	most := 0
	for _, at := range times {
		bucket := int64(at.Sub(start) / window)
		buckets[bucket]++
		most = max(most, buckets[bucket])
	}
	return most
}

// When a route's fastest owner runs out, the next owner continues at its own
// points. It is not checked on every shared route at once, and its routes do
// not gather in the narrow band the fast owner kept them in.
func TestFastestOwnerRunningOutLeavesTheOthersOnTheirOwnCadence(t *testing.T) {
	const routes = 3_000
	sixHours := every(2, 6*time.Hour)
	before := Compute(testNow, []Allowance{every(1, 5*time.Minute), sixHours})
	after := Compute(testNow, []Allowance{{WorkspaceID: 1, MonthlyChecks: 10, UsedChecks: 10, ChecksPerCycle: 1, RequestedCheckInterval: 5 * time.Minute}, sixHours})
	ranOut := testNow.Add(3 * time.Hour)
	snapshotAt := func(now time.Time) *Snapshot {
		if now.Before(ranOut) {
			return before
		}
		return after
	}
	var slowChecks []time.Time
	for routeID := uint64(1); routeID <= routes; routeID++ {
		route := queueRoute(noPacer, before, routeID, testNow, 1, 2)
		route.runUntil(ranOut.Add(12*time.Hour), noPacer, snapshotAt, 0)
		perRoute := 0
		for _, at := range route.checks[2] {
			if !at.Before(ranOut) {
				slowChecks = append(slowChecks, at)
				perRoute++
			}
		}
		if perRoute != 2 {
			t.Fatalf("route %d: six-hour owner checked %d times in the twelve hours after the fast owner ran out", routeID, perRoute)
		}
		for _, at := range route.checks[1] {
			if !at.Before(ranOut) {
				t.Fatalf("route %d: an owner without allowance was checked", routeID)
			}
		}
	}
	// Evenly spread, ten minutes hold routes/36 checks.
	even := routes / 36
	if most := busiest(slowChecks, ranOut, 10*time.Minute); most > even*3/2 {
		t.Fatalf("%d of the six-hour owner's checks fell into ten minutes; evenly spread is %d", most, even)
	}
}

// A workspace that lengthens its interval has its routes spread across the new
// interval, not kept in the band of the old one.
func TestLongerIntervalSpreadsAWorkspacesRoutesAgain(t *testing.T) {
	const routes = 3_000
	short := Compute(testNow, []Allowance{every(1, 5*time.Minute)})
	long := Compute(testNow, []Allowance{every(1, 6*time.Hour)})
	changed := testNow.Add(time.Hour)
	snapshotAt := func(now time.Time) *Snapshot {
		if now.Before(changed) {
			return short
		}
		return long
	}
	var checks []time.Time
	for routeID := uint64(1); routeID <= routes; routeID++ {
		route := queueRoute(noPacer, short, routeID, testNow, 1)
		route.runUntil(changed.Add(12*time.Hour), noPacer, snapshotAt, 0)
		perRoute := 0
		for _, at := range route.checks[1] {
			if !at.Before(changed) {
				checks = append(checks, at)
				perRoute++
			}
		}
		if perRoute != 2 {
			t.Fatalf("route %d was checked %d times in twelve hours at a six-hour interval", routeID, perRoute)
		}
	}
	even := routes / 36
	if most := busiest(checks, changed, 10*time.Minute); most > even*3/2 {
		t.Fatalf("%d checks fell into ten minutes after the interval grew; evenly spread is %d", most, even)
	}
}

func TestRouteWithoutCheckableOwnerIsRevisitedWithoutBeingChecked(t *testing.T) {
	snapshot := Compute(testNow, []Allowance{
		{WorkspaceID: 1, MonthlyChecks: 1, UsedChecks: 1, ChecksPerCycle: 1},
		// A 2.5-day cadence: slower than the revisit bound.
		{WorkspaceID: 2, MonthlyChecks: 6, ChecksPerCycle: 1},
	})
	if _, holds := ownPoint(noPacer, snapshot, 1, 1, testNow); holds {
		t.Fatal("an owner without allowance holds the route in the queue")
	}
	var first []time.Time
	for routeID := uint64(1); routeID <= 600; routeID++ {
		exhausted := queueRoute(noPacer, snapshot, routeID, testNow, 1)
		first = append(first, exhausted.due)
		exhausted.runUntil(testNow.Add(48*time.Hour), noPacer, always(snapshot), 0)
		if len(exhausted.checks[1]) != 0 || len(exhausted.dequeues) != 8 {
			t.Fatalf("route %d: %d checks and %d revisits in 48 hours, want 0 and 8", routeID, len(exhausted.checks[1]), len(exhausted.dequeues))
		}
		slow := queueRoute(noPacer, snapshot, routeID, testNow, 2)
		slow.runUntil(testNow.Add(48*time.Hour), noPacer, always(snapshot), 0)
		for _, gap := range gaps(slow.dequeues) {
			if gap > MaxRouteInterval {
				t.Fatalf("route %d waited %s between revisits", routeID, gap)
			}
		}
	}
	// Revisits are spread by route, so they do not all return at once.
	if most := busiest(first, testNow, 10*time.Minute); most > 600/36*2 {
		t.Fatalf("%d of 600 routes are revisited within the same ten minutes", most)
	}
}

func TestOwnerWithoutPublishedCadenceIsCheckedAndRefreshRequested(t *testing.T) {
	snapshot := Compute(testNow, []Allowance{every(1, 10*time.Minute)})
	p := NewPacer()
	// The route is dequeued for the known owner; the new one is served too.
	point, _ := ownPoint(p, snapshot, 1, 9, testNow)
	cycle := p.BeginRouteCycle(snapshot, 9, point, point)
	if !cycle.Due(1) || !cycle.Due(7) {
		t.Fatal("a workspace created since the last refresh was skipped")
	}
	select {
	case <-p.refreshCh:
	default:
		t.Fatal("meeting an unknown workspace did not request a refresh")
	}
	cycle.Reschedule(7, point)
	if next := cycle.Next(point); next.Sub(point) <= 0 || next.Sub(point) > FallbackInterval {
		t.Fatalf("a route with an unknown owner is due again after %s, want within the fallback interval", next.Sub(point))
	}
	if !snapshot.ScrapeDue(7, 1, testNow, time.Minute) {
		t.Fatal("new workspaces must not be skipped by the scraper before the next refresh")
	}

	// Before the first snapshot every owner is served, by a nil Pacer too.
	for _, pacer := range []*Pacer{NewPacer(), nil} {
		startup := pacer.BeginRouteCycle(nil, 9, testNow, testNow)
		if !startup.Due(1) || !startup.Due(2) {
			t.Fatal("checks must run before the first snapshot")
		}
		startup.Reschedule(1, testNow)
		startup.Reschedule(2, testNow)
		if next := startup.Next(testNow); next.Sub(testNow) <= 0 || next.Sub(testNow) > FallbackInterval {
			t.Fatalf("startup route is due again after %s, want within the fallback interval", next.Sub(testNow))
		}
	}

	// A route whose owners the worker cannot see is not starved.
	empty := noPacer.BeginRouteCycle(snapshot, 9, testNow, testNow)
	if next := empty.Next(testNow); next.Sub(testNow) != 10*time.Minute {
		t.Fatalf("a route without owners is due again after %s, want the fastest cadence", next.Sub(testNow))
	}
	if next := noPacer.FirstRouteCheck(nil, 9, nil, testNow); next.Sub(testNow) != FallbackInterval {
		t.Fatalf("a route without owners is first due after %s before the first load", next.Sub(testNow))
	}
}

// A route waits for the earliest point of any owner, so an import that adds a
// faster owner yields an earlier due time than the one the route was queued at.
func TestFirstRouteCheckIsTheEarliestOwnerPoint(t *testing.T) {
	snapshot := Compute(testNow, []Allowance{every(1, time.Minute), every(2, 6*time.Hour), every(3, 30*time.Minute)})
	for routeID := uint64(1); routeID <= 200; routeID++ {
		slow := noPacer.FirstRouteCheck(snapshot, routeID, owners(2), testNow)
		shared := noPacer.FirstRouteCheck(snapshot, routeID, owners(2, 1, 3), testNow)
		earliest := slow
		for _, id := range []uint{1, 3} {
			if point, _ := ownPoint(noPacer, snapshot, id, routeID, testNow); point.Before(earliest) {
				earliest = point
			}
		}
		if !shared.Equal(earliest) || shared.Sub(testNow) > time.Minute {
			t.Fatalf("route %d: first due %s after the import with a one-minute owner, %s without", routeID, shared.Sub(testNow), slow.Sub(testNow))
		}
		served := 0
		for _, id := range []uint{1, 2, 3} {
			if dueAt(noPacer, snapshot, id, routeID, shared, shared) {
				served++
			}
		}
		if served == 0 {
			t.Fatalf("route %d: no owner is checked at the point that set the due time", routeID)
		}
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

// The scraper's slower subscribers are served on their grid too, so a
// recomputed automatic scrape cadence keeps a subscriber's fetches regular. A
// subscriber gets the first fetch after each of its points, so its fetches
// are a span or half a span apart to within one fetch interval: exactly, when
// the fetch interval divides the spans.
func TestSlowerScrapeSubscriberStaysRegularWhileItsCadenceIsRecomputed(t *testing.T) {
	for _, fetchInterval := range []time.Duration{30 * time.Minute, 47 * time.Minute} {
		var fetches []time.Time
		scheduled := testNow
		const hours = 300
		for index := 0; scheduled.Before(testNow.Add(hours * time.Hour)); index++ {
			// About every five hours, moved a little at every fetch.
			cycles := float64(testRemaining)/float64(5*time.Hour) + float64(index%7)*0.01
			snapshot := Compute(testNow, []Allowance{
				{WorkspaceID: 1, UnlimitedScrapes: true, RequestedScrapeInterval: fetchInterval, ScrapesPerCycle: 1},
				{WorkspaceID: 2, MonthlyScrapes: uint64(cycles * 1_000), ScrapesPerCycle: 1_000},
			})
			if snapshot.ScrapeDue(2, 11, scheduled, fetchInterval) {
				fetches = append(fetches, scheduled)
			}
			scheduled = scheduled.Add(fetchInterval)
		}
		// Spans double from the 15-minute floor: 8 hours is the first that is
		// not shorter than the cadence.
		var uneven time.Duration
		if (4 * time.Hour % fetchInterval) != 0 {
			uneven = fetchInterval
		}
		for _, gap := range gaps(fetches) {
			if (gap < 4*time.Hour-uneven || gap > 4*time.Hour+uneven) && (gap < 8*time.Hour-uneven || gap > 8*time.Hour+uneven) {
				t.Fatalf("fetches every %s: slower subscriber fetched %s apart, want 4h or 8h", fetchInterval, gap)
			}
		}
		if len(fetches) < 54 || len(fetches) > 66 {
			t.Fatalf("fetches every %s: slower subscriber got %d fetches in %d hours at about one per five hours", fetchInterval, len(fetches), hours)
		}
	}
}

// Instances refresh at different moments, so a route's cycles are decided on
// snapshots of different ages. The points of an automatic cadence do not move
// with its value, so the instances agree on them.
func TestInstancesWithSnapshotsOfDifferentAgesKeepAnAutomaticCadence(t *testing.T) {
	const perCycle = 1_000
	snapshots := make([]*Snapshot, 0, 3)
	for _, cadence := range []time.Duration{47 * time.Minute, 47*time.Minute + 14*time.Second, 46*time.Minute + 50*time.Second} {
		snapshots = append(snapshots, Compute(testNow, []Allowance{
			{WorkspaceID: 2, ChecksPerCycle: perCycle, MonthlyChecks: uint64(float64(testRemaining) / float64(cadence) * perCycle)},
			every(1, 5*time.Minute),
		}))
	}
	turn := 0
	alternating := func(time.Time) *Snapshot {
		turn++
		return snapshots[turn%len(snapshots)]
	}
	const routes, hours = 40, 14 * 24
	total := 0
	for routeID := uint64(1); routeID <= routes; routeID++ {
		route := queueRoute(noPacer, snapshots[0], routeID, testNow, 1, 2)
		route.runUntil(testNow.Add(hours*time.Hour), noPacer, alternating, 0)
		total += len(route.checks[2])
		for _, gap := range gaps(route.checks[2]) {
			if gap < 27*time.Minute || gap > 69*time.Minute {
				t.Fatalf("route %d: checked %s apart on snapshots of different ages, want 32m or 1h4m to within the fast owner's 5m", routeID, gap)
			}
		}
	}
	if want := float64(routes*hours) * float64(time.Hour) / float64(47*time.Minute); math.Abs(float64(total)-want) > want*0.03 {
		t.Fatalf("%d checks on snapshots of different ages, want about %.0f", total, want)
	}
}

// A route's due time is a point computed on one instance's clock. An instance
// whose clock is behind dequeues it "early" and must still serve the point.
func TestDequeueOnAClockThatIsBehindStillServesThePoint(t *testing.T) {
	snapshot := Compute(testNow, []Allowance{every(1, 10*time.Minute)})
	point, _ := ownPoint(noPacer, snapshot, 1, 9, testNow)
	behind := point.Add(-2 * time.Second)
	if !dueAt(noPacer, snapshot, 1, 9, point, behind) {
		t.Fatal("the owner is not served at its point on a clock that is two seconds behind")
	}
	cycle := noPacer.BeginRouteCycle(snapshot, 9, point, behind)
	var served Selection
	cycle.Select(owners(1), &served)
	cycle.Reschedule(1, behind)
	if next := cycle.Next(behind); !next.Equal(point.Add(10 * time.Minute)) {
		t.Fatalf("the route is due again at %s, want the owner's next point %s", next, point.Add(10*time.Minute))
	}
	// A route that waited in the queue is served for the points it missed,
	// once, and returns to the next point after the dequeue.
	late := point.Add(25 * time.Minute)
	cycle = noPacer.BeginRouteCycle(snapshot, 9, point, late)
	if cycle.Select(owners(1), &served); !served.Any {
		t.Fatal("a route that waited 25 minutes is not served")
	}
	cycle.Reschedule(1, late)
	if next := cycle.Next(late); !next.Equal(point.Add(30 * time.Minute)) {
		t.Fatalf("after waiting 25 minutes the route is due again at %s, want %s", next, point.Add(30*time.Minute))
	}
}

func TestPausedCheckingServesNoOwnerAndKeepsTheRouteOnItsPoints(t *testing.T) {
	store := useFakeStore(t, 1_000_000)
	if err := store.p.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	snapshot := store.p.Current()
	point, holds := ownPoint(store.p, snapshot, 7, 1, time.Now())
	if !holds || !dueAt(store.p, snapshot, 7, 1, point, point) {
		t.Fatal("the owner is not due at its own point")
	}
	store.p.SetPauseAfter(time.Minute)
	store.p.failingSince.Store(time.Now().Add(-2 * time.Minute).UnixNano())
	cycle := store.p.BeginRouteCycle(snapshot, 1, point, point)
	if cycle.Due(7) {
		t.Fatal("an owner was due while checking is paused")
	}
	following, _ := ownPoint(store.p, snapshot, 7, 1, point)
	if next := cycle.Next(point); !next.Equal(following) && next.Sub(point) > MaxRouteInterval {
		t.Fatalf("a paused route is due again at %s, want its owner's next point %s or a revisit", next, following)
	}
}
