package checker

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"magpie/internal/checkpace"
	"magpie/internal/domain"
	"magpie/internal/support"
)

// withPace returns a runtime whose Pacer publishes snapshot, which may be nil
// to stand for the time before the first allowance load.
func withPace(snapshot *checkpace.Snapshot) *Runtime {
	pace := checkpace.NewPacer()
	pace.Publish(snapshot)
	return NewRuntime(pace)
}

func paceFor(now time.Time, allowances ...checkpace.Allowance) *checkpace.Snapshot {
	return checkpace.Compute(now, allowances)
}

func TestSelectDueWorkspacesKeepsSingleOwnerRouteWithoutCopy(t *testing.T) {
	now := time.Now()
	r := withPace(paceFor(now, checkpace.Allowance{WorkspaceID: 1, MonthlyChecks: 1_000_000, ChecksPerCycle: 10}))
	proxy := domain.Proxy{ID: 1, Workspaces: []domain.Workspace{{ID: 1}}}
	got, anyDue := r.selectDueWorkspaces(proxy, now)
	if !anyDue || len(got.Workspaces) != 1 || &got.Workspaces[0] != &proxy.Workspaces[0] {
		t.Fatalf("single-owner route should be due without copying: due=%v workspaces=%v", anyDue, got.Workspaces)
	}
	if allocations := testing.AllocsPerRun(100, func() { _, _ = r.selectDueWorkspaces(proxy, now) }); allocations != 0 {
		t.Fatalf("due selection allocated %.0f times per cycle", allocations)
	}
}

func TestSelectDueWorkspacesSkipsExhaustedAndSlowOwners(t *testing.T) {
	now := time.Now()
	_, end := checkpace.UsagePeriod(now)
	remaining := end.Sub(now)
	r := withPace(paceFor(now,
		checkpace.Allowance{WorkspaceID: 1, MonthlyChecks: uint64(remaining/time.Minute) + 1, ChecksPerCycle: 1},
		checkpace.Allowance{WorkspaceID: 2, MonthlyChecks: 10, UsedChecks: 10, ChecksPerCycle: 1},
		checkpace.Allowance{WorkspaceID: 3, MonthlyChecks: uint64(remaining/(30*time.Minute)) + 1, ChecksPerCycle: 1},
	))
	proxy := domain.Proxy{ID: 9, Workspaces: []domain.Workspace{{ID: 2}, {ID: 1}, {ID: 3}}}
	slowDue := 0
	scheduled := now
	for cycle := 0; cycle < 300; cycle++ {
		got, anyDue := r.selectDueWorkspaces(proxy, scheduled)
		if !anyDue {
			t.Fatal("the fastest owner must be due every cycle")
		}
		for _, workspace := range got.Workspaces {
			switch workspace.ID {
			case 2:
				t.Fatal("an exhausted owner was selected")
			case 3:
				slowDue++
			}
		}
		scheduled = scheduled.Add(time.Minute)
	}
	if len(proxy.Workspaces) != 3 {
		t.Fatalf("selection mutated the route's owners: %v", proxy.Workspaces)
	}
	if slowDue < 9 || slowDue > 11 {
		t.Fatalf("30-minute owner selected %d times in 300 one-minute cycles, want about 10", slowDue)
	}
}

func TestSelectDueWorkspacesReportsIdleCycle(t *testing.T) {
	now := time.Now()
	r := withPace(paceFor(now, checkpace.Allowance{WorkspaceID: 1, MonthlyChecks: 5, UsedChecks: 5, ChecksPerCycle: 1}))
	if _, anyDue := r.selectDueWorkspaces(domain.Proxy{ID: 1, Workspaces: []domain.Workspace{{ID: 1}}}, now); anyDue {
		t.Fatal("a route whose only owner is out of allowance must not be checked")
	}
}

func TestSelectDueWorkspacesChecksEveryoneBeforeFirstSnapshot(t *testing.T) {
	r := withPace(nil)
	proxy := domain.Proxy{ID: 1, Workspaces: []domain.Workspace{{ID: 1}, {ID: 2}}}
	if got, anyDue := r.selectDueWorkspaces(proxy, time.Now()); !anyDue || len(got.Workspaces) != 2 {
		t.Fatalf("startup selection = %v, %v", got.Workspaces, anyDue)
	}
}

// BenchmarkSelectDueWorkspaces measures the per-dequeue cadence decision. It is
// map lookups and integer arithmetic over the route's owners.
func BenchmarkSelectDueWorkspaces(b *testing.B) {
	now := time.Now()
	_, end := checkpace.UsagePeriod(now)
	remaining := end.Sub(now)
	for _, owners := range []int{1, 8} {
		b.Run(fmt.Sprintf("owners=%d", owners), func(b *testing.B) {
			allowances := make([]checkpace.Allowance, 0, owners)
			proxy := domain.Proxy{ID: 77}
			for i := 1; i <= owners; i++ {
				cadence := time.Duration(i) * 5 * time.Minute
				allowances = append(allowances, checkpace.Allowance{WorkspaceID: uint(i), MonthlyChecks: uint64(remaining / cadence), ChecksPerCycle: 1})
				proxy.Workspaces = append(proxy.Workspaces, domain.Workspace{ID: uint(i)})
			}
			// Refresh attaches the check meter, so the measurement includes
			// reading each owner's metered usage.
			r := useMeteredPace(b, allowances...)
			// Cycles stay within one month, as live routes do.
			scheduled, _ := checkpace.UsagePeriod(now)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = r.selectDueWorkspaces(proxy, scheduled.Add(time.Duration(i%8000)*5*time.Minute))
			}
		})
	}
}

// useMeteredPace returns a runtime whose own Pacer has loaded allowances, so
// its snapshot is connected to that Pacer's check meter.
func useMeteredPace(t testing.TB, allowances ...checkpace.Allowance) *Runtime {
	t.Helper()
	pace := checkpace.NewPacer()
	pace.Configure(checkpace.Store{Load: func(context.Context, time.Time) ([]checkpace.Allowance, error) { return allowances, nil }})
	if err := pace.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	return NewRuntime(pace)
}

func usedChecks(t *testing.T, r *Runtime, workspaceID uint) uint64 {
	t.Helper()
	pace, ok := r.pace.Current().Pace(workspaceID)
	if !ok {
		t.Fatalf("workspace %d has no pace", workspaceID)
	}
	return pace.CurrentUsedChecks()
}

// failingJudge makes every request fail at the transport and calls onSend as
// each request goes out.
func failingJudge(t *testing.T, onSend func()) {
	t.Helper()
	originalFactory := checkerTransportFactory
	resetCheckerHTTPClientCacheForTests()
	t.Cleanup(func() { checkerTransportFactory = originalFactory; resetCheckerHTTPClientCacheForTests() })
	checkerTransportFactory = func(domain.Proxy, *domain.Judge, string, string, ...uint16) (http.RoundTripper, func(), error) {
		return roundTripFunc(func(*http.Request) (*http.Response, error) {
			onSend()
			return nil, errors.New("unreachable")
		}), nil, nil
	}
}

func failingAssignment(protocol string, retries uint8, checks ...workspaceCheck) *requestAssignment {
	return &requestAssignment{
		judge: &domain.Judge{ID: 1, FullString: "http://127.0.0.1/"}, proxyProtocol: protocol, transportProtocol: support.TransportTCP,
		protocolID: 1, timeout: 1000, retries: retries, budgetSet: true, checks: checks,
	}
}

func TestRequestsAreChargedBeforeTheyAreSentAndStopAtTheAllowance(t *testing.T) {
	// One check left, and two protocol tests with five retries each.
	r := useMeteredPace(t, checkpace.Allowance{WorkspaceID: 201, MonthlyChecks: 10, UsedChecks: 9, ChecksPerCycle: 1})
	sent := 0
	failingJudge(t, func() {
		sent++
		if used := usedChecks(t, r, 201); used != 9+uint64(sent) {
			t.Errorf("request %d went out with %d checks charged", sent, used)
		}
	})
	originalEnqueue := enqueueProxyStatistic
	t.Cleanup(func() { enqueueProxyStatistic = originalEnqueue })
	enqueueProxyStatistic = func(domain.ProxyStatistic, []uint) {
		t.Error("a statistic was reported for a test that never finished")
	}
	assignments := map[string]*requestAssignment{
		"http":  failingAssignment("http", 5, workspaceCheck{workspaceID: 201, regex: "default"}),
		"https": failingAssignment("https", 5, workspaceCheck{workspaceID: 201, regex: "default"}),
	}
	hasChecks := map[uint]bool{201: true}
	if err := r.processJudgeAssignments(domain.Proxy{ID: 9}, assignments, map[uint]bool{}, hasChecks, 1000, 5, false); err != nil {
		t.Fatal(err)
	}
	if sent != 1 || usedChecks(t, r, 201) != 10 {
		t.Fatalf("sent %d requests and charged %d checks with one check left; want 1 and 10", sent, usedChecks(t, r, 201))
	}
	if hasChecks[201] {
		t.Fatal("an unfinished test would count as a failed check")
	}
}

func TestSharedRequestDropsOnlyTheExhaustedWorkspace(t *testing.T) {
	r := useMeteredPace(t,
		checkpace.Allowance{WorkspaceID: 202, MonthlyChecks: 10, UsedChecks: 9, ChecksPerCycle: 1},
		checkpace.Allowance{WorkspaceID: 203, MonthlyChecks: 1_000, ChecksPerCycle: 1},
		checkpace.Allowance{WorkspaceID: 206, MonthlyChecks: 1_000, ChecksPerCycle: 1},
	)
	sent := 0
	failingJudge(t, func() { sent++ })
	var reported []uint
	originalEnqueue := enqueueProxyStatistic
	t.Cleanup(func() { enqueueProxyStatistic = originalEnqueue })
	// The statistics pipeline drops the result: charging does not depend on it.
	enqueueProxyStatistic = func(_ domain.ProxyStatistic, workspaceIDs []uint) { reported = workspaceIDs }
	shared := []workspaceCheck{{workspaceID: 202, regex: "a"}, {workspaceID: 203, regex: "a"}, {workspaceID: 206, regex: "b"}}
	assignments := map[string]*requestAssignment{"http": failingAssignment("http", 2, shared...)}
	hasChecks := map[uint]bool{202: true, 203: true, 206: true}
	if err := r.processJudgeAssignments(domain.Proxy{ID: 9}, assignments, map[uint]bool{}, hasChecks, 1000, 2, false); err != nil {
		t.Fatal(err)
	}
	if sent != 3 || usedChecks(t, r, 202) != 10 || usedChecks(t, r, 203) != 3 || usedChecks(t, r, 206) != 3 {
		t.Fatalf("sent %d, charged %d, %d and %d; want 3 requests, 10, 3 and 3 checks", sent, usedChecks(t, r, 202), usedChecks(t, r, 203), usedChecks(t, r, 206))
	}
	if len(reported) != 2 || reported[0] != 203 || reported[1] != 206 || hasChecks[202] || !hasChecks[203] || !hasChecks[206] {
		t.Fatalf("reported to %v with eligibility %v; only workspaces 203 and 206 finished the test", reported, hasChecks)
	}
	if len(shared) != 3 || shared[0].workspaceID != 202 {
		t.Fatal("dropping a workspace changed the assignment's own checks")
	}
}

func TestRequestMeterDoesNotAllocateWhileAllowanceLasts(t *testing.T) {
	r := useMeteredPace(t,
		checkpace.Allowance{WorkspaceID: 204, MonthlyChecks: 1 << 40, ChecksPerCycle: 1},
		checkpace.Allowance{WorkspaceID: 205, MonthlyChecks: 1 << 40, ChecksPerCycle: 1},
	)
	checks := []workspaceCheck{{workspaceID: 204}, {workspaceID: 205}}
	meter := &requestMeter{pace: r.pace}
	if allocations := testing.AllocsPerRun(100, func() {
		meter.reset(checks)
		if !meter.admit() || !meter.admit() {
			t.Fatal("request refused")
		}
	}); allocations != 0 {
		t.Fatalf("admitting requests allocated %.0f times", allocations)
	}
}

// One request costs one charge per workspace, and workspaces that run out are
// removed in the same pass, whichever of them it is and however many. The
// charges are read from the batch the Pacer flushes after each request, so
// they are the counts the usage meter would write.
func TestRequestMeterChargesEachWorkspaceOncePerRequest(t *testing.T) {
	const owners = 9
	allowances := make([]checkpace.Allowance, 0, owners)
	assignment := make([]workspaceCheck, 0, owners)
	for id := uint(1); id <= owners; id++ {
		allowances = append(allowances, checkpace.Allowance{WorkspaceID: id, MonthlyChecks: 1 << 40, ChecksPerCycle: 1})
		assignment = append(assignment, workspaceCheck{workspaceID: id})
	}
	type charge struct{ attempts, protocolChecks uint64 }
	var flushed map[uint]charge
	pace := checkpace.NewPacer()
	pace.Configure(checkpace.Store{
		Load: func(context.Context, time.Time) ([]checkpace.Allowance, error) {
			return append([]checkpace.Allowance(nil), allowances...), nil
		},
		FlushUsage: func(_ context.Context, batch checkpace.UsageBatch) error {
			for _, usage := range batch.Usage {
				got := flushed[usage.WorkspaceID]
				flushed[usage.WorkspaceID] = charge{got.attempts + usage.Attempts, got.protocolChecks + usage.ProtocolChecks}
			}
			return nil
		},
	})
	ctx := context.Background()
	meter := &requestMeter{pace: pace}
	meter.reset(assignment)
	served := func() []uint {
		ids := make([]uint, 0, len(meter.checks))
		for _, check := range meter.checks {
			ids = append(ids, check.workspaceID)
		}
		return ids
	}
	for index, step := range []struct {
		runOut []uint
		want   []uint
		admit  bool
	}{
		{nil, []uint{1, 2, 3, 4, 5, 6, 7, 8, 9}, true},
		{[]uint{1, 4, 5, 9}, []uint{2, 3, 6, 7, 8}, true}, // first, adjacent and last; copies
		{[]uint{3, 8}, []uint{2, 6, 7}, true},             // in place
		{[]uint{2, 6, 7}, nil, false},
	} {
		for _, id := range step.runOut {
			allowances[id-1].UsedChecks = allowances[id-1].MonthlyChecks
		}
		// The reload publishes the exhaustion; its flush carries the
		// previous request's charges, which were checked already.
		flushed = map[uint]charge{}
		if err := pace.Refresh(ctx); err != nil {
			t.Fatal(err)
		}
		flushed = map[uint]charge{}
		if meter.admit() != step.admit || fmt.Sprint(served()) != fmt.Sprint(step.want) {
			t.Fatalf("after %v ran out the request serves %v, want %v", step.runOut, served(), step.want)
		}
		if err := pace.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		if len(flushed) != len(step.want) {
			t.Fatalf("%d workspaces were charged for a request serving %v: %v", len(flushed), step.want, flushed)
		}
		for _, id := range step.want {
			// Only the first request of the assignment starts a protocol check.
			want := charge{attempts: 1}
			if index == 0 {
				want.protocolChecks = 1
			}
			if flushed[id] != want {
				t.Fatalf("workspace %d was charged %+v for one request, want %+v", id, flushed[id], want)
			}
		}
	}
	for index, check := range assignment {
		if check.workspaceID != uint(index+1) {
			t.Fatalf("dropping workspaces changed the assignment's own checks: %v", assignment)
		}
	}
}

// A checker runtime reads snapshots from and charges requests to the Pacer it
// was constructed with, and only that one. There is no package-level Pacer to
// fall back on, so a runtime without one is refused.
func TestCheckerRuntimeChargesOnlyItsOwnPacer(t *testing.T) {
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("NewRuntime accepted a nil Pacer")
			}
		}()
		NewRuntime(nil)
	}()
	allowance := checkpace.Allowance{WorkspaceID: 207, MonthlyChecks: 1_000, ChecksPerCycle: 1}
	first, second := useMeteredPace(t, allowance), useMeteredPace(t, allowance)
	if first.pace == second.pace {
		t.Fatal("two runtimes share a Pacer")
	}
	failingJudge(t, func() {})
	originalEnqueue := enqueueProxyStatistic
	t.Cleanup(func() { enqueueProxyStatistic = originalEnqueue })
	enqueueProxyStatistic = func(domain.ProxyStatistic, []uint) {}
	assignments := map[string]*requestAssignment{"http": failingAssignment("http", 2, workspaceCheck{workspaceID: 207, regex: "default"})}
	if err := first.processJudgeAssignments(domain.Proxy{ID: 9}, assignments, map[uint]bool{}, map[uint]bool{207: true}, 1000, 2, false); err != nil {
		t.Fatal(err)
	}
	if got := usedChecks(t, first, 207); got != 3 {
		t.Fatalf("the runtime's own Pacer counts %d of its 3 requests", got)
	}
	if got := usedChecks(t, second, 207); got != 0 {
		t.Fatalf("another runtime's Pacer counts %d requests it never sent", got)
	}
	// Due selection reads the same Pacer's snapshot: an exhausted owner on
	// one Pacer is not exhausted on another.
	exhausted := withPace(paceFor(time.Now(), checkpace.Allowance{WorkspaceID: 207, MonthlyChecks: 3, UsedChecks: 3, ChecksPerCycle: 1}))
	route := domain.Proxy{ID: 9, Workspaces: []domain.Workspace{{ID: 207}}}
	if _, due := exhausted.selectDueWorkspaces(route, time.Now()); due {
		t.Fatal("due selection ignored its Pacer's exhausted owner")
	}
	if _, due := second.selectDueWorkspaces(route, time.Now()); !due {
		t.Fatal("due selection read another runtime's snapshot")
	}
}

// BenchmarkRequestMeterAdmit measures the work added before each outbound
// request: per owner, a snapshot map read, an atomic load and two atomic adds.
// It must grow in proportion to the owners sharing the request.
func BenchmarkRequestMeterAdmit(b *testing.B) {
	for _, owners := range []int{1, 8, 64, 512} {
		b.Run(fmt.Sprintf("owners=%d", owners), func(b *testing.B) {
			allowances := make([]checkpace.Allowance, 0, owners)
			checks := make([]workspaceCheck, 0, owners)
			for i := 1; i <= owners; i++ {
				id := uint(300 + owners*10 + i)
				allowances = append(allowances, checkpace.Allowance{WorkspaceID: id, MonthlyChecks: 1 << 62, ChecksPerCycle: 1})
				checks = append(checks, workspaceCheck{workspaceID: id})
			}
			r := useMeteredPace(b, allowances...)
			meter := &requestMeter{pace: r.pace}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				meter.reset(checks)
				meter.admit()
			}
		})
	}
}
