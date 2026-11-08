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

func withPace(t *testing.T, snapshot *checkpace.Snapshot) {
	t.Helper()
	previous := currentPace
	currentPace = func() *checkpace.Snapshot { return snapshot }
	t.Cleanup(func() { currentPace = previous })
}

func paceFor(now time.Time, allowances ...checkpace.Allowance) *checkpace.Snapshot {
	return checkpace.Compute(now, allowances)
}

func TestSelectDueWorkspacesKeepsSingleOwnerRouteWithoutCopy(t *testing.T) {
	now := time.Now()
	withPace(t, paceFor(now, checkpace.Allowance{WorkspaceID: 1, MonthlyChecks: 1_000_000, ChecksPerCycle: 10}))
	proxy := domain.Proxy{ID: 1, Workspaces: []domain.Workspace{{ID: 1}}}
	got, anyDue := selectDueWorkspaces(proxy, now)
	if !anyDue || len(got.Workspaces) != 1 || &got.Workspaces[0] != &proxy.Workspaces[0] {
		t.Fatalf("single-owner route should be due without copying: due=%v workspaces=%v", anyDue, got.Workspaces)
	}
	if allocations := testing.AllocsPerRun(100, func() { _, _ = selectDueWorkspaces(proxy, now) }); allocations != 0 {
		t.Fatalf("due selection allocated %.0f times per cycle", allocations)
	}
}

func TestSelectDueWorkspacesSkipsExhaustedAndSlowOwners(t *testing.T) {
	now := time.Now()
	_, end := checkpace.UsagePeriod(now)
	remaining := end.Sub(now)
	withPace(t, paceFor(now,
		checkpace.Allowance{WorkspaceID: 1, MonthlyChecks: uint64(remaining/time.Minute) + 1, ChecksPerCycle: 1},
		checkpace.Allowance{WorkspaceID: 2, MonthlyChecks: 10, UsedChecks: 10, ChecksPerCycle: 1},
		checkpace.Allowance{WorkspaceID: 3, MonthlyChecks: uint64(remaining/(30*time.Minute)) + 1, ChecksPerCycle: 1},
	))
	proxy := domain.Proxy{ID: 9, Workspaces: []domain.Workspace{{ID: 2}, {ID: 1}, {ID: 3}}}
	slowDue := 0
	scheduled := now
	for cycle := 0; cycle < 300; cycle++ {
		got, anyDue := selectDueWorkspaces(proxy, scheduled)
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
	withPace(t, paceFor(now, checkpace.Allowance{WorkspaceID: 1, MonthlyChecks: 5, UsedChecks: 5, ChecksPerCycle: 1}))
	if _, anyDue := selectDueWorkspaces(domain.Proxy{ID: 1, Workspaces: []domain.Workspace{{ID: 1}}}, now); anyDue {
		t.Fatal("a route whose only owner is out of allowance must not be checked")
	}
}

func TestSelectDueWorkspacesChecksEveryoneBeforeFirstSnapshot(t *testing.T) {
	withPace(t, nil)
	proxy := domain.Proxy{ID: 1, Workspaces: []domain.Workspace{{ID: 1}, {ID: 2}}}
	if got, anyDue := selectDueWorkspaces(proxy, time.Now()); !anyDue || len(got.Workspaces) != 2 {
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
			checkpace.Configure(checkpace.Store{Load: func(context.Context, time.Time) ([]checkpace.Allowance, error) { return allowances, nil }})
			if err := checkpace.Refresh(context.Background()); err != nil {
				b.Fatal(err)
			}
			snapshot := checkpace.Current()
			b.Cleanup(func() { checkpace.Configure(checkpace.Store{}); checkpace.Publish(nil) })
			previous := currentPace
			currentPace = func() *checkpace.Snapshot { return snapshot }
			b.Cleanup(func() { currentPace = previous })
			// Cycles stay within one month, as live routes do.
			scheduled, _ := checkpace.UsagePeriod(now)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = selectDueWorkspaces(proxy, scheduled.Add(time.Duration(i%8000)*5*time.Minute))
			}
		})
	}
}

// useMeteredPace publishes a snapshot connected to the check meter. The meter
// lives for the process, so each test uses its own workspace IDs.
func useMeteredPace(t testing.TB, allowances ...checkpace.Allowance) {
	t.Helper()
	checkpace.Configure(checkpace.Store{Load: func(context.Context, time.Time) ([]checkpace.Allowance, error) { return allowances, nil }})
	if err := checkpace.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	snapshot := checkpace.Current()
	t.Cleanup(func() { checkpace.Configure(checkpace.Store{}); checkpace.Publish(nil) })
	previous := currentPace
	currentPace = func() *checkpace.Snapshot { return snapshot }
	t.Cleanup(func() { currentPace = previous })
}

func usedChecks(t *testing.T, workspaceID uint) uint64 {
	t.Helper()
	pace, ok := currentPace().Pace(workspaceID)
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
	useMeteredPace(t, checkpace.Allowance{WorkspaceID: 201, MonthlyChecks: 10, UsedChecks: 9, ChecksPerCycle: 1})
	sent := 0
	failingJudge(t, func() {
		sent++
		if used := usedChecks(t, 201); used != 9+uint64(sent) {
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
	if err := processJudgeAssignments(domain.Proxy{ID: 9}, assignments, map[uint]bool{}, hasChecks, 1000, 5, false); err != nil {
		t.Fatal(err)
	}
	if sent != 1 || usedChecks(t, 201) != 10 {
		t.Fatalf("sent %d requests and charged %d checks with one check left; want 1 and 10", sent, usedChecks(t, 201))
	}
	if hasChecks[201] {
		t.Fatal("an unfinished test would count as a failed check")
	}
}

func TestSharedRequestDropsOnlyTheExhaustedWorkspace(t *testing.T) {
	useMeteredPace(t,
		checkpace.Allowance{WorkspaceID: 202, MonthlyChecks: 10, UsedChecks: 9, ChecksPerCycle: 1},
		checkpace.Allowance{WorkspaceID: 203, MonthlyChecks: 1_000, ChecksPerCycle: 1},
	)
	sent := 0
	failingJudge(t, func() { sent++ })
	var reported []uint
	originalEnqueue := enqueueProxyStatistic
	t.Cleanup(func() { enqueueProxyStatistic = originalEnqueue })
	// The statistics pipeline drops the result: charging does not depend on it.
	enqueueProxyStatistic = func(_ domain.ProxyStatistic, workspaceIDs []uint) { reported = workspaceIDs }
	shared := []workspaceCheck{{workspaceID: 202, regex: "a"}, {workspaceID: 203, regex: "a"}, {workspaceID: 203, regex: "b"}}
	assignments := map[string]*requestAssignment{"http": failingAssignment("http", 2, shared...)}
	hasChecks := map[uint]bool{202: true, 203: true}
	if err := processJudgeAssignments(domain.Proxy{ID: 9}, assignments, map[uint]bool{}, hasChecks, 1000, 2, false); err != nil {
		t.Fatal(err)
	}
	if sent != 3 || usedChecks(t, 202) != 10 || usedChecks(t, 203) != 3 {
		t.Fatalf("sent %d, charged %d and %d; want 3 requests, 10 and 3 checks", sent, usedChecks(t, 202), usedChecks(t, 203))
	}
	if len(reported) != 1 || reported[0] != 203 || hasChecks[202] || !hasChecks[203] {
		t.Fatalf("reported to %v with eligibility %v; only workspace 203 finished the test", reported, hasChecks)
	}
	if len(shared) != 3 || shared[0].workspaceID != 202 {
		t.Fatal("dropping a workspace changed the assignment's own checks")
	}
}

func TestRequestMeterDoesNotAllocateWhileAllowanceLasts(t *testing.T) {
	useMeteredPace(t,
		checkpace.Allowance{WorkspaceID: 204, MonthlyChecks: 1 << 40, ChecksPerCycle: 1},
		checkpace.Allowance{WorkspaceID: 205, MonthlyChecks: 1 << 40, ChecksPerCycle: 1},
	)
	checks := []workspaceCheck{{workspaceID: 204}, {workspaceID: 205}, {workspaceID: 204}}
	meter := &requestMeter{}
	if allocations := testing.AllocsPerRun(100, func() {
		meter.reset(checks)
		if !meter.admit() || !meter.admit() {
			t.Fatal("request refused")
		}
	}); allocations != 0 {
		t.Fatalf("admitting requests allocated %.0f times", allocations)
	}
}

// BenchmarkRequestMeterAdmit measures the work added before each outbound
// request: per owner, a snapshot map read, an atomic load and two atomic adds.
func BenchmarkRequestMeterAdmit(b *testing.B) {
	for _, owners := range []int{1, 8} {
		b.Run(fmt.Sprintf("owners=%d", owners), func(b *testing.B) {
			allowances := make([]checkpace.Allowance, 0, owners)
			checks := make([]workspaceCheck, 0, owners)
			for i := 1; i <= owners; i++ {
				id := uint(300 + owners*10 + i)
				allowances = append(allowances, checkpace.Allowance{WorkspaceID: id, MonthlyChecks: 1 << 62, ChecksPerCycle: 1})
				checks = append(checks, workspaceCheck{workspaceID: id})
			}
			useMeteredPace(b, allowances...)
			meter := &requestMeter{}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				meter.reset(checks)
				meter.admit()
			}
		})
	}
}
