package checker

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"magpie/internal/checkpace"
	"magpie/internal/domain"
	proxyqueue "magpie/internal/jobs/queue/proxy"
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
			scheduled := now
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = selectDueWorkspaces(proxy, scheduled)
				scheduled = scheduled.Add(5 * time.Minute)
			}
		})
	}
}

func TestEveryRequestIsChargedEvenWhenStatisticsAreDropped(t *testing.T) {
	originalCheck, originalEnqueue, originalCharge := checkProxyWithRetries, enqueueProxyStatistic, chargeChecks
	t.Cleanup(func() {
		checkProxyWithRetries, enqueueProxyStatistic, chargeChecks = originalCheck, originalEnqueue, originalCharge
	})
	charged := map[uint]uint64{}
	chargeChecks = func(workspaceID uint, requests uint64) { charged[workspaceID] += requests }
	// The statistics pipeline is overloaded and drops every result.
	enqueueProxyStatistic = func(domain.ProxyStatistic, []uint) {}
	// Two retries after the first request: three requests.
	checkProxyWithRetries = func(domain.Proxy, *domain.Judge, string, string, uint16, uint8) (string, error, int64, uint8) {
		return "", errors.New("timeout"), 0, 2
	}
	assignments := map[string]*requestAssignment{
		"1_http_tcp": {
			judge: &domain.Judge{ID: 1}, proxyProtocol: "http", transportProtocol: support.TransportTCP, protocolID: 1,
			// Workspace 4 has two profiles on the same request.
			checks: []workspaceCheck{{workspaceID: 4, regex: "a"}, {workspaceID: 5, regex: "a"}, {workspaceID: 4, regex: "b"}},
		},
	}
	if err := processJudgeAssignments(domain.Proxy{ID: 9}, assignments, map[uint]bool{}, 1000, 2, false); err != nil {
		t.Fatal(err)
	}
	if charged[4] != 3 || charged[5] != 3 {
		t.Fatalf("charged = %v, want three requests for each workspace", charged)
	}

	// A lost lease after two requests still charges the two that were sent.
	charged = map[uint]uint64{}
	checkProxyWithRetries = func(domain.Proxy, *domain.Judge, string, string, uint16, uint8) (string, error, int64, uint8) {
		return "", errors.Join(proxyqueue.ErrProxyLeaseLost, errors.New("expired")), 0, 2
	}
	if err := processJudgeAssignments(domain.Proxy{ID: 9}, assignments, map[uint]bool{}, 1000, 2, false); !errors.Is(err, proxyqueue.ErrProxyLeaseLost) {
		t.Fatalf("err = %v", err)
	}
	if charged[4] != 2 || charged[5] != 2 {
		t.Fatalf("charged after a lost lease = %v, want two requests each", charged)
	}
}

func TestChargeCheckAttemptsDoesNotAllocate(t *testing.T) {
	originalCharge := chargeChecks
	t.Cleanup(func() { chargeChecks = originalCharge })
	chargeChecks = func(uint, uint64) {}
	checks := []workspaceCheck{{workspaceID: 1}, {workspaceID: 2}, {workspaceID: 1}}
	if allocations := testing.AllocsPerRun(100, func() { chargeCheckAttempts(checks, 3) }); allocations != 0 {
		t.Fatalf("charging allocated %.0f times", allocations)
	}
}
