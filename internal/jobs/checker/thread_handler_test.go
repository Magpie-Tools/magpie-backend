package checker

import (
	"errors"
	"runtime"
	"testing"
	"time"

	"magpie/internal/domain"
	"magpie/internal/support"
)

func TestCheckProxyWithRetries_PerformsInitialAttemptWhenRetriesZero(t *testing.T) {
	proxy := domain.Proxy{
		IP:   "127.0.0.1",
		Port: 8080,
	}
	judge := &domain.Judge{
		FullString: "://invalid-url",
	}

	html, err, _, attempt := CheckProxyWithRetries(proxy, judge, "http", support.TransportTCP, 100, 0)
	if err == nil {
		t.Fatalf("expected at least one attempt and an error for invalid judge URL, got nil error and html=%q", html)
	}
	if attempt != 0 {
		t.Fatalf("expected first attempt index 0 for retries=0, got %d", attempt)
	}
}

func TestProcessJudgeAssignments_EnqueuesOneStatisticPerNetworkCheck(t *testing.T) {
	originalCheck := checkProxyWithRetries
	originalEnqueue := enqueueProxyStatistic
	t.Cleanup(func() {
		checkProxyWithRetries = originalCheck
		enqueueProxyStatistic = originalEnqueue
	})

	checkProxyWithRetries = func(domain.Proxy, *domain.Judge, string, string, uint16, uint8) (string, error, int64, uint8) {
		return "ok", nil, 88, 1
	}

	var captured []domain.ProxyStatistic
	var capturedWorkspaceIDs [][]uint
	enqueueProxyStatistic = func(stat domain.ProxyStatistic, workspaceIDs []uint) {
		captured = append(captured, stat)
		capturedWorkspaceIDs = append(capturedWorkspaceIDs, append([]uint(nil), workspaceIDs...))
	}

	proxy := domain.Proxy{ID: 99}
	judge := &domain.Judge{ID: 7}
	assignments := map[string]*requestAssignment{
		"7_http_tcp": {
			judge:             judge,
			proxyProtocol:     "http",
			transportProtocol: support.TransportTCP,
			protocolID:        1,
			checks: []workspaceCheck{
				{workspaceID: 1, regex: "ok"},
				{workspaceID: 2, regex: "ok"},
				{workspaceID: 3, regex: "missing"},
			},
		},
	}
	workspaceSuccess := map[uint]bool{
		1: false,
		2: false,
		3: false,
	}

	processJudgeAssignments(proxy, assignments, workspaceSuccess, 1000, 2, false)

	if len(captured) != 1 {
		t.Fatalf("expected 1 proxy statistic, got %d", len(captured))
	}
	stat := captured[0]
	if stat.ProxyID != proxy.ID {
		t.Fatalf("unexpected proxy id = %d, want %d", stat.ProxyID, proxy.ID)
	}
	if stat.JudgeID != judge.ID {
		t.Fatalf("unexpected judge id = %d, want %d", stat.JudgeID, judge.ID)
	}
	if stat.ProtocolID != 1 {
		t.Fatalf("unexpected protocol id = %d, want 1", stat.ProtocolID)
	}
	if !stat.Alive {
		t.Fatal("expected aggregated statistic to be alive when any regex matches")
	}
	if stat.LevelID == nil {
		t.Fatal("expected level id to be set for alive aggregated statistic")
	}
	if !workspaceSuccess[1] || !workspaceSuccess[2] || workspaceSuccess[3] {
		t.Fatalf("unexpected workspace success map: %#v", workspaceSuccess)
	}
	if len(capturedWorkspaceIDs) != 1 {
		t.Fatalf("expected 1 captured workspace ID set, got %d", len(capturedWorkspaceIDs))
	}
	if len(capturedWorkspaceIDs[0]) != 3 {
		t.Fatalf("captured workspace IDs = %#v, want 3 unique IDs", capturedWorkspaceIDs[0])
	}
}

func TestProcessJudgeAssignments_RecordsFailedCheckOnce(t *testing.T) {
	originalCheck := checkProxyWithRetries
	originalEnqueue := enqueueProxyStatistic
	t.Cleanup(func() {
		checkProxyWithRetries = originalCheck
		enqueueProxyStatistic = originalEnqueue
	})

	checkProxyWithRetries = func(domain.Proxy, *domain.Judge, string, string, uint16, uint8) (string, error, int64, uint8) {
		return "", errors.New("failed"), 250, 3
	}

	var captured []domain.ProxyStatistic
	enqueueProxyStatistic = func(stat domain.ProxyStatistic, _ []uint) {
		captured = append(captured, stat)
	}

	proxy := domain.Proxy{ID: 44}
	judge := &domain.Judge{ID: 9}
	assignments := map[string]*requestAssignment{
		"9_https_tcp": {
			judge:             judge,
			proxyProtocol:     "https",
			transportProtocol: support.TransportTCP,
			protocolID:        2,
			checks: []workspaceCheck{
				{workspaceID: 10, regex: "ok"},
				{workspaceID: 11, regex: "ok"},
			},
		},
	}
	workspaceSuccess := map[uint]bool{
		10: false,
		11: false,
	}

	processJudgeAssignments(proxy, assignments, workspaceSuccess, 1000, 1, false)

	if len(captured) != 1 {
		t.Fatalf("expected 1 proxy statistic, got %d", len(captured))
	}
	stat := captured[0]
	if stat.Alive {
		t.Fatal("expected failed check statistic to be dead")
	}
	if stat.LevelID != nil {
		t.Fatal("expected level id to be nil for dead statistic")
	}
	if workspaceSuccess[10] || workspaceSuccess[11] {
		t.Fatalf("expected workspaces to remain unsuccessful on failed check, got %#v", workspaceSuccess)
	}
}

func TestRefreshProxyUsers_RemovesMissingOwnershipOnce(t *testing.T) {
	originalGetUsers := getWorkspacesForChecker
	clearCheckerWorkspaceCache()
	t.Cleanup(func() {
		getWorkspacesForChecker = originalGetUsers
		clearCheckerWorkspaceCache()
	})

	getWorkspacesForChecker = func(ids []uint) (map[uint]domain.Workspace, error) {
		if len(ids) != 2 {
			t.Fatalf("lookup ids = %#v, want both queued workspaces", ids)
		}
		return map[uint]domain.Workspace{
			4: {ID: 4, Timeout: 2500, Retries: 3},
		}, nil
	}

	proxy := domain.Proxy{Workspaces: []domain.Workspace{
		{ID: 4, Timeout: 100},
		{ID: 9, Timeout: 100},
	}}
	refreshed, payloadChanged := refreshProxyWorkspaces(proxy)
	if !payloadChanged {
		t.Fatal("missing workspace did not mark queue payload as changed")
	}
	if len(refreshed.Workspaces) != 1 || refreshed.Workspaces[0].ID != 4 {
		t.Fatalf("refreshed workspaces = %#v, want only workspace 4", refreshed.Workspaces)
	}
	if refreshed.Workspaces[0].Timeout != 2500 || refreshed.Workspaces[0].Retries != 3 {
		t.Fatalf("workspace settings were not refreshed: %#v", refreshed.Workspaces[0])
	}
}

func TestRefreshProxyUsers_PreservesUnknownUsersOnLookupFailure(t *testing.T) {
	originalGetUsers := getWorkspacesForChecker
	clearCheckerWorkspaceCache()
	t.Cleanup(func() {
		getWorkspacesForChecker = originalGetUsers
		clearCheckerWorkspaceCache()
	})

	getWorkspacesForChecker = func([]uint) (map[uint]domain.Workspace, error) {
		return nil, errors.New("database unavailable")
	}

	proxy := domain.Proxy{Workspaces: []domain.Workspace{{ID: 4}, {ID: 9}}}
	refreshed, payloadChanged := refreshProxyWorkspaces(proxy)
	if payloadChanged {
		t.Fatal("lookup failure must not remove workspaces from the queue payload")
	}
	if len(refreshed.Workspaces) != len(proxy.Workspaces) {
		t.Fatalf("lookup failure changed workspaces from %#v to %#v", proxy.Workspaces, refreshed.Workspaces)
	}
}

func clearCheckerWorkspaceCache() {
	workspaceCache.Range(func(key, _ any) bool {
		workspaceCache.Delete(key)
		return true
	})
}

func TestRequestCheckerWorkerStop_DoesNotBlockWithoutListener(t *testing.T) {
	originalStopChannel := stopChannel
	stopChannel = make(chan struct{})
	t.Cleanup(func() {
		stopChannel = originalStopChannel
	})

	done := make(chan bool, 1)
	go func() {
		done <- requestCheckerWorkerStop()
	}()

	select {
	case ok := <-done:
		if ok {
			t.Fatal("requestCheckerWorkerStop should return false when no worker is listening")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("requestCheckerWorkerStop blocked without listener")
	}
}

func TestRequestCheckerWorkerStop_SucceedsWithListener(t *testing.T) {
	originalStopChannel := stopChannel
	stopChannel = make(chan struct{})
	t.Cleanup(func() {
		stopChannel = originalStopChannel
	})

	stopped := make(chan struct{})
	go func() {
		<-stopChannel
		close(stopped)
	}()

	deadline := time.Now().Add(100 * time.Millisecond)
	delivered := false
	for time.Now().Before(deadline) {
		if requestCheckerWorkerStop() {
			delivered = true
			break
		}
		runtime.Gosched()
	}
	if !delivered {
		t.Fatal("requestCheckerWorkerStop should return true when a worker listener is available")
	}

	select {
	case <-stopped:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected stop signal to be delivered")
	}
}
