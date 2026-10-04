package checker

import (
	"fmt"
	"magpie/internal/api/dto"
	"magpie/internal/checkerconfig"
	"magpie/internal/domain"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func TestTaggedAssignmentsShareProfileBudgetAndCaptureSettings(t *testing.T) {
	oldLookup, oldCheck, oldEnqueue := lookupCheckerWorkspace, checkProxyWithRetries, enqueueProxyStatistic
	t.Cleanup(func() {
		lookupCheckerWorkspace = oldLookup
		checkProxyWithRetries = oldCheck
		enqueueProxyStatistic = oldEnqueue
	})
	judge := &domain.Judge{ID: 1, FullString: "http://127.0.0.1/"}
	judge.SetUp()
	js := []domain.JudgeWithRegex{{Judge: judge, Regex: "accepted"}}
	w := domain.Workspace{ID: 1, SOCKS5Protocol: true, Timeout: 7500, Retries: 2, AutoRemoveFailingProxies: true, UseHttpsForSocks: false}
	settings := w.DefaultCheckerSettings()
	timeout, retries := uint16(1200), uint8(0)
	settings.Rules = []dto.TagCheckerRule{{TagID: 1, Mode: "add", Protocols: []string{"http"}, Timeout: &timeout, Retries: &retries}}
	w.CheckerConfig = settings
	snap := checkerconfig.Build(w, []domain.ProxyTagAssignment{{WorkspaceID: 1, ProxyID: 3, ProxyTagID: 1}}, js, nil)
	lookupCheckerWorkspace = func(id uint) (*checkerconfig.Workspace, bool) { return snap, true }
	getWorkspacesOld := getWorkspacesForChecker
	t.Cleanup(func() { getWorkspacesForChecker = getWorkspacesOld })
	getWorkspacesForChecker = func([]uint) (map[uint]domain.Workspace, error) {
		t.Fatal("worker loaded settings from database")
		return nil, nil
	}
	proxy := domain.Proxy{ID: 3, Workspaces: []domain.Workspace{{ID: 1}}}
	refreshed, changed := refreshProxyWorkspaces(proxy)
	if changed {
		t.Fatal("settings refresh must not rewrite queue payload")
	}
	assignments, success, checks, maxTimeout, maxRetries := buildRequestAssignments(refreshed)
	if len(assignments) != 2 || !checks[1] {
		t.Fatal(assignments, checks)
	}
	capturedKeys := snap.Plan(3).Keys
	snap = checkerconfig.Build(domain.Workspace{ID: 1, Timeout: 9000, Retries: 4}, nil, js, nil)
	calls := map[string][2]uint16{}
	checkProxyWithRetries = func(_ domain.Proxy, _ *domain.Judge, protocol, transport string, timeout uint16, retries uint8) (string, error, int64, uint8) {
		calls[protocol] = [2]uint16{timeout, uint16(retries)}
		if transport != "tcp" {
			t.Fatal(transport)
		}
		return "accepted", nil, 1, 0
	}
	var statistics []domain.ProxyStatistic
	enqueueProxyStatistic = func(stat domain.ProxyStatistic, _ []uint) { statistics = append(statistics, stat) }
	processJudgeAssignments(refreshed, assignments, success, maxTimeout, maxRetries, false)
	if calls["http"] != [2]uint16{1200, 0} || calls["socks5"] != [2]uint16{1200, 0} {
		t.Fatal(calls)
	}
	for _, stat := range statistics {
		if stat.CheckEvidence[0].ConfigKey != capturedKeys[stat.ProtocolID-1] {
			t.Fatal("in-flight settings changed")
		}
	}
	// Empty selection creates no check and therefore no failure event.
	assignments, _, checks, _, _ = buildRequestAssignments(refreshed)
	if len(assignments) != 0 || checks[1] {
		t.Fatal("empty plan created failure eligibility")
	}
}

func TestSharedRequestKeepsWorkspaceValidationEvidence(t *testing.T) {
	oldCheck, oldEnqueue := checkProxyWithRetries, enqueueProxyStatistic
	t.Cleanup(func() { checkProxyWithRetries = oldCheck; enqueueProxyStatistic = oldEnqueue })
	checkProxyWithRetries = func(domain.Proxy, *domain.Judge, string, string, uint16, uint8) (string, error, int64, uint8) {
		return "HEADER-A", nil, 1, 0
	}
	var captured []domain.ProxyStatistic
	enqueueProxyStatistic = func(stat domain.ProxyStatistic, _ []uint) { captured = append(captured, stat) }
	assignments := map[string]*requestAssignment{"shared": {judge: &domain.Judge{ID: 1}, protocolID: 1, proxyProtocol: "http", transportProtocol: "tcp", timeout: 1000, budgetSet: true, checks: []userCheck{
		{userID: 1, regex: "default", headers: []string{"HEADER-A"}, configKey: "first"},
		{userID: 2, regex: "DEFAULT", headers: []string{"HEADER-B"}, configKey: "second"},
	}}}
	success := map[uint]bool{}
	processJudgeAssignments(domain.Proxy{ID: 1}, assignments, success, 8000, 4, false)
	if len(captured) != 1 || len(captured[0].CheckEvidence) != 2 || !captured[0].CheckEvidence[0].Alive || captured[0].CheckEvidence[1].Alive || !success[1] || success[2] {
		t.Fatal(captured, success)
	}
}

func TestProtocolTimeoutAndRetriesExecuteAgainstProxy(t *testing.T) {
	var requests atomic.Int64
	proxyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		select {
		case <-r.Context().Done():
			return
		case <-time.After(100 * time.Millisecond):
			fmt.Fprint(w, "accepted")
		}
	}))
	defer proxyServer.Close()
	resetCheckerHTTPClientCacheForTests()
	t.Cleanup(resetCheckerHTTPClientCacheForTests)
	host, port, _ := net.SplitHostPort(proxyServer.Listener.Addr().String())
	n, _ := strconv.Atoi(port)
	proxy := domain.Proxy{IP: host, Port: uint16(n)}
	judge := &domain.Judge{FullString: "http://judge.invalid/"}
	judge.SetUp()
	_, err, _, attempt := CheckProxyWithRetries(proxy, judge, "http", "tcp", 20, 2)
	if err == nil || attempt != 2 || requests.Load() != 3 {
		t.Fatal(err, attempt, requests.Load())
	}
	_, err, _, attempt = CheckProxyWithRetries(proxy, judge, "http", "tcp", 500, 0)
	if err != nil || attempt != 0 || requests.Load() != 4 {
		t.Fatal(err, attempt, requests.Load())
	}
	first, _ := getCheckerHTTPClient(proxy, judge, "http", "tcp", 20)
	second, _ := getCheckerHTTPClient(proxy, judge, "http", "tcp", 500)
	if first == second {
		t.Fatal("different timeout reused old transport")
	}
	tcp := second.Transport.(*http.Transport)
	if tcp.TLSHandshakeTimeout != 500*time.Millisecond {
		t.Fatal(tcp.TLSHandshakeTimeout)
	}
}
