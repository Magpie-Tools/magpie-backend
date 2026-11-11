package checker

import (
	"testing"

	"magpie/internal/checkerconfig"
	"magpie/internal/domain"
)

// Owners with the same judge set must pick the same judge each cycle so their
// requests merge, even though each snapshot keeps its own round-robin counter.
func TestOwnersWithSameJudgeSetShareOneRequestPerProtocol(t *testing.T) {
	oldLookup := lookupCheckerWorkspace
	t.Cleanup(func() { lookupCheckerWorkspace = oldLookup })

	const judgeCount = 5
	shared := make([]domain.JudgeWithRegex, 0, judgeCount)
	strict := make([]domain.JudgeWithRegex, 0, judgeCount)
	for id := uint(1); id <= judgeCount; id++ {
		judge := &domain.Judge{ID: id, FullString: "http://judge.invalid/" + string(rune('a'+id))}
		judge.SetUp()
		shared = append(shared, domain.JudgeWithRegex{Judge: judge, Regex: "default"})
		strict = append(strict, domain.JudgeWithRegex{Judge: judge, Regex: "strict"})
	}
	other := &domain.Judge{ID: 99, FullString: "http://other.invalid/"}
	other.SetUp()
	otherSet := []domain.JudgeWithRegex{{Judge: other, Regex: "default"}, shared[0]}

	snapshots := map[uint]*checkerconfig.Workspace{}
	owners := make([]domain.Workspace, 0, 12)
	for id := uint(1); id <= 12; id++ {
		w := domain.Workspace{ID: id, HTTPProtocol: true, Timeout: 5000, Retries: 1}
		judges := shared
		switch {
		case id == 4:
			judges = strict // same judges, different validation
		case id == 12:
			judges = otherSet // different judge set rotates independently
		}
		snapshots[id] = checkerconfig.Build(w, nil, judges, nil)
		owners = append(owners, domain.Workspace{ID: id})
	}
	// Desynchronize counters as long-running snapshots would be.
	for id := uint(1); id <= 11; id++ {
		for skip := uint(0); skip < id; skip++ {
			snapshots[id].Default.NextJudgeIndex(0)
		}
	}
	lookupCheckerWorkspace = func(id uint) (*checkerconfig.Workspace, bool) { return snapshots[id], true }

	seen := map[uint]bool{}
	for cycle := 0; cycle < judgeCount*2; cycle++ {
		proxy := domain.Proxy{ID: 7, Workspaces: append([]domain.Workspace(nil), owners...)}
		assignments, _, checks, _, _ := buildRequestAssignments(proxy)
		var sharedAssignment *requestAssignment
		otherChecks := 0
		for _, assignment := range assignments {
			for _, check := range assignment.checks {
				if check.workspaceID == 12 {
					otherChecks++
				}
			}
			if len(assignment.checks) >= 11 {
				sharedAssignment = assignment
			}
		}
		if sharedAssignment == nil || otherChecks != 1 || len(assignments) > 2 {
			t.Fatalf("cycle %d split shared owners: %d assignments", cycle, len(assignments))
		}
		seen[sharedAssignment.judge.ID] = true
		for _, check := range sharedAssignment.checks {
			want := "default"
			if check.workspaceID == 4 {
				want = "strict"
			}
			if check.regex != want {
				t.Fatalf("workspace %d validated with %q", check.workspaceID, check.regex)
			}
		}
		for id := uint(1); id <= 12; id++ {
			if !checks[id] {
				t.Fatalf("workspace %d lost eligibility", id)
			}
		}
	}
	if len(seen) != judgeCount {
		t.Fatalf("shared selection stopped rotating: %v", seen)
	}
}

func TestSingleOwnerJudgeRotationIsUnchanged(t *testing.T) {
	oldLookup := lookupCheckerWorkspace
	t.Cleanup(func() { lookupCheckerWorkspace = oldLookup })
	var list []domain.JudgeWithRegex
	for id := uint(1); id <= 3; id++ {
		judge := &domain.Judge{ID: id, FullString: "http://judge.invalid/" + string(rune('a'+id))}
		judge.SetUp()
		list = append(list, domain.JudgeWithRegex{Judge: judge, Regex: "default"})
	}
	snapshot := checkerconfig.Build(domain.Workspace{ID: 1, HTTPProtocol: true, Timeout: 1000}, nil, list, nil)
	lookupCheckerWorkspace = func(uint) (*checkerconfig.Workspace, bool) { return snapshot, true }
	for cycle := 0; cycle < 6; cycle++ {
		assignments, _, _, _, _ := buildRequestAssignments(domain.Proxy{ID: 1, Workspaces: []domain.Workspace{{ID: 1}}})
		for _, assignment := range assignments {
			if want := uint(cycle%3 + 1); assignment.judge.ID != want {
				t.Fatalf("cycle %d judge %d, want %d", cycle, assignment.judge.ID, want)
			}
		}
	}
}

// BenchmarkBuildRequestAssignmentsSharedRoute models a scraped route owned by
// 1,000 workspaces on default settings with five judges and two protocols.
func BenchmarkBuildRequestAssignmentsSharedRoute(b *testing.B) {
	oldLookup := lookupCheckerWorkspace
	b.Cleanup(func() { lookupCheckerWorkspace = oldLookup })
	var list []domain.JudgeWithRegex
	for id := uint(1); id <= 5; id++ {
		judge := &domain.Judge{ID: id, FullString: "http://judge.invalid/" + string(rune('a'+id))}
		judge.SetUp()
		list = append(list, domain.JudgeWithRegex{Judge: judge, Regex: "default"})
	}
	const owners = 1000
	snapshots := make(map[uint]*checkerconfig.Workspace, owners)
	workspaces := make([]domain.Workspace, 0, owners)
	for id := uint(1); id <= owners; id++ {
		snapshots[id] = checkerconfig.Build(domain.Workspace{ID: id, HTTPProtocol: true, SOCKS5Protocol: true, Timeout: 5000, Retries: 1}, nil, list, nil)
		for skip := uint(0); skip < id%5; skip++ {
			snapshots[id].Default.NextJudgeIndex(0)
			snapshots[id].Default.NextJudgeIndex(3)
		}
		workspaces = append(workspaces, domain.Workspace{ID: id})
	}
	lookupCheckerWorkspace = func(id uint) (*checkerconfig.Workspace, bool) { return snapshots[id], true }
	proxy := domain.Proxy{ID: 1, Workspaces: make([]domain.Workspace, owners)}
	requests := 0
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		copy(proxy.Workspaces, workspaces)
		assignments, _, _, _, _ := buildRequestAssignments(proxy)
		requests += len(assignments)
	}
	b.ReportMetric(float64(requests)/float64(b.N), "requests/check")
}

// A route payload can list an owner twice until it is rewritten. The request
// meter charges once per check, so the owner must not gain a second one.
func TestRepeatedOwnerIsServedOncePerAssignment(t *testing.T) {
	oldLookup := lookupCheckerWorkspace
	t.Cleanup(func() { lookupCheckerWorkspace = oldLookup })
	judge := &domain.Judge{ID: 1, FullString: "http://judge.invalid/"}
	judge.SetUp()
	judges := []domain.JudgeWithRegex{{Judge: judge, Regex: "default"}}
	snapshots := map[uint]*checkerconfig.Workspace{}
	for id := uint(1); id <= 2; id++ {
		snapshots[id] = checkerconfig.Build(domain.Workspace{ID: id, HTTPProtocol: true, HTTPSProtocol: true, Timeout: 5000, Retries: 1}, nil, judges, nil)
	}
	lookupCheckerWorkspace = func(id uint) (*checkerconfig.Workspace, bool) { return snapshots[id], true }

	proxy := domain.Proxy{ID: 7, Workspaces: []domain.Workspace{{ID: 1}, {ID: 2}, {ID: 1}}}
	assignments, _, hasChecks, _, _ := buildRequestAssignments(proxy)
	if len(assignments) == 0 || !hasChecks[1] || !hasChecks[2] {
		t.Fatalf("built %d assignments with eligibility %v", len(assignments), hasChecks)
	}
	for key, assignment := range assignments {
		served := map[uint]bool{}
		for _, check := range assignment.checks {
			if served[check.workspaceID] {
				t.Fatalf("assignment %s holds two checks of workspace %d", key, check.workspaceID)
			}
			served[check.workspaceID] = true
		}
		if len(served) != 2 {
			t.Fatalf("assignment %s serves %v, want both owners", key, served)
		}
	}
}
