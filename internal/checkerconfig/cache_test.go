package checkerconfig

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"magpie/internal/api/dto"
	"magpie/internal/domain"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
)

func TestSnapshotKeysAndSharedPlans(t *testing.T) {
	w := domain.Workspace{ID: 1, HTTPProtocol: true, Timeout: 7500, Retries: 2}
	settings := w.DefaultCheckerSettings()
	settings.Rules = []dto.TagCheckerRule{{TagID: 1, Mode: "add", Protocols: []string{"socks5"}}}
	w.CheckerConfig = settings
	assignments := []domain.ProxyTagAssignment{{WorkspaceID: 1, ProxyID: 2, ProxyTagID: 1}, {WorkspaceID: 2, ProxyID: 4, ProxyTagID: 1}, {WorkspaceID: 1, ProxyID: 3, ProxyTagID: 1}}
	judge := &domain.Judge{ID: 9, FullString: "http://127.0.0.1"}
	judge.SetUp()
	js := []domain.JudgeWithRegex{{Judge: judge, Regex: "accept"}}
	original := Build(w, assignments, js, []string{})
	if original.Plan(2) != original.Plan(3) || original.Plan(4) != original.Default || original.Default.Headers == nil {
		t.Fatal("plans not deduplicated or workspace isolation failed")
	}
	if original.Plan(2).Keys[0] != original.Default.Keys[0] || original.Plan(2).Keys[3] == "" {
		t.Fatal("unchanged HTTP key must be reusable; added SOCKS key must exist")
	}
	base := original.Default.Keys[0]
	timeout := settings.Defaults
	timeout.Timeout++
	settings.Defaults = timeout
	if Build(w, nil, js, nil).Default.Keys[0] == base {
		t.Fatal("timeout did not change identity")
	}
	timeout.Timeout--
	settings.Defaults = timeout
	js[0].Regex = "reject"
	if Build(w, nil, js, nil).Default.Keys[0] == base {
		t.Fatal("validation did not change identity")
	}
	js[0].Regex = "accept"
	if Build(w, nil, js, []string{"HEADER"}).Default.Keys[0] == base {
		t.Fatal("headers did not change identity")
	}
}

func TestRefreshKeepsCompleteSnapshotOnFailureAndRejectsOlderGeneration(t *testing.T) {
	old := &Workspace{Value: domain.Workspace{ID: 3}, Generation: 10}
	state.Lock()
	state.workspaces = map[uint]*Workspace{3: old}
	state.ready = true
	state.loader = func(context.Context, uint) (*Workspace, error) { return nil, errors.New("offline") }
	state.Unlock()
	t.Cleanup(func() {
		state.Lock()
		state.loader = nil
		state.ready = false
		state.workspaces = map[uint]*Workspace{}
		state.Unlock()
	})
	if Refresh(context.Background(), 3) == nil {
		t.Fatal("expected loader error")
	}
	if got, ready := Lookup(3); got != old || !ready {
		t.Fatal("partial refresh replaced snapshot")
	}
	state.Lock()
	state.loader = func(context.Context, uint) (*Workspace, error) { return &Workspace{Generation: 9}, nil }
	state.Unlock()
	if err := Refresh(context.Background(), 3); err != nil {
		t.Fatal(err)
	}
	if got, _ := Lookup(3); got != old {
		t.Fatal("stale generation replaced snapshot")
	}
	var reads atomic.Int64
	state.Lock()
	state.loader = func(context.Context, uint) (*Workspace, error) { reads.Add(1); return old, nil }
	state.Unlock()
	for i := 0; i < 1000; i++ {
		Lookup(3)
	}
	if reads.Load() != 0 {
		t.Fatal("lookup called loader")
	}
}

func BenchmarkSnapshotLookup(b *testing.B) {
	w := Build(domain.Workspace{ID: 1, HTTPProtocol: true, Timeout: 1000}, nil, nil, nil)
	state.Lock()
	state.workspaces = map[uint]*Workspace{1: w}
	state.ready = true
	state.Unlock()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		snapshot, _ := Lookup(1)
		_ = snapshot.Plan(uint64(i)).Settings[0]
	}
}

func BenchmarkBuildTaggedSnapshot(b *testing.B) {
	w := domain.Workspace{ID: 1, HTTPProtocol: true, Timeout: 7500, Retries: 2}
	s := w.DefaultCheckerSettings()
	s.Rules = []dto.TagCheckerRule{{TagID: 1, Mode: "add", Protocols: []string{"socks5"}}}
	w.CheckerConfig = s
	assignments := make([]domain.ProxyTagAssignment, 1000000)
	for i := range assignments {
		assignments[i] = domain.ProxyTagAssignment{WorkspaceID: 1, ProxyID: uint64(i + 1), ProxyTagID: 1}
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	retained := Build(w, assignments, nil, nil)
	runtime.GC()
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(retained)
	var retainedBytes uint64
	if after.HeapAlloc > before.HeapAlloc {
		retainedBytes = after.HeapAlloc - before.HeapAlloc
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w := Build(w, assignments, nil, nil)
		if w.Proxies.Len() != 1000000 {
			b.Fatal(w.Proxies.Len())
		}
	}
	b.ReportMetric(float64(retainedBytes), "retained-bytes")
}

func BenchmarkEvidenceSerialization(b *testing.B) {
	for _, workspaces := range []int{0, 1, 8} {
		b.Run(fmt.Sprint(workspaces), func(b *testing.B) {
			stat := domain.ProxyStatistic{ProxyID: 1, ProtocolID: 1, JudgeID: 1, TransportProtocol: "tcp", CheckTimeout: 7500, CheckRetries: 2}
			for i := 0; i < workspaces; i++ {
				stat.CheckEvidence = append(stat.CheckEvidence, domain.WorkspaceCheckEvidence{WorkspaceID: uint(i + 1), ConfigKey: strings.Repeat("a", 64), Alive: true})
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := json.Marshal(stat); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
