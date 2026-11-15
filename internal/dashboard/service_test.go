package dashboard

import (
	"context"
	"strings"
	"testing"
	"time"

	"magpie/internal/api/dto"
)

func ptr[T any](value T) *T { return &value }

func TestStatusFollowsTheReadTable(t *testing.T) {
	current := func(age float64) snapshotState {
		return snapshotState{WorkspaceGeneration: 3, CheckerGeneration: ptr(uint64(3)), HasHealth: true, Payload: []byte(`{}`), AgeSeconds: ptr(age)}
	}
	oldGeneration := func(age float64) snapshotState {
		state := current(age)
		state.CheckerGeneration = ptr(uint64(2))
		return state
	}
	dirty := func(state snapshotState) snapshotState { state.WorkspaceDirty = true; return state }
	failed := func(state snapshotState) snapshotState { state.FailedAt = ptr(time.Now()); return state }
	for _, test := range []struct {
		name  string
		state snapshotState
		want  Status
	}{
		{"current and young", current(5), StatusFresh},
		{"current, 30 s old", current(30), StatusStale},
		{"current, nine minutes old", current(9 * 60), StatusStale},
		{"current, older than ten minutes", current(11 * 60), StatusPending},
		{"health from an older generation", oldGeneration(5), StatusPending},
		{"no health built yet", func() snapshotState { s := current(5); s.HasHealth = false; return s }(), StatusPending},
		// Dirtiness outdates health but never makes a view pending by itself.
		{"dirty and young", dirty(oldGeneration(5)), StatusFresh},
		{"dirty and old", dirty(oldGeneration(60)), StatusStale},
		{"no row", snapshotState{WorkspaceGeneration: 1}, StatusPending},
		{"row before its first publish", snapshotState{WorkspaceGeneration: 1, RowExists: true}, StatusPending},
		{"failed with a payload", failed(oldGeneration(60)), StatusStale},
		{"failed without a payload", failed(snapshotState{RowExists: true}), StatusFailed},
		// Row 1 wins over a recorded failure.
		{"failed but fresh", failed(current(5)), StatusFresh},
	} {
		if got := statusOf(test.state); got != test.want {
			t.Errorf("%s: status %s, want %s", test.name, got, test.want)
		}
	}
}

func TestAssembleServesHealthThatIsNotCurrentAsOutdated(t *testing.T) {
	encoded, encodedHealth, err := encodeSnapshot(
		payload{Inventory: inventoryPart{TotalScraped: 7, ProxyCount: 9, CountryBreakdown: []countryCount{{Country: "DE", Count: 9}}}},
		&healthPart{AliveCount: 4, JudgeValidProxies: []judgeCount{{JudgeURL: "j", EliteProxies: 1}}, Fastest: []dto.ProxyFastestAlive{{ID: 1}}},
	)
	if err != nil {
		t.Fatal(err)
	}
	healthAt := time.Date(2026, 11, 15, 9, 0, 0, 0, time.UTC)
	base := snapshotState{WorkspaceGeneration: 3, CheckerGeneration: ptr(uint64(3)), HasHealth: true, Payload: encoded, Health: encodedHealth,
		HealthCollectedAt: &healthAt, AgeSeconds: ptr(float64(20 * 60)), TotalChecks: ptr(int64(50)), ChecksThisMonth: ptr(int64(5))}

	view, err := assemble(base)
	if err != nil {
		t.Fatal(err)
	}
	// Age alone never hides a part.
	if view.Status != StatusPending || *view.TotalScraped() != 7 || *view.ProxyCount() != 9 || *view.AliveCount() != 4 || len(view.FastestAliveProxies(10)) != 1 || view.TotalChecks != 50 || view.ChecksThisMonth != 5 {
		t.Fatalf("old current snapshot: %+v", view)
	}
	if view.HealthOutdated || view.CheckerSettingsPending || view.HealthCollectedAt == nil || !view.HealthCollectedAt.Equal(healthAt) {
		t.Fatalf("current health flags: %+v", view)
	}

	// A newer generation keeps serving the previous health, marked outdated.
	newer := base
	newer.WorkspaceGeneration = 4
	view, _ = assemble(newer)
	if !view.HealthOutdated || view.CheckerSettingsPending || view.AliveCount() == nil || *view.AliveCount() != 4 || len(view.JudgeValidProxies()) != 1 || len(view.FastestAliveProxies(10)) != 1 || !view.HealthCollectedAt.Equal(healthAt) {
		t.Fatalf("health of an older generation: %+v", view)
	}

	// So does a dirty workspace, even at the health's own generation.
	dirty := base
	dirty.WorkspaceDirty = true
	view, _ = assemble(dirty)
	if !view.HealthOutdated || !view.CheckerSettingsPending || view.AliveCount() == nil || *view.AliveCount() != 4 || len(view.JudgeValidProxies()) != 1 {
		t.Fatalf("health while settings apply: %+v", view)
	}

	// Without any health built, health parts are absent, not outdated.
	none := dirty
	none.HasHealth, none.Health, none.HealthCollectedAt = false, nil, nil
	view, _ = assemble(none)
	if view.HealthOutdated || view.AliveCount() != nil || view.JudgeValidProxies() != nil || view.RecentProxyChecks(8) != nil || view.FastestAliveProxies(10) != nil || view.HealthCollectedAt != nil {
		t.Fatalf("view without health: %+v", view)
	}

	// Live check figures appear in every status, including without a payload.
	empty, _ := assemble(snapshotState{TotalChecks: ptr(int64(3)), ChecksThisMonth: ptr(int64(0))})
	if empty.Status != StatusPending || empty.TotalChecks != 3 || empty.TotalScraped() != nil || empty.CountryBreakdown() != nil || empty.ReputationBreakdown() != nil {
		t.Fatalf("view without a snapshot: %+v", empty)
	}
}

func TestRecentChecksSliceKeepsTheQueryOrder(t *testing.T) {
	at := func(minute int) *time.Time { value := time.Date(2026, 11, 1, 0, minute, 0, 0, time.UTC); return &value }
	health := healthPart{RecentCandidates: []recentCandidate{
		// The read model's order (ordinal) differs from the final order: the
		// final order uses current evidence.
		{Ordinal: 1, ID: 10, Alive: false, LatestCheck: at(9)},
		{Ordinal: 2, ID: 11, Alive: true, LatestCheck: nil},
		{Ordinal: 3, ID: 12, Alive: true, LatestCheck: at(5)},
		{Ordinal: 4, ID: 9, Alive: true, LatestCheck: at(5)},
		{Ordinal: 5, ID: 13, Alive: false, LatestCheck: nil},
	}}
	ids := func(checks []dto.ProxyRecentCheck) []uint64 {
		result := make([]uint64, 0, len(checks))
		for _, check := range checks {
			result = append(result, check.ID)
		}
		return result
	}
	for limit, want := range map[int][]uint64{
		1: {10},
		2: {11, 10},
		3: {12, 11, 10},
		4: {9, 12, 11, 10},
		5: {9, 12, 11, 10, 13},
		8: {9, 12, 11, 10, 13},
	} {
		if got := ids(health.recentChecks(limit)); !equalIDs(got, want) {
			t.Errorf("limit %d: %v, want %v", limit, got, want)
		}
	}
	if got := health.recentChecks(2); !got[0].LatestCheck.IsZero() || !got[1].LatestCheck.Equal(*at(9)) {
		t.Fatalf("latest checks: %+v", got)
	}
}

func equalIDs(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestPayloadOverTheCapFails(t *testing.T) {
	large := payload{Inventory: inventoryPart{CountryBreakdown: []countryCount{{Country: strings.Repeat("x", maxPayloadBytes)}}}}
	if _, _, err := encodeSnapshot(large, nil); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversized payload: %v", err)
	}
	// The cap covers the payload and the health parts together.
	half := strings.Repeat("x", maxPayloadBytes/2)
	if _, _, err := encodeSnapshot(payload{Inventory: inventoryPart{CountryBreakdown: []countryCount{{Country: half}}}},
		&healthPart{JudgeValidProxies: []judgeCount{{JudgeURL: half}}}); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversized payload with health: %v", err)
	}
	if _, encodedHealth, err := encodeSnapshot(payload{}, nil); err != nil || encodedHealth != nil {
		t.Fatalf("build without health: %v %v", encodedHealth, err)
	}
	if decoded, err := decodePayload(nil); err != nil || decoded != nil {
		t.Fatalf("empty payload: %v %v", decoded, err)
	}
}

func TestComputedEmptyHealthListsAreNotNil(t *testing.T) {
	health := &healthPart{}
	if got := health.fastestAlive(10); got == nil || len(got) != 0 {
		t.Fatalf("fastest of an empty health part: %#v", got)
	}
	if got := health.recentChecks(8); got == nil || len(got) != 0 {
		t.Fatalf("recent checks of an empty health part: %#v", got)
	}
}

// After Stop, a claim registers nothing and touches no database: the service
// has no handle here, so any admission would panic.
func TestStoppedServiceClaimsNothing(t *testing.T) {
	service := NewService(nil, Config{})
	service.Stop()
	if f, attempted := service.claim(context.Background(), 1, false); f != nil || attempted {
		t.Fatalf("claim after Stop: %v %v", f, attempted)
	}
	if len(service.slots) != 0 || len(service.flights) != 0 {
		t.Fatal("a stopped service registered a build")
	}
}
