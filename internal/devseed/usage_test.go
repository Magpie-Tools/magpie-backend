package devseed

import (
	"testing"
	"time"

	"magpie/internal/domain"
	testdb "magpie/internal/testkit/db"
)

// History that crosses a month boundary splits into one usage row per month,
// charging Attempt+1 requests per result to both request counters, after one
// pre-meter month with check attempts only.
func TestSeedDevelopmentUsageSplitsMonths(t *testing.T) {
	db := testdb.OpenSQLite(t)
	workspace := domain.Workspace{Name: "Usage"}
	if err := db.Create(&workspace).Error; err != nil {
		t.Fatal(err)
	}
	for _, row := range []any{&domain.Protocol{ID: 1, Name: "http"}, &domain.Judge{ID: 1, FullString: "http://judge.example.test/"}} {
		if err := db.Create(row).Error; err != nil {
			t.Fatal(err)
		}
	}
	for id := uint64(1); id <= 2; id++ {
		if err := db.Create(&domain.Proxy{ID: id, IP: "192.0.2.1", Port: uint16(8000 + id)}).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Create(&domain.ManagedProxy{WorkspaceID: workspace.ID, ProxyID: id}).Error; err != nil {
			t.Fatal(err)
		}
	}
	october := time.Date(2026, time.October, 31, 23, 0, 0, 0, time.UTC)
	november := time.Date(2026, time.November, 1, 1, 0, 0, 0, time.UTC)
	for _, stat := range []domain.ProxyStatistic{
		{ProxyID: 1, ProtocolID: 1, JudgeID: 1, Attempt: 0, CreatedAt: october},
		{ProxyID: 2, ProtocolID: 1, JudgeID: 1, Attempt: 2, CreatedAt: october},
		{ProxyID: 1, ProtocolID: 1, JudgeID: 1, Attempt: 1, CreatedAt: november},
	} {
		if err := db.Omit("Proxy", "Protocol", "Judge", "Level").Create(&stat).Error; err != nil {
			t.Fatal(err)
		}
	}
	options := DevelopmentSeedOptions{Now: time.Date(2026, time.November, 2, 12, 0, 0, 0, time.UTC), HistoryDays: 3}
	if err := seedDevelopmentUsage(db, workspace, 2, 2, options); err != nil {
		t.Fatal(err)
	}
	var periods []domain.WorkspaceUsagePeriod
	if err := db.Where("workspace_id = ?", workspace.ID).Order("period_start").Find(&periods).Error; err != nil {
		t.Fatal(err)
	}
	type row struct {
		start                      time.Time
		attempts, checks, protocol uint64
	}
	want := []row{
		{time.Date(2026, time.September, 1, 0, 0, 0, 0, time.UTC), 2 * 4 * 30, 0, 0},
		{time.Date(2026, time.October, 1, 0, 0, 0, 0, time.UTC), 4, 4, 2},
		{time.Date(2026, time.November, 1, 0, 0, 0, 0, time.UTC), 2, 2, 1},
	}
	if len(periods) != len(want) {
		t.Fatalf("periods: %+v", periods)
	}
	for i, period := range periods {
		got := row{period.PeriodStart.UTC(), period.CheckAttempts, period.Checks, period.ProtocolChecks}
		if got != want[i] {
			t.Fatalf("period %d: %+v, want %+v", i, got, want[i])
		}
	}
}
