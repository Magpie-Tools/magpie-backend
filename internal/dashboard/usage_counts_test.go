package dashboard

import (
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"
	"magpie/internal/domain"
	testdb "magpie/internal/testkit/db"
)

func TestDashboardUsageCounts(t *testing.T) {
	for _, dialect := range []string{"sqlite", "postgres"} {
		t.Run(dialect, func(t *testing.T) {
			var db *gorm.DB
			if dialect == "postgres" {
				db = testdb.OpenPostgres(t)
			} else {
				db = testdb.OpenSQLite(t)
			}
			now := time.Date(2026, time.November, 15, 12, 0, 0, 0, time.UTC)
			current := time.Date(2026, time.November, 1, 0, 0, 0, 0, time.UTC)
			october := current.AddDate(0, -1, 0)
			september := current.AddDate(0, -2, 0)
			workspaces := []domain.Workspace{{Name: "No usage"}, {Name: "Earlier periods only"}, {Name: "Current period"}, {Name: "Other"}}
			if err := db.Create(&workspaces).Error; err != nil {
				t.Fatal(err)
			}
			none, earlier, withCurrent, other := workspaces[0].ID, workspaces[1].ID, workspaces[2].ID, workspaces[3].ID
			rows := []domain.WorkspaceUsagePeriod{
				// Before the meter existed only check_attempts was written.
				{WorkspaceID: earlier, PeriodStart: september, PeriodEnd: october, CheckAttempts: 40},
				{WorkspaceID: earlier, PeriodStart: october, PeriodEnd: current, CheckAttempts: 7, Checks: 9},
				{WorkspaceID: withCurrent, PeriodStart: october, PeriodEnd: current, CheckAttempts: 100, Checks: 30},
				// Lossy ingestion: check_attempts is below the metered checks.
				{WorkspaceID: withCurrent, PeriodStart: current, PeriodEnd: current.AddDate(0, 1, 0), CheckAttempts: 5, Checks: 12},
				{WorkspaceID: other, PeriodStart: current, PeriodEnd: current.AddDate(0, 1, 0), CheckAttempts: 1000, Checks: 1000},
			}
			if err := db.Create(&rows).Error; err != nil {
				t.Fatal(err)
			}
			for _, test := range []struct {
				name        string
				workspaceID uint
				want        dashboardUsageCounts
			}{
				{"no usage rows", none, dashboardUsageCounts{}},
				{"earlier periods only", earlier, dashboardUsageCounts{TotalChecks: 49}},
				{"current period", withCurrent, dashboardUsageCounts{TotalChecks: 112, ChecksThisMonth: 12}},
			} {
				got, err := loadDashboardUsageCounts(db, test.workspaceID, now)
				if err != nil {
					t.Fatal(test.name, err)
				}
				if got != test.want {
					t.Fatalf("%s: got %+v, want %+v", test.name, got, test.want)
				}
				if got.TotalChecks < got.ChecksThisMonth {
					t.Fatalf("%s: total below this month: %+v", test.name, got)
				}
			}
		})
	}
}

// The usage read stays a primary-key range read with thousands of workspaces,
// several periods each, and an absent current period.
func TestDashboardUsageCountsPlanPostgres(t *testing.T) {
	db := testdb.OpenPostgres(t)
	if err := db.Exec(`INSERT INTO workspaces (name, created_at, updated_at)
		SELECT 'usage plan ' || g, now(), now() FROM generate_series(1, 3000) g`).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`INSERT INTO workspace_usage_periods (workspace_id, period_start, period_end, check_attempts, checks)
		SELECT w.id, p.start, p.start + interval '1 month', 10, 20
		FROM workspaces w CROSS JOIN (VALUES (timestamptz '2026-08-01'), (timestamptz '2026-09-01'), (timestamptz '2026-10-01')) p(start)`).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("ANALYZE workspace_usage_periods").Error; err != nil {
		t.Fatal(err)
	}
	var workspaceID uint
	if err := db.Raw("SELECT id FROM workspaces ORDER BY id DESC LIMIT 1").Scan(&workspaceID).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.November, 2, 0, 0, 0, 0, time.UTC)
	got, err := loadDashboardUsageCounts(db, workspaceID, now)
	if err != nil || got != (dashboardUsageCounts{TotalChecks: 60}) {
		t.Fatalf("absent current period: %+v %v", got, err)
	}
	var plan []string
	if err := db.Raw("EXPLAIN "+dashboardUsageCountsSQL, workspaceID, workspaceID, now).Scan(&plan).Error; err != nil {
		t.Fatal(err)
	}
	for _, line := range plan {
		if strings.Contains(line, "Seq Scan") {
			t.Fatalf("usage read scans the table:\n%s", strings.Join(plan, "\n"))
		}
	}
}
