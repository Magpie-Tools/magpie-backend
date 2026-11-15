package dashboard

import (
	"context"
	"strings"
	"testing"
	"time"

	"magpie/internal/domain"
	testdb "magpie/internal/testkit/db"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestSnapshotReadUsageFiguresPostgres(t *testing.T) {
	db := testdb.OpenPostgres(t)
	store := NewStore(db)
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
		name                   string
		workspaceID            uint
		total, checksThisMonth int64
	}{
		{"no usage rows", none, 0, 0},
		{"earlier periods only", earlier, 49, 0},
		{"current period", withCurrent, 112, 12},
	} {
		state, err := store.read(context.Background(), test.workspaceID, true, now)
		if err != nil {
			t.Fatal(test.name, err)
		}
		if state.TotalChecks == nil || state.ChecksThisMonth == nil || *state.TotalChecks != test.total || *state.ChecksThisMonth != test.checksThisMonth {
			t.Fatalf("%s: total=%v this month=%v, want %d and %d", test.name, state.TotalChecks, state.ChecksThisMonth, test.total, test.checksThisMonth)
		}
		// A re-read during a wait carries neither figure nor the payload.
		light, err := store.read(context.Background(), test.workspaceID, false, now)
		if err != nil || light.TotalChecks != nil || light.ChecksThisMonth != nil || light.Payload != nil {
			t.Fatalf("%s: re-read %+v %v", test.name, light, err)
		}
	}
}

// The joined read stays a set of primary-key reads with thousands of
// workspaces, several usage periods each, and an absent current period.
func TestSnapshotReadPlanPostgres(t *testing.T) {
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
	if err := db.Exec(`INSERT INTO dashboard_snapshots (workspace_id, payload_version, payload) SELECT id, 1, '\x7b7d'::bytea FROM workspaces`).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("ANALYZE workspaces; ANALYZE workspace_usage_periods; ANALYZE dashboard_snapshots").Error; err != nil {
		t.Fatal(err)
	}
	var workspaceID uint
	if err := db.Raw("SELECT id FROM workspaces ORDER BY id DESC LIMIT 1").Scan(&workspaceID).Error; err != nil {
		t.Fatal(err)
	}
	log := &statementLog{Interface: logger.Default.LogMode(logger.Silent)}
	store := NewStore(db.Session(&gorm.Session{Logger: log}))
	now := time.Date(2026, time.November, 2, 0, 0, 0, 0, time.UTC)
	state, err := store.read(context.Background(), workspaceID, true, now)
	if err != nil || state.TotalChecks == nil || *state.TotalChecks != 60 || *state.ChecksThisMonth != 0 {
		t.Fatalf("absent current period: %+v %v", state, err)
	}
	// The snapshot and both usage figures come from one statement.
	if statements := log.take(); len(statements) != 1 {
		t.Fatalf("the full read ran %d statements: %v", len(statements), statements)
	}
	query, args := store.readStatement(workspaceID, true, now)
	var plan []string
	if err := db.Raw("EXPLAIN "+query, args...).Scan(&plan).Error; err != nil {
		t.Fatal(err)
	}
	for _, line := range plan {
		if strings.Contains(line, "Seq Scan") {
			t.Fatalf("the read scans a table:\n%s", strings.Join(plan, "\n"))
		}
	}
}
