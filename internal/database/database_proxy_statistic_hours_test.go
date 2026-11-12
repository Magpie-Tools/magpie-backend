package database

import (
	"testing"
	"time"

	"magpie/internal/domain"
	testdb "magpie/internal/testkit/db"

	"gorm.io/gorm"
)

func TestGetProxyStatisticHoursGroupsWorkspaceVerdicts(t *testing.T) {
	assertProxyStatisticHoursGroupWorkspaceVerdicts(t, testdb.OpenSQLite(t))
}

func TestGetProxyStatisticHoursGroupsWorkspaceVerdictsPostgres(t *testing.T) {
	assertProxyStatisticHoursGroupWorkspaceVerdicts(t, testdb.OpenPostgres(t))
}

func assertProxyStatisticHoursGroupWorkspaceVerdicts(t *testing.T, db *gorm.DB) {
	t.Helper()
	a, b, proxy, judge := testdb.SeedCheckerEvidence(t, db)
	hour := time.Date(2026, 11, 12, 14, 0, 0, 0, time.UTC)

	stat := func(at time.Time, alive bool, responseTime uint16, evidence ...domain.WorkspaceCheckEvidence) domain.ProxyStatistic {
		return domain.ProxyStatistic{ProxyID: proxy.ID, ProtocolID: 1, JudgeID: judge.ID, Alive: alive, ResponseTime: responseTime, CheckEvidence: evidence, CreatedAt: at}
	}
	rows := []domain.ProxyStatistic{
		// Before the window.
		stat(hour.Add(-time.Minute), true, 900),
		// First hour: legacy rows without evidence use the physical verdict.
		stat(hour.Add(5*time.Minute), true, 1000),
		stat(hour.Add(40*time.Minute), false, 5000),
		// Second hour: the workspace verdict overrides the physical one.
		stat(hour.Add(70*time.Minute), true, 2000,
			domain.WorkspaceCheckEvidence{WorkspaceID: a.ID, Alive: false},
			domain.WorkspaceCheckEvidence{WorkspaceID: b.ID, Alive: true}),
		// Evidence for another workspace only: hidden from workspace a.
		stat(hour.Add(80*time.Minute), true, 3000,
			domain.WorkspaceCheckEvidence{WorkspaceID: b.ID, Alive: true}),
	}
	if err := db.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}

	got, err := getProxyStatisticHoursWithDB(db, a.ID, proxy.ID, hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("workspace a hours = %+v", got)
	}
	if !got[0].Hour.Equal(hour) || got[0].Checks != 2 || got[0].Failed != 1 || got[0].AvgResponseTime != 1000 {
		t.Fatalf("first hour = %+v", got[0])
	}
	if !got[1].Hour.Equal(hour.Add(time.Hour)) || got[1].Checks != 1 || got[1].Failed != 1 || got[1].AvgResponseTime != 0 {
		t.Fatalf("second hour = %+v", got[1])
	}

	got, err = getProxyStatisticHoursWithDB(db, b.ID, proxy.ID, hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[1].Checks != 2 || got[1].Failed != 0 || got[1].AvgResponseTime != 2500 {
		t.Fatalf("workspace b hours = %+v", got)
	}
}

func TestGetProxyStatisticHoursRequiresManagedProxy(t *testing.T) {
	assertProxyStatisticHoursRequireManagedProxy(t, testdb.OpenSQLite(t))
}

func TestGetProxyStatisticHoursRequiresManagedProxyPostgres(t *testing.T) {
	assertProxyStatisticHoursRequireManagedProxy(t, testdb.OpenPostgres(t))
}

func assertProxyStatisticHoursRequireManagedProxy(t *testing.T, db *gorm.DB) {
	t.Helper()
	a, _, proxy, judge := testdb.SeedCheckerEvidence(t, db)
	other := domain.Workspace{Name: "Outsider"}
	if err := db.Create(&other).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := db.Create(&domain.ProxyStatistic{ProxyID: proxy.ID, ProtocolID: 1, JudgeID: judge.ID, Alive: true, ResponseTime: 100, CreatedAt: now}).Error; err != nil {
		t.Fatal(err)
	}
	since := now.Add(-time.Hour)
	if got, err := getProxyStatisticHoursWithDB(db, a.ID, proxy.ID, since); err != nil || len(got) != 1 {
		t.Fatalf("owner hours = %+v, %v", got, err)
	}
	if got, err := getProxyStatisticHoursWithDB(db, other.ID, proxy.ID, since); err != nil || len(got) != 0 {
		t.Fatalf("outsider hours = %+v, %v", got, err)
	}
}
