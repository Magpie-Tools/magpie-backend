package database

import (
	"reflect"
	"testing"
	"time"

	"magpie/internal/domain"
	testdb "magpie/internal/testkit/db"

	"gorm.io/gorm"
)

func TestProxyProtocolHealth(t *testing.T) {
	assertProxyProtocolHealth(t, testdb.OpenSQLite(t))
}

func TestProxyProtocolHealthPostgres(t *testing.T) {
	assertProxyProtocolHealth(t, testdb.OpenPostgres(t))
}

func assertProxyProtocolHealth(t *testing.T, db *gorm.DB) {
	t.Helper()
	a, b, proxy, judge := testdb.SeedCheckerEvidence(t, db)
	elite := domain.AnonymityLevel{Name: "elite"}
	if err := db.Create(&elite).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	add := func(minutesAgo, protocolID int, alive bool, evidence []domain.WorkspaceCheckEvidence) {
		t.Helper()
		row := domain.ProxyStatistic{ProxyID: proxy.ID, ProtocolID: protocolID, JudgeID: judge.ID, Alive: alive,
			CheckEvidence: evidence, CreatedAt: now.Add(-time.Duration(minutesAgo) * time.Minute)}
		if err := db.Create(&row).Error; err != nil {
			t.Fatal(err)
		}
	}
	verdict := func(key string, alive bool) []domain.WorkspaceCheckEvidence {
		return []domain.WorkspaceCheckEvidence{{WorkspaceID: a.ID, ConfigKey: key, Alive: alive}}
	}

	// HTTP: 22 checks with the current key; only the newest 20 count. Every third one failed.
	for minutesAgo := 22; minutesAgo >= 1; minutesAgo-- {
		add(minutesAgo, 1, true, verdict("http-now", minutesAgo%3 != 0))
	}
	add(0, 1, true, verdict("http-old", false))                                                   // other settings
	add(0, 1, false, []domain.WorkspaceCheckEvidence{{WorkspaceID: b.ID, ConfigKey: "http-now"}}) // other workspace
	add(0, 2, false, verdict("http-now", false))                                                  // other protocol
	// SOCKS5 legacy evidence: no key, so every check this workspace can see counts.
	add(3, 4, false, nil)
	add(2, 4, true, nil)
	add(1, 4, true, []domain.WorkspaceCheckEvidence{{WorkspaceID: b.ID, Alive: false}})

	current := []domain.ProxyLatestStatistic{
		{ProtocolID: 4, ConfigKey: "", TransportProtocol: "tcp", Alive: true, ResponseTime: 640, CheckedAt: now.Add(-2 * time.Minute)},
		{ProtocolID: 1, ConfigKey: "http-now", TransportProtocol: "quic", Alive: true, ResponseTime: 410, Attempt: 1, LevelID: &elite.ID, CheckedAt: now.Add(-time.Minute)},
	}
	health, err := proxyProtocolHealth(db, a.ID, proxy.ID, current)
	if err != nil {
		t.Fatal(err)
	}
	if len(health) != 2 || health[0].Protocol != "http" || health[1].Protocol != "socks5" {
		t.Fatalf("protocols = %+v", health)
	}
	http := health[0]
	if http.Transport != "quic" || http.ResponseTime != 410 || http.Attempt != 1 || http.AnonymityLevel != "elite" || !http.CheckedAt.Equal(now.Add(-time.Minute)) {
		t.Fatalf("http = %+v", http)
	}
	want := make([]bool, 0, 20)
	for minutesAgo := 20; minutesAgo >= 1; minutesAgo-- {
		want = append(want, minutesAgo%3 != 0)
	}
	if !reflect.DeepEqual(http.Recent, want) {
		t.Fatalf("http recent = %v, want %v", http.Recent, want)
	}
	if health[1].AnonymityLevel != "Unknown" || !reflect.DeepEqual(health[1].Recent, []bool{false, true}) {
		t.Fatalf("socks5 = %+v", health[1])
	}

	if empty, err := proxyProtocolHealth(db, a.ID, proxy.ID, nil); err != nil || empty == nil || len(empty) != 0 {
		t.Fatalf("no current checks = %v, %v", empty, err)
	}
}
