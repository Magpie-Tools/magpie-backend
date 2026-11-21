package database

import (
	"testing"
	"time"

	"magpie/internal/api/dto"
	"magpie/internal/domain"
	testdb "magpie/internal/testkit/db"

	"gorm.io/gorm"
)

func TestGetProxyStatisticsPage(t *testing.T) {
	assertProxyStatisticsPage(t, testdb.OpenSQLite(t))
}

func TestGetProxyStatisticsPagePostgres(t *testing.T) {
	assertProxyStatisticsPage(t, testdb.OpenPostgres(t))
}

func assertProxyStatisticsPage(t *testing.T, db *gorm.DB) {
	t.Helper()
	a, b, proxy, judge := testdb.SeedCheckerEvidence(t, db)
	other := domain.Judge{FullString: "https://judge.example/headers"}
	if err := db.Create(&other).Error; err != nil {
		t.Fatal(err)
	}
	levels := map[string]int{}
	for _, name := range []string{"elite", "anonymous", "transparent"} {
		level := domain.AnonymityLevel{Name: name}
		if err := db.Create(&level).Error; err != nil {
			t.Fatal(err)
		}
		levels[name] = level.ID
	}
	level := func(name string) *int {
		if name == "" {
			return nil
		}
		id := levels[name]
		return &id
	}

	now := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	type seed struct {
		minutesAgo int
		protocolID int // 1 http, 2 https, 3 socks4, 4 socks5 (domain.CheckerProtocols order)
		transport  string
		alive      bool
		ms         uint16
		attempt    uint8
		level      string
		judgeID    uint
		evidence   []domain.WorkspaceCheckEvidence
	}
	seeds := []seed{
		{1, 1, "tcp", true, 300, 0, "elite", judge.ID, nil},
		{2, 4, "quic", true, 900, 1, "anonymous", other.ID, nil},
		{3, 4, "tcp", false, 2500, 2, "", judge.ID, nil},
		{30 * 60, 2, "tcp", true, 120, 0, "transparent", other.ID, nil},
		// Workspace a's verdict overrides the physical one.
		{5, 1, "tcp", true, 400, 0, "elite", judge.ID, []domain.WorkspaceCheckEvidence{{WorkspaceID: a.ID, Alive: false}, {WorkspaceID: b.ID, Alive: true}}},
		// Only attributed to workspace b: hidden from a.
		{6, 1, "tcp", true, 500, 0, "elite", judge.ID, []domain.WorkspaceCheckEvidence{{WorkspaceID: b.ID, Alive: true}}},
	}
	for _, s := range seeds {
		row := domain.ProxyStatistic{ProxyID: proxy.ID, ProtocolID: s.protocolID, TransportProtocol: s.transport, Alive: s.alive,
			ResponseTime: s.ms, Attempt: s.attempt, LevelID: level(s.level), JudgeID: s.judgeID, CheckEvidence: s.evidence,
			CreatedAt: now.Add(-time.Duration(s.minutesAgo) * time.Minute)}
		if err := db.Create(&row).Error; err != nil {
			t.Fatal(err)
		}
	}

	get := func(q dto.ProxyStatisticsQuery) dto.ProxyStatisticsPage {
		t.Helper()
		page, err := getProxyStatisticsPageWithDB(db, a.ID, proxy.ID, q, now)
		if err != nil {
			t.Fatal(err)
		}
		return page
	}
	times := func(page dto.ProxyStatisticsPage) []uint16 {
		out := make([]uint16, 0, len(page.Statistics))
		for _, s := range page.Statistics {
			out = append(out, s.ResponseTime)
		}
		return out
	}
	expect := func(name string, got, want []uint16) {
		t.Helper()
		if len(got) != len(want) {
			t.Fatalf("%s: got %v, want %v", name, got, want)
		}
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("%s: got %v, want %v", name, got, want)
			}
		}
	}

	// Default: newest first, workspace-visible rows only, totals and counts.
	all := get(dto.ProxyStatisticsQuery{Page: 1, PageSize: 20})
	expect("default order", times(all), []uint16{300, 900, 2500, 400, 120})
	if all.Total != 5 || all.Counts.Alive != 3 || all.Counts.Dead != 2 {
		t.Fatalf("totals = %d, counts = %+v", all.Total, all.Counts)
	}
	if all.Statistics[3].Alive {
		t.Fatalf("workspace verdict not applied: %+v", all.Statistics[3])
	}
	if len(all.Options.Protocols) != 4 || len(all.Options.Judges) != 2 {
		t.Fatalf("options = %+v", all.Options)
	}
	if all.Options.Protocols[0] != (dto.ProxyStatisticProtocolOption{Protocol: "http", Transport: "tcp"}) {
		t.Fatalf("protocol options order = %+v", all.Options.Protocols)
	}

	// Paging.
	second := get(dto.ProxyStatisticsQuery{Page: 2, PageSize: 2})
	expect("page 2", times(second), []uint16{2500, 400})
	if second.Total != 5 {
		t.Fatalf("page total = %d", second.Total)
	}
	if beyond := get(dto.ProxyStatisticsQuery{Page: 9, PageSize: 2}); len(beyond.Statistics) != 0 || beyond.Total != 5 {
		t.Fatalf("beyond last page = %+v", beyond)
	}

	// Status filter: total follows it, counts ignore it.
	dead := get(dto.ProxyStatisticsQuery{Page: 1, Status: "dead"})
	expect("dead", times(dead), []uint16{2500, 400})
	if dead.Total != 2 || dead.Counts.Alive != 3 || dead.Counts.Dead != 2 {
		t.Fatalf("dead totals = %d %+v", dead.Total, dead.Counts)
	}

	// Protocol and transport, judge, period.
	expect("socks5/quic", times(get(dto.ProxyStatisticsQuery{Page: 1, Protocol: "SOCKS5", Transport: "quic"})), []uint16{900})
	expect("judge", times(get(dto.ProxyStatisticsQuery{Page: 1, JudgeID: other.ID})), []uint16{900, 120})
	lastHour := get(dto.ProxyStatisticsQuery{Page: 1, Hours: 1})
	expect("last hour", times(lastHour), []uint16{300, 900, 2500, 400})
	if lastHour.Counts.Alive != 2 || lastHour.Counts.Dead != 2 {
		t.Fatalf("counts follow the other filters: %+v", lastHour.Counts)
	}

	// Settings: without a current configuration nothing is current and everything is previous or legacy.
	if current := get(dto.ProxyStatisticsQuery{Page: 1, Settings: "current"}); current.Total != 0 {
		t.Fatalf("current without configuration = %d", current.Total)
	}
	if previous := get(dto.ProxyStatisticsQuery{Page: 1, Settings: "previous"}); previous.Total != 5 {
		t.Fatalf("previous = %d", previous.Total)
	}

	// Response: dead checks (no latency) last in both directions.
	expect("response desc", times(get(dto.ProxyStatisticsQuery{Page: 1, Sort: "response", Order: "desc"})), []uint16{900, 300, 120, 2500, 400})
	expect("response asc", times(get(dto.ProxyStatisticsQuery{Page: 1, Sort: "response", Order: "asc"})), []uint16{120, 300, 900, 400, 2500})

	// Anonymity: ranked transparent < anonymous < elite, unknown last in both directions.
	expect("anonymity desc", times(get(dto.ProxyStatisticsQuery{Page: 1, Sort: "anonymity", Order: "desc"})), []uint16{300, 400, 900, 120, 2500})
	expect("anonymity asc", times(get(dto.ProxyStatisticsQuery{Page: 1, Sort: "anonymity", Order: "asc"})), []uint16{120, 900, 300, 400, 2500})

	// Attempt, protocol and judge (judge sorts without the scheme).
	expect("attempt asc", times(get(dto.ProxyStatisticsQuery{Page: 1, Sort: "attempt", Order: "asc"})), []uint16{300, 400, 120, 900, 2500})
	expect("protocol asc", times(get(dto.ProxyStatisticsQuery{Page: 1, Sort: "protocol", Order: "asc"})), []uint16{300, 400, 120, 900, 2500})
	expect("judge asc", times(get(dto.ProxyStatisticsQuery{Page: 1, Sort: "judge", Order: "asc"})), []uint16{300, 2500, 400, 900, 120})

	// Unknown values fall back to the unfiltered, newest-first first page.
	junk := get(dto.ProxyStatisticsQuery{Page: -3, PageSize: 5000, Sort: "drop", Order: "sideways", Status: "zombie", Settings: "x", Hours: 7})
	if junk.Page != 1 || junk.PageSize != 100 || junk.Total != 5 {
		t.Fatalf("normalised query = %+v", junk)
	}
}
