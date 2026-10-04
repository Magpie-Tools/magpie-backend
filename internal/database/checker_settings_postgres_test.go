package database

import (
	"context"
	"gorm.io/gorm"
	"magpie/internal/api/dto"
	"magpie/internal/domain"
	"reflect"
	"testing"
	"time"
)

func testCheckerProfiles() *dto.CheckerSettings {
	w := domain.Workspace{HTTPProtocol: true, Timeout: 1000, Retries: 0, TransportProtocol: "tcp"}
	s := w.DefaultCheckerSettings()
	return s
}

func seedCheckerEvidenceTest(t *testing.T, db *gorm.DB) (domain.Workspace, domain.Workspace, domain.Proxy, domain.Judge) {
	t.Helper()
	for i, name := range domain.CheckerProtocols {
		if err := db.Create(&domain.Protocol{ID: i + 1, Name: name}).Error; err != nil {
			t.Fatal(err)
		}
	}
	judge := domain.Judge{FullString: "http://127.0.0.1:8080/"}
	if err := db.Create(&judge).Error; err != nil {
		t.Fatal(err)
	}
	a, b := domain.Workspace{Name: "Accept", CheckerConfig: testCheckerProfiles()}, domain.Workspace{Name: "Reject", CheckerConfig: testCheckerProfiles()}
	if err := db.Create(&a).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&b).Error; err != nil {
		t.Fatal(err)
	}
	for id, regex := range map[uint]string{a.ID: "accepted", b.ID: "different"} {
		if err := db.Create(&domain.WorkspaceJudge{WorkspaceID: id, JudgeID: judge.ID, Regex: regex}).Error; err != nil {
			t.Fatal(err)
		}
	}
	proxy := domain.Proxy{IP: "192.0.2.1", Port: 8080}
	if err := db.Create(&proxy).Error; err != nil {
		t.Fatal(err)
	}
	for _, id := range []uint{a.ID, b.ID} {
		if err := db.Create(&domain.ManagedProxy{WorkspaceID: id, ProxyID: proxy.ID}).Error; err != nil {
			t.Fatal(err)
		}
	}
	return a, b, proxy, judge
}

func TestCheckerHealthIsolatesWorkspaceConfigurationAndDelayedResultsPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	a, b, proxy, judge := seedCheckerEvidenceTest(t, db)
	ctx := context.Background()
	sa, err := LoadCheckerWorkspace(ctx, a.ID)
	if err != nil {
		t.Fatal(err)
	}
	sb, err := LoadCheckerWorkspace(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	if sa.Default.Keys[0] == sb.Default.Keys[0] {
		t.Fatal("judge validation not attributed")
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	evidence := []domain.WorkspaceCheckEvidence{{WorkspaceID: a.ID, ConfigKey: sa.Default.Keys[0], Alive: true}, {WorkspaceID: b.ID, ConfigKey: sb.Default.Keys[0], Alive: false}}
	stat := domain.ProxyStatistic{ProxyID: proxy.ID, ProtocolID: 1, JudgeID: judge.ID, Alive: true, TransportProtocol: "tcp", CheckTimeout: 1000, CheckEvidence: evidence, WorkspaceIDs: []uint{a.ID, b.ID}, CreatedAt: now}
	if err := InsertProxyStatistics(ctx, []domain.ProxyStatistic{stat}, 100); err != nil {
		t.Fatal(err)
	}
	assertAlive := func(id uint, want *bool) {
		t.Helper()
		detail, err := GetProxyDetail(id, proxy.ID)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(detail.Alive, want) {
			t.Fatalf("workspace %d alive=%v want=%v", id, detail.Alive, want)
		}
	}
	assertAlive(a.ID, new(true))
	assertAlive(b.ID, new(false))
	for id, want := range map[uint]int{a.ID: 1, b.ID: 0} {
		routes, err := aliveProxiesForProtocol(db, id, 1, nil, "", nil)
		if err != nil || len(routes) != want {
			t.Fatalf("workspace %d TCP candidates=%d err=%v", id, len(routes), err)
		}
	}
	if err := refreshUserProxyFilterIndexes(db, "WHERE up.proxy_id = ?", proxy.ID); err != nil {
		t.Fatal(err)
	}
	for id, want := range map[uint]bool{a.ID: true, b.ID: false} {
		var row domain.WorkspaceProxyFilterIndex
		if err := db.Where("workspace_id = ? AND proxy_id = ?", id, proxy.ID).First(&row).Error; err != nil {
			t.Fatal(err)
		}
		if row.Alive != want {
			t.Fatal(id, row.Alive)
		}
	}
	// Edit timeout; an in-flight old success must stay historical.
	defaults := a.CheckerConfig.Defaults
	defaults.Timeout = 2000
	a.CheckerConfig.Defaults = defaults
	if err := db.Model(&a).Select("CheckerConfig").Updates(a).Error; err != nil {
		t.Fatal(err)
	}
	changed, err := LoadCheckerWorkspace(ctx, a.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertAlive(a.ID, nil)
	stat.CreatedAt = now.Add(time.Second)
	stat.CheckEvidence = evidence[:1]
	stat.WorkspaceIDs = []uint{a.ID}
	if err := InsertProxyStatistics(ctx, []domain.ProxyStatistic{stat}, 100); err != nil {
		t.Fatal(err)
	}
	assertAlive(a.ID, nil)
	assertAlive(b.ID, new(false))
	if err := refreshUserProxyFilterIndexes(db, "WHERE up.proxy_id = ?", proxy.ID); err != nil {
		t.Fatal(err)
	}
	dead := buildProxyListFilterQuery(a.ID, dto.ProxyListFilters{Status: "dead"})
	var ids []uint64
	if err := dead.Pluck("ufi.proxy_id", &ids).Error; err != nil || len(ids) > 0 {
		t.Fatal("unknown counted as dead", ids, err)
	}
	stat.CreatedAt = now.Add(2 * time.Second)
	stat.CheckTimeout = 2000
	stat.CheckEvidence = []domain.WorkspaceCheckEvidence{{WorkspaceID: a.ID, ConfigKey: changed.Default.Keys[0], Alive: true}}
	if err := InsertProxyStatistics(ctx, []domain.ProxyStatistic{stat}, 100); err != nil {
		t.Fatal(err)
	}
	assertAlive(a.ID, new(true))
	history, err := GetProxyStatistics(a.ID, proxy.ID, 50)
	if err != nil || len(history) != 3 {
		t.Fatal(history, err)
	}
	if !history[0].Current || history[1].Current || history[2].Current {
		t.Fatal("old history marked current", history)
	}
	bHistory, err := GetProxyStatistics(b.ID, proxy.ID, 50)
	if err != nil || len(bHistory) != 1 || bHistory[0].Alive {
		t.Fatal("shared physical verdict leaked", bHistory, err)
	}
	if deleted, err := PruneObsoleteCheckerEvidence(ctx, 100); err != nil || deleted != 1 {
		t.Fatal(deleted, err)
	}
	// QUIC health cannot supply TCP rotation or TCP uptime evidence.
	defaults.Transport = "quic"
	a.CheckerConfig.Defaults = defaults
	if err := db.Model(&a).Select("CheckerConfig").Updates(a).Error; err != nil {
		t.Fatal(err)
	}
	quic, err := LoadCheckerWorkspace(ctx, a.ID)
	if err != nil {
		t.Fatal(err)
	}
	stat.CreatedAt = now.Add(3 * time.Second)
	stat.TransportProtocol = "quic"
	stat.CheckEvidence[0].ConfigKey = quic.Default.Keys[0]
	if err := InsertProxyStatistics(ctx, []domain.ProxyStatistic{stat}, 100); err != nil {
		t.Fatal(err)
	}
	assertAlive(a.ID, new(true))
	routes, err := aliveProxiesForProtocol(db, a.ID, 1, nil, "", nil)
	if err != nil || len(routes) != 0 {
		t.Fatal("QUIC qualified TCP", routes, err)
	}
	percentage := float64(1)
	routes, err = aliveProxiesForProtocol(db, b.ID, 1, nil, "min", &percentage)
	if err != nil || len(routes) != 0 {
		t.Fatal(routes, err)
	}
}

func TestCheckerTagRulesProjectionAndLegacySavePostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	a, b, proxy, _ := seedCheckerEvidenceTest(t, db)
	user := domain.User{Email: "settings@example.test", Password: "hash"}
	if err := db.Create(&user).Error; err != nil {
		t.Fatal(err)
	}
	tag := domain.ProxyTag{WorkspaceID: a.ID, Name: "Tagged", Color: "#22C55E"}
	if err := tag.Normalize(); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&tag).Error; err != nil {
		t.Fatal(err)
	}
	timeout := uint16(3000)
	a.CheckerConfig.Rules = []dto.TagCheckerRule{{TagID: tag.ID, Mode: "add", Protocols: []string{"socks5"}, Timeout: &timeout}}
	if err := db.Model(&a).Select("CheckerConfig").Updates(a).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := ReplaceProxyTags(a.ID, proxy.ID, []uint64{tag.ID}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := LoadCheckerWorkspace(context.Background(), a.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !snapshot.Plan(proxy.ID).Settings[3].Enabled || snapshot.Plan(proxy.ID).Settings[3].Timeout != 3000 || snapshot.Plan(proxy.ID).Settings[0].Timeout != 3000 {
		t.Fatal(snapshot.Plan(proxy.ID).Settings)
	}
	var plans []domain.ProxyCheckerPlan
	if err := db.Where("workspace_id = ? AND proxy_id = ?", a.ID, proxy.ID).Find(&plans).Error; err != nil || len(plans) != 2 {
		t.Fatal(plans, err)
	}
	for _, plan := range plans {
		if (plan.ProtocolID != 1 && plan.ProtocolID != 4) || plan.ConfigKey != snapshot.Plan(proxy.ID).Keys[plan.ProtocolID-1] {
			t.Fatal("shared timeout must project both changed protocol keys", plans)
		}
	}
	if _, err := ReplaceProxyTags(b.ID, proxy.ID, []uint64{tag.ID}); err == nil {
		t.Fatal("foreign tag accepted")
	}
	// An older client's preference save omits checker_settings and retains rules.
	if err := db.First(&a, a.ID).Error; err != nil {
		t.Fatal(err)
	}
	before := a.CheckerConfig
	legacy := a.ToUserSettings(nil, nil, domain.WorkspaceMemberPreference{})
	legacy.CheckerSettings = nil
	if err := UpdateWorkspaceSettings(a.ID, user.ID, legacy); err != nil {
		t.Fatal(err)
	}
	var saved domain.Workspace
	if err := db.First(&saved, a.ID).Error; err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, saved.CheckerConfig) {
		t.Fatal("legacy save erased profiles", saved.CheckerConfig)
	}
	if err := DeleteProxyTag(a.ID, tag.ID); err != nil {
		t.Fatal(err)
	}
	if err := db.First(&saved, a.ID).Error; err != nil {
		t.Fatal(err)
	}
	if len(saved.CheckerConfig.Rules) != 0 {
		t.Fatal("deleted tag rule retained")
	}
	snapshot, err = LoadCheckerWorkspace(context.Background(), a.ID)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Plan(proxy.ID) != snapshot.Default {
		t.Fatal("deleted tag override retained")
	}
	// Defaults and rule order are atomic when any referenced tag is invalid.
	invalid := saved.ToUserSettings(nil, nil, domain.WorkspaceMemberPreference{})
	invalid.CheckerSettings = testCheckerProfiles()
	invalid.CheckerSettings.Rules = []dto.TagCheckerRule{{TagID: 999, Mode: "remove", Protocols: []string{"http"}}}
	if err := UpdateWorkspaceSettings(a.ID, user.ID, invalid); err == nil {
		t.Fatal("invalid rule saved")
	}
	var after domain.Workspace
	if err := db.First(&after, a.ID).Error; err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(saved.CheckerConfig, after.CheckerConfig) {
		t.Fatal("partial settings committed")
	}
}

func TestCheckerAttributionMigrationPreservesLegacyPartitionHistoryPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	a, _, proxy, judge := seedCheckerEvidenceTest(t, db)
	now := time.Now().UTC().Truncate(time.Microsecond)
	if err := InsertProxyStatistics(context.Background(), []domain.ProxyStatistic{{ProxyID: proxy.ID, ProtocolID: 1, JudgeID: judge.ID, Alive: true, CreatedAt: now}}, 100); err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{
		"ALTER TABLE proxy_latest_statistics DROP CONSTRAINT proxy_latest_statistics_pkey",
		"ALTER TABLE proxy_latest_statistics DROP COLUMN workspace_id, DROP COLUMN config_key, DROP COLUMN transport_protocol",
		"ALTER TABLE proxy_latest_statistics ADD PRIMARY KEY (proxy_id, protocol_id)",
		"ALTER TABLE proxy_statistics DROP COLUMN check_evidence, DROP COLUMN transport_protocol, DROP COLUMN check_timeout, DROP COLUMN check_retries",
	} {
		if err := db.Exec(sql).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := prepareCheckerSettingsSchema(db); err != nil {
		t.Fatal(err)
	}
	if err := ensureCheckerSettingsSchema(db); err != nil {
		t.Fatal(err)
	}
	if err := ensureProxyStatisticsPartitionSchema(db); err != nil {
		t.Fatal(err)
	}
	if err := prepareCheckerSettingsSchema(db); err != nil {
		t.Fatal("second migration", err)
	}
	if err := ensureCheckerSettingsSchema(db); err != nil {
		t.Fatal("second attribution migration", err)
	}
	var count int64
	if err := db.Model(&domain.ProxyStatistic{}).Count(&count).Error; err != nil || count != 1 {
		t.Fatal("legacy history lost", count, err)
	}
	var keys []struct{ Column string }
	if err := db.Raw(`SELECT a.attname AS column FROM pg_constraint c JOIN unnest(c.conkey) n ON true JOIN pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=n WHERE c.conrelid='proxy_latest_statistics'::regclass AND c.contype='p' ORDER BY a.attname`).Scan(&keys).Error; err != nil || len(keys) != 4 {
		t.Fatal(keys, err)
	}
	if _, err := LoadCheckerWorkspace(context.Background(), a.ID); err != nil {
		t.Fatal(err)
	}
	detail, err := GetProxyDetail(a.ID, proxy.ID)
	if err != nil || detail.Alive != nil {
		t.Fatal("legacy evidence was fabricated as current", detail, err)
	}
	stats, err := GetProxyStatistics(a.ID, proxy.ID, 50)
	if err != nil || len(stats) != 1 || stats[0].Current || stats[0].Transport != "" {
		t.Fatal(stats, err)
	}
}

func TestPendingCheckerProjectionSuppressesHealthWithoutPruningEvidencePostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	a, _, proxy, judge := seedCheckerEvidenceTest(t, db)
	ctx := context.Background()
	snapshot, err := LoadCheckerWorkspace(ctx, a.ID)
	if err != nil {
		t.Fatal(err)
	}
	stat := domain.ProxyStatistic{ProxyID: proxy.ID, ProtocolID: 1, JudgeID: judge.ID, Alive: true, TransportProtocol: "tcp", CheckTimeout: 1000, CreatedAt: time.Now().UTC(), WorkspaceIDs: []uint{a.ID}, CheckEvidence: []domain.WorkspaceCheckEvidence{{WorkspaceID: a.ID, ConfigKey: snapshot.Default.Keys[0], Alive: true}}}
	if err := InsertProxyStatistics(ctx, []domain.ProxyStatistic{stat}, 100); err != nil {
		t.Fatal(err)
	}
	if err := refreshUserProxyFilterIndexes(db, "WHERE up.workspace_id = ?", a.ID); err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&domain.Workspace{}).Where("id = ?", a.ID).UpdateColumn("checker_dirty", true).Error; err != nil {
		t.Fatal(err)
	}
	// Simulate a committed edit whose projection refresh failed. Old indexed
	// results and cached dashboard lists cannot qualify it as alive or dead.
	rows, total := GetProxyInfoPageWithFilters(a.ID, 1, 40, "", dto.ProxyListFilters{})
	if total != 1 || len(rows) != 1 || rows[0].HealthKnown || rows[0].Alive || !rows[0].LatestCheck.IsZero() {
		t.Fatalf("pending health leaked: %+v total=%d", rows, total)
	}
	for _, status := range []string{"alive", "dead"} {
		_, count := GetProxyInfoPageWithFilters(a.ID, 1, 40, "", dto.ProxyListFilters{Status: status})
		if count != 0 {
			t.Fatalf("pending proxy matched %s", status)
		}
	}
	detail, err := GetProxyDetail(a.ID, proxy.ID)
	if err != nil || detail.Alive != nil {
		t.Fatal("pending detail health", detail, err)
	}
	routes, err := aliveProxiesForProtocol(db, a.ID, 1, nil, "", nil)
	if err != nil || len(routes) != 0 {
		t.Fatal("pending rotation candidates", routes, err)
	}
	if deleted, err := PruneObsoleteCheckerEvidence(ctx, 100); err != nil || deleted != 0 {
		t.Fatal("pending refresh pruned valid evidence", deleted, err)
	}
	// A statistics flush while pending can rebuild indexes as unknown. A
	// successful refresh must restore still-applicable evidence atomically.
	if err := refreshUserProxyFilterIndexes(db, "WHERE up.workspace_id = ?", a.ID); err != nil {
		t.Fatal(err)
	}
	refreshed, err := LoadCheckerWorkspace(ctx, a.ID)
	if err != nil || refreshed.Value.CheckerDirty {
		t.Fatal(refreshed, err)
	}
	rows, total = GetProxyInfoPageWithFilters(a.ID, 1, 40, "", dto.ProxyListFilters{})
	if total != 1 || len(rows) != 1 || !rows[0].HealthKnown || !rows[0].Alive {
		t.Fatal("refresh did not restore evidence", rows, total)
	}
}
