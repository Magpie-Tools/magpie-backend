package database

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"magpie/internal/domain"
	"magpie/internal/support"
)

func TestBackendLongevityMonthlyPartitionHorizon(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	t.Setenv(envProxyStatisticsPartitionPrecreateMonths, "2")
	t.Setenv(envProxyStatisticsPartitionPastMonths, "2")
	// A DEFAULT partition for a same-named table elsewhere must not suppress
	// this table's own DEFAULT partition during startup or maintenance.
	otherSchema := fmt.Sprintf("longevity_other_partition_%d", time.Now().UnixNano())
	if err := db.Exec("CREATE SCHEMA " + quoteIdentifier(otherSchema)).Error; err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Exec("DROP SCHEMA " + quoteIdentifier(otherSchema) + " CASCADE").Error })
	otherParent := quoteIdentifier(otherSchema) + ".proxy_statistics"
	if err := db.Exec("CREATE TABLE " + otherParent + " (created_at timestamptz NOT NULL) PARTITION BY RANGE(created_at)").Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("CREATE TABLE " + quoteIdentifier(otherSchema) + ".other_default PARTITION OF " + otherParent + " DEFAULT").Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("DROP TABLE proxy_statistics CASCADE").Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("CREATE TABLE proxy_statistics (id bigint GENERATED ALWAYS AS IDENTITY, created_at timestamptz NOT NULL, PRIMARY KEY(id,created_at)) PARTITION BY RANGE(created_at)").Error; err != nil {
		t.Fatal(err)
	}
	started := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	if err := ensureProxyStatisticsMonthlyPartitions(db, started); err != nil {
		t.Fatal(err)
	}
	later := started.AddDate(0, 3, 0)
	if err := db.Exec("INSERT INTO proxy_statistics(created_at) VALUES (?)", later).Error; err != nil {
		t.Fatal(err)
	}
	var relation string
	if err := db.Raw("SELECT tableoid::regclass::text FROM proxy_statistics WHERE created_at = ?", later).Scan(&relation).Error; err != nil {
		t.Fatal(err)
	}
	t.Logf("migration_date=%s after_three_months=%s physical_partition=%s", started.Format(time.DateOnly), later.Format(time.DateOnly), relation)
	if err := ensureProxyStatisticsMonthlyPartitions(db, later); err != nil {
		t.Fatalf("monthly maintenance cannot catch up once future data reached DEFAULT: %v", err)
	}
	if err := db.Raw("SELECT tableoid::regclass::text FROM proxy_statistics WHERE created_at = ?", later).Scan(&relation).Error; err != nil {
		t.Fatal(err)
	}
	if relation != "proxy_statistics_2027_01" {
		t.Fatalf("recovered partition = %s", relation)
	}
	t.Setenv("DB_AUTO_MIGRATE", "false")
	if err := MaintainProxyStatisticsPartitions(context.Background(), later.AddDate(0, 1, 0)); err != nil {
		t.Fatal(err)
	}
}

func TestBackendLongevityEventLedgerStorage(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	const total = 200000
	for start := 0; start < total; start += 5000 {
		stats := make([]domain.ProxyStatistic, 5000)
		for i := range stats {
			stats[i].EventStream = "magpie:proxy_statistics:stream"
			stats[i].EventID = fmt.Sprintf("%d-0", 1700000000000+start+i)
		}
		accepted, err := acceptProxyStatisticEvents(db, stats)
		if err != nil || len(accepted) != len(stats) {
			t.Fatalf("accept events: %d %v", len(accepted), err)
		}
	}
	var count, bytes int64
	if err := db.Model(&domain.ProxyStatisticEvent{}).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Raw("SELECT pg_total_relation_size('proxy_statistic_events'::regclass)").Scan(&bytes).Error; err != nil {
		t.Fatal(err)
	}
	t.Logf("ledger_rows=%d relation_bytes=%d bytes_per_event=%.1f", count, bytes, float64(bytes)/float64(count))
	if count != total {
		t.Fatalf("ledger count=%d", count)
	}
}

func TestBackendLongevityDeletedProxyDailyCounters(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	a, _, proxy, judge := seedCheckerEvidenceTest(t, db)
	stats := []domain.ProxyStatistic{}
	for day := 0; day < 180; day++ {
		stats = append(stats, domain.ProxyStatistic{ProxyID: proxy.ID, ProtocolID: 1, JudgeID: judge.ID, Alive: true, WorkspaceIDs: []uint{a.ID}, CreatedAt: time.Now().UTC().AddDate(0, 0, -day-1)})
	}
	if err := InsertProxyStatistics(context.Background(), stats, 500); err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("DELETE FROM user_proxies WHERE proxy_id = ?", proxy.ID).Error; err != nil {
		t.Fatal(err)
	}
	deleted, err := DeleteOrphanProxies(context.Background())
	if err != nil || deleted != 1 {
		t.Fatalf("production orphan cleanup: removed=%d err=%v", deleted, err)
	}
	var rows int64
	if err := db.Model(&domain.ProxyDailyCheck{}).Where("proxy_id = ?", proxy.ID).Count(&rows).Error; err != nil {
		t.Fatal(err)
	}
	t.Logf("deleted_proxy=%d orphan_daily_counter_rows=%d", proxy.ID, rows)
	if rows != 0 {
		t.Fatalf("deleted route still has %d daily counters with no owning route", rows)
	}
}

func TestBackendLongevityExpiredDNSCacheChurn(t *testing.T) {
	saved := dnsCache
	dnsCache = support.NewExpiryCache[string, []string](20_000)
	t.Cleanup(func() { dnsCache = saved })
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	const total = 200000
	for i := 0; i < total; i++ {
		ip := fmt.Sprintf("10.%d.%d.%d", i>>16, i>>8&255, i&255)
		dnsCache.Set(ip, []string{fmt.Sprintf("customer-%d.provider.example", i)}, time.Now().Add(-180*24*time.Hour))
	}
	dnsCache.Set("192.0.2.1", []string{"active.example"}, time.Now().Add(time.Hour))
	PruneDNSCache(time.Now())
	_ = getCachedDNS("192.0.2.1")
	runtime.GC()
	runtime.ReadMemStats(&after)
	count := dnsCache.Len()
	t.Logf("expired_ips=%d retained_keys=%d retained_heap_delta_bytes=%d", total, count, int64(after.HeapAlloc)-int64(before.HeapAlloc))
	if count > 1 {
		t.Fatalf("DNS cache retains %d expired, unused entries", count-1)
	}
}

func TestBackendLongevityAbandonedEmailRetention(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	old := time.Now().UTC().Add(-180 * 24 * time.Hour)
	rows := []domain.EmailOutbox{{Kind: "password_reset", ToAddress: "fixture@example.invalid", Subject: "fixture", Body: strings.Repeat("x", 8192), Status: domain.EmailOutboxStatusAbandoned, CreatedAt: old, UpdatedAt: old}, {Kind: "password_reset", ToAddress: "fixture@example.invalid", Subject: "fixture", Body: "sent", Status: domain.EmailOutboxStatusSent, SentAt: &old, CreatedAt: old, UpdatedAt: old}}
	if err := db.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	deleted, err := DeleteSentEmailOutbox(context.Background(), time.Now().UTC().Add(-24*time.Hour), 500)
	if err != nil {
		t.Fatal(err)
	}
	var abandoned int64
	if err := db.Model(&domain.EmailOutbox{}).Where("status = ?", domain.EmailOutboxStatusAbandoned).Count(&abandoned).Error; err != nil {
		t.Fatal(err)
	}
	t.Logf("old_sent_messages_removed=%d old_abandoned_messages_retained=%d", deleted, abandoned)
	if abandoned != 0 {
		t.Fatal("terminal failed mail is retained beyond outbox retention")
	}
}
