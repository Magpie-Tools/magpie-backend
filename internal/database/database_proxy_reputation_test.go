package database

import (
	"context"
	"testing"
	"time"

	"magpie/internal/domain"

	"gorm.io/gorm"
)

func TestRecalculateProxyReputationsKeepsOriginalDatabase(t *testing.T) {
	db := setupRotatingProxyTestDB(t)
	if err := db.AutoMigrate(&domain.AbuseIPDBCheck{}); err != nil {
		t.Fatal(err)
	}
	_, _, proxy, judge := seedCheckerEvidenceTest(t, db)
	stat := domain.ProxyStatistic{ProxyID: proxy.ID, JudgeID: judge.ID, ProtocolID: 1,
		Alive: true, ResponseTime: 100, CreatedAt: time.Now().UTC()}
	if err := db.Create(&stat).Error; err != nil {
		t.Fatal(err)
	}

	// Reproduce teardown replacing the global after the job's first query.
	// Every later read and the final upsert must still use the original DB.
	const callback = "test:replace_reputation_database"
	if err := db.Callback().Row().After("gorm:row").Register(callback, func(_ *gorm.DB) {
		DB = nil
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Callback().Row().Remove(callback) })
	if err := RecalculateProxyReputations(context.Background(), []uint64{proxy.ID}); err != nil {
		t.Fatal(err)
	}
	if DB != nil {
		t.Fatal("test did not replace the global database handle")
	}
	var rows []domain.ProxyReputation
	if err := db.Where("proxy_id = ?", proxy.ID).Order("kind").Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].Kind != "http" || rows[1].Kind != domain.ProxyReputationKindOverall {
		t.Fatalf("reputations were not written to the original database: %+v", rows)
	}
}

func TestDedupeProxyReputationsForUpsert_KeepsLastEntryPerProxyKind(t *testing.T) {
	in := []domain.ProxyReputation{
		{ProxyID: 1, Kind: "http", Score: 10},
		{ProxyID: 2, Kind: "overall", Score: 11},
		{ProxyID: 1, Kind: "HTTP", Score: 12},
		{ProxyID: 1, Kind: " https ", Score: 20},
		{ProxyID: 1, Kind: "https", Score: 21},
	}

	got := dedupeProxyReputationsForUpsert(in)
	if len(got) != 3 {
		t.Fatalf("dedupe length = %d, want 3", len(got))
	}

	if got[0].ProxyID != 1 || got[0].Kind != "HTTP" || got[0].Score != 12 {
		t.Fatalf("unexpected deduped http entry: %#v", got[0])
	}
	if got[1].ProxyID != 2 || got[1].Kind != "overall" || got[1].Score != 11 {
		t.Fatalf("unexpected deduped overall entry: %#v", got[1])
	}
	if got[2].ProxyID != 1 || got[2].Kind != "https" || got[2].Score != 21 {
		t.Fatalf("unexpected deduped https entry: %#v", got[2])
	}
}

func TestProxyReputationAdvisoryLockKey_DeterministicAndNormalized(t *testing.T) {
	a := proxyReputationAdvisoryLockKey(42, " HTTPS ")
	b := proxyReputationAdvisoryLockKey(42, "https")
	c := proxyReputationAdvisoryLockKey(43, "https")

	if a != b {
		t.Fatalf("expected normalized kinds to produce same lock key, got %d and %d", a, b)
	}
	if a == c {
		t.Fatalf("expected different proxy ids to produce different lock keys, got %d", a)
	}
}
