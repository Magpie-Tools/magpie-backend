package database

import (
	"context"
	"fmt"
	"testing"
	"time"

	"gorm.io/gorm"
)

func readModelNoopFilterRows(t *testing.T, db *gorm.DB, proxyID uint64) string {
	t.Helper()
	var rows string
	if err := db.Raw("SELECT jsonb_agg(to_jsonb(u) - 'updated_at' ORDER BY workspace_id)::text FROM user_proxy_filter_indexes u WHERE proxy_id = ?", proxyID).Scan(&rows).Error; err != nil {
		t.Fatal(err)
	}
	return rows
}

func TestReadModelNoopFilterRepairsEveryContentFieldPostgres(t *testing.T) {
	db, proxyID, owners := seedReadModelFanoutTest(t)
	pairs := make([]readModelProjectionPair, len(owners))
	for i, owner := range owners {
		pairs[i] = readModelProjectionPair{WorkspaceID: owner, ProxyID: proxyID}
	}
	if err := refreshWorkspaceProxyFilterPairs(db, pairs); err != nil {
		t.Fatal(err)
	}
	want := readModelNoopFilterRows(t, db, proxyID)
	authoritative := readModelSubprefixAuthoritativeState(t, db)
	// Corrupt one projected value at a time. Every source-derived value,
	// including NULL health/reputation and false protocol flags, must be repaired.
	changes := []struct{ column, expression string }{
		{"state", "'paused'"}, {"pause_reason", "'manual'"},
		{"host", "'stale.invalid'"}, {"ip_address", "'192.0.2.123'::inet"},
		{"port", "port + 1"}, {"country", "'stale'"}, {"country_key", "'stale'"},
		{"estimated_type", "'stale'"}, {"type_key", "'stale'"},
		{"anonymity_level", "'stale'"}, {"anonymity_key", "'stale'"},
		{"alive", "NOT alive"}, {"latest_check", "latest_check - interval '1 second'"},
		{"response_time", "response_time + 1"}, {"attempt", "attempt + 1"},
		{"health_overall", "CASE WHEN health_overall IS NULL THEN 1 ELSE NULL END"},
		{"health_http", "CASE WHEN health_http IS NULL THEN 1 ELSE NULL END"},
		{"health_https", "CASE WHEN health_https IS NULL THEN 1 ELSE NULL END"},
		{"health_socks4", "CASE WHEN health_socks4 IS NULL THEN 1 ELSE NULL END"},
		{"health_socks5", "CASE WHEN health_socks5 IS NULL THEN 1 ELSE NULL END"},
		{"alive_http", "NOT alive_http"}, {"alive_https", "NOT alive_https"},
		{"alive_socks4", "NOT alive_socks4"}, {"alive_socks5", "NOT alive_socks5"},
		{"reputation_label", "'stale'"},
		{"reputation_score", "CASE WHEN reputation_score IS NULL THEN 1 ELSE NULL END"},
	}
	for _, change := range changes {
		t.Run(change.column, func(t *testing.T) {
			if err := db.Exec(fmt.Sprintf("UPDATE user_proxy_filter_indexes SET %s = %s WHERE workspace_id = ? AND proxy_id = ?",
				change.column, change.expression), owners[0], proxyID).Error; err != nil {
				t.Fatal(err)
			}
			if got := readModelNoopFilterRows(t, db, proxyID); got == want {
				t.Fatal("fixture mutation did not change projected content")
			}
			if err := refreshWorkspaceProxyFilterPairs(db, pairs); err != nil {
				t.Fatal(err)
			}
			if got := readModelNoopFilterRows(t, db, proxyID); got != want {
				t.Fatal("refresh skipped a changed content field", change.column, got, want)
			}
		})
	}
	if got := readModelSubprefixAuthoritativeState(t, db); got != authoritative {
		t.Fatal("projection refresh changed authoritative ownership, settings or history")
	}
}

func TestReadModelNoopFilterAvoidsUnchangedRowLocksPostgres(t *testing.T) {
	db, proxyID, owners := seedReadModelFanoutTest(t)
	pair := []readModelProjectionPair{{WorkspaceID: owners[0], ProxyID: proxyID}}
	if err := refreshWorkspaceProxyFilterPairs(db, pair); err != nil {
		t.Fatal(err)
	}
	var updatedAt time.Time
	if err := db.Raw("SELECT updated_at FROM user_proxy_filter_indexes WHERE workspace_id=? AND proxy_id=?",
		owners[0], proxyID).Scan(&updatedAt).Error; err != nil {
		t.Fatal(err)
	}
	blocker := db.Begin()
	if blocker.Error != nil {
		t.Fatal(blocker.Error)
	}
	defer blocker.Rollback()
	if err := blocker.Exec("SELECT 1 FROM user_proxy_filter_indexes WHERE workspace_id=? AND proxy_id=? FOR UPDATE",
		owners[0], proxyID).Error; err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := refreshWorkspaceProxyFilterPairs(db.WithContext(ctx), pair); err != nil {
		t.Fatal("an unchanged refresh waited for a projection row lock", err)
	}
	var after time.Time
	if err := db.Raw("SELECT updated_at FROM user_proxy_filter_indexes WHERE workspace_id=? AND proxy_id=?",
		owners[0], proxyID).Scan(&after).Error; err != nil || !after.Equal(updatedAt) {
		t.Fatal("an unchanged refresh moved UpdatedAt", updatedAt, after, err)
	}
}

func TestReadModelNoopFilterChangedRowStillLocksAndRetriesPostgres(t *testing.T) {
	db, proxyID, owners := seedReadModelFanoutTest(t)
	pair := []readModelProjectionPair{{WorkspaceID: owners[0], ProxyID: proxyID}}
	if err := refreshWorkspaceProxyFilterPairs(db, pair); err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("UPDATE proxies SET country='DE' WHERE id=?", proxyID).Error; err != nil {
		t.Fatal(err)
	}
	blocker := db.Begin()
	if blocker.Error != nil {
		t.Fatal(blocker.Error)
	}
	defer blocker.Rollback()
	if err := blocker.Exec("SELECT 1 FROM user_proxy_filter_indexes WHERE workspace_id=? AND proxy_id=? FOR UPDATE",
		owners[0], proxyID).Error; err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if err := refreshWorkspaceProxyFilterPairs(db.WithContext(ctx), pair); err == nil {
		t.Fatal("a changed row bypassed its required conflict lock")
	}
	var country string
	if err := db.Raw("SELECT country FROM user_proxy_filter_indexes WHERE workspace_id=? AND proxy_id=?",
		owners[0], proxyID).Scan(&country).Error; err != nil || country == "DE" {
		t.Fatal("canceled refresh committed changed content", country, err)
	}
	if err := blocker.Rollback().Error; err != nil {
		t.Fatal(err)
	}
	if err := refreshWorkspaceProxyFilterPairs(db, pair); err != nil {
		t.Fatal(err)
	}
	if err := db.Raw("SELECT country FROM user_proxy_filter_indexes WHERE workspace_id=? AND proxy_id=?",
		owners[0], proxyID).Scan(&country).Error; err != nil || country != "DE" {
		t.Fatal("retry lost changed authoritative content", country, err)
	}
}
