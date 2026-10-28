package database

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"magpie/internal/domain"

	"gorm.io/gorm"
)

type readModelProjectionPair struct {
	WorkspaceID uint
	ProxyID     uint64
}

// A physical route can belong to many workspaces. Keep the expanded suffix
// between ticks so each query has a pair budget and committed prefixes survive
// a later timeout. New invalidations remain in readModelDirtyProxyIDs.
type readModelProjectionBatch struct {
	proxyIDs []uint64
	pairs    []readModelProjectionPair
	next     int
	window   readModelProjectionWindow

	// Keep a timed-out interval's original end until every pair has committed.
	// The batch window controls later growth without forgetting that deadline.
	retryEnd   int
	retryWidth int
}

var readModelPendingBatch *readModelProjectionBatch // protected by readModelDirtyMu

// The projection worker calls these methods under readModelDirtyMu. A failed
// statement never advances next, including a commit whose response was lost.
func (b *readModelProjectionBatch) prefixEnd(pairBudget int) int {
	end := min(b.next+b.window.pairLimit(pairBudget), len(b.pairs))
	if b.retryEnd > b.next && b.retryWidth > 0 {
		end = min(end, b.retryEnd, b.next+b.retryWidth)
	}
	return end
}

func (b *readModelProjectionBatch) retainDeadlinePrefix(parentErr, attemptErr, operationErr error, start, end int) bool {
	// A permanent SQL error can race the child deadline. Require the operation
	// itself to report that deadline as well; unknown errors keep their width.
	if parentErr != nil || attemptErr != context.DeadlineExceeded || !errors.Is(operationErr, context.DeadlineExceeded) || start != b.next || end > len(b.pairs) || end-start <= 1 {
		return false
	}
	if b.retryEnd <= start {
		b.retryEnd = end
	}
	b.retryWidth = max(1, (end-start)/2)
	b.window.deadline(end - start)
	return true
}

func (b *readModelProjectionBatch) commitPrefix(end int) {
	b.next = end
	if end >= b.retryEnd {
		b.retryEnd, b.retryWidth = 0, 0
	}
}

func readModelProjectionPairBudget() int {
	// Each pair uses two extended-protocol parameters. Keep the existing SQL
	// chunk ceiling even if an operator raises the queue-drain setting.
	return min(deleteChunkSize, readModelRefreshBatchSize())
}

func readModelProxyWorkPending() bool {
	readModelDirtyMu.Lock()
	defer readModelDirtyMu.Unlock()
	return len(readModelDirtyProxyIDs) > 0 || len(readModelDirtyRound) > 0 || readModelPendingBatch != nil
}

func loadReadModelProjectionPairs(tx *gorm.DB, proxyIDs []uint64) ([]readModelProjectionPair, error) {
	var pairs []readModelProjectionPair
	// Keep shared owners together inside the pair limit. The projection reads
	// metadata once per distinct physical route in each statement; workspace-first
	// prefixes repeat those lookups for every owner. The upsert itself still sorts
	// by workspace and route before acquiring projection row locks.
	if err := tx.Table("user_proxies").Select("workspace_id, proxy_id").
		Where("proxy_id IN ?", proxyIDs).Order("proxy_id, workspace_id").Scan(&pairs).Error; err != nil {
		return nil, fmt.Errorf("read model: discover proxy owner pairs: %w", err)
	}
	return pairs, nil
}

func refreshWorkspaceProxyFilterPairs(tx *gorm.DB, pairs []readModelProjectionPair) error {
	if tx == nil || len(pairs) == 0 {
		return nil
	}
	if !tx.Migrator().HasTable(&domain.WorkspaceProxyFilterIndex{}) {
		return tx.Statement.Context.Err()
	}
	values := make([]string, len(pairs))
	args := make([]any, 0, 2*len(pairs))
	for i, pair := range pairs {
		values[i] = "(?::bigint, ?::bigint)"
		args = append(args, pair.WorkspaceID, pair.ProxyID)
	}
	// Parameterize ownership as well as evidence/metadata. A tuple IN predicate
	// can become a global ownership scan for a large, sparse set of pairs.
	scope := `SELECT up.workspace_id, up.proxy_id, up.state, up.pause_reason
FROM (VALUES ` + strings.Join(values, ",") + `) requested(workspace_id, proxy_id)
JOIN LATERAL (
	SELECT up.workspace_id, up.proxy_id, up.state, up.pause_reason
	FROM user_proxies up
	WHERE up.workspace_id = requested.workspace_id AND up.proxy_id = requested.proxy_id
	OFFSET 0
) up ON TRUE`
	return refreshWorkspaceProxyFilterScope(tx, scope, args...)
}

func refreshAllWorkspaceProxyFilterIndexesBounded(db *gorm.DB) error {
	if db == nil || !db.Migrator().HasTable(&domain.WorkspaceProxyFilterIndex{}) {
		return nil
	}
	var after readModelProjectionPair
	for {
		if err := db.Statement.Context.Err(); err != nil {
			return err
		}
		var pairs []readModelProjectionPair
		if err := db.Table("user_proxies").Select("workspace_id, proxy_id").
			Where("(workspace_id, proxy_id) > (?, ?)", after.WorkspaceID, after.ProxyID).
			Order("workspace_id, proxy_id").Limit(readModelProjectionPairBudget()).Scan(&pairs).Error; err != nil {
			return fmt.Errorf("read model: enumerate rebuild pairs: %w", err)
		}
		if len(pairs) == 0 {
			return db.Statement.Context.Err()
		}
		if err := refreshWorkspaceProxyFilterPairs(db, pairs); err != nil {
			return err
		}
		after = pairs[len(pairs)-1]
	}
}

// RefreshReadModelIndexes repairs derived state using production projection
// queries. Run with writers stopped after an unclean shutdown; in-memory dirty
// IDs cannot survive that shutdown. A canceled or partial rebuild returns an
// error and can be rerun without changing inventory, credentials or checker keys.
func RefreshReadModelIndexes(ctx context.Context) error {
	if DB == nil {
		return fmt.Errorf("database not initialised")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	db := DB.WithContext(ctx)
	for _, model := range []any{&domain.WorkspaceProxyFilterIndex{}, &domain.WorkspaceScrapeSourceStat{}} {
		if !db.Migrator().HasTable(model) {
			if err := ctx.Err(); err != nil {
				return err
			}
			return fmt.Errorf("read model: required repair table for %T is missing", model)
		}
	}
	for _, repair := range []struct {
		derived, ownership, key string
		budget                  int
	}{
		{"user_proxy_filter_indexes", "user_proxies", "proxy_id", readModelProjectionPairBudget()},
		{"user_scrape_source_stats", "user_scrape_site", "scrape_site_id", 100},
	} {
		if err := removeOrphanReadModelRows(db, repair.derived, repair.ownership, repair.key, repair.budget); err != nil {
			return err
		}
	}
	if err := refreshAllWorkspaceProxyFilterIndexesBounded(db); err != nil {
		return err
	}
	var after sourceStatsKey
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		var pairs []sourceStatsKey
		if err := db.Table("user_scrape_site").Select("workspace_id, scrape_site_id").
			Where("(workspace_id, scrape_site_id) > (?, ?)", after.WorkspaceID, after.ScrapeSiteID).
			Order("workspace_id, scrape_site_id").Limit(100).Scan(&pairs).Error; err != nil {
			return fmt.Errorf("read model: enumerate source rebuild pairs: %w", err)
		}
		if len(pairs) == 0 {
			return ctx.Err()
		}
		values := make([][]any, len(pairs))
		for i, pair := range pairs {
			values[i] = []any{pair.WorkspaceID, pair.ScrapeSiteID}
		}
		if err := refreshWorkspaceScrapeSourceStats(db, "WHERE (uss.workspace_id, ss.id) IN ?", values); err != nil {
			return err
		}
		after = pairs[len(pairs)-1]
	}
}

type readModelRepairKey struct {
	WorkspaceID uint
	RowID       uint64
}

func removeOrphanReadModelRows(db *gorm.DB, derived, ownership, key string, budget int) error {
	var after readModelRepairKey
	for {
		if err := db.Statement.Context.Err(); err != nil {
			return err
		}
		var keys []readModelRepairKey
		// Page the derived primary key before probing ownership. A global
		// anti-join followed by LIMIT would rescan the full inventory per page.
		if err := db.Table(derived).Select("workspace_id, "+key+" AS row_id").
			Where("(workspace_id, "+key+") > (?, ?)", after.WorkspaceID, after.RowID).
			Order("workspace_id, " + key).Limit(budget).Scan(&keys).Error; err != nil {
			return fmt.Errorf("read model: enumerate orphan candidates in %s: %w", derived, err)
		}
		if len(keys) == 0 {
			return db.Statement.Context.Err()
		}
		if err := deleteOrphanReadModelRows(db, derived, ownership, key, keys); err != nil {
			return err
		}
		after = keys[len(keys)-1]
	}
}

func deleteOrphanReadModelRows(db *gorm.DB, derived, ownership, key string, keys []readModelRepairKey) error {
	if len(keys) == 0 {
		return nil
	}
	values := make([]string, len(keys))
	args := make([]any, 0, 2*len(keys))
	for i, pair := range keys {
		values[i] = "(?::bigint, ?::bigint)"
		args = append(args, pair.WorkspaceID, pair.RowID)
	}
	// Resolve both keys with exact primary-key probes. The outer TID lookup
	// also stays bounded when every selected row still has valid ownership.
	// CTIDs are consumed in this same statement, never persisted across pages.
	query := `WITH orphan_rows AS MATERIALIZED (
	SELECT existing_row.ctid
	FROM (VALUES ` + strings.Join(values, ",") + `) requested(workspace_id, row_id)
	JOIN LATERAL (
		SELECT derived_row.ctid FROM ` + derived + ` derived_row
		WHERE derived_row.workspace_id=requested.workspace_id AND derived_row.` + key + `=requested.row_id
		OFFSET 0
	) existing_row ON TRUE
	LEFT JOIN LATERAL (
		SELECT TRUE AS present FROM ` + ownership + ` owner_row
		WHERE owner_row.workspace_id=requested.workspace_id AND owner_row.` + key + `=requested.row_id
		OFFSET 0
	) current_owner ON TRUE
	WHERE current_owner.present IS NULL
)
DELETE FROM ` + derived + ` WHERE ctid=ANY(ARRAY(SELECT ctid FROM orphan_rows))`
	if err := db.Exec(query, args...).Error; err != nil {
		return fmt.Errorf("read model: remove orphan rows from %s: %w", derived, err)
	}
	return db.Statement.Context.Err()
}
