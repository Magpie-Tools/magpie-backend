package readmodel

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"magpie/internal/domain"
	"magpie/internal/evidence"
	"magpie/internal/platform/pg"
	"magpie/internal/platform/schema"
	"magpie/internal/support"

	"github.com/charmbracelet/log"
	"gorm.io/gorm"
)

const (
	defaultReadModelRefreshIntervalSeconds = 5
	defaultReadModelRefreshBatchSize       = 5000
	defaultReadModelRefreshMaxBatches      = 4
	defaultReadModelRefreshTimeoutSeconds  = 30
	readModelDrainRetryDelay               = 250 * time.Millisecond
	readModelRefreshStageOwnerDiscovery    = 1
	readModelRefreshStageProjection        = 2
	readModelRefreshStageSourceLookup      = 3
	envReadModelRefreshIntervalSeconds     = "READ_MODEL_REFRESH_INTERVAL_SECONDS"
	envReadModelRefreshBatchSize           = "READ_MODEL_REFRESH_BATCH_SIZE"
	envReadModelRefreshMaxBatches          = "READ_MODEL_REFRESH_MAX_BATCHES"
	envReadModelRefreshTimeoutSeconds      = "READ_MODEL_REFRESH_TIMEOUT_SECONDS"
)

var (
	readModelDirtyMu       sync.Mutex
	readModelFlushMu       sync.Mutex
	readModelDirtyProxyIDs = make(map[uint64]struct{})
	readModelDirtyRound    []uint64 // remaining sorted IDs from one closed dirty set
	sourceStatsDirtyMu     sync.Mutex
	sourceStatsDirty       = make(map[sourceStatsKey]struct{})
)

type sourceStatsKey struct {
	WorkspaceID  uint
	ScrapeSiteID uint64
}

func EnsureReadModelBackfill(db *gorm.DB) error {
	if db == nil || !pg.IsPostgresDialect(db) {
		return nil
	}

	if err := schema.EnsureReadModelSchema(db); err != nil {
		return err
	}

	if db.Migrator().HasTable(&domain.WorkspaceProxyFilterIndex{}) {
		var count int64
		if err := db.Model(&domain.WorkspaceProxyFilterIndex{}).Count(&count).Error; err != nil {
			return fmt.Errorf("read model: count proxy filter index: %w", err)
		}
		var missingHosts int64
		if count > 0 {
			if err := db.Model(&domain.WorkspaceProxyFilterIndex{}).
				Where("host IS NULL OR BTRIM(host) = ''").
				Count(&missingHosts).Error; err != nil {
				return fmt.Errorf("read model: count missing proxy hosts: %w", err)
			}
		}
		if count == 0 || missingHosts > 0 {
			if err := refreshAllWorkspaceProxyFilterIndexes(db); err != nil {
				return err
			}
			log.Info("Proxy filter read model backfilled")
		}
	}

	if db.Migrator().HasTable(&domain.WorkspaceScrapeSourceStat{}) {
		var count int64
		if err := db.Model(&domain.WorkspaceScrapeSourceStat{}).Count(&count).Error; err != nil {
			return fmt.Errorf("read model: count scrape-source stats: %w", err)
		}
		if count == 0 {
			if err := refreshAllWorkspaceScrapeSourceStats(db); err != nil {
				return err
			}
			log.Info("Scrape-source stats read model backfilled")
		}
	}

	return finalizeReadModelHostSchema(db)
}

func finalizeReadModelHostSchema(db *gorm.DB) error {
	if db == nil || !pg.IsPostgresDialect(db) || !db.Migrator().HasTable(&domain.WorkspaceProxyFilterIndex{}) {
		return nil
	}

	var missing int64
	if err := db.Model(&domain.WorkspaceProxyFilterIndex{}).Where("host IS NULL OR BTRIM(host) = ''").Count(&missing).Error; err != nil {
		return fmt.Errorf("read model: verify proxy hosts: %w", err)
	}
	if missing > 0 {
		return fmt.Errorf("read model: %d proxy rows have no host", missing)
	}

	stmts := []string{
		`DROP INDEX IF EXISTS idx_user_proxy_filter_ip_int`,
		`ALTER TABLE user_proxy_filter_indexes ALTER COLUMN host SET NOT NULL`,
		`ALTER TABLE user_proxy_filter_indexes ALTER COLUMN ip_address DROP NOT NULL`,
		`ALTER TABLE user_proxy_filter_indexes DROP COLUMN IF EXISTS ip`,
		`ALTER TABLE user_proxy_filter_indexes DROP COLUMN IF EXISTS ip_int`,
	}
	for _, stmt := range stmts {
		if err := db.Exec(stmt).Error; err != nil {
			return fmt.Errorf("read model host schema: %w", err)
		}
	}

	return nil
}

func QueueReadModelRefreshForProxyIDs(proxyIDs []uint64) {
	if len(proxyIDs) == 0 {
		return
	}

	readModelDirtyMu.Lock()
	for _, proxyID := range proxyIDs {
		if proxyID != 0 {
			// An unseen ID in the closed round will read authoritative state
			// after this committed invalidation. Coalesce it there. Popped IDs
			// are in flight and must retain another notification in the map.
			next := sort.Search(len(readModelDirtyRound), func(i int) bool { return readModelDirtyRound[i] >= proxyID })
			if next < len(readModelDirtyRound) && readModelDirtyRound[next] == proxyID {
				continue
			}
			readModelDirtyProxyIDs[proxyID] = struct{}{}
		}
	}
	readModelDirtyMu.Unlock()

}

func StartReadModelRefreshRoutine(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}

	interval := readModelRefreshInterval()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var sourcesWG sync.WaitGroup
	sourcesWG.Add(1)
	go func() {
		defer sourcesWG.Done()
		runSourceStatsRefresh(ctx)
	}()

	for {
		select {
		case <-ctx.Done():
			// A canceled source query can requeue its batch. Join it before
			// the proxy drain generates the final source invalidations.
			sourcesWG.Wait()
			drainReadModelRefreshes(context.Background())
			return
		case <-ticker.C:
			flushReadModelDirtyProxyIDs(ctx, false)
		}
	}
}

func drainReadModelRefreshes(ctx context.Context) {
	for {
		flushReadModelDirtyProxyIDs(ctx, true)
		proxyPending := readModelProxyWorkPending()
		if !proxyPending {
			for flushDirtySourceStats(ctx) {
			}
		}
		sourceStatsDirtyMu.Lock()
		sourcePending := len(sourceStatsDirty) > 0
		sourceStatsDirtyMu.Unlock()
		if !proxyPending && !sourcePending {
			return
		}
		// A bounded query can fail or time out and requeue its batch. Keep the
		// worker active so Runtime.Shutdown cannot report a successful drain.
		// Its outer deadline remains responsible for reporting unresolved work.
		timer := time.NewTimer(readModelDrainRetryDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func flushReadModelDirtyProxyIDs(ctx context.Context, drainAll bool) {
	// Runtime has one projection worker. Serialize direct invocations as well
	// so two flushes cannot consume the same retained prefix concurrently.
	readModelFlushMu.Lock()
	defer readModelFlushMu.Unlock()
	if pg.DB == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	maxBatches := readModelRefreshMaxBatches()
	for processed := 0; drainAll || processed < maxBatches; processed++ {
		refreshTimeout := readModelRefreshTimeout()
		batchCtx, cancel := context.WithTimeout(ctx, refreshTimeout)
		db := pg.DB.WithContext(batchCtx)
		var ownerDuration time.Duration
		readModelDirtyMu.Lock()
		batch := readModelPendingBatch
		readModelDirtyMu.Unlock()
		if batch == nil {
			proxyIDs := popDirtyReadModelProxyIDs(readModelProjectionPairBudget())
			if len(proxyIDs) == 0 {
				cancel()
				return
			}
			ownerStart := time.Now()
			pairs, err := loadReadModelProjectionPairs(db, proxyIDs)
			ownerDuration = time.Since(ownerStart)
			if err == nil {
				err = batchCtx.Err()
			}
			if err != nil {
				cancel()
				requeueDirtyReadModelProxyIDs(proxyIDs)
				log.Warn("read model refresh: owner discovery failed", "error", err, "count", len(proxyIDs),
					"stage", readModelRefreshStageOwnerDiscovery, "cursor", 0, "attempted_pairs", 0, "owner_ms", ownerDuration.Milliseconds())
				return
			}
			batch = &readModelProjectionBatch{proxyIDs: proxyIDs, pairs: pairs}
			readModelDirtyMu.Lock()
			readModelPendingBatch = batch
			readModelDirtyMu.Unlock()
		}
		start := time.Now()
		readModelDirtyMu.Lock()
		prefixStart := batch.next
		end := batch.prefixEnd(readModelProjectionPairBudget())
		readModelDirtyMu.Unlock()
		err := refreshWorkspaceProxyFilterPairs(db, batch.pairs[prefixStart:end])
		projectionDuration := time.Since(start)
		failureMessage := "read model refresh: proxy filter index failed"
		if err == nil {
			err = batchCtx.Err()
			failureMessage = "read model refresh: batch canceled"
		}
		if err != nil {
			readModelDirtyMu.Lock()
			batch.window.failed()
			batch.retainDeadlinePrefix(ctx.Err(), batchCtx.Err(), err, prefixStart, end)
			retryEnd, retryWidth := batch.retryEnd, batch.retryWidth
			readModelDirtyMu.Unlock()
			cancel()
			log.Warn(failureMessage, "error", err, "count", end-prefixStart,
				"stage", readModelRefreshStageProjection, "cursor", prefixStart, "attempted_pairs", end-prefixStart,
				"owner_pairs", len(batch.pairs), "retry_end", retryEnd, "retry_pair_limit", retryWidth,
				"owner_ms", ownerDuration.Milliseconds(), "projection_ms", projectionDuration.Milliseconds())
			return
		}
		readModelDirtyMu.Lock()
		batch.commitPrefix(end)
		if batch.retryEnd == 0 {
			batch.window.committed(end-prefixStart, projectionDuration, refreshTimeout, readModelProjectionPairBudget())
		}
		retryEnd, retryWidth := batch.retryEnd, batch.retryWidth
		readModelDirtyMu.Unlock()
		var sourceDuration time.Duration
		if end == len(batch.pairs) {
			sourceStart := time.Now()
			err := refreshWorkspaceScrapeSourceStatsForProxyIDs(db, batch.proxyIDs)
			sourceDuration = time.Since(sourceStart)
			if err != nil {
				cancel()
				log.Warn("read model refresh: scrape-source stats failed", "error", err, "count", len(batch.proxyIDs),
					"stage", readModelRefreshStageSourceLookup, "cursor", end, "attempted_pairs", 0,
					"owner_pairs", len(batch.pairs), "retry_end", retryEnd, "retry_pair_limit", retryWidth,
					"owner_ms", ownerDuration.Milliseconds(), "projection_ms", projectionDuration.Milliseconds(), "source_ms", sourceDuration.Milliseconds())
				return
			}
			if err := batchCtx.Err(); err != nil {
				cancel()
				log.Warn("read model refresh: source lookup canceled", "error", err, "count", len(batch.proxyIDs),
					"stage", readModelRefreshStageSourceLookup, "cursor", end, "attempted_pairs", 0, "owner_pairs", len(batch.pairs),
					"owner_ms", ownerDuration.Milliseconds(), "projection_ms", projectionDuration.Milliseconds(), "source_ms", sourceDuration.Milliseconds())
				return
			}
			readModelDirtyMu.Lock()
			readModelPendingBatch = nil
			readModelDirtyMu.Unlock()
		}
		cancel()
		stage := readModelRefreshStageProjection
		if end == prefixStart {
			stage = readModelRefreshStageSourceLookup
		}
		log.Debug("Read models refreshed", "pair_count", end-prefixStart, "duration", time.Since(start),
			"stage", stage, "cursor", end, "attempted_pairs", end-prefixStart,
			"owner_pairs", len(batch.pairs), "retry_end", retryEnd, "retry_pair_limit", retryWidth,
			"owner_ms", ownerDuration.Milliseconds(), "projection_ms", projectionDuration.Milliseconds(), "source_ms", sourceDuration.Milliseconds())
	}
}

func popDirtyReadModelProxyIDs(limit int) []uint64 {
	if limit <= 0 {
		limit = defaultReadModelRefreshBatchSize
	}

	readModelDirtyMu.Lock()
	defer readModelDirtyMu.Unlock()

	if len(readModelDirtyRound) == 0 {
		if len(readModelDirtyProxyIDs) == 0 {
			return nil
		}
		// Drain a closed set in route order so consecutive statements can reuse
		// nearby ownership/projection index pages. Invalidations outside its
		// unseen suffix stay in the map, including changes to an in-flight route.
		readModelDirtyRound = make([]uint64, 0, len(readModelDirtyProxyIDs))
		for proxyID := range readModelDirtyProxyIDs {
			readModelDirtyRound = append(readModelDirtyRound, proxyID)
		}
		clear(readModelDirtyProxyIDs)
		sort.Slice(readModelDirtyRound, func(i, j int) bool { return readModelDirtyRound[i] < readModelDirtyRound[j] })
	}
	count := min(limit, len(readModelDirtyRound))
	// The retained owner batch must not keep an exhausted million-ID backing
	// array alive. This is the same bounded ID allocation as the old map pop.
	proxyIDs := append([]uint64(nil), readModelDirtyRound[:count]...)
	readModelDirtyRound = readModelDirtyRound[count:]
	if len(readModelDirtyRound) == 0 {
		readModelDirtyRound = nil
	}
	return proxyIDs
}

func requeueDirtyReadModelProxyIDs(proxyIDs []uint64) {
	if len(proxyIDs) == 0 {
		return
	}

	readModelDirtyMu.Lock()
	for _, proxyID := range proxyIDs {
		if proxyID != 0 {
			readModelDirtyProxyIDs[proxyID] = struct{}{}
		}
	}
	readModelDirtyMu.Unlock()
}

func readModelRefreshInterval() time.Duration {
	seconds := support.GetEnvInt(envReadModelRefreshIntervalSeconds, defaultReadModelRefreshIntervalSeconds)
	if seconds <= 0 {
		seconds = defaultReadModelRefreshIntervalSeconds
	}
	return time.Duration(seconds) * time.Second
}

func readModelRefreshBatchSize() int {
	size := support.GetEnvInt(envReadModelRefreshBatchSize, defaultReadModelRefreshBatchSize)
	if size <= 0 {
		return defaultReadModelRefreshBatchSize
	}
	return size
}

func readModelRefreshMaxBatches() int {
	maxBatches := support.GetEnvInt(envReadModelRefreshMaxBatches, defaultReadModelRefreshMaxBatches)
	if maxBatches <= 0 {
		return defaultReadModelRefreshMaxBatches
	}
	return maxBatches
}

func readModelRefreshTimeout() time.Duration {
	seconds := support.GetEnvInt(envReadModelRefreshTimeoutSeconds, defaultReadModelRefreshTimeoutSeconds)
	if seconds <= 0 {
		seconds = defaultReadModelRefreshTimeoutSeconds
	}
	return time.Duration(seconds) * time.Second
}

func refreshAllWorkspaceProxyFilterIndexes(db *gorm.DB) error {
	return refreshAllWorkspaceProxyFilterIndexesBounded(db)
}

func refreshWorkspaceProxyFilterIndexesForProxyIDs(tx *gorm.DB, proxyIDs []uint64) error {
	if tx == nil || len(proxyIDs) == 0 {
		return nil
	}
	for start := 0; start < len(proxyIDs); start += pg.DeleteChunkSize {
		end := min(start+pg.DeleteChunkSize, len(proxyIDs))
		pairs, err := loadReadModelProjectionPairs(tx, proxyIDs[start:end])
		if err != nil {
			return err
		}
		for next := 0; next < len(pairs); next += readModelProjectionPairBudget() {
			if err := refreshWorkspaceProxyFilterPairs(tx, pairs[next:min(next+readModelProjectionPairBudget(), len(pairs))]); err != nil {
				return err
			}
		}
	}
	return tx.Statement.Context.Err()
}

func RefreshWorkspaceProxyFilterIndexesForWorkspaceProxyIDs(tx *gorm.DB, workspaceID uint, proxyIDs []uint64) error {
	if workspaceID == 0 || len(proxyIDs) == 0 {
		return nil
	}
	if tx == nil {
		return nil
	}
	for start := 0; start < len(proxyIDs); start += pg.DeleteChunkSize {
		end := start + pg.DeleteChunkSize
		if end > len(proxyIDs) {
			end = len(proxyIDs)
		}
		if err := RefreshWorkspaceProxyFilterIndexes(tx, "WHERE up.workspace_id = ? AND up.proxy_id IN ?", workspaceID, proxyIDs[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func RefreshWorkspaceProxyFilterIndexes(tx *gorm.DB, where string, args ...interface{}) error {
	if tx == nil || !tx.Migrator().HasTable(&domain.WorkspaceProxyFilterIndex{}) {
		return nil
	}

	return refreshWorkspaceProxyFilterScope(tx, `SELECT up.workspace_id, up.proxy_id, up.state, up.pause_reason FROM user_proxies up `+where, args...)
}

func refreshWorkspaceProxyFilterScope(tx *gorm.DB, scope string, args ...interface{}) error {
	query := `
WITH scope AS MATERIALIZED (
	` + scope + `
),
metadata AS MATERIALIZED (
	SELECT p.*, pr.label AS reputation_label, pr.score AS reputation_score
	FROM (SELECT DISTINCT proxy_id FROM scope) c
	JOIN LATERAL (
		SELECT p.id, p.host, p.ip_address, p.port, p.country, p.estimated_type
		FROM proxies p WHERE p.id = c.proxy_id OFFSET 0
	) p ON TRUE
	LEFT JOIN LATERAL (
		SELECT pr.label, pr.score FROM proxy_reputations pr
		WHERE pr.proxy_id = p.id AND pr.kind = 'overall' OFFSET 0
	) pr ON TRUE
),
evidence AS MATERIALIZED (
	SELECT pls.*, s.workspace_id AS checking_workspace_id
	FROM scope s
	JOIN workspaces w ON w.id = s.workspace_id
	JOIN LATERAL (
		SELECT l.*
		FROM proxy_latest_statistics l
		WHERE l.proxy_id = s.proxy_id
			AND l.workspace_id = CASE WHEN w.checker_generation = 0 THEN 0 ELSE s.workspace_id END
		OFFSET 0
	) pls ON TRUE
	LEFT JOIN LATERAL (
		SELECT cp.config_key FROM proxy_checker_plans cp
		WHERE cp.workspace_id = s.workspace_id AND cp.proxy_id = s.proxy_id AND cp.protocol_id = pls.protocol_id
		OFFSET 0
	) cp ON TRUE
	WHERE w.checker_generation = 0
		OR (w.checker_generation > 0 AND pls.config_key = COALESCE(cp.config_key,
			CASE pls.protocol_id WHEN 1 THEN w.checker_http_key WHEN 2 THEN w.checker_https_key WHEN 3 THEN w.checker_socks4_key WHEN 4 THEN w.checker_socks5_key ELSE '' END) AND pls.config_key <> '')
),
latest AS (
	SELECT checking_workspace_id, proxy_id, level_id, response_time, attempt, checked_at
	FROM (
		SELECT
			pls.checking_workspace_id,
			pls.proxy_id,
			pls.level_id,
			COALESCE(pls.response_time, 0) AS response_time,
			COALESCE(pls.attempt, 0) AS attempt,
			pls.checked_at,
			ROW_NUMBER() OVER (PARTITION BY pls.checking_workspace_id, pls.proxy_id ORDER BY pls.checked_at DESC, pls.statistic_id DESC) AS row_num
		FROM evidence pls
	) ranked
	WHERE row_num = 1
),
health AS (
	SELECT
		pls.checking_workspace_id,
		pls.proxy_id,
		ROUND(100.0 * AVG(CASE WHEN pls.alive THEN 1 ELSE 0 END)::numeric, 1) AS health_overall,
		MAX(CASE WHEN LOWER(proto.name) = 'http' THEN CASE WHEN pls.alive THEN 100.0 ELSE 0.0 END END) AS health_http,
		MAX(CASE WHEN LOWER(proto.name) = 'https' THEN CASE WHEN pls.alive THEN 100.0 ELSE 0.0 END END) AS health_https,
		MAX(CASE WHEN LOWER(proto.name) = 'socks4' THEN CASE WHEN pls.alive THEN 100.0 ELSE 0.0 END END) AS health_socks4,
		MAX(CASE WHEN LOWER(proto.name) = 'socks5' THEN CASE WHEN pls.alive THEN 100.0 ELSE 0.0 END END) AS health_socks5,
		BOOL_OR(pls.alive AND LOWER(proto.name) = 'http') AS alive_http,
		BOOL_OR(pls.alive AND LOWER(proto.name) = 'https') AS alive_https,
		BOOL_OR(pls.alive AND LOWER(proto.name) = 'socks4') AS alive_socks4,
		BOOL_OR(pls.alive AND LOWER(proto.name) = 'socks5') AS alive_socks5
	FROM evidence pls
	JOIN protocols proto ON proto.id = pls.protocol_id
	GROUP BY pls.checking_workspace_id, pls.proxy_id
),
rows AS (
	SELECT
		up.workspace_id,
		p.id AS proxy_id,
		up.state,
		up.pause_reason,
		p.host,
		p.ip_address,
		p.port,
		COALESCE(NULLIF(TRIM(p.country), ''), 'N/A') AS country,
		LOWER(COALESCE(NULLIF(TRIM(p.country), ''), 'n/a')) AS country_key,
		COALESCE(NULLIF(TRIM(p.estimated_type), ''), 'N/A') AS estimated_type,
		LOWER(COALESCE(NULLIF(TRIM(p.estimated_type), ''), 'n/a')) AS type_key,
		COALESCE(al.name, 'N/A') AS anonymity_level,
		LOWER(COALESCE(al.name, 'n/a')) AS anonymity_key,
		COALESCE(health.health_overall, 0) > 0 AS alive,
		COALESCE(latest.checked_at, '0001-01-01 00:00:00'::timestamp) AS latest_check,
		COALESCE(latest.response_time, 0) AS response_time,
		COALESCE(latest.attempt, 0) AS attempt,
		health.health_overall,
		health.health_http,
		health.health_https,
		health.health_socks4,
		health.health_socks5,
		COALESCE(health.alive_http, FALSE) AS alive_http,
		COALESCE(health.alive_https, FALSE) AS alive_https,
		COALESCE(health.alive_socks4, FALSE) AS alive_socks4,
		COALESCE(health.alive_socks5, FALSE) AS alive_socks5,
		LOWER(COALESCE(NULLIF(p.reputation_label, ''), 'unknown')) AS reputation_label,
		p.reputation_score
	FROM scope up
	JOIN metadata p ON p.id = up.proxy_id
	LEFT JOIN latest ON latest.proxy_id = p.id AND latest.checking_workspace_id = up.workspace_id
	LEFT JOIN anonymity_levels al ON al.id = latest.level_id
	LEFT JOIN health ON health.proxy_id = p.id AND health.checking_workspace_id = up.workspace_id
)
INSERT INTO user_proxy_filter_indexes (
	workspace_id, proxy_id, state, pause_reason, host, ip_address, port,
	country, country_key, estimated_type, type_key, anonymity_level, anonymity_key,
	alive, latest_check, response_time, attempt,
	health_overall, health_http, health_https, health_socks4, health_socks5,
	alive_http, alive_https, alive_socks4, alive_socks5,
	reputation_label, reputation_score, created_at, updated_at
)
SELECT
	workspace_id, proxy_id, state, pause_reason, host, ip_address, port,
	country, country_key, estimated_type, type_key, anonymity_level, anonymity_key,
	alive, latest_check, response_time, attempt,
	health_overall, health_http, health_https, health_socks4, health_socks5,
	alive_http, alive_https, alive_socks4, alive_socks5,
	reputation_label, reputation_score, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
FROM rows
WHERE NOT EXISTS (
	SELECT 1 FROM user_proxy_filter_indexes existing
	WHERE existing.workspace_id = rows.workspace_id AND existing.proxy_id = rows.proxy_id
		AND ROW(
			existing.state, existing.pause_reason, existing.host,
			existing.ip_address, existing.port, existing.country,
			existing.country_key, existing.estimated_type, existing.type_key,
			existing.anonymity_level, existing.anonymity_key, existing.alive,
			existing.latest_check, existing.response_time, existing.attempt,
			existing.health_overall, existing.health_http, existing.health_https,
			existing.health_socks4, existing.health_socks5, existing.alive_http,
			existing.alive_https, existing.alive_socks4, existing.alive_socks5,
			existing.reputation_label, existing.reputation_score
		) IS NOT DISTINCT FROM ROW(
			rows.state, rows.pause_reason, rows.host,
			rows.ip_address, rows.port, rows.country,
			rows.country_key, rows.estimated_type, rows.type_key,
			rows.anonymity_level, rows.anonymity_key, rows.alive,
			rows.latest_check, rows.response_time, rows.attempt,
			rows.health_overall, rows.health_http, rows.health_https,
			rows.health_socks4, rows.health_socks5, rows.alive_http,
			rows.alive_https, rows.alive_socks4, rows.alive_socks5,
			rows.reputation_label, rows.reputation_score
		)
)
ORDER BY workspace_id, proxy_id
ON CONFLICT (workspace_id, proxy_id) DO UPDATE SET
	state = EXCLUDED.state,
	pause_reason = EXCLUDED.pause_reason,
	host = EXCLUDED.host,
	ip_address = EXCLUDED.ip_address,
	port = EXCLUDED.port,
	country = EXCLUDED.country,
	country_key = EXCLUDED.country_key,
	estimated_type = EXCLUDED.estimated_type,
	type_key = EXCLUDED.type_key,
	anonymity_level = EXCLUDED.anonymity_level,
	anonymity_key = EXCLUDED.anonymity_key,
	alive = EXCLUDED.alive,
	latest_check = EXCLUDED.latest_check,
	response_time = EXCLUDED.response_time,
	attempt = EXCLUDED.attempt,
	health_overall = EXCLUDED.health_overall,
	health_http = EXCLUDED.health_http,
	health_https = EXCLUDED.health_https,
	health_socks4 = EXCLUDED.health_socks4,
	health_socks5 = EXCLUDED.health_socks5,
	alive_http = EXCLUDED.alive_http,
	alive_https = EXCLUDED.alive_https,
	alive_socks4 = EXCLUDED.alive_socks4,
	alive_socks5 = EXCLUDED.alive_socks5,
	reputation_label = EXCLUDED.reputation_label,
	reputation_score = EXCLUDED.reputation_score,
	updated_at = CURRENT_TIMESTAMP
WHERE ROW(
		user_proxy_filter_indexes.state, user_proxy_filter_indexes.pause_reason, user_proxy_filter_indexes.host, user_proxy_filter_indexes.ip_address,
		user_proxy_filter_indexes.port, user_proxy_filter_indexes.country, user_proxy_filter_indexes.country_key, user_proxy_filter_indexes.estimated_type,
		user_proxy_filter_indexes.type_key, user_proxy_filter_indexes.anonymity_level, user_proxy_filter_indexes.anonymity_key, user_proxy_filter_indexes.alive,
		user_proxy_filter_indexes.latest_check, user_proxy_filter_indexes.response_time, user_proxy_filter_indexes.attempt, user_proxy_filter_indexes.health_overall,
		user_proxy_filter_indexes.health_http, user_proxy_filter_indexes.health_https, user_proxy_filter_indexes.health_socks4, user_proxy_filter_indexes.health_socks5,
		user_proxy_filter_indexes.alive_http, user_proxy_filter_indexes.alive_https, user_proxy_filter_indexes.alive_socks4, user_proxy_filter_indexes.alive_socks5,
		user_proxy_filter_indexes.reputation_label, user_proxy_filter_indexes.reputation_score
	)
	IS DISTINCT FROM ROW(
		EXCLUDED.state, EXCLUDED.pause_reason, EXCLUDED.host, EXCLUDED.ip_address,
		EXCLUDED.port, EXCLUDED.country, EXCLUDED.country_key, EXCLUDED.estimated_type,
		EXCLUDED.type_key, EXCLUDED.anonymity_level, EXCLUDED.anonymity_key, EXCLUDED.alive,
		EXCLUDED.latest_check, EXCLUDED.response_time, EXCLUDED.attempt, EXCLUDED.health_overall,
		EXCLUDED.health_http, EXCLUDED.health_https, EXCLUDED.health_socks4, EXCLUDED.health_socks5,
		EXCLUDED.alive_http, EXCLUDED.alive_https, EXCLUDED.alive_socks4, EXCLUDED.alive_socks5,
		EXCLUDED.reputation_label, EXCLUDED.reputation_score
	)
`
	// OFFSET 0 keeps ownership, metadata, evidence and plan lookups parameterized.
	// Read metadata once per physical route, then expand the selected owners.
	// Materialize evidence once for both aggregates. Keep committed keys while
	// checker_dirty is set; public readers continue to mask pending health.
	// Skip rows whose complete content already matches before entering INSERT.
	// ON CONFLICT still checks again after acquiring a changed row's lock. This
	// keeps duplicate invalidations from locking unchanged projection rows and
	// preserves UpdatedAt, while concurrent changes retain the conflict guard.
	if err := tx.Exec(query, args...).Error; err != nil {
		return fmt.Errorf("read model: refresh proxy filter index: %w", err)
	}
	return nil
}

func refreshAllWorkspaceScrapeSourceStats(db *gorm.DB) error {
	return RefreshWorkspaceScrapeSourceStats(db, "")
}

func RefreshWorkspaceScrapeSourceStatsForSites(tx *gorm.DB, siteIDs []uint64) error {
	if tx == nil || len(siteIDs) == 0 {
		return nil
	}
	for start := 0; start < len(siteIDs); start += pg.DeleteChunkSize {
		end := start + pg.DeleteChunkSize
		if end > len(siteIDs) {
			end = len(siteIDs)
		}
		if err := RefreshWorkspaceScrapeSourceStats(tx, "WHERE ss.id IN ?", siteIDs[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func RefreshWorkspaceScrapeSourceStatsForWorkspaceSites(tx *gorm.DB, workspaceID uint, siteIDs []uint64) error {
	if tx == nil || workspaceID == 0 || len(siteIDs) == 0 {
		return nil
	}
	for start := 0; start < len(siteIDs); start += pg.DeleteChunkSize {
		end := start + pg.DeleteChunkSize
		if end > len(siteIDs) {
			end = len(siteIDs)
		}
		if err := RefreshWorkspaceScrapeSourceStats(tx, "WHERE uss.workspace_id = ? AND ss.id IN ?", workspaceID, siteIDs[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func refreshWorkspaceScrapeSourceStatsForProxyIDs(tx *gorm.DB, proxyIDs []uint64) error {
	if tx == nil || len(proxyIDs) == 0 || !tx.Migrator().HasTable(&domain.WorkspaceScrapeSourceStat{}) {
		return nil
	}

	for start := 0; start < len(proxyIDs); start += pg.DeleteChunkSize {
		end := start + pg.DeleteChunkSize
		if end > len(proxyIDs) {
			end = len(proxyIDs)
		}

		var pairs []sourceStatsKey
		if err := tx.Table("proxy_scrape_site pss").
			Distinct("uss.workspace_id, pss.scrape_site_id").
			Joins("JOIN user_scrape_site uss ON uss.scrape_site_id = pss.scrape_site_id").
			Joins("JOIN user_proxies up ON up.workspace_id = uss.workspace_id AND up.proxy_id = pss.proxy_id").
			Where("pss.proxy_id IN ?", proxyIDs[start:end]).
			Scan(&pairs).Error; err != nil {
			return fmt.Errorf("read model: load scrape-source ids for proxies: %w", err)
		}
		sourceStatsDirtyMu.Lock()
		for _, pair := range pairs {
			sourceStatsDirty[pair] = struct{}{}
		}
		sourceStatsDirtyMu.Unlock()
	}
	return nil
}

// Source health aggregation has its own coalescing queue so repeated checks
// from a large source cannot delay proxy-list projection updates.
func runSourceStatsRefresh(ctx context.Context) {
	seconds := max(1, support.GetEnvInt("SCRAPE_SOURCE_STATS_REFRESH_INTERVAL_SECONDS", 30))
	ticker := time.NewTicker(time.Duration(seconds) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			flushDirtySourceStats(ctx)
		}
	}
}

// flushDirtySourceStats persists at most one batch. False means the queue is
// empty or a failed batch was requeued. The final drain checks the queue before
// reporting completion and backs off before retrying a failed batch.
func flushDirtySourceStats(ctx context.Context) bool {
	if pg.DB == nil {
		return false
	}
	limit := max(1, min(1000, support.GetEnvInt("SCRAPE_SOURCE_STATS_REFRESH_BATCH_SIZE", 100)))
	sourceStatsDirtyMu.Lock()
	pairs := make([]sourceStatsKey, 0, min(limit, len(sourceStatsDirty)))
	for pair := range sourceStatsDirty {
		pairs = append(pairs, pair)
		delete(sourceStatsDirty, pair)
		if len(pairs) >= limit {
			break
		}
	}
	sourceStatsDirtyMu.Unlock()
	if len(pairs) == 0 {
		return false
	}
	values := make([][]any, len(pairs))
	for i, pair := range pairs {
		values[i] = []any{pair.WorkspaceID, pair.ScrapeSiteID}
	}
	refreshCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	err := RefreshWorkspaceScrapeSourceStats(pg.DB.WithContext(refreshCtx), "WHERE (uss.workspace_id, ss.id) IN ?", values)
	// HasTable can hide a canceled schema lookup behind a false result. Keep
	// the batch even if cancellation raced an otherwise successful refresh.
	if err == nil {
		err = refreshCtx.Err()
	}
	cancel()
	if err != nil {
		sourceStatsDirtyMu.Lock()
		for _, pair := range pairs {
			sourceStatsDirty[pair] = struct{}{}
		}
		sourceStatsDirtyMu.Unlock()
		log.Warn("read model: coalesced source stats refresh failed", "error", err, "pairs", len(pairs))
		return false
	}
	return true
}

func RefreshWorkspaceScrapeSourceStatsForWorkspaceProxyIDs(tx *gorm.DB, workspaceID uint, proxyIDs []uint64) error {
	if tx == nil || workspaceID == 0 || len(proxyIDs) == 0 || !tx.Migrator().HasTable(&domain.WorkspaceScrapeSourceStat{}) {
		return nil
	}

	affectedSiteIDs := make([]uint64, 0)
	seenSites := make(map[uint64]struct{})
	for start := 0; start < len(proxyIDs); start += pg.DeleteChunkSize {
		end := start + pg.DeleteChunkSize
		if end > len(proxyIDs) {
			end = len(proxyIDs)
		}

		var siteIDs []uint64
		if err := tx.Table("proxy_scrape_site pss").
			Distinct("pss.scrape_site_id").
			Joins("JOIN user_scrape_site uss ON uss.scrape_site_id = pss.scrape_site_id AND uss.workspace_id = ?", workspaceID).
			Where("pss.proxy_id IN ?", proxyIDs[start:end]).
			Pluck("pss.scrape_site_id", &siteIDs).Error; err != nil {
			return fmt.Errorf("read model: load user scrape-source ids for proxies: %w", err)
		}
		for _, siteID := range siteIDs {
			if _, seen := seenSites[siteID]; !seen {
				seenSites[siteID] = struct{}{}
				affectedSiteIDs = append(affectedSiteIDs, siteID)
			}
		}
	}
	return RefreshWorkspaceScrapeSourceStatsForWorkspaceSites(tx, workspaceID, affectedSiteIDs)
}

func RefreshWorkspaceScrapeSourceStats(tx *gorm.DB, where string, args ...interface{}) error {
	if tx == nil || !tx.Migrator().HasTable(&domain.WorkspaceScrapeSourceStat{}) {
		return nil
	}

	query := `
WITH rows AS (
	SELECT
		uss.workspace_id,
		ss.id AS scrape_site_id,
		ss.url,
		LOWER(split_part(ss.url, '://', 1)) AS protocol_key,
		COUNT(up.proxy_id) AS proxy_count,
		COALESCE(SUM(CASE WHEN pos.overall_alive IS TRUE THEN 1 ELSE 0 END), 0) AS alive_count,
		COALESCE(SUM(CASE WHEN pos.overall_alive IS FALSE THEN 1 ELSE 0 END), 0) AS dead_count,
		COALESCE(SUM(CASE WHEN up.proxy_id IS NOT NULL AND pos.proxy_id IS NULL THEN 1 ELSE 0 END), 0) AS unknown_count,
		uss.created_at AS added_at
	FROM user_scrape_site uss
	JOIN scrape_sites ss ON ss.id = uss.scrape_site_id
	LEFT JOIN (proxy_scrape_site pss JOIN user_proxies up ON up.proxy_id = pss.proxy_id)
		ON pss.scrape_site_id = ss.id AND up.workspace_id = uss.workspace_id
	LEFT JOIN LATERAL (` + evidence.CurrentOverallStatusForProxySQL("up.workspace_id", "up.proxy_id") + `) pos ON TRUE
	` + where + `
	GROUP BY uss.workspace_id, ss.id, ss.url, uss.created_at
)
INSERT INTO user_scrape_source_stats (
	workspace_id, scrape_site_id, url, protocol_key,
	proxy_count, alive_count, dead_count, unknown_count,
	added_at, created_at, updated_at
)
SELECT
	workspace_id, scrape_site_id, url, protocol_key,
	proxy_count, alive_count, dead_count, unknown_count,
	added_at, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
FROM rows
ON CONFLICT (workspace_id, scrape_site_id) DO UPDATE SET
	url = EXCLUDED.url,
	protocol_key = EXCLUDED.protocol_key,
	proxy_count = EXCLUDED.proxy_count,
	alive_count = EXCLUDED.alive_count,
	dead_count = EXCLUDED.dead_count,
	unknown_count = EXCLUDED.unknown_count,
	added_at = EXCLUDED.added_at,
	updated_at = CURRENT_TIMESTAMP
`
	// Index the committed key projection even while a newer edit is pending.
	// Public readers mask pending health. This preserves unchanged routes so
	// clearing the pending flag requires rewriting only actual plan deltas.
	query = strings.ReplaceAll(query, "NOT w.checker_dirty AND ", "")
	query = strings.ReplaceAll(query, " AND NOT w.checker_dirty", "")
	if err := tx.Exec(query, args...).Error; err != nil {
		return fmt.Errorf("read model: refresh scrape-source stats: %w", err)
	}
	return nil
}
