package database

import (
	"context"
	"fmt"
)

// Retain every modern pointer while its workspace projection is pending.
// Otherwise, match only this pointer's owner and committed sparse/default key.
// Legacy pointers remain pinned while any generation-zero owner exists.
// Ordered successor probes verify equality after LIMIT, keeping ownership reads
// on the composite primary key without scanning a route's other owners.
const obsoleteCheckerEvidencePredicateSQL = `
(candidate.workspace_id = 0 AND NOT EXISTS (
    SELECT 1 FROM user_proxies up JOIN workspaces w ON w.id = up.workspace_id
    WHERE up.proxy_id = candidate.proxy_id AND w.checker_generation = 0
    LIMIT 1 OFFSET 0
)) OR (candidate.workspace_id > 0
    AND NOT EXISTS (SELECT 1 FROM workspaces w WHERE w.id = candidate.workspace_id AND w.checker_dirty)
    AND NOT EXISTS (
        SELECT 1 FROM workspaces w
        LEFT JOIN proxy_checker_plans cp ON cp.workspace_id = w.id AND cp.proxy_id = candidate.proxy_id AND cp.protocol_id = candidate.protocol_id
        WHERE w.id = candidate.workspace_id AND w.checker_generation > 0
          AND candidate.config_key <> '' AND candidate.config_key = COALESCE(cp.config_key,
            CASE candidate.protocol_id WHEN 1 THEN w.checker_http_key WHEN 2 THEN w.checker_https_key
            WHEN 3 THEN w.checker_socks4_key WHEN 4 THEN w.checker_socks5_key ELSE '' END)
          AND EXISTS (SELECT 1 FROM (
            SELECT up.workspace_id, up.proxy_id FROM user_proxies up
            WHERE (up.workspace_id, up.proxy_id) >= (candidate.workspace_id, candidate.proxy_id)
            ORDER BY up.workspace_id, up.proxy_id LIMIT 1 OFFSET 0
          ) owned WHERE owned.workspace_id = candidate.workspace_id AND owned.proxy_id = candidate.proxy_id)
    ))`

type checkerEvidencePruneKey struct {
	WorkspaceID uint
	ConfigKey   string
	ProxyID     uint64
	ProtocolID  int
}

// CheckerEvidencePruneCursor resumes one finite primary-key scan. The upper
// key is fixed at its first page so new pointers cannot postpone the next pass.
type CheckerEvidencePruneCursor struct {
	after   checkerEvidencePruneKey
	through checkerEvidencePruneKey
}

type CheckerEvidencePrunePage struct {
	Scanned int64
	Deleted int64
	// Nil marks the end of this scan. The next call starts a new finite scan,
	// including invalidations that occurred behind the previous cursor.
	Next *CheckerEvidencePruneCursor
}

// PruneObsoleteCheckerEvidencePage bounds examined pointers as well as deletes.
// A current-only page still advances the cursor; it never expands global health.
// Page keys are covered by the primary index. Physical tuple reads are deferred
// until eligibility has identified obsolete pointers, then use verified seeks.
func PruneObsoleteCheckerEvidencePage(ctx context.Context, cursor *CheckerEvidencePruneCursor, limit int) (CheckerEvidencePrunePage, error) {
	if DB == nil {
		return CheckerEvidencePrunePage{}, fmt.Errorf("checker evidence pruning: database not initialised")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if !isPostgresDialect(DB) {
		// Keep SQLite's existing retention semantics without PostgreSQL's
		// physical tuple IDs or data-modifying CTEs. Production uses PostgreSQL.
		deleted, err := PruneObsoleteCheckerEvidence(ctx, limit)
		return CheckerEvidencePrunePage{Scanned: deleted, Deleted: deleted}, err
	}
	if limit <= 0 {
		limit = deleteChunkSize
	}
	boundary := `SELECT workspace_id, config_key, proxy_id, protocol_id FROM proxy_latest_statistics
ORDER BY workspace_id DESC, config_key DESC, proxy_id DESC, protocol_id DESC LIMIT 1`
	var args []any
	after := ""
	if cursor != nil {
		boundary = `SELECT ?::bigint AS workspace_id, ?::text AS config_key, ?::bigint AS proxy_id, ?::integer AS protocol_id`
		args = append(args, cursor.through.WorkspaceID, cursor.through.ConfigKey, cursor.through.ProxyID, cursor.through.ProtocolID)
		after = `AND (l.workspace_id, l.config_key, l.proxy_id, l.protocol_id) > (?, ?, ?, ?)`
		args = append(args, cursor.after.WorkspaceID, cursor.after.ConfigKey, cursor.after.ProxyID, cursor.after.ProtocolID)
	}
	args = append(args, limit, limit)
	query := `WITH boundary AS MATERIALIZED (` + boundary + `),
candidates AS MATERIALIZED (
    SELECT l.workspace_id, l.config_key, l.proxy_id, l.protocol_id
    FROM proxy_latest_statistics l
    WHERE (l.workspace_id, l.config_key, l.proxy_id, l.protocol_id) <=
        (SELECT workspace_id, config_key, proxy_id, protocol_id FROM boundary)
    ` + after + `
    ORDER BY l.workspace_id, l.config_key, l.proxy_id, l.protocol_id LIMIT ?
), obsolete AS MATERIALIZED (
    SELECT candidate.* FROM candidates candidate WHERE ` + obsoleteCheckerEvidencePredicateSQL + `
), obsolete_tids AS MATERIALIZED (
    SELECT pointer.ctid AS pointer_tid FROM obsolete candidate
    JOIN LATERAL (
        SELECT l.ctid, l.workspace_id, l.config_key, l.proxy_id, l.protocol_id
        FROM proxy_latest_statistics l
        WHERE (l.workspace_id, l.config_key, l.proxy_id, l.protocol_id) >=
            (candidate.workspace_id, candidate.config_key, candidate.proxy_id, candidate.protocol_id)
        ORDER BY l.workspace_id, l.config_key, l.proxy_id, l.protocol_id LIMIT 1 OFFSET 0
    ) pointer ON (pointer.workspace_id, pointer.config_key, pointer.proxy_id, pointer.protocol_id) =
        (candidate.workspace_id, candidate.config_key, candidate.proxy_id, candidate.protocol_id)
), removed AS (
    DELETE FROM proxy_latest_statistics target
    WHERE target.ctid = ANY (ARRAY(SELECT pointer_tid FROM obsolete_tids)) RETURNING 1
)
SELECT (SELECT COUNT(*) FROM candidates) AS scanned,
       (SELECT COUNT(*) FROM removed) AS deleted,
       (SELECT COUNT(*) FROM candidates) < ? OR
           (tail.workspace_id, tail.config_key, tail.proxy_id, tail.protocol_id) >=
           (boundary.workspace_id, boundary.config_key, boundary.proxy_id, boundary.protocol_id) AS complete,
       COALESCE(tail.workspace_id, 0) AS workspace_id, COALESCE(tail.config_key, '') AS config_key,
       COALESCE(tail.proxy_id, 0) AS proxy_id, COALESCE(tail.protocol_id, 0) AS protocol_id,
       boundary.workspace_id AS through_workspace_id, boundary.config_key AS through_config_key,
       boundary.proxy_id AS through_proxy_id, boundary.protocol_id AS through_protocol_id
FROM boundary LEFT JOIN LATERAL (
    SELECT workspace_id, config_key, proxy_id, protocol_id FROM candidates
    ORDER BY workspace_id DESC, config_key DESC, proxy_id DESC, protocol_id DESC LIMIT 1
) tail ON TRUE`
	var row struct {
		Scanned, Deleted   int64
		Complete           bool
		WorkspaceID        uint
		ConfigKey          string
		ProxyID            uint64
		ProtocolID         int
		ThroughWorkspaceID uint
		ThroughConfigKey   string
		ThroughProxyID     uint64
		ThroughProtocolID  int
	}
	if err := DB.WithContext(ctx).Raw(query, args...).Scan(&row).Error; err != nil {
		return CheckerEvidencePrunePage{}, fmt.Errorf("checker evidence pruning: scan page: %w", err)
	}
	page := CheckerEvidencePrunePage{Scanned: row.Scanned, Deleted: row.Deleted}
	if row.Scanned > 0 && !row.Complete {
		page.Next = &CheckerEvidencePruneCursor{after: checkerEvidencePruneKey{row.WorkspaceID, row.ConfigKey, row.ProxyID, row.ProtocolID},
			through: checkerEvidencePruneKey{row.ThroughWorkspaceID, row.ThroughConfigKey, row.ThroughProxyID, row.ThroughProtocolID}}
	}
	return page, nil
}
