package dashboard

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"magpie/internal/checkpace"
	"magpie/internal/platform/pg"

	"gorm.io/gorm"
)

// admissionLockKey serializes claims and retention deletes across the
// cluster, so the count of active leases checked by a claim is exact.
const admissionLockKey int64 = 941_843_230_016

var errWorkspaceNotFound = errors.New("dashboard: workspace not found")

// Store owns the dashboard_snapshots SQL. It holds the pool handle and opens
// its own transactions; every time comparison runs on the database clock.
// The SQL is PostgreSQL-only.
type Store struct {
	db      *gorm.DB
	version int16
}

// NewStore returns a Store on the pool handle db.
func NewStore(db *gorm.DB) *Store {
	return &Store{db: db, version: payloadVersion}
}

// snapshotState is one run of the read statement: the workspace's committed
// checker version, its snapshot row (zero when there is none), the derived
// clock comparisons and, on a full read, the live usage figures.
type snapshotState struct {
	WorkspaceGeneration uint64 `gorm:"column:workspace_generation"`
	WorkspaceDirty      bool   `gorm:"column:workspace_dirty"`
	RowExists           bool   `gorm:"column:row_exists"`

	CheckerGeneration *uint64    `gorm:"column:checker_generation"`
	HealthValid       *bool      `gorm:"column:health_valid"`
	CollectedAt       *time.Time `gorm:"column:collected_at"`
	ComputedAt        *time.Time `gorm:"column:computed_at"`
	FailedAt          *time.Time `gorm:"column:failed_at"`
	Payload           []byte     `gorm:"column:payload"`

	// AgeSeconds is now() - computed_at; nil before the first publish.
	AgeSeconds *float64 `gorm:"column:age_seconds"`
	Rebuilding bool     `gorm:"column:rebuilding"`
	Eligible   bool     `gorm:"column:eligible"`

	TotalChecks     *int64 `gorm:"column:total_checks"`
	ChecksThisMonth *int64 `gorm:"column:checks_this_month"`
}

func (s snapshotState) age() (time.Duration, bool) {
	if s.AgeSeconds == nil {
		return 0, false
	}
	return time.Duration(*s.AgeSeconds * float64(time.Second)), true
}

func (s snapshotState) hasPayload() bool { return len(s.Payload) > 0 }

// healthCurrent: the stored health parts belong to the committed generation.
func (s snapshotState) healthCurrent() bool {
	return s.HealthValid != nil && *s.HealthValid && s.CheckerGeneration != nil &&
		*s.CheckerGeneration == s.WorkspaceGeneration && !s.WorkspaceDirty
}

// healthMissing: health could be shown, but the stored parts are not current.
// A dirty workspace masks health instead, so it never counts as missing.
func (s snapshotState) healthMissing() bool { return !s.WorkspaceDirty && !s.healthCurrent() }

// read runs the read statement. full adds the payload and the two usage
// figures; a re-read during a wait omits them.
func (st *Store) read(ctx context.Context, workspaceID uint, full bool, now time.Time) (snapshotState, error) {
	query, args := st.readStatement(workspaceID, full, now)
	var states []snapshotState
	if err := st.db.WithContext(ctx).Raw(query, args...).Scan(&states).Error; err != nil {
		return snapshotState{}, err
	}
	if len(states) == 0 {
		return snapshotState{}, fmt.Errorf("%w: %d", errWorkspaceNotFound, workspaceID)
	}
	return states[0], nil
}

// readStatement is the read statement and its arguments.
func (st *Store) readStatement(workspaceID uint, full bool, now time.Time) (string, []any) {
	payloadColumn, usageColumns := "NULL::bytea", "NULL::bigint AS total_checks, NULL::bigint AS checks_this_month"
	args := []any{}
	if full {
		periodStart, _ := checkpace.UsagePeriod(now)
		payloadColumn = "s.payload"
		// COALESCE wraps the scalar subquery: one that finds no row yields
		// NULL, which a COALESCE inside it would never see.
		usageColumns = `(SELECT COALESCE(SUM(GREATEST(u.checks, u.check_attempts)), 0)
	   FROM workspace_usage_periods u WHERE u.workspace_id = w.id) AS total_checks,
	COALESCE((SELECT u.checks FROM workspace_usage_periods u
	           WHERE u.workspace_id = w.id AND u.period_start = ?), 0) AS checks_this_month`
		args = append(args, periodStart)
	}
	args = append(args, st.version, workspaceID)
	return `SELECT w.checker_generation AS workspace_generation, w.checker_dirty AS workspace_dirty,
	s.workspace_id IS NOT NULL AS row_exists,
	s.checker_generation, s.health_valid, s.collected_at, s.computed_at, s.failed_at,
	` + payloadColumn + ` AS payload,
	EXTRACT(EPOCH FROM now() - s.computed_at)::float8 AS age_seconds,
	COALESCE(s.lease_until > now(), false) AS rebuilding,
	COALESCE(s.next_eligible_at <= now(), true) AS eligible,
	` + usageColumns + `
FROM workspaces w
LEFT JOIN dashboard_snapshots s ON s.workspace_id = w.id AND s.payload_version = ?
WHERE w.id = ?`, args
}

// ensureRow inserts the bare snapshot row. It runs in its own transaction, so
// a wait for the workspace row's foreign-key lock blocks only this request,
// and for at most the lock timeout.
func (st *Store) ensureRow(ctx context.Context, workspaceID uint) (bool, error) {
	err := st.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec("SET LOCAL lock_timeout = '" + lockTimeout + "'").Error; err != nil {
			return err
		}
		return tx.Exec(`INSERT INTO dashboard_snapshots (workspace_id, payload_version) VALUES (?, ?) ON CONFLICT DO NOTHING`,
			workspaceID, st.version).Error
	})
	if pg.IsLockNotAvailable(err) {
		return false, nil
	}
	return err == nil, err
}

// lease is an admitted claim.
type lease struct {
	Epoch     int64     `gorm:"column:lease_epoch"`
	ClaimedAt time.Time `gorm:"column:claimed_at"`
}

// claim admits one build of the workspace's snapshot if every rule still holds
// under the admission lock: the row exists, no lease is active, the cooldown
// has passed, the snapshot needs a build (none yet, older than the fresh
// window, or health not current on a clean workspace) and fewer than
// clusterLimit leases are active. now() is the transaction start, so every
// comparison leans towards refusing. A lock timeout means not admitted.
func (st *Store) claim(ctx context.Context, workspaceID uint, clusterLimit int, leaseFor time.Duration) (lease, bool, error) {
	var claimed []lease
	err := st.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec("SET LOCAL lock_timeout = '" + lockTimeout + "'").Error; err != nil {
			return err
		}
		if err := tx.Exec("SELECT pg_advisory_xact_lock(?)", admissionLockKey).Error; err != nil {
			return err
		}
		return tx.Raw(`UPDATE dashboard_snapshots s
   SET lease_epoch = nextval('dashboard_lease_epoch_seq'),
       lease_until = now() + make_interval(secs => ?),
       claimed_at = now(),
       next_eligible_at = now() + make_interval(secs => ?)
  FROM workspaces w
 WHERE s.workspace_id = ? AND s.payload_version = ? AND w.id = s.workspace_id
   AND (s.lease_until IS NULL OR s.lease_until <= now())
   AND (s.next_eligible_at IS NULL OR s.next_eligible_at <= now())
   AND (s.computed_at IS NULL
        OR s.computed_at <= now() - make_interval(secs => ?)
        OR (NOT w.checker_dirty
            AND NOT (COALESCE(s.health_valid, false) AND s.checker_generation IS NOT DISTINCT FROM w.checker_generation)))
   AND (SELECT count(*) FROM dashboard_snapshots active WHERE active.lease_until > now()) < ?
RETURNING s.lease_epoch, s.claimed_at`,
			leaseFor.Seconds(), claimCooldown.Seconds(), workspaceID, st.version, freshFor.Seconds(), clusterLimit).Scan(&claimed).Error
	})
	if pg.IsLockNotAvailable(err) {
		return lease{}, false, nil
	}
	if err != nil || len(claimed) == 0 {
		return lease{}, false, err
	}
	return claimed[0], true, nil
}

// workspaceVersion reads the committed checker generation and dirty flag.
func (st *Store) workspaceVersion(ctx context.Context, workspaceID uint) (uint64, bool, error) {
	var rows []struct {
		CheckerGeneration uint64
		CheckerDirty      bool
	}
	if err := st.db.WithContext(ctx).Raw(`SELECT checker_generation, checker_dirty FROM workspaces WHERE id = ?`, workspaceID).Scan(&rows).Error; err != nil {
		return 0, false, err
	}
	if len(rows) == 0 {
		return 0, false, fmt.Errorf("%w: %d", errWorkspaceNotFound, workspaceID)
	}
	return rows[0].CheckerGeneration, rows[0].CheckerDirty, nil
}

// readOnly runs one data statement in its own read-only transaction with a
// transaction-local statement timeout, never a session setting on the pool.
// The timeout is the budget left on ctx once the transaction holds its
// connection, so time spent waiting for the pool does not extend it.
func (st *Store) readOnly(ctx context.Context, run func(*gorm.DB) error) error {
	return st.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		timeout := defaultBuildDeadline
		if deadline, ok := ctx.Deadline(); ok {
			timeout = time.Until(deadline)
		}
		if timeout <= 0 {
			return errBuildExpired
		}
		if err := tx.Exec("SELECT set_config('statement_timeout', ?, true)", fmt.Sprintf("%d", max(int64(1), timeout.Milliseconds()))).Error; err != nil {
			return err
		}
		return run(tx)
	}, &sql.TxOptions{ReadOnly: true})
}

// publish stores a build if its lease is still the row's and still active.
func (st *Store) publish(ctx context.Context, workspaceID uint, epoch int64, encoded []byte, generation uint64, healthValid bool) (bool, error) {
	result := st.db.WithContext(ctx).Exec(`UPDATE dashboard_snapshots
   SET payload = ?, checker_generation = ?, health_valid = ?,
       collected_at = claimed_at, computed_at = now(),
       lease_until = NULL, failed_at = NULL, failure_count = 0
 WHERE workspace_id = ? AND payload_version = ? AND lease_epoch = ? AND lease_until > now()`,
		encoded, generation, healthValid, workspaceID, st.version, epoch)
	return result.RowsAffected == 1, result.Error
}

// fail records a failed build of a lease that has not been released, and
// backs off the next claim exponentially up to ten minutes.
func (st *Store) fail(ctx context.Context, workspaceID uint, epoch int64) error {
	return st.db.WithContext(ctx).Exec(`UPDATE dashboard_snapshots
   SET lease_until = NULL, failed_at = now(),
       failure_count = LEAST(failure_count + 1, 100),
       next_eligible_at = GREATEST(next_eligible_at,
           now() + LEAST(make_interval(secs => ?) * power(2, LEAST(failure_count, 5)), interval '10 minutes'))
 WHERE workspace_id = ? AND payload_version = ? AND lease_epoch = ? AND lease_until IS NOT NULL`,
		claimCooldown.Seconds(), workspaceID, st.version, epoch).Error
}

// release clears a lease without recording a failure, for builds canceled by
// shutdown. It is fenced by the epoch alone.
func (st *Store) release(ctx context.Context, workspaceID uint, epoch int64) error {
	return st.db.WithContext(ctx).Exec(`UPDATE dashboard_snapshots SET lease_until = NULL
 WHERE workspace_id = ? AND payload_version = ? AND lease_epoch = ?`, workspaceID, st.version, epoch).Error
}

// DeleteExpired deletes, in bounded batches, snapshot rows untouched for
// seven days whose lease is inactive and whose cooldown has passed. It runs
// under the admission lock; a lock timeout skips the batch. It returns the
// number of rows deleted.
func (st *Store) DeleteExpired(ctx context.Context) (int64, error) {
	var total int64
	for {
		var deleted int64
		err := st.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			if err := tx.Exec("SET LOCAL lock_timeout = '" + lockTimeout + "'").Error; err != nil {
				return err
			}
			if err := tx.Exec("SELECT pg_advisory_xact_lock(?)", admissionLockKey).Error; err != nil {
				return err
			}
			const expired = `(s.lease_until IS NULL OR s.lease_until <= now())
     AND (s.next_eligible_at IS NULL OR s.next_eligible_at <= now())
     AND GREATEST(s.created_at, s.computed_at, s.claimed_at, s.failed_at) < now() - make_interval(secs => ?)`
			result := tx.Exec(`WITH candidates AS (
  SELECT s.workspace_id, s.payload_version FROM dashboard_snapshots s
   WHERE `+expired+`
   ORDER BY s.workspace_id, s.payload_version
   LIMIT ? FOR UPDATE SKIP LOCKED
)
DELETE FROM dashboard_snapshots s USING candidates c
 WHERE s.workspace_id = c.workspace_id AND s.payload_version = c.payload_version
   AND `+expired, retainFor.Seconds(), retentionBatch, retainFor.Seconds())
			deleted = result.RowsAffected
			return result.Error
		})
		if pg.IsLockNotAvailable(err) {
			return total, nil
		}
		if err != nil {
			return total, err
		}
		total += deleted
		if deleted < retentionBatch || ctx.Err() != nil {
			return total, ctx.Err()
		}
	}
}
