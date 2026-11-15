package domain

import "time"

// DashboardSnapshot is one workspace's shared dashboard snapshot for one
// payload version (ADR 0017, amended by ADR 0018). Any backend instance may build it on demand
// after claiming a lease; every lease takes a new epoch from
// dashboard_lease_epoch_seq, and publish and failure updates are fenced by
// that epoch. Nil columns are unknown: a row may exist before its first
// build publishes.
type DashboardSnapshot struct {
	WorkspaceID    uint      `gorm:"primaryKey;autoIncrement:false"`
	PayloadVersion int16     `gorm:"primaryKey;autoIncrement:false;type:smallint"`
	Workspace      Workspace `gorm:"constraint:OnDelete:CASCADE" json:"-"`
	CreatedAt      time.Time `gorm:"not null;default:CURRENT_TIMESTAMP"`

	// Health is the last health parts a build computed on a clean workspace,
	// as JSON, under CheckerGeneration. A build that cannot compute current
	// health (the workspace is dirty, or its generation changed during the
	// build) keeps them, so a dashboard can show the previous results, marked
	// outdated, while new checker settings apply (ADR 0018).
	// HealthCollectedAt is the claim time of the build that computed them.
	Health            []byte
	CheckerGeneration *uint64
	HealthCollectedAt *time.Time
	// CollectedAt is the published build's claim time, so the data is at
	// least that old. ComputedAt is its publish time, the freshness clock.
	CollectedAt *time.Time
	ComputedAt  *time.Time
	ClaimedAt   *time.Time
	// NextEligibleAt is the earliest time another claim may be admitted: the
	// 30 s cooldown after a claim, or the backoff after failures. NULL means
	// eligible now; a '-infinity' column default would be rewritten by every
	// AutoMigrate, taking an exclusive lock at each startup.
	NextEligibleAt *time.Time
	LeaseEpoch     *int64
	// LeaseUntil is set while a build holds the lease; a partial index covers
	// the rows that have one, for the cluster-wide admission count.
	LeaseUntil   *time.Time `gorm:"index:idx_dashboard_snapshots_lease_until,where:lease_until IS NOT NULL"`
	FailedAt     *time.Time
	FailureCount int16 `gorm:"not null;default:0;type:smallint"`
	// Payload is the versioned JSON inventory payload. Payload and Health
	// together are at most 256 KiB.
	Payload []byte
}

func (DashboardSnapshot) TableName() string { return "dashboard_snapshots" }
