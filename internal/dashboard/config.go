package dashboard

import (
	"strings"
	"time"

	"magpie/internal/support"

	"github.com/charmbracelet/log"
)

// Environment variables that tune snapshot builds. The defaults are the
// agreed starting values; ADR 0017 sets them from measurements.
const (
	envBuildDeadline             = "DASHBOARD_BUILD_DEADLINE"
	envBuildClusterLimit         = "DASHBOARD_BUILD_CLUSTER_LIMIT"
	envBuildInstanceLimit        = "DASHBOARD_BUILD_INSTANCE_LIMIT"
	envBuildStatementConcurrency = "DASHBOARD_BUILD_STATEMENT_CONCURRENCY"
)

const (
	defaultBuildDeadline             = 30 * time.Second
	defaultBuildClusterLimit         = 4
	defaultBuildInstanceLimit        = 2
	defaultBuildStatementConcurrency = 2

	// freshFor is how long a published snapshot is served without a claim.
	freshFor = 30 * time.Second
	// staleFor is the oldest snapshot served as STALE; older ones are
	// served as PENDING while a rebuild is attempted.
	staleFor = 10 * time.Minute
	// claimCooldown spaces two claims of one workspace across the cluster.
	claimCooldown = 30 * time.Second
	// leaseGrace keeps a lease past the build deadline, so a build that
	// ends at its deadline can still publish or record its failure.
	leaseGrace = 15 * time.Second
	// maxWait bounds how long a request waits for a build.
	maxWait = 2 * time.Second
	// pollInterval is how often a waiting request re-reads a row whose
	// lease another instance holds.
	pollInterval = 250 * time.Millisecond
	// lockTimeout bounds every wait for a row or the admission lock.
	lockTimeout = "250ms"
	// maxPayloadBytes is the hard cap of one encoded payload.
	maxPayloadBytes = 256 << 10
	// retainFor is how long an untouched snapshot row is kept.
	retainFor = 7 * 24 * time.Hour
	// retentionBatch bounds one retention delete.
	retentionBatch = 500
)

// Config tunes snapshot builds.
type Config struct {
	// BuildDeadline bounds one build, independently of the HTTP write
	// timeout. The lease lasts BuildDeadline plus 15 s.
	BuildDeadline time.Duration
	// ClusterLimit is the most leases admitted at once across all instances.
	ClusterLimit int
	// InstanceLimit is the most builds one instance runs at once.
	InstanceLimit int
	// StatementConcurrency is the most data statements one build runs at once.
	StatementConcurrency int
}

// ConfigFromEnv reads Config from the environment, falling back to the
// defaults for unset or invalid values.
func ConfigFromEnv() Config {
	cfg := Config{
		BuildDeadline:        defaultBuildDeadline,
		ClusterLimit:         positiveEnvInt(envBuildClusterLimit, defaultBuildClusterLimit),
		InstanceLimit:        positiveEnvInt(envBuildInstanceLimit, defaultBuildInstanceLimit),
		StatementConcurrency: positiveEnvInt(envBuildStatementConcurrency, defaultBuildStatementConcurrency),
	}
	if raw := strings.TrimSpace(support.GetEnv(envBuildDeadline, "")); raw != "" {
		if deadline, err := time.ParseDuration(raw); err == nil && deadline > 0 {
			cfg.BuildDeadline = deadline
		} else {
			log.Warn("Invalid dashboard build deadline; using default", "env", envBuildDeadline, "value", raw, "default", defaultBuildDeadline)
		}
	}
	return cfg
}

func positiveEnvInt(key string, fallback int) int {
	if value := support.GetEnvInt(key, fallback); value > 0 {
		return value
	}
	log.Warn("Invalid dashboard build limit; using default", "env", key, "default", fallback)
	return fallback
}

func (cfg Config) withDefaults() Config {
	if cfg.BuildDeadline <= 0 {
		cfg.BuildDeadline = defaultBuildDeadline
	}
	if cfg.ClusterLimit <= 0 {
		cfg.ClusterLimit = defaultBuildClusterLimit
	}
	if cfg.InstanceLimit <= 0 {
		cfg.InstanceLimit = defaultBuildInstanceLimit
	}
	if cfg.StatementConcurrency <= 0 {
		cfg.StatementConcurrency = defaultBuildStatementConcurrency
	}
	return cfg
}
