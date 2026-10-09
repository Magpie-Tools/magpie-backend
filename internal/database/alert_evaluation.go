package database

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"gorm.io/gorm"
	"magpie/internal/domain"
)

const AlertWindow = 15 * time.Minute
const AlertMinimumSamples = 20
const AlertSustainDuration = 2 * time.Minute

type AlertObservation struct {
	Value         *float64
	Samples       int64
	UnknownReason string
}

type alertMetricKey struct {
	WorkspaceID uint
	ProtocolID  int
	Transport   string
}
type alertCheckAggregate struct {
	WorkspaceID  uint
	ProtocolID   int
	Transport    string
	Samples      int64
	Successes    int64
	LatencyTotal float64
}

// Aggregate once per pass, never once per rule or per proxy. No ownership join:
// failures from a route subsequently paused/deleted remain in the window.
// These are historical checker attempts, not current route-health verdicts.
func loadAlertCheckAggregates(tx *gorm.DB, workspaceIDs []uint, now time.Time) (map[alertMetricKey]alertCheckAggregate, error) {
	result := map[alertMetricKey]alertCheckAggregate{}
	if len(workspaceIDs) == 0 {
		return result, nil
	}
	evidence := "jsonb_to_recordset(COALESCE(ps.check_evidence, '[]'::jsonb)) AS e(workspace_id bigint, config_key text, alive boolean)"
	workspaceExpr, aliveExpr, keyExpr := "e.workspace_id", "e.alive", "e.config_key"
	if tx.Dialector.Name() == "sqlite" {
		evidence = "json_each(COALESCE(ps.check_evidence, '[]')) AS e"
		workspaceExpr, aliveExpr, keyExpr = "json_extract(e.value, '$.workspace_id')", "json_extract(e.value, '$.alive')", "json_extract(e.value, '$.config_key')"
	}
	var rows []alertCheckAggregate
	err := tx.Raw("SELECT "+workspaceExpr+" AS workspace_id, ps.protocol_id, ps.transport_protocol AS transport, COUNT(*) AS samples, SUM(CASE WHEN "+aliveExpr+" THEN 1 ELSE 0 END) AS successes, SUM(CASE WHEN "+aliveExpr+" THEN ps.response_time ELSE 0 END) AS latency_total FROM proxy_statistics ps CROSS JOIN "+evidence+" WHERE ps.created_at >= ? AND ps.created_at <= ? AND "+workspaceExpr+" IN ? AND COALESCE("+keyExpr+", '') <> '' GROUP BY "+workspaceExpr+", ps.protocol_id, ps.transport_protocol", now.Add(-AlertWindow), now, workspaceIDs).Scan(&rows).Error
	for _, row := range rows {
		result[alertMetricKey{row.WorkspaceID, row.ProtocolID, row.Transport}] = row
	}
	return result, err
}

type alertRotator struct {
	ID               uint64
	WorkspaceID      uint
	Name             string
	ProtocolID       int
	ReputationLabels domain.StringList
	UptimeFilterType string
	UptimePercentage *float64
}

// EvaluateAlerts reads existing projections/history and commits only rule state
// and incident transitions. It adds zero operations to individual proxy checks.
func EvaluateAlerts(ctx context.Context, now time.Time) error {
	config := defaultAlertEvaluationConfig()
	// SQLite has a single writer. Production PostgreSQL uses four workers.
	if DB.Dialector.Name() == "sqlite" {
		config.workers = 1
	}
	return evaluateAlerts(ctx, now, config)
}

type alertEvaluationConfig struct {
	workers            int
	aggregateTimeout   time.Duration
	measurementTimeout time.Duration
	workspaceTimeout   time.Duration
	stateTimeout       time.Duration
	loadAggregates     func(*gorm.DB, []uint, time.Time) (map[alertMetricKey]alertCheckAggregate, error)
	observeRoutes      func(*gorm.DB, domain.Workspace, *alertRotator, time.Time) AlertObservation
}

func defaultAlertEvaluationConfig() alertEvaluationConfig {
	return alertEvaluationConfig{workers: 4, aggregateTimeout: 15 * time.Second, measurementTimeout: 5 * time.Second, workspaceTimeout: 10 * time.Second, stateTimeout: 3 * time.Second,
		loadAggregates: loadAlertCheckAggregates}
}

type alertWorkspaceBatch struct {
	workspaceID uint
	attempt     *time.Time
	rules       []domain.AlertRule
}

// Rules arrive oldest-attempt first. Workspace batches follow the least
// recently attempted workspace, including unsuccessful attempts.
func fairAlertWorkspaceBatches(rules []domain.AlertRule) []*alertWorkspaceBatch {
	byWorkspace := map[uint]*alertWorkspaceBatch{}
	batches := []*alertWorkspaceBatch{}
	for _, rule := range rules {
		batch := byWorkspace[rule.WorkspaceID]
		if batch == nil {
			batch = &alertWorkspaceBatch{workspaceID: rule.WorkspaceID}
			byWorkspace[rule.WorkspaceID] = batch
			batches = append(batches, batch)
		}
		batch.rules = append(batch.rules, rule)
		if rule.LastEvaluationAttemptAt != nil && (batch.attempt == nil || rule.LastEvaluationAttemptAt.After(*batch.attempt)) {
			batch.attempt = rule.LastEvaluationAttemptAt
		}
	}
	sort.Slice(batches, func(i, j int) bool {
		a, b := batches[i], batches[j]
		if a.attempt == nil && b.attempt != nil {
			return true
		}
		if a.attempt != nil && b.attempt == nil {
			return false
		}
		if a.attempt != nil && !a.attempt.Equal(*b.attempt) {
			return a.attempt.Before(*b.attempt)
		}
		return a.workspaceID < b.workspaceID
	})
	return batches
}

func evaluateAlerts(ctx context.Context, now time.Time, config alertEvaluationConfig) error {
	tx := DB.WithContext(ctx)
	var rules []domain.AlertRule
	if err := tx.Where("enabled = ?", true).Order("CASE WHEN last_evaluation_attempt_at IS NULL THEN 0 ELSE 1 END, last_evaluation_attempt_at, id").Find(&rules).Error; err != nil {
		return err
	}
	if len(rules) == 0 {
		return nil
	}
	workspaceIDs, metricWorkspaceIDs := []uint{}, []uint{}
	seen, metricSeen := map[uint]bool{}, map[uint]bool{}
	for _, rule := range rules {
		if !seen[rule.WorkspaceID] {
			seen[rule.WorkspaceID] = true
			workspaceIDs = append(workspaceIDs, rule.WorkspaceID)
		}
		if rule.Metric != domain.AlertMetricUsableRoutes && !metricSeen[rule.WorkspaceID] {
			metricSeen[rule.WorkspaceID] = true
			metricWorkspaceIDs = append(metricWorkspaceIDs, rule.WorkspaceID)
		}
	}
	var workspaceRows []domain.Workspace
	if err := tx.Select("id", "name", "checker_dirty", "checker_generation").Where("id IN ?", workspaceIDs).Find(&workspaceRows).Error; err != nil {
		return err
	}
	workspaces := map[uint]domain.Workspace{}
	for _, workspace := range workspaceRows {
		workspaces[workspace.ID] = workspace
	}
	var rotatorRows []alertRotator
	if err := tx.Model(&domain.RotatingProxy{}).Select("id", "workspace_id", "name", "protocol_id", "reputation_labels", "uptime_filter_type", "uptime_percentage").Where("workspace_id IN ?", workspaceIDs).Scan(&rotatorRows).Error; err != nil {
		return err
	}
	rotators := map[uint64]alertRotator{}
	for _, row := range rotatorRows {
		rotators[row.ID] = row
	}
	// A history scan has its own deadline, leaving time to record Unknown and
	// evaluate route-count rules even if the scan times out.
	aggregateCtx, cancelAggregate := context.WithTimeout(ctx, config.aggregateTimeout)
	aggregates, aggregateErr := config.loadAggregates(DB.WithContext(aggregateCtx), metricWorkspaceIDs, now)
	cancelAggregate()
	totals := map[uint]alertCheckAggregate{}
	for key, aggregate := range aggregates {
		total := totals[key.WorkspaceID]
		total.Samples += aggregate.Samples
		total.Successes += aggregate.Successes
		total.LatencyTotal += aggregate.LatencyTotal
		totals[key.WorkspaceID] = total
	}
	// Whole-workspace counts share one read of each existing projection. Pool
	// filters still use their exact eligibility query, once per distinct scope.
	workspaceCounts := map[uint]AlertObservation{}
	var countErr error
	if config.observeRoutes == nil {
		countIDs := []uint{}
		countSeen := map[uint]bool{}
		for _, rule := range rules {
			if rule.Metric == domain.AlertMetricUsableRoutes && rule.RotatorID == nil && !countSeen[rule.WorkspaceID] {
				countIDs = append(countIDs, rule.WorkspaceID)
				countSeen[rule.WorkspaceID] = true
			}
		}
		countCtx, cancelCounts := context.WithTimeout(ctx, config.measurementTimeout)
		workspaceCounts, countErr = loadAlertWorkspaceRouteObservations(DB.WithContext(countCtx), countIDs, now)
		cancelCounts()
	}
	evaluateWorkspace := func(batch *alertWorkspaceBatch) error {
		// Claim the batch once, rather than writing an attempt per rule.
		attemptCtx, cancelAttempt := context.WithTimeout(ctx, config.stateTimeout)
		started, err := recordAlertEvaluationAttempts(attemptCtx, batch.rules, now)
		cancelAttempt()
		if err != nil || len(started) == 0 {
			return err
		}
		workspace, exists := workspaces[batch.workspaceID]
		if !exists {
			return nil
		}
		measurementCtx, cancelMeasurements := context.WithTimeout(ctx, config.workspaceTimeout)
		defer cancelMeasurements()
		counts := map[string]AlertObservation{}
		observations := make([]alertRuleObservation, 0, len(started))
		for _, rule := range batch.rules {
			if !started[rule.ID] {
				continue
			}
			input := alertRuleObservation{snapshot: rule, scopeName: workspace.Name}
			var rotator *alertRotator
			if rule.RotatorID != nil {
				row, ok := rotators[*rule.RotatorID]
				if !ok || row.WorkspaceID != rule.WorkspaceID {
					input.missingRotator = true
					observations = append(observations, input)
					continue
				}
				rotator, input.scopeName = &row, row.Name
			}
			observation := AlertObservation{}
			if workspace.CheckerDirty {
				observation.UnknownReason = "Checker settings are being applied"
			} else if rule.Metric == domain.AlertMetricUsableRoutes {
				if rotator == nil && config.observeRoutes == nil {
					input.observation = workspaceCounts[workspace.ID]
					if countErr != nil {
						input.observation = AlertObservation{UnknownReason: "Route measurements are unavailable"}
					}
					observations = append(observations, input)
					continue
				}
				cacheKey := "workspace"
				if rotator != nil {
					cacheKey = buildAliveProxyCacheKey(rotator.ProtocolID, rotator.ReputationLabels.Clone(), rotator.UptimeFilterType, rotator.UptimePercentage)
				}
				var cached bool
				observation, cached = counts[cacheKey]
				if !cached {
					if measurementCtx.Err() != nil {
						observation.UnknownReason = "Workspace route measurements timed out"
					} else {
						countCtx, cancelCount := context.WithTimeout(measurementCtx, config.measurementTimeout)
						observe := config.observeRoutes
						if observe == nil {
							observe = observeUsableAlertRoutes
						}
						observation = observe(DB.WithContext(countCtx), workspace, rotator, now)
						if countCtx.Err() != nil {
							observation = AlertObservation{UnknownReason: "Route measurements timed out"}
						}
						cancelCount()
					}
					counts[cacheKey] = observation
				}
			} else if aggregateErr != nil {
				observation.UnknownReason = "Check measurements are unavailable"
			} else {
				aggregate := totals[rule.WorkspaceID]
				if rotator != nil {
					aggregate = aggregates[alertMetricKey{rule.WorkspaceID, rotator.ProtocolID, "tcp"}]
				}
				observation.Samples = aggregate.Samples
				if rule.Metric == domain.AlertMetricLatency {
					observation.Samples = aggregate.Successes
				}
				if observation.Samples < AlertMinimumSamples {
					observation.UnknownReason = "Fewer than 20 check samples in the last 15 minutes"
				} else {
					value := 100 * float64(aggregate.Successes) / float64(aggregate.Samples)
					if rule.Metric == domain.AlertMetricLatency {
						value = aggregate.LatencyTotal / float64(aggregate.Successes)
					}
					observation.Value = &value
				}
			}
			input.observation = observation
			observations = append(observations, input)
		}
		cancelMeasurements()
		// Persist all of this workspace's observations and transitions together.
		stateCtx, cancelState := context.WithTimeout(ctx, config.stateTimeout)
		defer cancelState()
		return applyAlertObservations(stateCtx, workspace, observations, now)
	}
	batches := fairAlertWorkspaceBatches(rules)
	jobs := make(chan *alertWorkspaceBatch)
	var workers sync.WaitGroup
	var resultMu sync.Mutex
	var result error
	for i := 0; i < min(config.workers, len(batches)); i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for batch := range jobs {
				if ctx.Err() != nil {
					return
				}
				if err := evaluateWorkspace(batch); err != nil {
					resultMu.Lock()
					result = errors.Join(result, fmt.Errorf("evaluate alert workspace %d: %w", batch.workspaceID, err))
					resultMu.Unlock()
				}
			}
		}()
	}
dispatch:
	for _, batch := range batches {
		select {
		case <-ctx.Done():
			break dispatch
		case jobs <- batch:
		}
	}
	close(jobs)
	workers.Wait()
	return errors.Join(result, aggregateErr, countErr, ctx.Err())
}

func loadAlertWorkspaceRouteObservations(tx *gorm.DB, workspaceIDs []uint, now time.Time) (map[uint]AlertObservation, error) {
	result := map[uint]AlertObservation{}
	if len(workspaceIDs) == 0 {
		return result, nil
	}
	for _, id := range workspaceIDs {
		value := 0.0
		result[id] = AlertObservation{Value: &value}
	}
	var active []struct {
		WorkspaceID       uint
		CheckerGeneration uint64
	}
	if err := tx.Table("user_proxies up").Select("up.workspace_id, w.checker_generation").
		Joins("JOIN workspaces w ON w.id = up.workspace_id").
		Where("up.workspace_id IN ? AND up.state = ?", workspaceIDs, domain.ManagedProxyStateActive).
		Group("up.workspace_id, w.checker_generation").Scan(&active).Error; err != nil {
		return nil, fmt.Errorf("alert active routes: %w", err)
	}
	activeIDs := make([]uint, len(active))
	legacyIDs := []uint{}
	for i, row := range active {
		activeIDs[i] = row.WorkspaceID
		if row.CheckerGeneration == 0 {
			legacyIDs = append(legacyIDs, row.WorkspaceID)
		}
	}
	if len(activeIDs) == 0 {
		return result, nil
	}
	var counts []struct {
		WorkspaceID uint
		Samples     int64
		AliveCount  int64
	}
	// Split attributed and legacy evidence into keyed joins. The general current
	// evidence query joins by physical proxy before filtering its workspace; a
	// route shared by thousands of workspaces would produce a quadratic join.
	if err := tx.Raw(`SELECT workspace_id,
SUM(CASE WHEN checked_at >= ? AND checked_at <= ? THEN 1 ELSE 0 END) AS samples,
COUNT(DISTINCT CASE WHEN checker_generation > 0 AND alive THEN proxy_id END) AS alive_count
FROM (
SELECT up.workspace_id, up.proxy_id, pls.alive, pls.checked_at, w.checker_generation
FROM user_proxies up JOIN workspaces w ON w.id = up.workspace_id AND w.checker_generation > 0 AND NOT w.checker_dirty
JOIN proxy_latest_statistics pls ON pls.workspace_id = up.workspace_id AND pls.proxy_id = up.proxy_id
LEFT JOIN proxy_checker_plans cp ON cp.workspace_id = up.workspace_id AND cp.proxy_id = up.proxy_id AND cp.protocol_id = pls.protocol_id
WHERE up.workspace_id IN ? AND up.state = ? AND pls.config_key <> '' AND pls.config_key = COALESCE(cp.config_key,
CASE pls.protocol_id WHEN 1 THEN w.checker_http_key WHEN 2 THEN w.checker_https_key WHEN 3 THEN w.checker_socks4_key WHEN 4 THEN w.checker_socks5_key ELSE '' END)
UNION ALL
SELECT up.workspace_id, up.proxy_id, pls.alive, pls.checked_at, w.checker_generation
FROM user_proxies up JOIN workspaces w ON w.id = up.workspace_id AND w.checker_generation = 0 AND NOT w.checker_dirty
JOIN proxy_latest_statistics pls ON pls.workspace_id = 0 AND pls.proxy_id = up.proxy_id
WHERE up.workspace_id IN ? AND up.state = ?
) evidence GROUP BY workspace_id`, now.Add(-AlertWindow), now, activeIDs, domain.ManagedProxyStateActive, activeIDs, domain.ManagedProxyStateActive).
		Scan(&counts).Error; err != nil {
		return nil, fmt.Errorf("alert current route counts: %w", err)
	}
	samples := map[uint]int64{}
	alive := map[uint]int64{}
	for _, row := range counts {
		samples[row.WorkspaceID] = row.Samples
		alive[row.WorkspaceID] = row.AliveCount
	}
	if len(legacyIDs) > 0 {
		var legacy []struct {
			WorkspaceID uint
			AliveCount  int64
		}
		if err := tx.Table("user_proxies up").Select("up.workspace_id, COUNT(DISTINCT up.proxy_id) AS alive_count").
			Joins("JOIN workspaces w ON w.id = up.workspace_id AND w.checker_generation = 0 AND NOT w.checker_dirty").
			Joins("JOIN proxy_overall_statuses pos ON pos.proxy_id = up.proxy_id AND pos.overall_alive").
			Where("up.workspace_id IN ? AND up.state = ?", legacyIDs, domain.ManagedProxyStateActive).
			Group("up.workspace_id").Scan(&legacy).Error; err != nil {
			return nil, fmt.Errorf("alert legacy route counts: %w", err)
		}
		for _, row := range legacy {
			alive[row.WorkspaceID] = row.AliveCount
		}
	}
	for _, id := range activeIDs {
		if samples[id] == 0 {
			result[id] = AlertObservation{UnknownReason: "No current check evidence in the last 15 minutes"}
		} else {
			value := float64(alive[id])
			result[id] = AlertObservation{Value: &value, Samples: samples[id]}
		}
	}
	return result, nil
}

func observeUsableAlertRoutes(tx *gorm.DB, workspace domain.Workspace, rotator *alertRotator, now time.Time) AlertObservation {
	var active, recent int64
	if err := tx.Model(&domain.ManagedProxy{}).Where("workspace_id = ? AND state = ?", workspace.ID, domain.ManagedProxyStateActive).Count(&active).Error; err != nil {
		return AlertObservation{UnknownReason: "Route measurements are unavailable"}
	}
	if active == 0 {
		value := 0.0
		return AlertObservation{Value: &value}
	}
	freshness := currentLatestStatistics(tx, workspace.ID).Joins("JOIN user_proxies active_up ON active_up.workspace_id = pls.checking_workspace_id AND active_up.proxy_id = pls.proxy_id AND active_up.state = ?", domain.ManagedProxyStateActive).Where("pls.checked_at >= ? AND pls.checked_at <= ?", now.Add(-AlertWindow), now)
	if rotator != nil {
		freshness = freshness.Where("pls.protocol_id = ? AND pls.transport_protocol = ?", rotator.ProtocolID, "tcp")
	}
	if err := freshness.Count(&recent).Error; err != nil {
		return AlertObservation{UnknownReason: "Route measurements are unavailable"}
	}
	if recent == 0 {
		return AlertObservation{UnknownReason: "No current check evidence in the last 15 minutes"}
	}
	var count int64
	if rotator != nil {
		value, err := countAliveProxiesForProtocol(tx, workspace.ID, rotator.ProtocolID, rotator.ReputationLabels.Clone(), rotator.UptimeFilterType, rotator.UptimePercentage)
		if err != nil {
			return AlertObservation{UnknownReason: "Route measurements are unavailable"}
		}
		count = int64(value)
	} else {
		counts, err := aliveProxyCountByWorkspace(tx, []uint{workspace.ID})
		if err != nil {
			return AlertObservation{UnknownReason: "Route measurements are unavailable"}
		}
		count = counts[workspace.ID]
	}
	value := float64(count)
	return AlertObservation{Value: &value, Samples: recent}
}
