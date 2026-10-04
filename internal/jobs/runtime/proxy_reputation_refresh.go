package runtime

import (
	"context"
	"sync"
	"time"

	"magpie/internal/database"
	"magpie/internal/support"

	"github.com/charmbracelet/log"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	reputationRefreshBacklog = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "magpie_proxy_reputation_refresh_pending", Help: "Persisted routes awaiting reputation refresh across all instances.",
	})
	reputationRefreshOldest = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "magpie_proxy_reputation_refresh_oldest_age_seconds", Help: "Age of the oldest persisted reputation refresh request.",
	})
)

func init() {
	prometheus.MustRegister(reputationRefreshBacklog, reputationRefreshOldest)
}

func reputationRefreshSettings() (workers, batch int, interval time.Duration) {
	workers = max(1, min(32, support.GetEnvInt("PROXY_REPUTATION_REFRESH_WORKERS", 4)))
	batch = max(1, min(5000, support.GetEnvInt("PROXY_REPUTATION_REFRESH_BATCH_SIZE", 1000)))
	interval = time.Duration(max(1, support.GetEnvInt("PROXY_REPUTATION_REFRESH_INTERVAL_SECONDS", 1))) * time.Second
	return
}

func runProxyReputationCoordinator(ctx context.Context) {
	workers, batch, interval := reputationRefreshSettings()
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				processed, err := refreshPendingProxyReputations(ctx, batch)
				if err != nil && ctx.Err() == nil {
					log.Error("Failed to refresh proxy reputations", "error", err)
				}
				if err == nil && processed > 0 {
					continue // Drain a backlog without a per-minute capacity ceiling.
				}
				timer := time.NewTimer(interval)
				select {
				case <-ctx.Done():
					timer.Stop()
				case <-timer.C:
				}
			}
		}()
	}
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	updateReputationRefreshMetrics(ctx)
	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return // Unfinished requests survive shutdown in PostgreSQL.
		case <-ticker.C:
			updateReputationRefreshMetrics(ctx)
		}
	}
}

func refreshPendingProxyReputations(ctx context.Context, batch int) (int, error) {
	claimCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	jobs, err := database.ClaimProxyReputationRefreshes(claimCtx, batch, 2*time.Minute)
	cancel()
	if err != nil || len(jobs) == 0 {
		return 0, err
	}
	proxyIDs := make([]uint64, len(jobs))
	for i, job := range jobs {
		proxyIDs[i] = job.ProxyID
	}
	recalcCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	err = database.RecalculateProxyReputations(recalcCtx, proxyIDs)
	cancel()
	// Release promptly on cancellation too; a failed release is recovered by
	// claim expiry. Never discard a request when recalculation fails.
	completeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if completeErr := database.CompleteProxyReputationRefreshes(completeCtx, jobs, err == nil); completeErr != nil {
		return len(jobs), completeErr
	}
	return len(jobs), err
}

func updateReputationRefreshMetrics(ctx context.Context) {
	metricCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	count, oldest, err := database.ProxyReputationRefreshBacklog(metricCtx)
	if err != nil {
		return
	}
	reputationRefreshBacklog.Set(float64(count))
	age := 0.0
	if !oldest.IsZero() {
		age = max(0, time.Since(oldest).Seconds())
	}
	reputationRefreshOldest.Set(age)
}
