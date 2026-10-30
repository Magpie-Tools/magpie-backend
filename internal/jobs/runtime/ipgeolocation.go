package runtime

import (
	"context"
	"errors"
	"time"

	"magpie/internal/config"
	"magpie/internal/database"
	"magpie/internal/ipgeolocation"
	"magpie/internal/support"

	"github.com/charmbracelet/log"
)

func StartIPGeolocationRoutine(ctx context.Context) {
	err := support.RunWithLeader(ctx, "magpie:leader:ipgeolocation", support.DefaultLeadershipTTL, func(leaderCtx context.Context) {
		client := ipgeolocation.Client{}
		for leaderCtx.Err() == nil {
			wait, err := runIPGeolocationOnce(leaderCtx, client, time.Now().UTC())
			if err != nil && leaderCtx.Err() == nil {
				log.Warn("IPGeolocation.io background enrichment failed", "error", err)
			}
			// Poll configuration at most once a minute, while the persisted
			// next-request deadline still governs external requests.
			wait = max(time.Second, min(wait, time.Minute))
			select {
			case <-leaderCtx.Done():
				return
			case <-time.After(wait):
			}
		}
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Error("IPGeolocation.io routine stopped", "error", err)
	}
}

func runIPGeolocationOnce(ctx context.Context, client ipgeolocation.Client, now time.Time) (time.Duration, error) {
	cfg := config.GetConfig().Plugins.IPGeolocation
	if !cfg.Active() {
		return time.Minute, nil
	}
	state, err := database.GetIPGeolocationState(ctx)
	if err != nil {
		return time.Minute, err
	}
	// Discovery continues while cached IPs are due or the API budget is used
	// up. Refreshing cached IPs must not prevent discovery of other routes.
	if state.Policy != database.IPGeolocationPolicy(cfg) || state.NextDiscoveryAt == nil || !state.NextDiscoveryAt.After(now) {
		if _, err := database.DiscoverIPGeolocationIPs(ctx, cfg, now); err != nil {
			return time.Minute, err
		}
	}
	worked, err := database.ApplyIPGeolocationBatch(ctx, cfg)
	if err != nil {
		return time.Minute, err
	}
	if worked {
		return time.Second, nil
	}
	if state.NextRequestAt != nil && state.NextRequestAt.After(now) {
		return state.NextRequestAt.Sub(now), nil
	}
	if state.BudgetDay == now.Format("2006-01-02") && state.UsedCredits+ipgeolocation.Cost(cfg.IncludeSecurity) > int(cfg.DailyCreditLimit) {
		return now.Truncate(24 * time.Hour).Add(24 * time.Hour).Sub(now), nil
	}
	row, err := database.NextIPGeolocationLookup(ctx, cfg, now)
	if err != nil {
		return time.Minute, err
	}
	if row == nil {
		return time.Minute, nil
	}
	reserved, wait, err := database.ReserveIPGeolocationCredits(ctx, row.IP, now, cfg)
	if err != nil || !reserved {
		return wait, err
	}
	requestCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	response, lookupErr := client.Lookup(requestCtx, cfg.APIKey, row.IP, cfg.IncludeSecurity)
	cancel()
	current := config.GetConfig().Plugins.IPGeolocation
	if current.APIKey != cfg.APIKey || !current.Active() || database.IPGeolocationPolicy(current) != database.IPGeolocationPolicy(cfg) {
		lookupErr = errors.New("IPGeolocation.io settings changed during the lookup; result was not applied")
	}
	// Settle a possible charge even when leadership/shutdown canceled the
	// request. This short write uses no network requests to the provider.
	finishCtx, finishCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer finishCancel()
	if err := database.CompleteIPGeolocationLookup(finishCtx, row.IP, cfg, response, lookupErr, time.Now().UTC()); err != nil {
		return time.Minute, err
	}
	if lookupErr != nil {
		return time.Minute, lookupErr
	}
	return time.Second, nil // Apply the cached result before waiting for the next API slot.
}
