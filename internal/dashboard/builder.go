package dashboard

import (
	"context"
	"errors"
	"sync"

	"magpie/internal/domain"
	"magpie/internal/evidence"

	"golang.org/x/sync/errgroup"
	"gorm.io/gorm"
)

var errBuildExpired = errors.New("dashboard: build deadline expired")

// built is the outcome of one build: the payload, the checker generation
// read after the claim, and whether the health parts belong to it.
type built struct {
	payload     payload
	generation  uint64
	healthValid bool
}

// build runs the existing dashboard SQL for one workspace on ctx, which
// carries the build deadline. Each data statement runs in its own read-only
// transaction with a statement timeout capped at the remaining budget, at
// most statementConcurrency at once. An expired build issues no further data
// SQL, and every branch is joined before build returns.
func (st *Store) build(ctx context.Context, workspaceID uint, statementConcurrency int) (built, error) {
	generation, dirty, err := st.workspaceVersion(ctx, workspaceID)
	if err != nil {
		return built{}, err
	}
	var (
		mu        sync.Mutex
		inventory inventoryPart
		health    healthPart
	)
	statements := []func(*gorm.DB) error{
		func(tx *gorm.DB) error {
			summary, err := loadDashboardInventorySummary(tx, workspaceID)
			if err != nil {
				return err
			}
			mu.Lock()
			defer mu.Unlock()
			inventory.CountryBreakdown, inventory.ReputationBreakdown, inventory.TopReputationProxy = inventoryParts(summary)
			return nil
		},
		func(tx *gorm.DB) error {
			// The latest scraped snapshot, or the current scraped route count
			// when the workspace has none yet, in one statement. COALESCE
			// evaluates the fallback only when no snapshot exists.
			var total int64
			if err := tx.Raw(`SELECT COALESCE(
	(SELECT ps.count FROM proxy_snapshots ps WHERE ps.workspace_id = ? AND ps.metric = ?
	  ORDER BY ps.created_at DESC, ps.id DESC LIMIT 1),
	(SELECT COUNT(*) FROM user_proxies up WHERE up.workspace_id = ? AND up.state = ?
	    AND EXISTS (SELECT 1 FROM proxy_scrape_site s WHERE s.proxy_id = up.proxy_id))
) AS total_scraped`, workspaceID, domain.ProxySnapshotMetricScraped, workspaceID, domain.ManagedProxyStateActive).Scan(&total).Error; err != nil {
				return err
			}
			mu.Lock()
			defer mu.Unlock()
			inventory.TotalScraped = total
			return nil
		},
		func(tx *gorm.DB) error {
			var owned int64
			if err := tx.Model(&domain.ManagedProxy{}).Where("workspace_id = ?", workspaceID).Count(&owned).Error; err != nil {
				return err
			}
			mu.Lock()
			defer mu.Unlock()
			inventory.ProxyCount = owned
			return nil
		},
	}
	// A dirty workspace masks health, so its health statements are skipped.
	if !dirty {
		statements = append(statements,
			func(tx *gorm.DB) error {
				rows, err := queryJudgeBreakdown(tx, workspaceID)
				if err != nil {
					return err
				}
				judges := make([]judgeCount, 0, len(rows))
				for _, row := range rows {
					judges = append(judges, judgeCount{JudgeURL: row.JudgeUrl, EliteProxies: row.EliteProxies, AnonymousProxies: row.AnonymousProxies, TransparentProxies: row.TransparentProxies})
				}
				mu.Lock()
				defer mu.Unlock()
				health.JudgeValidProxies = judges
				return nil
			},
			func(tx *gorm.DB) error {
				counts, err := evidence.AliveProxyCountByWorkspace(tx, []uint{workspaceID})
				if err != nil {
					return err
				}
				mu.Lock()
				defer mu.Unlock()
				health.AliveCount = counts[workspaceID]
				return nil
			},
			func(tx *gorm.DB) error {
				rows, err := queryRecentProxyChecks(tx, workspaceID, recentCandidateLimit)
				if err != nil {
					return err
				}
				candidates := make([]recentCandidate, 0, len(rows))
				for _, row := range rows {
					candidate := recentCandidate{Ordinal: row.Ordinal, ID: row.ID, IP: row.IPAddress, Port: row.Port, ResponseTime: row.ResponseTime, Alive: row.Alive}
					if row.LatestCheck.Valid {
						latest := row.LatestCheck.Time
						candidate.LatestCheck = &latest
					}
					candidates = append(candidates, candidate)
				}
				mu.Lock()
				defer mu.Unlock()
				health.RecentCandidates = candidates
				return nil
			},
			func(tx *gorm.DB) error {
				fastest, err := queryFastestAliveProxies(tx, workspaceID, fastestLimit)
				if err != nil {
					return err
				}
				mu.Lock()
				defer mu.Unlock()
				health.Fastest = fastest
				return nil
			},
		)
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(max(1, statementConcurrency))
	for _, statement := range statements {
		group.Go(func() error {
			if err := groupCtx.Err(); err != nil {
				return err
			}
			return st.readOnly(groupCtx, statement)
		})
	}
	if err := group.Wait(); err != nil {
		return built{}, err
	}
	if err := ctx.Err(); err != nil {
		return built{}, err
	}
	after, dirtyAfter, err := st.workspaceVersion(ctx, workspaceID)
	if err != nil {
		return built{}, err
	}
	result := built{payload: payload{Inventory: inventory}, generation: generation, healthValid: !dirty && !dirtyAfter && after == generation}
	if result.healthValid {
		result.payload.Health = &health
	}
	return result, nil
}

// inventoryParts maps the inventory summary exactly as the info loader does.
func inventoryParts(summary dashboardInventorySummary) ([]countryCount, reputationBreakdown, *topReputationProxy) {
	countries := make([]countryCount, 0, len(summary.Countries))
	for _, row := range summary.Countries {
		countries = append(countries, countryCount{Country: normalizeDashboardCountry(row.Country), Count: row.Count})
	}
	var reputations reputationBreakdown
	for _, row := range summary.Reputations {
		switch row.Label {
		case "good":
			reputations.Good = row.Count
		case "neutral":
			reputations.Neutral = row.Count
		case "poor":
			reputations.Poor = row.Count
		default:
			reputations.Unknown += row.Count
		}
	}
	var top *topReputationProxy
	if summary.TopProxyFound {
		top = &topReputationProxy{ProxyID: summary.TopProxy.ProxyID, IP: summary.TopProxy.IP, Port: uint16(summary.TopProxy.Port), Score: summary.TopProxy.Score, Label: summary.TopProxy.Label}
	}
	return countries, reputations, top
}
