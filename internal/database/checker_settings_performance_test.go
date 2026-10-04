package database

import (
	"context"
	"fmt"
	"magpie/internal/domain"
	"testing"
	"time"
)

// These measurements isolate attribution fanout in the existing persistence
// pipeline. Both variants use the same ownership/usage records and schema.
func TestCheckerEvidencePersistenceOperationBudgetPostgres(t *testing.T) {
	const routes = 1000
	for _, workspaces := range []int{1, 8} {
		t.Run(fmt.Sprint(workspaces), func(t *testing.T) {
			db := setupProxyIngestionPostgresTest(t)
			judge := domain.Judge{FullString: "http://127.0.0.1"}
			if err := db.Create(&judge).Error; err != nil {
				t.Fatal(err)
			}
			if err := db.Create(&domain.Protocol{ID: 1, Name: "http"}).Error; err != nil {
				t.Fatal(err)
			}
			wsIDs := []uint{}
			for i := 0; i < workspaces; i++ {
				w := domain.Workspace{Name: fmt.Sprint(i), CheckerGeneration: 1, CheckerHTTPKey: "current"}
				if err := db.Create(&w).Error; err != nil {
					t.Fatal(err)
				}
				wsIDs = append(wsIDs, w.ID)
			}
			proxies := make([]domain.Proxy, routes)
			for i := range proxies {
				proxies[i] = domain.Proxy{IP: fmt.Sprintf("192.0.%d.%d", i/256, i%256), Port: 8080}
			}
			if err := db.CreateInBatches(&proxies, 1000).Error; err != nil {
				t.Fatal(err)
			}
			ownership := make([]domain.ManagedProxy, 0, routes*workspaces)
			for _, p := range proxies {
				for _, id := range wsIDs {
					ownership = append(ownership, domain.ManagedProxy{WorkspaceID: id, ProxyID: p.ID})
				}
			}
			if err := db.CreateInBatches(&ownership, 1000).Error; err != nil {
				t.Fatal(err)
			}
			counter := &failureActionQueryCounter{Interface: db.Logger}
			db.Logger = counter
			var baselineCount, attributedCount int
			for _, attributed := range []bool{false, true} {
				total := time.Duration(0)
				maxQueries := 0
				for iteration := 0; iteration < 4; iteration++ {
					stats := make([]domain.ProxyStatistic, routes)
					now := time.Now().UTC()
					for i, p := range proxies {
						stats[i] = domain.ProxyStatistic{ProxyID: p.ID, ProtocolID: 1, JudgeID: judge.ID, Alive: true, TransportProtocol: "tcp", CheckTimeout: 1000, WorkspaceIDs: wsIDs, CreatedAt: now}
						if attributed {
							for _, id := range wsIDs {
								stats[i].CheckEvidence = append(stats[i].CheckEvidence, domain.WorkspaceCheckEvidence{WorkspaceID: id, ConfigKey: "current", Alive: true})
							}
						}
					}
					counter.queries = 0
					started := time.Now()
					if err := InsertProxyStatistics(context.Background(), stats, 1000); err != nil {
						t.Fatal(err)
					}
					elapsed := time.Since(started)
					if iteration > 0 {
						total += elapsed
						if counter.queries > maxQueries {
							maxQueries = counter.queries
						}
					}
				}
				t.Logf("workspaces=%d attributed=%t physical_events=%d statements/batch=%d mean_batch_ms=%.2f events/second=%.0f", workspaces, attributed, routes, maxQueries, float64(total.Microseconds())/3000, float64(routes*3)/total.Seconds())
				if attributed {
					attributedCount = maxQueries
				} else {
					baselineCount = maxQueries
				}
			}
			// Latest rows are chunked only when fanout crosses the parameter-safe batch.
			if workspaces == 1 && attributedCount != baselineCount-1 {
				t.Fatalf("one workspace statement budget changed: %d -> %d", baselineCount, attributedCount)
			}
			if workspaces == 8 && attributedCount-baselineCount != 2 {
				t.Fatalf("eight workspace chunk budget changed: %d -> %d", baselineCount, attributedCount)
			}
		})
	}
}
