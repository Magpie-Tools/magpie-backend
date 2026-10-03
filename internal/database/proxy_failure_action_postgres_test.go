package database

import (
	"context"
	"fmt"
	"testing"
	"time"

	"magpie/internal/domain"

	"gorm.io/gorm/logger"
)

type failureActionQueryCounter struct {
	logger.Interface
	queries int
}

func (counter *failureActionQueryCounter) Trace(context.Context, time.Time, func() (string, int64), error) {
	counter.queries++
}

// Run with MAGPIE_TEST_POSTGRES_DSN. Each case uses an isolated schema and the
// production read models, source statistics, usage records, and tag cascades.
func TestFailureActionsPostgresLifecycleAndOperationBudget(t *testing.T) {
	const routeCount = 10000
	const transitions = 32
	queriesByAction := make(map[string]int)
	for _, action := range []string{domain.FailureActionPause, domain.FailureActionDelete} {
		t.Run(action, func(t *testing.T) {
			db := setupProxyIngestionPostgresTest(t)
			workspace := domain.Workspace{Name: "Failure actions", FailureAction: action}
			if err := db.Create(&workspace).Error; err != nil {
				t.Fatal(err)
			}
			if err := db.Create(&domain.WorkspaceSubscription{WorkspaceID: workspace.ID, OverageMode: domain.WorkspaceOverageUnlimited}).Error; err != nil {
				t.Fatal(err)
			}
			site := domain.ScrapeSite{URL: "https://failure-actions.example.test/proxies"}
			if err := db.Create(&site).Error; err != nil {
				t.Fatal(err)
			}
			if err := db.Create(&domain.WorkspaceScrapeSite{WorkspaceID: workspace.ID, ScrapeSiteID: site.ID, FetchMode: domain.ScrapeFetchHTTP}).Error; err != nil {
				t.Fatal(err)
			}
			input := make([]domain.Proxy, routeCount)
			for index := range input {
				input[index] = domain.Proxy{IP: fmt.Sprintf("10.10.%d.%d", index>>8, index&255), Port: 8080}
			}
			proxies, err := InsertAndGetProxiesWithWorkspace(input, workspace.ID)
			if err != nil {
				t.Fatal(err)
			}
			if len(proxies[0].Workspaces) != 1 || proxies[0].Workspaces[0].FailureAction != action {
				t.Fatal("checker hydration lost failure action")
			}
			if err := AssociateProxiesToScrapeSite(site.ID, proxies); err != nil {
				t.Fatal(err)
			}
			tag := domain.ProxyTag{WorkspaceID: workspace.ID, Name: "Failure test"}
			if err := db.Create(&tag).Error; err != nil {
				t.Fatal(err)
			}
			assignments := make([]domain.ProxyTagAssignment, transitions)
			for index := range assignments {
				assignments[index] = domain.ProxyTagAssignment{WorkspaceID: workspace.ID, ProxyID: proxies[index].ID, ProxyTagID: tag.ID}
			}
			if err := db.Create(&assignments).Error; err != nil {
				t.Fatal(err)
			}
			if err := db.Model(&domain.ManagedProxy{}).Where("workspace_id = ?", workspace.ID).Update("consecutive_failures", 2).Error; err != nil {
				t.Fatal(err)
			}
			counter := &failureActionQueryCounter{Interface: db.Logger}
			db.Logger = counter
			started := time.Now()
			for _, proxy := range proxies[:transitions] {
				var changed bool
				var inactive []domain.Proxy
				if action == domain.FailureActionDelete {
					changed, inactive, err = DeleteActiveManagedProxy(workspace.ID, proxy.ID)
				} else {
					changed, inactive, err = PauseManagedProxy(workspace.ID, proxy.ID, domain.ManagedProxyPauseReasonFailure)
				}
				if err != nil || !changed || len(inactive) != 1 {
					t.Fatalf("action=%s changed=%v inactive=%d error=%v", action, changed, len(inactive), err)
				}
			}
			elapsed := time.Since(started)
			queriesByAction[action] = counter.queries
			db.Logger = counter.Interface
			t.Logf("%s: %d routes, %d transitions, %d SQL statements, %.2f ms/transition", action, routeCount, transitions, counter.queries, float64(elapsed.Microseconds())/1000/transitions)
			var usage domain.WorkspaceUsagePeriod
			if err := db.First(&usage, "workspace_id = ?", workspace.ID).Error; err != nil {
				t.Fatal(err)
			}
			if usage.ActiveRoutes != routeCount-transitions {
				t.Fatalf("active usage = %d", usage.ActiveRoutes)
			}
			var stat domain.WorkspaceScrapeSourceStat
			if err := db.First(&stat, "workspace_id = ? AND scrape_site_id = ?", workspace.ID, site.ID).Error; err != nil {
				t.Fatal(err)
			}
			var indexCount, tagCount int64
			if err := db.Model(&domain.WorkspaceProxyFilterIndex{}).Where("workspace_id = ?", workspace.ID).Count(&indexCount).Error; err != nil {
				t.Fatal(err)
			}
			if err := db.Model(&domain.ProxyTagAssignment{}).Where("workspace_id = ?", workspace.ID).Count(&tagCount).Error; err != nil {
				t.Fatal(err)
			}
			wantStored, wantTags := int64(routeCount), int64(transitions)
			if action == domain.FailureActionDelete {
				wantStored -= transitions
				wantTags = 0
			}
			if int64(stat.ProxyCount) != wantStored || indexCount != wantStored || tagCount != wantTags {
				t.Fatalf("read models after %s: source=%d index=%d tags=%d", action, stat.ProxyCount, indexCount, tagCount)
			}
			if _, err := InsertAndGetProxiesWithWorkspace(input[:1], workspace.ID); err != nil {
				t.Fatal(err)
			}
			var managed domain.ManagedProxy
			if err := db.First(&managed, "workspace_id = ? AND proxy_id = ?", workspace.ID, proxies[0].ID).Error; err != nil {
				t.Fatal(err)
			}
			wantState, wantFailures := domain.ManagedProxyStatePaused, uint16(2)
			if action == domain.FailureActionDelete {
				wantState, wantFailures = domain.ManagedProxyStateActive, 0
			}
			if managed.State != wantState || managed.ConsecutiveFailures != wantFailures {
				t.Fatalf("reimport after %s: state=%s failures=%d", action, managed.State, managed.ConsecutiveFailures)
			}
		})
	}
	if len(queriesByAction) == 2 && queriesByAction[domain.FailureActionDelete] > queriesByAction[domain.FailureActionPause] {
		t.Fatalf("delete added lifecycle queries: pause=%d delete=%d", queriesByAction[domain.FailureActionPause], queriesByAction[domain.FailureActionDelete])
	}
}
