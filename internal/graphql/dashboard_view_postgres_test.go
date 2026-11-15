package graphql

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"magpie/internal/dashboard"
	"magpie/internal/domain"
	"magpie/internal/platform/pg"
	"magpie/internal/platform/schema"
	testdb "magpie/internal/testkit/db"

	gql "github.com/graphql-go/graphql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type snapshotReadLog struct {
	logger.Interface
	mu    sync.Mutex
	reads int
}

func (l *snapshotReadLog) Trace(_ context.Context, _ time.Time, fc func() (string, int64), _ error) {
	if sql, _ := fc(); strings.Contains(sql, "LEFT JOIN dashboard_snapshots") {
		l.mu.Lock()
		l.reads++
		l.mu.Unlock()
	}
}

func (l *snapshotReadLog) take() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	reads := l.reads
	l.reads = 0
	return reads
}

// Aliases of dashboardView share one resolution per request, so a client
// cannot stack one bounded wait per alias up to the field guard's limit.
func TestDashboardViewResolvesOncePerRequestPostgres(t *testing.T) {
	db := testdb.OpenPostgres(t)
	previous := pg.DB
	pg.DB = db
	t.Cleanup(func() { pg.DB = previous })
	if err := schema.EnsureDashboardSnapshotSchema(db); err != nil {
		t.Fatal(err)
	}
	user := domain.User{Email: "viewer@example.test", Password: "unused", Role: "user"}
	if err := db.Create(&user).Error; err != nil {
		t.Fatal(err)
	}
	workspace := domain.Workspace{Name: "Aliased"}
	if err := db.Create(&workspace).Error; err != nil {
		t.Fatal(err)
	}
	reads := &snapshotReadLog{Interface: logger.Default.LogMode(logger.Silent)}
	views := dashboard.NewService(db.Session(&gorm.Session{Logger: reads}), dashboard.Config{})
	defer views.Stop()
	built, err := NewSchema(views)
	if err != nil {
		t.Fatal(err)
	}
	run := func(query string) *gql.Result {
		t.Helper()
		ctx := WithRequestScope(WithWorkspaceAccess(WithUserID(context.Background(), user.ID), workspace.ID, "owner"))
		result := gql.Do(gql.Params{Schema: built, RequestString: query, Context: ctx})
		if len(result.Errors) > 0 {
			t.Fatal(result.Errors)
		}
		return result
	}
	// Publish a fresh snapshot, then read it through four aliases.
	run(`{ viewer { dashboardView { status } } }`)
	reads.take()
	result := run(`{ viewer {
		a: dashboardView { status info { totalChecks checksThisMonth totalScraped } }
		b: dashboardView { status proxyCount }
		c: dashboardView { recentProxyChecks(limit: 3) { id } fastestAliveProxies(limit: 3) { id } }
		d: dashboardView { checkerSettingsPending healthOutdated healthCollectedAt collectedAt }
	} }`)
	if got := reads.take(); got != 1 {
		t.Fatalf("four aliases ran %d snapshot reads, want 1", got)
	}
	viewer := result.Data.(map[string]interface{})["viewer"].(map[string]interface{})
	a := viewer["a"].(map[string]interface{})
	info := a["info"].(map[string]interface{})
	if a["status"] != "FRESH" || viewer["b"].(map[string]interface{})["status"] != "FRESH" || info["totalChecks"] != float64(0) || info["checksThisMonth"] != float64(0) {
		t.Fatalf("aliased views: %+v", viewer)
	}
	// A first build's health is current and was collected with the snapshot.
	d := viewer["d"].(map[string]interface{})
	if d["checkerSettingsPending"] != false || d["healthOutdated"] != false || d["healthCollectedAt"] == nil || d["healthCollectedAt"] != d["collectedAt"] {
		t.Fatalf("health flags: %+v", d)
	}
	// A workspace without routes has an empty, valid health part.
	for _, list := range []string{"recentProxyChecks", "fastestAliveProxies"} {
		if entries, ok := viewer["c"].(map[string]interface{})[list].([]interface{}); !ok || len(entries) != 0 {
			t.Fatalf("%s of an empty workspace is not an empty list: %#v", list, viewer["c"])
		}
	}
}
