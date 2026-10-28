package graphql

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"magpie/internal/database"
	"magpie/internal/domain"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type graphQLProxyPageLog struct {
	logger.Interface
	counts int
}

func (l *graphQLProxyPageLog) Trace(_ context.Context, _ time.Time, sql func() (string, int64), _ error) {
	query, _ := sql()
	query = strings.ReplaceAll(query, "`", `"`)
	if strings.Contains(strings.ToLower(query), "count(*)") && strings.Contains(query, `"user_proxies"`) {
		l.counts++
	}
}

func TestBuildProxyPageReusesExactCountAndPreservesFailureCount(t *testing.T) {
	queryLog := &graphQLProxyPageLog{Interface: logger.Default}
	db, err := gorm.Open(sqlite.Open(fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name())), &gorm.Config{Logger: queryLog})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(&domain.User{}, &domain.Workspace{}, &domain.Proxy{}, &domain.ManagedProxy{},
		&domain.WorkspaceProxyFilterIndex{}, &domain.ProxyReputation{}, &domain.ProxyTag{}, &domain.ProxyTagAssignment{}); err != nil {
		t.Fatal(err)
	}
	previous := database.DB
	database.DB = db
	t.Cleanup(func() { database.DB = previous })
	for _, statement := range []string{
		`INSERT INTO workspaces (id,name,checker_dirty) VALUES (1,'GraphQL owner',false)`,
		`INSERT INTO proxies (id,host,port,country,estimated_type) VALUES (1,'192.0.2.1',8080,'N/A','N/A'),(2,'192.0.2.2',8080,'N/A','N/A'),(3,'192.0.2.3',8080,'N/A','N/A')`,
		`INSERT INTO user_proxies (workspace_id,proxy_id,state) VALUES (1,1,'active'),(1,2,'paused'),(1,3,'archived')`,
		`INSERT INTO proxy_reputations (proxy_id,kind,label,score) VALUES (1,'overall','unknown',0),(2,'overall','unknown',0),(3,'overall','unknown',0)`,
		`INSERT INTO user_proxy_filter_indexes (workspace_id,proxy_id,host,port,state,alive,latest_check) VALUES
(1,1,'192.0.2.1',8080,'active',true,'2026-10-20 12:00:00'),
(1,2,'192.0.2.2',8080,'paused',false,'2026-10-20 12:00:00'),
(1,3,'192.0.2.3',8080,'archived',false,'2026-10-20 12:00:00')`,
	} {
		if err := db.Exec(statement).Error; err != nil {
			t.Fatal(err)
		}
	}
	for _, test := range []struct{ page, size int }{{1, 3}, {2, 0}} {
		queryLog.counts = 0
		page := buildProxyPage(1, test.page)
		if page["totalCount"] != 3 || page["pageSize"] != test.size || queryLog.counts != 1 {
			t.Fatalf("page=%v count queries=%d", page, queryLog.counts)
		}
	}
	if err := db.Migrator().DropTable(&domain.WorkspaceProxyFilterIndex{}); err != nil {
		t.Fatal(err)
	}
	queryLog.counts = 0
	failed := buildProxyPage(1, 1)
	if failed["totalCount"] != 3 || failed["pageSize"] != 0 || queryLog.counts != 1 {
		t.Fatalf("failed projection read changed the ownership total: page=%v count queries=%d", failed, queryLog.counts)
	}
}
