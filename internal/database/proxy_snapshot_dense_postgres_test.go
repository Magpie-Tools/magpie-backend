package database

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"magpie/internal/domain"

	"gorm.io/gorm"
)

func TestSnapshotAliveCountsDenseOwnershipPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	const routes = 10000
	if err := db.Create(&domain.Protocol{ID: 1, Name: "http"}).Error; err != nil {
		t.Fatal(err)
	}
	ids := make([]uint, 8)
	for i := range ids {
		w := domain.Workspace{Name: fmt.Sprintf("Dense snapshot owner %d", i), CheckerGeneration: 1, CheckerHTTPKey: fmt.Sprintf("dense-%d", i)}
		if err := db.Create(&w).Error; err != nil {
			t.Fatal(err)
		}
		ids[i] = w.ID
	}
	for _, statement := range []struct {
		query string
		args  []any
	}{
		{`INSERT INTO proxies (id,host,port,country,estimated_type) SELECT n,'192.0.2.1',8080,'','' FROM generate_series(1,?) n`, []any{routes}},
		{`INSERT INTO user_proxies (workspace_id,proxy_id,state) SELECT w.id,n,'active' FROM workspaces w CROSS JOIN generate_series(1,?) n WHERE w.id IN ?`, []any{routes, ids}},
		{`INSERT INTO proxy_latest_statistics (workspace_id,config_key,transport_protocol,proxy_id,protocol_id,alive,statistic_id,response_time,checked_at)
SELECT w.id,w.checker_http_key,'tcp',n,1,TRUE,n,37,CURRENT_TIMESTAMP FROM workspaces w CROSS JOIN generate_series(1,?) n WHERE w.id IN ?`, []any{routes, ids}},
		{`ANALYZE user_proxies`, nil},
		{`ANALYZE proxy_latest_statistics`, nil},
	} {
		if err := db.Exec(statement.query, statement.args...).Error; err != nil {
			t.Fatal(err)
		}
	}
	for _, owners := range []int{1, 8} {
		t.Run(fmt.Sprintf("owners-%d", owners), func(t *testing.T) {
			queries := &checkerScaleQueryLogger{Interface: db.Logger}
			measured := db.Session(&gorm.Session{Logger: queries})
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			counts, err := aliveProxyCountByWorkspace(measured.WithContext(ctx), ids[:owners])
			if err != nil || len(counts) != owners {
				t.Fatalf("counts=%v error=%v", counts, err)
			}
			for _, id := range ids[:owners] {
				if counts[id] != routes {
					t.Fatalf("owner %d count=%d want=%d", id, counts[id], routes)
				}
			}
			if len(queries.queries) != 1 {
				t.Fatalf("snapshot count used %d queries", len(queries.queries))
			}
			var encoded string
			if err := db.WithContext(ctx).Raw("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + queries.queries[0]).Row().Scan(&encoded); err != nil {
				t.Fatal(err)
			}
			writeSnapshotEvidence(t, fmt.Sprintf("dense-%d-plan.json", owners), []byte(encoded))
			var plans []struct{ Plan map[string]any }
			if err := json.Unmarshal([]byte(encoded), &plans); err != nil {
				t.Fatal(err)
			}
			var ownershipLoops, ownershipRows float64
			var visit func(map[string]any)
			visit = func(node map[string]any) {
				if node["Relation Name"] == "user_proxies" {
					loops, _ := node["Actual Loops"].(float64)
					rows, _ := node["Actual Rows"].(float64)
					removed, _ := node["Rows Removed by Filter"].(float64)
					ownershipLoops += loops
					ownershipRows += (rows + removed) * loops
				}
				if children, ok := node["Plans"].([]any); ok {
					for _, child := range children {
						visit(child.(map[string]any))
					}
				}
			}
			visit(plans[0].Plan)
			if ownershipLoops > 32 || ownershipRows > routes*float64(owners) {
				t.Fatalf("dense count repeated ownership probes or read unrelated owners: loops=%v rows=%v", ownershipLoops, ownershipRows)
			}
		})
	}
}
