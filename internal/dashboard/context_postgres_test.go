package dashboard

import (
	"strings"
	"testing"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestDashboardFastestMetadataIsBoundedPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	workspaces := seedCurrentLatestReadTest(t, db)
	for _, query := range []string{
		`INSERT INTO proxies (id,host,port,country,estimated_type) SELECT n,'198.51.100.1',8080,'DE','residential' FROM generate_series(15,10014) n`,
		`INSERT INTO proxy_reputations (proxy_id,kind,score,label) SELECT n,'overall',50,'neutral' FROM generate_series(15,10014) n`,
		`ANALYZE proxies`, `ANALYZE proxy_reputations`, `ANALYZE proxy_latest_statistics`, `ANALYZE user_proxies`,
	} {
		if err := db.Exec(query).Error; err != nil {
			t.Fatal(err)
		}
	}
	queryLog := &proxyPageReadLogger{Interface: logger.Default}
	const limit = 3
	rows, err := queryFastestAliveProxies(db.Session(&gorm.Session{Logger: queryLog}), workspaces[0].ID, limit)
	if err != nil || len(rows) != limit {
		t.Fatalf("fastest candidates=%+v error=%v", rows, err)
	}
	var generated string
	for _, query := range queryLog.queries {
		if strings.HasPrefix(query, "WITH candidates AS MATERIALIZED") {
			generated = query
		}
	}
	if generated == "" {
		t.Fatal("production fastest SQL did not materialize the bounded candidates", queryLog.queries)
	}
	plan := explainProxyRead(t, db, "dashboard-fastest-production-plan.json", generated)
	metadataLookups := make(map[string]float64)
	visitProxyReadPlan(plan, func(node map[string]any) {
		relation, _ := node["Relation Name"].(string)
		if relation == "proxies" || relation == "proxy_reputations" {
			if node["Node Type"] == "Seq Scan" {
				t.Fatal("metadata enrichment scanned the unbounded route/reputation table", node)
			}
			metadataLookups[relation] += node["Actual Loops"].(float64)
		}
	})
	for _, relation := range []string{"proxies", "proxy_reputations"} {
		if metadataLookups[relation] == 0 || metadataLookups[relation] > limit {
			t.Fatalf("%s lookups=%.0f exceed selected candidate limit=%d", relation, metadataLookups[relation], limit)
		}
	}
	t.Logf("actual production query limit=%d metadata lookups=%v; unrelated routes/reputations=10000", limit, metadataLookups)
}
