package database

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"magpie/internal/api/dto"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestProxyPageAndCountUseWorkspaceIndexesPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	if err := ensureReadModelSchema(db); err != nil {
		t.Fatal(err)
	}
	if err := ensureProxyListPerformanceSchema(db); err != nil {
		t.Fatal(err)
	}
	const routes = 10000
	for _, statement := range []string{
		`INSERT INTO workspaces (id,name) SELECT n,'Page owner '||n FROM generate_series(1,8) n`,
		fmt.Sprintf(`INSERT INTO proxies (id,host,port,country,estimated_type) SELECT n,'192.0.2.1',8080,'DE','residential' FROM generate_series(1,%d) n`, routes),
		fmt.Sprintf(`INSERT INTO user_proxies (workspace_id,proxy_id,state)
SELECT w,n,(ARRAY['active','paused','archived'])[1+n%%3]
FROM generate_series(1,8) w CROSS JOIN generate_series(1,%d) n`, routes),
		fmt.Sprintf(`INSERT INTO user_proxy_filter_indexes
(workspace_id,proxy_id,host,port,state,alive,latest_check,response_time,health_overall,country,estimated_type,anonymity_level)
SELECT w,n,'192.0.2.1',8080,(ARRAY['active','paused','archived'])[1+n%%3],n%%8=0,
'2026-10-01'::timestamptz+(n/32)*interval '1 second',37,80,
CASE WHEN w=1 THEN CASE WHEN n%%2=0 THEN 'DE' ELSE 'de' END ELSE 'other-owner' END,'residential','elite'
FROM generate_series(1,8) w CROSS JOIN generate_series(1,%d) n`, routes),
		`VACUUM (ANALYZE) user_proxies`,
		`VACUUM (ANALYZE) user_proxy_filter_indexes`,
		`ANALYZE workspaces`,
	} {
		if err := db.Exec(statement).Error; err != nil {
			t.Fatal(err)
		}
	}
	queryLog := &proxyPageReadLogger{Interface: logger.Default}
	DB = db.Session(&gorm.Session{Logger: queryLog})
	var got []int
	for page := 1; page <= 2; page++ {
		rows, total := GetProxyInfoPageWithFiltersAndOptions(1, page, 40, "", dto.ProxyListFilters{}, ProxyPageQueryOptions{})
		if total != routes || len(rows) != 40 {
			t.Fatalf("page=%d rows=%d total=%d", page, len(rows), total)
		}
		for _, row := range rows {
			got = append(got, row.Id)
		}
	}
	want := make([]int, routes)
	for i := range want {
		want[i] = i + 1
	}
	sort.Slice(want, func(i, j int) bool {
		a, b := want[i], want[j]
		if (a%8 == 0) != (b%8 == 0) {
			return a%8 == 0
		}
		if a/32 != b/32 {
			return a/32 > b/32
		}
		return a < b
	})
	if !reflect.DeepEqual(got, want[:80]) {
		t.Fatalf("default pagination changed ordering or repeats ties: got=%v want=%v", got, want[:80])
	}
	var pageQuery, countQuery string
	for _, query := range queryLog.queries {
		if pageQuery == "" && strings.Contains(query, "ufi.proxy_id AS id") {
			pageQuery = query
		}
		if countQuery == "" && strings.Contains(query, `FROM "user_proxies"`) && strings.Contains(query, "count(*)") {
			countQuery = query
		}
	}
	if pageQuery == "" || countQuery == "" {
		t.Fatal("did not capture production page/count queries", queryLog.queries)
	}
	pagePlan := explainProxyRead(t, db, "page-plan.json", pageQuery)
	pageIndex, filterRows := false, float64(0)
	visitProxyReadPlan(pagePlan, func(node map[string]any) {
		if node["Index Name"] == "idx_workspace_proxy_filter_page" {
			pageIndex = true
		}
		if node["Node Type"] == "Sort" {
			t.Fatal("default page sorted the complete workspace inventory", pagePlan)
		}
		if node["Relation Name"] == "user_proxy_filter_indexes" {
			filterRows += node["Actual Rows"].(float64) * node["Actual Loops"].(float64)
		}
	})
	if !pageIndex || filterRows > 40 {
		t.Fatalf("page index=%v rows visited=%.0f want<=40", pageIndex, filterRows)
	}
	countPlan := explainProxyRead(t, db, "count-plan.json", countQuery)
	var ownershipRows float64
	visitProxyReadPlan(countPlan, func(node map[string]any) {
		if node["Relation Name"] == "proxies" {
			t.Fatal("exact count read the shared proxy inventory", countPlan)
		}
		if node["Relation Name"] == "user_proxies" {
			ownershipRows += node["Actual Rows"].(float64) * node["Actual Loops"].(float64)
		}
	})
	if ownershipRows > routes {
		t.Fatalf("count expanded other owners: rows=%.0f want<=%d", ownershipRows, routes)
	}
	// The previous DISTINCT readers and the indexed successor query both use
	// the database's native collation. Compare their ordered values directly:
	// en_US and C can order the same case-sensitive strings differently.
	loadReference := func(column string) []string {
		t.Helper()
		var rows []proxyFilterValueRow
		if err := db.Table("user_proxy_filter_indexes ufi").
			Select("DISTINCT COALESCE(NULLIF("+column+", ''), 'N/A') AS value").
			Where("ufi.workspace_id = ?", 1).
			Order("value").
			Scan(&rows).Error; err != nil {
			t.Fatal(err)
		}
		values := make([]string, 0, len(rows))
		for _, row := range rows {
			if strings.TrimSpace(row.Value) != "" {
				values = append(values, row.Value)
			}
		}
		return values
	}
	wantCountries := loadReference("ufi.country")
	wantTypes := loadReference("ufi.estimated_type")
	wantAnonymity := loadReference("ufi.anonymity_level")
	queryLog.queries = nil
	options, err := GetProxyFilterOptions(1)
	if err != nil || !reflect.DeepEqual(options.Countries, wantCountries) || !reflect.DeepEqual(options.Types, wantTypes) || !reflect.DeepEqual(options.AnonymityLevels, wantAnonymity) {
		t.Fatalf("production options=%+v differ from native-collation DISTINCT countries=%v types=%v anonymity=%v error=%v", options, wantCountries, wantTypes, wantAnonymity, err)
	}
	countries := make(map[string]bool, len(options.Countries))
	for _, country := range options.Countries {
		countries[country] = true
	}
	if len(options.Countries) != 2 || !countries["DE"] || !countries["de"] || countries["other-owner"] || !reflect.DeepEqual(options.Types, []string{"residential"}) || !reflect.DeepEqual(options.AnonymityLevels, []string{"elite"}) {
		t.Fatal("filter options lost raw mixed-case values or leaked another owner's values", options)
	}
	t.Logf("native-collation DISTINCT countries=%v; production countries=%v; both raw case variants retained; other-owner excluded", wantCountries, options.Countries)
	if len(queryLog.queries) == 0 || !strings.HasPrefix(queryLog.queries[0], "WITH RECURSIVE") {
		t.Fatal("did not capture production option seeks", queryLog.queries)
	}
	optionPlan := explainProxyRead(t, db, "filter-production-plan.json", queryLog.queries[0])
	optionRows := float64(0)
	visitProxyReadPlan(optionPlan, func(node map[string]any) {
		if node["Relation Name"] == "user_proxy_filter_indexes" {
			if node["Node Type"] == "Seq Scan" {
				t.Fatal("production filter read scanned the heap", optionPlan)
			}
			optionRows += node["Actual Rows"].(float64) * node["Actual Loops"].(float64)
		}
	})
	if optionRows > 14 {
		t.Fatalf("production option read visited %.0f rows for four values", optionRows)
	}
	t.Logf("routes=%d owners=8 page_rows=%.0f count_rows=%.0f option_rows=%.0f", routes, filterRows, ownershipRows, optionRows)
}

func TestProxyListPerformanceSchemaRepairsInterruptedIndexesPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	if err := db.Exec(`INSERT INTO user_proxy_filter_indexes (workspace_id,proxy_id,port) VALUES (1,1,8080),(1,2,8080)`).Error; err != nil {
		t.Fatal(err)
	}
	names := []string{"idx_workspace_proxy_filter_page", "idx_workspace_proxy_filter_option_country", "idx_workspace_proxy_filter_option_type", "idx_workspace_proxy_filter_option_anonymity"}
	for _, name := range names {
		// A failed concurrent UNIQUE build leaves the same invalid catalog state
		// as an interrupted concurrent build, without timing a cancellation.
		if err := db.Exec("CREATE UNIQUE INDEX CONCURRENTLY " + name + " ON user_proxy_filter_indexes (country)").Error; err == nil {
			t.Fatal("fixture did not leave an invalid concurrent index", name)
		}
	}
	var invalid int64
	if err := db.Raw(`SELECT count(*) FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid WHERE NOT i.indisvalid AND c.relname IN ?`, names).Scan(&invalid).Error; err != nil || invalid != 4 {
		t.Fatalf("invalid fixture indexes=%d error=%v", invalid, err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		if err := ensureProxyListPerformanceSchema(db); err != nil {
			t.Fatal(err)
		}
		var valid int64
		if err := db.Raw(`SELECT count(*) FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid WHERE i.indisvalid AND i.indisready AND c.relname IN ?`, names).Scan(&valid).Error; err != nil || valid != 4 {
			t.Fatalf("startup %d usable indexes=%d error=%v", attempt, valid, err)
		}
	}
}

func TestProxyListPerformanceSchemaPreservesValidNamedIndexesPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	if err := db.Exec(`CREATE INDEX idx_workspace_proxy_filter_page ON user_proxy_filter_indexes (port)`).Error; err != nil {
		t.Fatal(err)
	}
	if err := ensureProxyListPerformanceSchema(db); err != nil {
		t.Fatal(err)
	}
	var definition string
	if err := db.Raw(`SELECT pg_get_indexdef('idx_workspace_proxy_filter_page'::regclass)`).Scan(&definition).Error; err != nil || !strings.HasSuffix(definition, "USING btree (port)") {
		t.Fatalf("startup replaced a different valid named customer index: %s error=%v", definition, err)
	}
}

func explainProxyRead(t *testing.T, db *gorm.DB, name, query string) map[string]any {
	t.Helper()
	var encoded string
	if err := db.Raw("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + query).Row().Scan(&encoded); err != nil {
		t.Fatal(err)
	}
	writeProxyReadEvidence(t, name, []byte(encoded))
	var documents []map[string]any
	if err := json.Unmarshal([]byte(encoded), &documents); err != nil {
		t.Fatal(err)
	}
	t.Logf("%s execution_ms=%v", name, documents[0]["Execution Time"])
	return documents[0]["Plan"].(map[string]any)
}

func visitProxyReadPlan(node map[string]any, visit func(map[string]any)) {
	visit(node)
	if children, ok := node["Plans"].([]any); ok {
		for _, child := range children {
			visitProxyReadPlan(child.(map[string]any), visit)
		}
	}
}
