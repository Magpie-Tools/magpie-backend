package database

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"
)

// Compare complete option reads against low-cardinality successor seeks in an
// isolated schema. The extra owners must not multiply the selected inventory.
func TestProxyFilterOptionsScalePostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	routes := 10000
	if value := os.Getenv("MAGPIE_TEST_FILTER_ROUTES"); value != "" {
		n, err := strconv.Atoi(value)
		if err != nil || n < 1024 {
			t.Fatal("invalid filter fixture route count", value)
		}
		routes = n
	}
	if err := db.Exec(`CREATE TABLE filter_option_scale (
workspace_id bigint NOT NULL, country varchar(56), estimated_type varchar(20), anonymity_level varchar(50))`).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(fmt.Sprintf(`INSERT INTO filter_option_scale
SELECT w,
CASE WHEN w=1 THEN (ARRAY['DE','de','US','N/A','',' ','  ','Äland','Åland','日本','DE','BR','NZ','ZA','GB','FR','IT','ES','IN','KR','CA'])[1+n%%21]
ELSE 'other-'||w||'-'||(n%%21) END,
(ARRAY['residential','Residential','datacenter','mobile','','N/A',' '])[1+n%%7],
(ARRAY['elite','Elite','transparent','','N/A'])[1+n%%5]
FROM generate_series(1,8) w CROSS JOIN generate_series(1,%d) n`, routes)).Error; err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		`CREATE INDEX filter_option_scale_cover ON filter_option_scale (workspace_id,country,estimated_type,anonymity_level)`,
		`VACUUM (ANALYZE) filter_option_scale`,
	} {
		if err := db.Exec(statement).Error; err != nil {
			t.Fatal(err)
		}
	}
	columns := []string{"country", "estimated_type", "anonymity_level"}
	kinds := []string{"country", "type", "anonymity"}
	branches := make([]string, 0, len(columns))
	for i, column := range columns {
		branches = append(branches, fmt.Sprintf(`SELECT DISTINCT '%s' AS kind, COALESCE(NULLIF(%s,''),'N/A') AS value FROM filter_option_scale WHERE workspace_id=1`, kinds[i], column))
	}
	baseline := strings.Join(branches, " UNION ALL ") + " ORDER BY kind,value"
	triples := `WITH combinations AS MATERIALIZED (
SELECT DISTINCT COALESCE(NULLIF(country,''),'N/A') AS country,
COALESCE(NULLIF(estimated_type,''),'N/A') AS estimated_type,
COALESCE(NULLIF(anonymity_level,''),'N/A') AS anonymity_level
FROM filter_option_scale WHERE workspace_id=1)
SELECT 'country' AS kind,country AS value FROM combinations
UNION SELECT 'type',estimated_type FROM combinations
UNION SELECT 'anonymity',anonymity_level FROM combinations ORDER BY kind,value`
	var expected []filterOptionScaleRow
	if err := db.Raw(baseline).Scan(&expected).Error; err != nil {
		t.Fatal(err)
	}
	measureFilterOptions(t, db, "cover-three-scans", baseline, routes, expected)
	measureFilterOptions(t, db, "cover-distinct-triples", triples, routes, expected)
	for _, column := range columns {
		if err := db.Exec(fmt.Sprintf(`CREATE INDEX filter_option_scale_%s ON filter_option_scale (workspace_id,(COALESCE(NULLIF(%s,''),'N/A')))`, column, column)).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Exec(`ANALYZE filter_option_scale`).Error; err != nil {
		t.Fatal(err)
	}
	query, args := proxyFilterOptionValuesQuery(db, 1)
	statement := db.Raw(query, args...).Statement
	seeks := db.Dialector.Explain(statement.SQL.String(), statement.Vars...)
	seeks = strings.ReplaceAll(seeks, "user_proxy_filter_indexes", "filter_option_scale")
	plan := measureFilterOptions(t, db, "indexed-successor-seeks", seeks, routes, expected)
	var documents []map[string]any
	if err := json.Unmarshal([]byte(plan), &documents); err != nil {
		t.Fatal(err)
	}
	var rowsVisited float64
	var visit func(map[string]any)
	visit = func(node map[string]any) {
		if node["Relation Name"] == "filter_option_scale" {
			if node["Node Type"] == "Seq Scan" {
				t.Fatal("successor query scanned the option heap", plan)
			}
			rowsVisited += node["Actual Rows"].(float64) * node["Actual Loops"].(float64)
		}
		if children, ok := node["Plans"].([]any); ok {
			for _, child := range children {
				visit(child.(map[string]any))
			}
		}
	}
	visit(documents[0]["Plan"].(map[string]any))
	if rowsVisited > float64(2*len(expected)+6) {
		t.Fatalf("successor seeks visited %.0f rows for %d distinct values", rowsVisited, len(expected))
	}
	t.Logf("successor plan rows_visited=%.0f distinct_values=%d", rowsVisited, len(expected))
	var indexes []struct {
		Name  string
		Bytes int64
	}
	if err := db.Raw(`SELECT c.relname AS name,pg_relation_size(c.oid) AS bytes FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
WHERE i.indrelid='filter_option_scale'::regclass ORDER BY c.relname`).Scan(&indexes).Error; err != nil {
		t.Fatal(err)
	}
	data, _ := json.MarshalIndent(indexes, "", "  ")
	writeProxyReadEvidence(t, "filter-index-sizes.json", data)
	t.Logf("filter fixture routes=%d owners=8 indexes=%s", routes, data)
}

type filterOptionScaleRow struct{ Kind, Value string }

func measureFilterOptions(t *testing.T, db *gorm.DB, name, query string, routes int, expected []filterOptionScaleRow) string {
	t.Helper()
	latencies := make([]float64, 10)
	for iteration := range latencies {
		var rows []filterOptionScaleRow
		start := time.Now()
		err := db.Raw(query).Scan(&rows).Error
		latencies[iteration] = float64(time.Since(start).Microseconds()) / 1000
		if err != nil || !reflect.DeepEqual(rows, expected) {
			t.Fatalf("%s changed exact option values/order: got=%v want=%v error=%v", name, rows, expected, err)
		}
	}
	var plan string
	if err := db.Raw("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + query).Row().Scan(&plan); err != nil {
		t.Fatal(err)
	}
	writeProxyReadEvidence(t, name+"-plan.json", []byte(plan))
	data, _ := json.MarshalIndent(map[string]any{"routes_per_workspace": routes, "owners": 8, "query": query, "latencies_ms": latencies, "values": expected}, "", "  ")
	writeProxyReadEvidence(t, name+"-latency.json", data)
	t.Logf("%s routes=%d owners=8 ten_reads_ms=%v", name, routes, latencies)
	return plan
}

func writeProxyReadEvidence(t testing.TB, name string, data []byte) {
	t.Helper()
	dir := os.Getenv("MAGPIE_TEST_PROXY_READ_EVIDENCE_DIR")
	if dir == "" {
		return
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), append(data, '\n'), 0644); err != nil {
		t.Fatal(err)
	}
}
