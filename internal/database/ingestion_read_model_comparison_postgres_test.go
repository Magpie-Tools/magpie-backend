package database

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"magpie/internal/domain"
	"magpie/internal/platform/schema"

	"gorm.io/gorm"
)

// TestReadModelComparisonPostgres is the comparison harness for moving the
// read model out of the core (ADR 0015, the readmodel pilot). It runs fixed
// rounds of statistics ingestion through InsertProxyStatistics followed by the
// read model's projection and records, per round, what the pilot must not
// make worse: ingest and projection latency, WAL written by each, Go
// allocation, and projection lag with the refresh routine running. It uses
// only public read-model functions, so the same harness runs on the base and
// on the pilot revision.
//
// It is opt-in: set MAGPIE_TEST_READ_MODEL_COMPARISON to a file and each run
// appends one JSON line to it. Run it alternately on both revisions against
// the same otherwise idle PostgreSQL server; WAL is read server-wide.
// MAGPIE_TEST_READ_MODEL_COMPARISON_LABEL names the revision in the output.
// The inventory and the rounds can be scaled with the _ROUTES, _OWNERS,
// _SOURCES, _ROUNDS, _BATCH and _LAG_ROUNDS suffixes of the same variable.
func TestReadModelComparisonPostgres(t *testing.T) {
	output := os.Getenv("MAGPIE_TEST_READ_MODEL_COMPARISON")
	if output == "" {
		t.Skip("MAGPIE_TEST_READ_MODEL_COMPARISON is not set")
	}
	setting := func(suffix string, fallback int) int {
		name := "MAGPIE_TEST_READ_MODEL_COMPARISON_" + suffix
		value := os.Getenv(name)
		if value == "" {
			return fallback
		}
		n, err := strconv.Atoi(value)
		if err != nil || n < 1 {
			t.Fatalf("invalid %s=%q", name, value)
		}
		return n
	}
	shape := comparisonShape{
		Routes:    setting("ROUTES", 20000),
		Owners:    setting("OWNERS", 4),
		Sources:   setting("SOURCES", 20),
		Rounds:    setting("ROUNDS", 8),
		Batch:     setting("BATCH", 5000),
		LagRounds: setting("LAG_ROUNDS", 10),
	}
	if shape.Batch > shape.Routes {
		t.Fatalf("batch %d exceeds the %d routes", shape.Batch, shape.Routes)
	}
	t.Setenv("READ_MODEL_REFRESH_INTERVAL_SECONDS", "1")
	t.Setenv("SCRAPE_SOURCE_STATS_REFRESH_INTERVAL_SECONDS", "1")

	db := setupProxyIngestionPostgresTest(t)
	if err := schema.EnsureReadModelSchema(db); err != nil {
		t.Fatal(err)
	}
	drainReadModel(t)
	owners, judge := seedComparisonInventory(t, db, shape)
	seedStart := time.Now()
	if err := RefreshReadModelIndexes(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("ANALYZE").Error; err != nil {
		t.Fatal(err)
	}
	result := comparisonResult{
		Label:        os.Getenv("MAGPIE_TEST_READ_MODEL_COMPARISON_LABEL"),
		Shape:        shape,
		StartedAt:    time.Now().UTC(),
		InitialBuild: time.Since(seedStart).Milliseconds(),
	}

	// Synchronous rounds: ingest, then drain the projection.
	next := 0
	for round := 0; round < shape.Rounds; round++ {
		batch, sentinel, at, alive := comparisonBatch(shape, owners, judge, round, &next)
		var before, afterIngest, afterProjection runtime.MemStats
		walStart := currentWAL(t, db)
		runtime.ReadMemStats(&before)
		start := time.Now()
		if err := InsertProxyStatistics(context.Background(), batch, len(batch)); err != nil {
			t.Fatal(err)
		}
		ingested := time.Now()
		runtime.ReadMemStats(&afterIngest)
		walIngested := currentWAL(t, db)
		drainReadModel(t)
		projected := time.Now()
		runtime.ReadMemStats(&afterProjection)
		walProjected := currentWAL(t, db)
		checkComparisonSentinel(t, db, owners, sentinel, at, alive)
		result.Rounds = append(result.Rounds, comparisonRound{
			IngestMS:          float64(ingested.Sub(start).Microseconds()) / 1000,
			ProjectionMS:      float64(projected.Sub(ingested).Microseconds()) / 1000,
			IngestWALBytes:    walDiff(t, db, walIngested, walStart),
			ProjectionWAL:     walDiff(t, db, walProjected, walIngested),
			IngestAllocBytes:  afterIngest.TotalAlloc - before.TotalAlloc,
			IngestAllocs:      afterIngest.Mallocs - before.Mallocs,
			ProjectionAlloc:   afterProjection.TotalAlloc - afterIngest.TotalAlloc,
			ProjectionAllocs:  afterProjection.Mallocs - afterIngest.Mallocs,
			HeapInUseAfterEnd: afterProjection.HeapInuse,
		})
	}

	// Projection lag with the refresh routine running, as in production: the
	// time from the ingest commit until the batch's last route shows the new
	// evidence. The routine's one-second tick bounds it from below.
	ctx, cancel := context.WithCancel(context.Background())
	routineDone := make(chan struct{})
	go func() {
		defer close(routineDone)
		StartReadModelRefreshRoutine(ctx)
	}()
	for round := 0; round < shape.LagRounds; round++ {
		batch, sentinel, at, alive := comparisonBatch(shape, owners, judge, shape.Rounds+round, &next)
		if err := InsertProxyStatistics(context.Background(), batch, len(batch)); err != nil {
			cancel()
			t.Fatal(err)
		}
		committed := time.Now()
		deadline := committed.Add(time.Minute)
		for !comparisonSentinelCurrent(t, db, owners[len(owners)-1].ID, sentinel, at, alive) {
			if time.Now().After(deadline) {
				cancel()
				t.Fatalf("the projection did not show round %d within a minute", round)
			}
			time.Sleep(5 * time.Millisecond)
		}
		result.LagMS = append(result.LagMS, float64(time.Since(committed).Microseconds())/1000)
	}
	cancel()
	<-routineDone
	result.FinishedAt = time.Now().UTC()

	result.summarize()
	line, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(output, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := file.Write(append(line, '\n')); err != nil {
		t.Fatal(err)
	}
	t.Logf("%s: %.0f statistics/s, ingest p50 %.1f ms, projection p50 %.1f ms, WAL %d+%d bytes/round, lag p50 %.0f ms",
		result.Label, result.Summary.StatisticsPerSecond, result.Summary.IngestP50MS, result.Summary.ProjectionP50MS,
		result.Summary.IngestWALPerRound, result.Summary.ProjectionWALPerRound, result.Summary.LagP50MS)
}

type comparisonShape struct {
	Routes, Owners, Sources, Rounds, Batch, LagRounds int
}

type comparisonRound struct {
	IngestMS, ProjectionMS            float64
	IngestWALBytes, ProjectionWAL     int64
	IngestAllocBytes, IngestAllocs    uint64
	ProjectionAlloc, ProjectionAllocs uint64
	HeapInUseAfterEnd                 uint64
}

type comparisonSummary struct {
	StatisticsPerSecond              float64
	IngestP50MS, IngestP95MS         float64
	ProjectionP50MS, ProjectionP95MS float64
	IngestWALPerRound                int64
	ProjectionWALPerRound            int64
	IngestAllocPerStatistic          float64
	ProjectionAllocPerStatistic      float64
	LagP50MS, LagMaxMS               float64
}

type comparisonResult struct {
	Label                 string
	Shape                 comparisonShape
	StartedAt, FinishedAt time.Time
	InitialBuild          int64
	Rounds                []comparisonRound
	LagMS                 []float64
	Summary               comparisonSummary
}

func (r *comparisonResult) summarize() {
	var ingest, projection []float64
	var total float64
	var ingestWAL, projectionWAL int64
	var ingestAlloc, projectionAlloc uint64
	for _, round := range r.Rounds {
		ingest = append(ingest, round.IngestMS)
		projection = append(projection, round.ProjectionMS)
		total += round.IngestMS + round.ProjectionMS
		ingestWAL += round.IngestWALBytes
		projectionWAL += round.ProjectionWAL
		ingestAlloc += round.IngestAllocBytes
		projectionAlloc += round.ProjectionAlloc
	}
	statistics := float64(len(r.Rounds) * r.Shape.Batch)
	rounds := int64(len(r.Rounds))
	r.Summary = comparisonSummary{
		StatisticsPerSecond:         statistics / (total / 1000),
		IngestP50MS:                 percentile(ingest, 0.5),
		IngestP95MS:                 percentile(ingest, 0.95),
		ProjectionP50MS:             percentile(projection, 0.5),
		ProjectionP95MS:             percentile(projection, 0.95),
		IngestWALPerRound:           ingestWAL / rounds,
		ProjectionWALPerRound:       projectionWAL / rounds,
		IngestAllocPerStatistic:     float64(ingestAlloc) / statistics,
		ProjectionAllocPerStatistic: float64(projectionAlloc) / statistics,
		LagP50MS:                    percentile(r.LagMS, 0.5),
		LagMaxMS:                    percentile(r.LagMS, 1),
	}
}

// percentile is the nearest-rank percentile of values.
func percentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := slices.Clone(values)
	slices.Sort(sorted)
	rank := int(p*float64(len(sorted))+0.999999) - 1
	return sorted[min(max(rank, 0), len(sorted)-1)]
}

// seedComparisonInventory creates owners that share every route, each with
// its own compiled HTTP key, and sources that split the routes between them.
// Every owner subscribes to every source.
func seedComparisonInventory(t *testing.T, db *gorm.DB, shape comparisonShape) ([]domain.Workspace, domain.Judge) {
	t.Helper()
	for i, name := range domain.CheckerProtocols {
		if err := db.Create(&domain.Protocol{ID: i + 1, Name: name}).Error; err != nil {
			t.Fatal(err)
		}
	}
	judge := domain.Judge{FullString: "http://127.0.0.1:8080/"}
	if err := db.Create(&judge).Error; err != nil {
		t.Fatal(err)
	}
	owners := make([]domain.Workspace, shape.Owners)
	for i := range owners {
		owners[i] = domain.Workspace{Name: fmt.Sprintf("Comparison owner %d", i), CheckerGeneration: 1, CheckerHTTPKey: fmt.Sprintf("comparison-http-%d", i)}
		if err := db.Create(&owners[i]).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Create(&domain.WorkspaceSubscription{WorkspaceID: owners[i].ID, OverageMode: domain.WorkspaceOverageUnlimited}).Error; err != nil {
			t.Fatal(err)
		}
	}
	sources := make([]string, shape.Sources)
	for i := range sources {
		site := domain.ScrapeSite{URL: fmt.Sprintf("http://comparison.invalid/source/%d", i)}
		if err := db.Create(&site).Error; err != nil {
			t.Fatal(err)
		}
		sources[i] = strconv.FormatUint(site.ID, 10)
		for _, owner := range owners {
			if err := db.Create(&domain.WorkspaceScrapeSite{WorkspaceID: owner.ID, ScrapeSiteID: site.ID, FetchMode: domain.ScrapeFetchHTTP}).Error; err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, statement := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO proxies (id,host,ip_address,port,country,estimated_type)
SELECT n,'198.18.' || (n/256)%256 || '.' || n%256,('198.18.' || (n/256)%256 || '.' || n%256)::inet,8080,'',''
FROM generate_series(1,?::bigint) n`, []any{shape.Routes}},
		{`ANALYZE proxies`, nil},
		{`INSERT INTO user_proxies (workspace_id,proxy_id,state)
SELECT w.id,n,'active' FROM workspaces w CROSS JOIN generate_series(1,?::bigint) n WHERE w.checker_generation=1`, []any{shape.Routes}},
		{`INSERT INTO proxy_scrape_site (proxy_id,scrape_site_id)
SELECT n,(?::bigint[])[1+(n%?)] FROM generate_series(1,?::bigint) n`, []any{"{" + strings.Join(sources, ",") + "}", shape.Sources, shape.Routes}},
		{`ANALYZE`, nil},
	} {
		if err := db.Exec(statement.sql, statement.args...).Error; err != nil {
			t.Fatal(err)
		}
	}
	return owners, judge
}

// comparisonBatch builds the next batch: consecutive routes, wrapping around
// the inventory, each with one HTTP result for every owner. Results alternate
// between alive and dead by round, so every round changes the projection.
func comparisonBatch(shape comparisonShape, owners []domain.Workspace, judge domain.Judge, round int, next *int) ([]domain.ProxyStatistic, uint64, time.Time, bool) {
	at := time.Now().UTC().Truncate(time.Microsecond)
	alive := round%2 == 0
	ownerIDs := make([]uint, len(owners))
	for i, owner := range owners {
		ownerIDs[i] = owner.ID
	}
	batch := make([]domain.ProxyStatistic, shape.Batch)
	var sentinel uint64
	for i := range batch {
		route := uint64(*next%shape.Routes) + 1
		*next++
		sentinel = max(sentinel, route)
		evidence := make([]domain.WorkspaceCheckEvidence, len(owners))
		for j, owner := range owners {
			evidence[j] = domain.WorkspaceCheckEvidence{WorkspaceID: owner.ID, ConfigKey: owner.CheckerHTTPKey, Alive: alive}
		}
		batch[i] = domain.ProxyStatistic{
			ProxyID: route, JudgeID: judge.ID, ProtocolID: 1, Alive: alive, ResponseTime: 40, TransportProtocol: "tcp",
			CheckTimeout: 1000, CreatedAt: at, CheckEvidence: evidence, WorkspaceIDs: ownerIDs,
		}
	}
	return batch, sentinel, at, alive
}

func comparisonSentinelCurrent(t *testing.T, db *gorm.DB, workspaceID uint, proxyID uint64, at time.Time, alive bool) bool {
	t.Helper()
	var row domain.WorkspaceProxyFilterIndex
	if err := db.First(&row, "workspace_id = ? AND proxy_id = ?", workspaceID, proxyID).Error; err != nil {
		t.Fatal(err)
	}
	return row.LatestCheck.Equal(at) && row.Alive == alive
}

func checkComparisonSentinel(t *testing.T, db *gorm.DB, owners []domain.Workspace, proxyID uint64, at time.Time, alive bool) {
	t.Helper()
	for _, owner := range owners {
		if !comparisonSentinelCurrent(t, db, owner.ID, proxyID, at, alive) {
			t.Fatalf("route %d of workspace %d does not show the round's evidence after the drain", proxyID, owner.ID)
		}
	}
}

func currentWAL(t *testing.T, db *gorm.DB) string {
	t.Helper()
	var lsn string
	if err := db.Raw("SELECT pg_current_wal_lsn()::text").Scan(&lsn).Error; err != nil {
		t.Fatal(err)
	}
	return lsn
}

func walDiff(t *testing.T, db *gorm.DB, later, earlier string) int64 {
	t.Helper()
	var bytes int64
	if err := db.Raw("SELECT pg_wal_lsn_diff(?::pg_lsn, ?::pg_lsn)::bigint", later, earlier).Scan(&bytes).Error; err != nil {
		t.Fatal(err)
	}
	return bytes
}
