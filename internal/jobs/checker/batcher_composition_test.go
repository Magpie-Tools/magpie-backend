package checker

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"magpie/internal/domain"
	"magpie/internal/platform/pg"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// batcherStatements records the statements a batcher issues on pg.DB.
type batcherStatements struct {
	logger.Interface
	mu         sync.Mutex
	match      string
	statements []string
}

func (c *batcherStatements) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	sql, _ := fc()
	if !strings.Contains(sql, c.match) {
		return
	}
	c.mu.Lock()
	c.statements = append(c.statements, sql)
	c.mu.Unlock()
}

func (c *batcherStatements) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.statements)
}

func captureBatcherStatements(t *testing.T, db *gorm.DB, match string) *batcherStatements {
	t.Helper()
	capture := &batcherStatements{Interface: db.Logger, match: match}
	previous := pg.DB
	pg.DB = db.Session(&gorm.Session{Logger: capture})
	t.Cleanup(func() { pg.DB = previous })
	return capture
}

func ceilDiv(value, divisor int) int { return (value + divisor - 1) / divisor }

// batcherWorkers counts the goroutines running a batcher loop.
func batcherWorkers(frame string) int {
	buffer := make([]byte, 1<<20)
	for {
		written := runtime.Stack(buffer, true)
		if written < len(buffer) {
			buffer = buffer[:written]
			break
		}
		buffer = make([]byte, 2*len(buffer))
	}
	workers := 0
	for _, stack := range strings.Split(string(buffer), "\n\n") {
		if strings.Contains(stack, frame) {
			workers++
		}
	}
	return workers
}

// waitForBatcherWorkers waits until a batcher loop runs in want goroutines. A
// goroutine that has not been scheduled yet shows only its entry wrapper.
func waitForBatcherWorkers(t *testing.T, frame string, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	workers := batcherWorkers(frame)
	for workers != want && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
		workers = batcherWorkers(frame)
	}
	if workers != want {
		t.Fatalf("%s runs in %d goroutines, want %d", frame, workers, want)
	}
}

var batcherLoops = []string{
	"checker.(*failureTracker).run(",
	"checker.(*ownershipVerifier).run(",
}

// A checker runtime owns one failure tracker and one ownership verifier, each
// drained by exactly one goroutine while its batchers run. Constructing it
// starts nothing, stopping it ends both loops, and independent runtimes (one
// per test) do not share batchers. Workers submit to their own runtime's
// batchers, which is what turns K concurrent cycles into shared statements.
func TestCheckerRuntimeOwnsOneOfEachBatcher(t *testing.T) {
	first, second := NewRuntime(), NewRuntime()
	if first.failures == nil || first.ownership == nil {
		t.Fatal("checker batchers are not constructed")
	}
	if first.failures == second.failures || first.ownership == second.ownership {
		t.Fatal("independent runtimes share a batcher")
	}
	for _, loop := range batcherLoops {
		waitForBatcherWorkers(t, loop, 0)
	}

	start := func(r *Runtime) func() {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			r.RunBatchers(ctx)
		}()
		return func() { cancel(); <-done }
	}
	stopFirst := start(first)
	for _, loop := range batcherLoops {
		waitForBatcherWorkers(t, loop, 1)
	}

	db := setupCheckerTestDB(t)
	user := domain.User{Email: "batchers@example.com", Password: "password123"}
	if err := db.Create(&user).Error; err != nil {
		t.Fatal(err)
	}
	createCheckerWorkspace(t, db, user)
	proxy := domain.Proxy{IP: "10.0.0.56", Port: 9090}
	if err := db.Create(&proxy).Error; err != nil {
		t.Fatal(err)
	}

	// The second runtime's batchers are not running, so a request submitted to
	// it is not served by the first runtime's loops.
	waiting, cancelWaiting := context.WithTimeout(context.Background(), 100*time.Millisecond)
	if _, err := second.verifyProxyOwnership(waiting, proxy.ID); err == nil {
		t.Fatal("a stopped runtime answered an ownership request")
	}
	cancelWaiting()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if owned, err := first.verifyProxyOwnership(ctx, proxy.ID); err != nil || owned {
		t.Fatalf("ownership verifier: owned=%v err=%v", owned, err)
	}
	if err := db.Create(&domain.ManagedProxy{WorkspaceID: user.ID, ProxyID: proxy.ID, ConsecutiveFailures: 2}).Error; err != nil {
		t.Fatal(err)
	}
	if owned, err := first.verifyProxyOwnership(ctx, proxy.ID); err != nil || !owned {
		t.Fatalf("ownership verifier after import: owned=%v err=%v", owned, err)
	}
	events := []failureEvent{{WorkspaceID: user.ID, Success: true, HasEligibleChecks: true}}
	if _, _, err := first.processFailureEvents(ctx, proxy.ID, events); err != nil {
		t.Fatalf("failure tracker: %v", err)
	}
	var streak int
	if err := db.Model(&domain.ManagedProxy{}).Where("workspace_id = ? AND proxy_id = ?", user.ID, proxy.ID).
		Pluck("consecutive_failures", &streak).Error; err != nil || streak != 0 {
		t.Fatalf("failure tracker left streak %d: %v", streak, err)
	}

	stopSecond := start(second)
	for _, loop := range batcherLoops {
		waitForBatcherWorkers(t, loop, 2)
	}
	stopFirst()
	stopSecond()
	for _, loop := range batcherLoops {
		waitForBatcherWorkers(t, loop, 0)
	}
}

// Stopping the batchers processes the requests they already received.
func TestStoppedBatchersProcessReceivedRequests(t *testing.T) {
	db := setupCheckerTestDB(t)
	user := domain.User{Email: "batchers-stop@example.com", Password: "password123"}
	if err := db.Create(&user).Error; err != nil {
		t.Fatal(err)
	}
	createCheckerWorkspace(t, db, user)
	proxy := domain.Proxy{IP: "10.0.0.57", Port: 9090}
	if err := db.Create(&proxy).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&domain.ManagedProxy{WorkspaceID: user.ID, ProxyID: proxy.ID, ConsecutiveFailures: 2}).Error; err != nil {
		t.Fatal(err)
	}
	r := NewRuntime()
	request := &failureRequest{
		ctx:     context.Background(),
		proxyID: proxy.ID,
		events:  []failureEvent{{WorkspaceID: user.ID, Success: true, HasEligibleChecks: true}},
		resp:    make(chan failureResponse, 1),
	}
	r.failures.requests <- request
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r.RunBatchers(ctx)
	select {
	case response := <-request.resp:
		if response.err != nil {
			t.Fatal(response.err)
		}
	default:
		t.Fatal("a received request was dropped when the batchers stopped")
	}
	var streak int
	if err := db.Model(&domain.ManagedProxy{}).Where("workspace_id = ? AND proxy_id = ?", user.ID, proxy.ID).
		Pluck("consecutive_failures", &streak).Error; err != nil || streak != 0 {
		t.Fatalf("stopped failure tracker left streak %d: %v", streak, err)
	}
}

// One batch issues one ownership statement however many requests it holds.
func TestOwnershipVerifierBatchIssuesOneStatement(t *testing.T) {
	db := setupCheckerTestDB(t)
	capture := captureBatcherStatements(t, db, "user_proxies")

	batch := make([]*ownershipRequest, 0, ownershipBatchMaxItems)
	for id := 1; id <= ownershipBatchMaxItems; id++ {
		batch = append(batch, &ownershipRequest{proxyID: uint64(id), ctx: context.Background(), resp: make(chan ownershipResult, 1)})
	}
	newOwnershipVerifier().processBatch(batch)
	if got := capture.count(); got != 1 {
		t.Fatalf("a full ownership batch issued %d statements, want 1", got)
	}
	for _, request := range batch {
		if result := <-request.resp; result.err != nil || result.owned {
			t.Fatalf("route %d: owned=%v err=%v", request.proxyID, result.owned, result.err)
		}
	}
}

// K concurrent cycles share ceil(K/ownershipBatchMaxItems) ownership
// statements. A batch also closes when its window elapses, so requests that
// take longer than a window to arrive may add one statement per window.
func TestOwnershipVerifierBatchesConcurrentRequests(t *testing.T) {
	r := startTestRuntime(t)
	db := setupCheckerTestDB(t)
	capture := captureBatcherStatements(t, db, "user_proxies")

	const requests = 4000
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	start := make(chan struct{})
	errs := make(chan error, requests)
	var submitted sync.WaitGroup
	for id := 1; id <= requests; id++ {
		submitted.Add(1)
		go func(proxyID uint64) {
			defer submitted.Done()
			<-start
			if owned, err := r.verifyProxyOwnership(ctx, proxyID); err != nil || owned {
				errs <- fmt.Errorf("route %d: owned=%v err=%v", proxyID, owned, err)
			}
		}(uint64(id))
	}
	began := time.Now()
	close(start)
	submitted.Wait()
	elapsed := time.Since(began)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	limit := ceilDiv(requests, ownershipBatchMaxItems) + int(elapsed/ownershipBatchWindow)
	if got := capture.count(); got == 0 || got > limit {
		t.Fatalf("%d concurrent ownership checks issued %d statements in %s, want 1..%d", requests, got, elapsed, limit)
	}
}

// K concurrent cycles share chunked streak statements: at most
// ceil(F/failureSQLChunkSize) increments and ceil(S/failureSQLChunkSize) resets
// per batch. PostgreSQL only; the SQLite fallback writes row by row.
func TestFailureTrackerBatchesConcurrentRequests(t *testing.T) {
	r := startTestRuntime(t)
	dsn := strings.TrimSpace(os.Getenv("MAGPIE_TEST_POSTGRES_DSN"))
	if dsn == "" {
		t.Skip("MAGPIE_TEST_POSTGRES_DSN is not set")
	}
	quiet := &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)}
	admin, err := gorm.Open(postgres.Open(dsn), quiet)
	if err != nil {
		t.Fatal(err)
	}
	adminSQL, _ := admin.DB()
	t.Cleanup(func() { _ = adminSQL.Close() })
	schema := fmt.Sprintf("failure_batching_%d", time.Now().UnixNano())
	if err := admin.Exec("CREATE SCHEMA " + schema).Error; err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Exec("DROP SCHEMA " + schema + " CASCADE").Error })
	db, err := gorm.Open(postgres.Open(dsn+" search_path="+schema), quiet)
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, _ := db.DB()
	t.Cleanup(func() { _ = sqlDB.Close() })

	const requests = 2000
	for _, statement := range []string{
		`CREATE TABLE user_proxies (workspace_id bigint NOT NULL,proxy_id bigint NOT NULL,
consecutive_failures integer NOT NULL DEFAULT 0,username text,password text,state text,
updated_at timestamptz,PRIMARY KEY(workspace_id,proxy_id))`,
		fmt.Sprintf(`INSERT INTO user_proxies SELECT 1,p,CASE WHEN p %% 2 = 0 THEN 3 ELSE 0 END,'','','active',now()
FROM generate_series(1,%d) p`, requests),
	} {
		if err := db.Exec(statement).Error; err != nil {
			t.Fatal(err)
		}
	}
	capture := captureBatcherStatements(t, db, "UPDATE user_proxies")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	start := make(chan struct{})
	errs := make(chan error, requests)
	var submitted sync.WaitGroup
	for id := 1; id <= requests; id++ {
		submitted.Add(1)
		go func(proxyID uint64) {
			defer submitted.Done()
			<-start
			// Even routes succeed and reset a streak of three; odd routes fail.
			events := []failureEvent{{WorkspaceID: 1, Success: proxyID%2 == 0, HasEligibleChecks: true}}
			if _, _, err := r.processFailureEvents(ctx, proxyID, events); err != nil {
				errs <- fmt.Errorf("route %d: %w", proxyID, err)
			}
		}(uint64(id))
	}
	began := time.Now()
	close(start)
	submitted.Wait()
	elapsed := time.Since(began)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	// Each batch adds at most one partial chunk per statement kind.
	batches := ceilDiv(requests, failureBatchMaxItems) + int(elapsed/failureBatchWindow)
	limit := 2*ceilDiv(requests/2, failureSQLChunkSize) + 2*(batches-1)
	if got := capture.count(); got == 0 || got > limit {
		t.Fatalf("%d concurrent failure reports issued %d statements in %s, want 1..%d", requests, got, elapsed, limit)
	}
	var reset, incremented int64
	db.Raw("SELECT count(*) FROM user_proxies WHERE proxy_id % 2 = 0 AND consecutive_failures = 0").Scan(&reset)
	db.Raw("SELECT count(*) FROM user_proxies WHERE proxy_id % 2 = 1 AND consecutive_failures = 1").Scan(&incremented)
	if reset != requests/2 || incremented != requests/2 {
		t.Fatalf("streaks: %d reset and %d incremented, want %d each", reset, incremented, requests/2)
	}
}
