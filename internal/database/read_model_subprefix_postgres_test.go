package database

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"magpie/internal/domain"

	"gorm.io/gorm"
)

// These tests require a fresh, owned disposable PostgreSQL service. The existing
// fixture gives each test a separate schema and restores DB/dirty globals.
// No test uses Parallel: the production coordinator has process-wide state.
type readModelSubprefixAttempts struct {
	mu     sync.Mutex
	widths []int
}

func captureReadModelSubprefixAttempts(t *testing.T, db *gorm.DB) *readModelSubprefixAttempts {
	t.Helper()
	attempts := &readModelSubprefixAttempts{}
	const callback = "test:subprefix_attempt_width"
	if err := db.Callback().Raw().Before("gorm:raw").Register(callback, func(tx *gorm.DB) {
		if strings.Contains(tx.Statement.SQL.String(), "INSERT INTO user_proxy_filter_indexes") {
			attempts.mu.Lock()
			attempts.widths = append(attempts.widths, len(tx.Statement.Vars)/2)
			attempts.mu.Unlock()
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Callback().Raw().Remove(callback) })
	return attempts
}

func (a *readModelSubprefixAttempts) snapshot() []int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]int(nil), a.widths...)
}

func requireReadModelSubprefixState(t *testing.T, cursor, total, retryEnd, retryWidth int) {
	t.Helper()
	readModelDirtyMu.Lock()
	defer readModelDirtyMu.Unlock()
	batch := readModelPendingBatch
	if batch == nil || batch.next != cursor || len(batch.pairs) != total || batch.retryEnd != retryEnd || batch.retryWidth != retryWidth {
		t.Fatalf("unexpected retained state: batch=%v want cursor/total/retryEnd/retryWidth=%d/%d/%d/%d", batch, cursor, total, retryEnd, retryWidth)
	}
}

func readModelSubprefixRowCount(t *testing.T, db *gorm.DB, proxyID uint64) int64 {
	t.Helper()
	var count int64
	if err := db.Model(&domain.WorkspaceProxyFilterIndex{}).Where("proxy_id=?", proxyID).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	return count
}

func readModelSubprefixAuthoritativeState(t *testing.T, db *gorm.DB) string {
	t.Helper()
	var state string
	if err := db.Raw(`SELECT jsonb_build_object(
'proxies',(SELECT jsonb_agg(to_jsonb(p) ORDER BY p.id) FROM proxies p),
'ownership',(SELECT jsonb_agg(to_jsonb(up) ORDER BY up.workspace_id,up.proxy_id) FROM user_proxies up),
'workspaces',(SELECT jsonb_agg(to_jsonb(w) ORDER BY w.id) FROM workspaces w),
'latest',(SELECT jsonb_agg(to_jsonb(l) ORDER BY to_jsonb(l)::text) FROM proxy_latest_statistics l),
'history',(SELECT jsonb_agg(to_jsonb(h) ORDER BY to_jsonb(h)::text) FROM proxy_statistics h))::text`).Scan(&state).Error; err != nil {
		t.Fatal(err)
	}
	return state
}

func finishReadModelSubprefixDrain(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	drainReadModelRefreshes(ctx)
	if ctx.Err() != nil || readModelProxyWorkPending() {
		t.Fatal("bounded test drain left unresolved projection work", ctx.Err())
	}
	sourceStatsDirtyMu.Lock()
	pending := len(sourceStatsDirty)
	sourceStatsDirtyMu.Unlock()
	if pending != 0 {
		t.Fatal("bounded test drain left source work", pending)
	}
}

func expireFirstReadModelSubprefix(t *testing.T, db *gorm.DB, afterCommit bool, entered chan<- struct{}) {
	t.Helper()
	t.Setenv("READ_MODEL_REFRESH_TIMEOUT_SECONDS", "1")
	var once sync.Once
	callback := "test:subprefix_child_deadline"
	handler := func(tx *gorm.DB) {
		if !strings.Contains(tx.Statement.SQL.String(), "INSERT INTO user_proxy_filter_indexes") || tx.Error != nil {
			return
		}
		once.Do(func() {
			if entered != nil {
				entered <- struct{}{}
			}
			<-tx.Statement.Context.Done()
			if !afterCommit {
				tx.AddError(tx.Statement.Context.Err())
			}
		})
	}
	var err error
	if afterCommit {
		err = db.Callback().Raw().After("gorm:raw").Register(callback, handler)
	} else {
		err = db.Callback().Raw().Before("gorm:raw").Register(callback, handler)
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Callback().Raw().Remove(callback) })
}

func installReadModelSubprefixDeadlineTrigger(t *testing.T, db *gorm.DB, workspaceID uint) func() error {
	t.Helper()
	if err := db.Exec(fmt.Sprintf(`CREATE FUNCTION test_subprefix_wait() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN IF NEW.workspace_id=%d THEN PERFORM pg_sleep(2); END IF; RETURN NEW; END $$`, workspaceID)).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`CREATE TRIGGER test_subprefix_wait BEFORE INSERT OR UPDATE ON user_proxy_filter_indexes
FOR EACH ROW EXECUTE FUNCTION test_subprefix_wait()`).Error; err != nil {
		t.Fatal(err)
	}
	remove := func() error {
		if err := db.Exec(`DROP TRIGGER IF EXISTS test_subprefix_wait ON user_proxy_filter_indexes`).Error; err != nil {
			return err
		}
		return db.Exec(`DROP FUNCTION IF EXISTS test_subprefix_wait()`).Error
	}
	t.Cleanup(func() { _ = remove() })
	return remove
}

func TestReadModelSubprefixAtomicDeadlineThenDurableProgressPostgres(t *testing.T) {
	db, proxyID, owners := seedReadModelFanoutTest(t)
	t.Setenv("READ_MODEL_REFRESH_BATCH_SIZE", "4")
	t.Setenv("READ_MODEL_REFRESH_TIMEOUT_SECONDS", "1")
	if err := db.Where("proxy_id=?", proxyID).Delete(&domain.WorkspaceProxyFilterIndex{}).Error; err != nil {
		t.Fatal(err)
	}
	before := readModelSubprefixAuthoritativeState(t, db)
	attempts := captureReadModelSubprefixAttempts(t, db)
	removeTrigger := installReadModelSubprefixDeadlineTrigger(t, db, owners[3])
	flushReadModelDirtyProxyIDs(context.Background(), false)
	if got := attempts.snapshot(); !reflect.DeepEqual(got, []int{4}) {
		t.Fatal("failed invocation retried immediately", got)
	}
	requireReadModelSubprefixState(t, 0, 8, 4, 2)
	if got := readModelSubprefixRowCount(t, db, proxyID); got != 0 {
		t.Fatal("timed-out atomic statement left partially committed rows", got)
	}
	if err := removeTrigger(); err != nil {
		t.Fatal(err)
	}
	flushReadModelDirtyProxyIDs(context.Background(), false)
	requireReadModelSubprefixState(t, 2, 8, 4, 2)
	if got := readModelSubprefixRowCount(t, db, proxyID); got != 2 {
		t.Fatal("successful subprefix did not commit independently", got)
	}
	var committed domain.WorkspaceProxyFilterIndex
	if err := db.First(&committed, "workspace_id=? AND proxy_id=?", owners[0], proxyID).Error; err != nil {
		t.Fatal(err)
	}
	flushReadModelDirtyProxyIDs(context.Background(), false)
	requireReadModelSubprefixState(t, 4, 8, 0, 0)
	finishReadModelSubprefixDrain(t)
	if got := attempts.snapshot(); !reflect.DeepEqual(got, []int{4, 2, 2, 2, 2}) {
		t.Fatal("retry skipped an interval or grew before two fast commits", got)
	}
	var unchanged domain.WorkspaceProxyFilterIndex
	if err := db.First(&unchanged, "workspace_id=? AND proxy_id=?", owners[0], proxyID).Error; err != nil || !committed.UpdatedAt.Equal(unchanged.UpdatedAt) {
		t.Fatal("later retry rewrote a committed prefix", err)
	}
	if readModelSubprefixRowCount(t, db, proxyID) != 8 || before != readModelSubprefixAuthoritativeState(t, db) {
		t.Fatal("complete drain lost an owner or changed authoritative data/history")
	}
}

func TestReadModelSubprefixParentCancellationDoesNotSplitPostgres(t *testing.T) {
	db, _, _ := seedReadModelFanoutTest(t)
	t.Setenv("READ_MODEL_REFRESH_BATCH_SIZE", "4")
	attempts := captureReadModelSubprefixAttempts(t, db)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const callback = "test:cancel_subprefix_parent"
	if err := db.Callback().Raw().Before("gorm:raw").Register(callback, func(tx *gorm.DB) {
		if strings.Contains(tx.Statement.SQL.String(), "INSERT INTO user_proxy_filter_indexes") {
			cancel()
			tx.AddError(ctx.Err())
		}
	}); err != nil {
		t.Fatal(err)
	}
	flushReadModelDirtyProxyIDs(ctx, false)
	requireReadModelSubprefixState(t, 0, 8, 0, 0)
	if got := attempts.snapshot(); !reflect.DeepEqual(got, []int{4}) || !readModelProxyWorkPending() {
		t.Fatal("canceled parent split, retried or dropped its work", got)
	}
	if err := db.Callback().Raw().Remove(callback); err != nil {
		t.Fatal(err)
	}
	finishReadModelSubprefixDrain(t)
}

func TestReadModelSubprefixPermanentErrorDoesNotSplitPostgres(t *testing.T) {
	for _, tc := range []struct {
		name        string
		err         error
		expireChild bool
	}{
		{"permanent_sql_failure", errors.New("injected permanent SQL failure"), false},
		{"operation_error_without_child_deadline", fmt.Errorf("operation deadline: %w", context.DeadlineExceeded), false},
		{"permanent_error_at_expired_child_deadline", errors.New("injected constraint failure"), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, _, _ := seedReadModelFanoutTest(t)
			t.Setenv("READ_MODEL_REFRESH_BATCH_SIZE", "4")
			if tc.expireChild {
				t.Setenv("READ_MODEL_REFRESH_TIMEOUT_SECONDS", "1")
			}
			attempts := captureReadModelSubprefixAttempts(t, db)
			const callback = "test:permanent_subprefix_failure"
			if err := db.Callback().Raw().Before("gorm:raw").Register(callback, func(tx *gorm.DB) {
				if strings.Contains(tx.Statement.SQL.String(), "INSERT INTO user_proxy_filter_indexes") {
					if tc.expireChild {
						<-tx.Statement.Context.Done()
					}
					tx.AddError(tc.err)
				}
			}); err != nil {
				t.Fatal(err)
			}
			flushReadModelDirtyProxyIDs(context.Background(), false)
			requireReadModelSubprefixState(t, 0, 8, 0, 0)
			if got := attempts.snapshot(); !reflect.DeepEqual(got, []int{4}) || !readModelProxyWorkPending() {
				t.Fatal("failure without an actual child deadline was split, retried or dropped", got)
			}
			if err := db.Callback().Raw().Remove(callback); err != nil {
				t.Fatal(err)
			}
			finishReadModelSubprefixDrain(t)
		})
	}
}

func TestReadModelSubprefixWidthOneDeadlineRemainsUnresolvedPostgres(t *testing.T) {
	db, _, _ := seedReadModelFanoutTest(t)
	t.Setenv("READ_MODEL_REFRESH_BATCH_SIZE", "1")
	attempts := captureReadModelSubprefixAttempts(t, db)
	expireFirstReadModelSubprefix(t, db, false, nil)
	flushReadModelDirtyProxyIDs(context.Background(), false)
	requireReadModelSubprefixState(t, 0, 8, 0, 0)
	if got := attempts.snapshot(); !reflect.DeepEqual(got, []int{1}) || !readModelProxyWorkPending() {
		t.Fatal("one-pair deadline reported completion or retried immediately", got)
	}
	finishReadModelSubprefixDrain(t)
}

func TestReadModelSubprefixAmbiguousCommitPreservesNoopTimestampPostgres(t *testing.T) {
	db, proxyID, owners := seedReadModelFanoutTest(t)
	t.Setenv("READ_MODEL_REFRESH_BATCH_SIZE", "4")
	attempts := captureReadModelSubprefixAttempts(t, db)
	before := readModelSubprefixAuthoritativeState(t, db)
	expireFirstReadModelSubprefix(t, db, true, nil)
	flushReadModelDirtyProxyIDs(context.Background(), false)
	requireReadModelSubprefixState(t, 0, 8, 4, 2)
	if got := readModelSubprefixRowCount(t, db, proxyID); got != 4 {
		t.Fatal("ambiguous-commit fixture did not actually commit before cancellation", got)
	}
	var committed []domain.WorkspaceProxyFilterIndex
	if err := db.Where("proxy_id=? AND workspace_id IN ?", proxyID, owners[:4]).Order("workspace_id").Find(&committed).Error; err != nil || len(committed) != 4 {
		t.Fatal("missing durable ambiguous prefix", len(committed), err)
	}
	finishReadModelSubprefixDrain(t)
	for _, previous := range committed {
		var row domain.WorkspaceProxyFilterIndex
		if err := db.First(&row, "workspace_id=? AND proxy_id=?", previous.WorkspaceID, proxyID).Error; err != nil || !previous.UpdatedAt.Equal(row.UpdatedAt) {
			t.Fatal("ambiguous retry rewrote unchanged durable content", err)
		}
	}
	if got := attempts.snapshot(); !reflect.DeepEqual(got, []int{4, 2, 2, 2, 2}) || before != readModelSubprefixAuthoritativeState(t, db) {
		t.Fatal("ambiguous retry lost its suffix or changed authoritative history", got)
	}
}

func TestReadModelSubprefixConcurrentInvalidationsAndOwnershipPostgres(t *testing.T) {
	db, proxyID, owners := seedReadModelFanoutTest(t)
	t.Setenv("READ_MODEL_REFRESH_BATCH_SIZE", "4")
	entered := make(chan struct{}, 1)
	expireFirstReadModelSubprefix(t, db, false, entered)
	t.Setenv("READ_MODEL_REFRESH_TIMEOUT_SECONDS", "3")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("owned projection call did not stop before fixture cleanup")
		}
	})
	go func() {
		flushReadModelDirtyProxyIDs(ctx, false)
		close(done)
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("projection did not reach the retained-pair barrier")
	}
	if err := db.Where("workspace_id=? AND proxy_id=?", owners[7], proxyID).Delete(&domain.ManagedProxy{}).Error; err != nil {
		t.Fatal(err)
	}
	newOwner := domain.Workspace{Name: "Subprefix new owner", CheckerGeneration: 1, CheckerHTTPKey: "subprefix-new-http"}
	if err := db.Create(&newOwner).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&domain.ManagedProxy{WorkspaceID: newOwner.ID, ProxyID: proxyID, State: domain.ManagedProxyStatePaused, PauseReason: domain.ManagedProxyPauseReasonManual}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&domain.ProxyLatestStatistic{WorkspaceID: newOwner.ID, ConfigKey: newOwner.CheckerHTTPKey, ProxyID: proxyID, ProtocolID: 1, Alive: true, CheckedAt: time.Now().UTC(), StatisticID: 20}).Error; err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
		t.Fatal("fixture mutations outlasted the in-flight deadline barrier")
	default:
	}
	QueueReadModelRefreshForProxyIDs([]uint64{proxyID})
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("deadline did not join the in-flight projection")
	}
	requireReadModelSubprefixState(t, 0, 8, 4, 2)
	readModelDirtyMu.Lock()
	_, queued := readModelDirtyProxyIDs[proxyID]
	readModelDirtyMu.Unlock()
	if !queued {
		t.Fatal("invalidation during the timed-out prefix was lost")
	}
	before := readModelSubprefixAuthoritativeState(t, db)
	finishReadModelSubprefixDrain(t)
	var count int64
	if err := db.Model(&domain.WorkspaceProxyFilterIndex{}).Where("workspace_id=? AND proxy_id=?", owners[7], proxyID).Count(&count).Error; err != nil || count != 0 {
		t.Fatal("captured old pair resurrected removed ownership", count, err)
	}
	var row domain.WorkspaceProxyFilterIndex
	if err := db.First(&row, "workspace_id=? AND proxy_id=?", newOwner.ID, proxyID).Error; err != nil || row.State != domain.ManagedProxyStatePaused || row.PauseReason != domain.ManagedProxyPauseReasonManual || !row.Alive {
		t.Fatal("invalidation during a split lost the new owner, lifecycle or current evidence", row, err)
	}
	if before != readModelSubprefixAuthoritativeState(t, db) {
		t.Fatal("projection retry changed authoritative inventory or history")
	}
}

func TestReadModelSubprefixOwnerDiscoveryDeadlineDoesNotCreateRetryIntervalPostgres(t *testing.T) {
	db, proxyID, _ := seedReadModelFanoutTest(t)
	t.Setenv("READ_MODEL_REFRESH_BATCH_SIZE", "4")
	t.Setenv("READ_MODEL_REFRESH_TIMEOUT_SECONDS", "1")
	attempts := captureReadModelSubprefixAttempts(t, db)
	var once sync.Once
	const callback = "test:subprefix_owner_deadline"
	if err := db.Callback().Row().Before("gorm:row").Register(callback, func(tx *gorm.DB) {
		if tx.Statement.Table == "user_proxies" && len(tx.Statement.Selects) == 1 && tx.Statement.Selects[0] == "workspace_id, proxy_id" {
			once.Do(func() {
				<-tx.Statement.Context.Done()
				tx.AddError(tx.Statement.Context.Err())
			})
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Callback().Row().Remove(callback) })
	flushReadModelDirtyProxyIDs(context.Background(), false)
	readModelDirtyMu.Lock()
	batch := readModelPendingBatch
	_, requeued := readModelDirtyProxyIDs[proxyID]
	readModelDirtyMu.Unlock()
	if batch != nil || !requeued || len(attempts.snapshot()) != 0 {
		t.Fatal("discovery deadline fabricated a pair interval, lost physical work or attempted projection", batch, requeued, attempts.snapshot())
	}
	finishReadModelSubprefixDrain(t)
	if got := attempts.snapshot(); !reflect.DeepEqual(got, []int{4, 4}) {
		t.Fatal("owner discovery timeout changed later projection widths", got)
	}
}

func TestReadModelSubprefixSourceDeadlineKeepsCompletedCursorWithoutSplittingPostgres(t *testing.T) {
	db, _, _ := seedReadModelFanoutTest(t)
	t.Setenv("READ_MODEL_REFRESH_BATCH_SIZE", "8")
	t.Setenv("READ_MODEL_REFRESH_TIMEOUT_SECONDS", "1")
	attempts := captureReadModelSubprefixAttempts(t, db)
	var once sync.Once
	const callback = "test:subprefix_source_deadline"
	if err := db.Callback().Row().Before("gorm:row").Register(callback, func(tx *gorm.DB) {
		if tx.Statement.TableExpr != nil && strings.Contains(tx.Statement.TableExpr.SQL, "proxy_scrape_site pss") {
			once.Do(func() {
				<-tx.Statement.Context.Done()
				tx.AddError(tx.Statement.Context.Err())
			})
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Callback().Row().Remove(callback) })
	flushReadModelDirtyProxyIDs(context.Background(), false)
	requireReadModelSubprefixState(t, 8, 8, 0, 0)
	if got := attempts.snapshot(); !reflect.DeepEqual(got, []int{8}) || !readModelProxyWorkPending() {
		t.Fatal("source deadline split or dropped the completed owner interval", got)
	}
	finishReadModelSubprefixDrain(t)
	if got := attempts.snapshot(); !reflect.DeepEqual(got, []int{8}) {
		t.Fatal("source-only retry replayed completed owner pairs", got)
	}
}

func TestReadModelSubprefixSourceOnlyRetryDoesNotReplayPairsPostgres(t *testing.T) {
	db, proxyID, _ := seedReadModelFanoutTest(t)
	t.Setenv("READ_MODEL_REFRESH_BATCH_SIZE", "4")
	expireFirstReadModelSubprefix(t, db, false, nil)
	flushReadModelDirtyProxyIDs(context.Background(), false)
	requireReadModelSubprefixState(t, 0, 8, 4, 2)
	if err := db.Exec("ALTER TABLE proxy_scrape_site RENAME TO hidden_proxy_scrape_site").Error; err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		flushReadModelDirtyProxyIDs(context.Background(), false)
	}
	requireReadModelSubprefixState(t, 6, 8, 0, 0)
	flushReadModelDirtyProxyIDs(context.Background(), false)
	requireReadModelSubprefixState(t, 8, 8, 0, 0)
	if !readModelProxyWorkPending() || readModelSubprefixRowCount(t, db, proxyID) != 8 {
		t.Fatal("source failure falsely drained or lost completed projection work")
	}
	if err := db.Exec("ALTER TABLE hidden_proxy_scrape_site RENAME TO proxy_scrape_site").Error; err != nil {
		t.Fatal(err)
	}
	const callback = "test:subprefix_completed_pairs_must_not_repeat"
	if err := db.Callback().Raw().Before("gorm:raw").Register(callback, func(tx *gorm.DB) {
		if strings.Contains(tx.Statement.SQL.String(), "INSERT INTO user_proxy_filter_indexes") {
			tx.AddError(errors.New("completed projection prefix repeated"))
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Callback().Raw().Remove(callback) })
	finishReadModelSubprefixDrain(t)
	var keys []sourceStatsKey
	if err := db.Table("user_scrape_site").Select("workspace_id, scrape_site_id").Find(&keys).Error; err != nil {
		t.Fatal(err)
	}
	assertSourceStatsCounts(t, db, keys, 1, 0)
}

func TestReadModelSubprefixOrdinaryPassLimitAndOperationCountsPostgres(t *testing.T) {
	db, _, owners := seedReadModelFanoutTest(t)
	t.Setenv("READ_MODEL_REFRESH_BATCH_SIZE", "2")
	t.Setenv("READ_MODEL_REFRESH_MAX_BATCHES", "4")
	t.Setenv("READ_MODEL_REFRESH_TIMEOUT_SECONDS", "1")
	queries := &checkerScaleQueryLogger{Interface: db.Logger}
	DB = db.Session(&gorm.Session{Logger: queries})
	attempts := captureReadModelSubprefixAttempts(t, db)
	removeTrigger := installReadModelSubprefixDeadlineTrigger(t, db, owners[1])
	flushReadModelDirtyProxyIDs(context.Background(), false)
	requireReadModelSubprefixState(t, 0, 8, 2, 1)
	if got := attempts.snapshot(); !reflect.DeepEqual(got, []int{2}) {
		t.Fatal("deadline retried in its original invocation", got)
	}
	if err := removeTrigger(); err != nil {
		t.Fatal(err)
	}
	flushReadModelDirtyProxyIDs(context.Background(), false)
	requireReadModelSubprefixState(t, 5, 8, 0, 0)
	if got := attempts.snapshot(); !reflect.DeepEqual(got, []int{2, 1, 1, 1, 2}) {
		t.Fatal("ordinary pass exceeded four statements or lost contiguous progress", got)
	}
	finishReadModelSubprefixDrain(t)
	counts := map[string]int{}
	for _, sql := range queries.queries {
		switch {
		case strings.Contains(sql, "INSERT INTO user_proxy_filter_indexes"):
			counts["projection_attempts"]++
		case strings.Contains(sql, "information_schema.tables") && strings.Contains(sql, "user_proxy_filter_indexes"):
			counts["projection_table_checks"]++
		case strings.Contains(sql, `FROM "user_proxies"`):
			counts["owner_discovery"]++
		}
	}
	if counts["projection_attempts"] != 7 || counts["projection_table_checks"] != 7 || counts["owner_discovery"] != 1 {
		t.Fatal("exceptional round trips or owner rediscovery changed unexpectedly", counts)
	}
	t.Logf("one injected deadline: %v; unchanged no-timeout reference is four projections and four projection table checks", counts)
}
