package database

import (
	"context"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"magpie/internal/domain"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

type reimportUsageQueries struct {
	logger.Interface
	mu     sync.Mutex
	counts int
	writes int
}

func (q *reimportUsageQueries) Trace(_ context.Context, _ time.Time, sql func() (string, int64), _ error) {
	query, _ := sql()
	query = strings.ToLower(strings.ReplaceAll(query, `"`, ""))
	q.mu.Lock()
	defer q.mu.Unlock()
	if strings.Contains(query, "select count(*) from user_proxies") {
		q.counts++
	}
	if strings.Contains(query, "insert into workspace_usage_periods") {
		q.writes++
	}
}

func reimportUsageFixture(t *testing.T, mode string, capacity uint64) (*gorm.DB, domain.Workspace) {
	t.Helper()
	db := setupProxyIngestionPostgresTest(t)
	w := domain.Workspace{Name: "Reimport usage"}
	if err := db.Create(&w).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&domain.WorkspaceSubscription{WorkspaceID: w.ID, OverageMode: mode, IncludedActiveRoutes: capacity}).Error; err != nil {
		t.Fatal(err)
	}
	return db, w
}

func reimportUsagePeriod(t *testing.T, db *gorm.DB, workspaceID uint) domain.WorkspaceUsagePeriod {
	t.Helper()
	var usage domain.WorkspaceUsagePeriod
	if err := db.Where("workspace_id = ?", workspaceID).Order("period_start DESC").First(&usage).Error; err != nil {
		t.Fatal(err)
	}
	return usage
}

func TestProxyReimportPreservesUsagePostgres(t *testing.T) {
	for _, mode := range []string{domain.WorkspaceOverageDisabled, domain.WorkspaceOverageUnlimited} {
		t.Run(mode, func(t *testing.T) {
			db, w := reimportUsageFixture(t, mode, 3)
			input := []domain.Proxy{{IP: "192.0.2.31", Port: 8080, Username: "first", Password: "secret"}, {IP: "192.0.2.32", Port: 8080}, {IP: "192.0.2.33", Port: 8080}}
			saved, err := InsertAndGetProxiesWithWorkspace(input, w.ID)
			if err != nil {
				t.Fatal(err)
			}
			for i, state := range []string{domain.ManagedProxyStateActive, domain.ManagedProxyStatePaused, domain.ManagedProxyStateArchived} {
				if err := SetManagedProxyState(w.ID, saved[i].ID, state); err != nil {
					t.Fatal(err)
				}
			}
			if err := db.Model(&domain.WorkspaceUsagePeriod{}).Where("workspace_id = ?", w.ID).
				Updates(map[string]any{"check_attempts": 77, "managed_requests": 33, "managed_bytes": 444}).Error; err != nil {
				t.Fatal(err)
			}
			before := reimportUsagePeriod(t, db, w.ID)
			queries := &reimportUsageQueries{Interface: logger.Default.LogMode(logger.Silent)}
			DB = db.Session(&gorm.Session{Logger: queries})
			again, err := InsertAndGetProxiesWithWorkspace(input, w.ID)
			DB = db
			if err != nil {
				t.Fatal(err)
			}
			if len(again) != len(saved) || queries.counts != 0 || queries.writes != 0 {
				t.Fatalf("reimport rows=%d inventory counts=%d usage writes=%d", len(again), queries.counts, queries.writes)
			}
			if after := reimportUsagePeriod(t, db, w.ID); !reflect.DeepEqual(before, after) {
				t.Fatal("reimport changed capacity, peak, usage deltas or timestamps")
			}
			for i, state := range []string{domain.ManagedProxyStateActive, domain.ManagedProxyStatePaused, domain.ManagedProxyStateArchived} {
				var managed domain.ManagedProxy
				if err := db.Where("workspace_id = ? AND proxy_id = ?", w.ID, saved[i].ID).First(&managed).Error; err != nil {
					t.Fatal(err)
				}
				if managed.State != state || managed.Username != input[i].Username || managed.Password != input[i].Password {
					t.Fatal("reimport changed lifecycle or lost encrypted credentials")
				}
			}
			t.Log("Existing active/paused/archived management reimport: inventory counts=0, usage writes=0; capacity, peak and deltas unchanged")
		})
	}
}

func TestProxyReimportMixedRowsRefreshUsagePostgres(t *testing.T) {
	db, w := reimportUsageFixture(t, domain.WorkspaceOverageDisabled, 2)
	first := domain.Proxy{IP: "192.0.2.41", Port: 8080}
	if _, err := InsertAndGetProxiesWithWorkspace([]domain.Proxy{first}, w.ID); err != nil {
		t.Fatal(err)
	}
	input := []domain.Proxy{first, {IP: "192.0.2.42", Port: 8080, Username: "second", Password: "second-secret"}, {IP: "192.0.2.43", Port: 8080}}
	if _, err := InsertAndGetProxiesWithWorkspace(input, w.ID); err != nil {
		t.Fatal(err)
	}
	usage := reimportUsagePeriod(t, db, w.ID)
	var active, paused int64
	if err := db.Model(&domain.ManagedProxy{}).Where("workspace_id = ? AND state = ?", w.ID, domain.ManagedProxyStateActive).Count(&active).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&domain.ManagedProxy{}).Where("workspace_id = ? AND state = ?", w.ID, domain.ManagedProxyStatePaused).Count(&paused).Error; err != nil {
		t.Fatal(err)
	}
	if active != 2 || paused != 1 || usage.ActiveRoutes != 2 || usage.PeakActiveRoutes != 2 {
		t.Fatalf("mixed import: active=%d paused=%d usage=%+v", active, paused, usage)
	}
}

func TestProxyReimportCredentialUpdateIsScopedPostgres(t *testing.T) {
	db, a := reimportUsageFixture(t, domain.WorkspaceOverageUnlimited, 0)
	b := domain.Workspace{Name: "Other owner"}
	if err := db.Create(&b).Error; err != nil {
		t.Fatal(err)
	}
	input := domain.Proxy{IP: "192.0.2.51", Port: 8080, Username: "original", Password: "original-secret"}
	saved, err := InsertAndGetProxiesWithWorkspace([]domain.Proxy{input}, a.ID, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := SetManagedProxyState(a.ID, saved[0].ID, domain.ManagedProxyStatePaused); err != nil {
		t.Fatal(err)
	}
	changed := saved[0]
	changed.Username, changed.Password = "changed", "changed-secret"
	if err := db.Transaction(func(tx *gorm.DB) error {
		return createWorkspaceAssociations(tx, []domain.Proxy{changed}, a.ID, map[string]managedProxyStatePlan{string(changed.Hash): {State: domain.ManagedProxyStateActive}})
	}); err != nil {
		t.Fatal(err)
	}
	for _, owner := range []uint{a.ID, b.ID} {
		var managed domain.ManagedProxy
		if err := db.Where("workspace_id = ? AND proxy_id = ?", owner, changed.ID).First(&managed).Error; err != nil {
			t.Fatal(err)
		}
		if owner == a.ID {
			if managed.Username != changed.Username || managed.Password != changed.Password || managed.State != domain.ManagedProxyStatePaused {
				t.Fatal("credential update lost credentials or changed lifecycle")
			}
		} else if managed.Username != input.Username || managed.Password != input.Password {
			t.Fatal("credential update crossed workspace ownership")
		}
	}
}

func TestProxyReimportDoesNotResurrectConcurrentDeletionPostgres(t *testing.T) {
	db, w := reimportUsageFixture(t, domain.WorkspaceOverageUnlimited, 0)
	input := []domain.Proxy{{IP: "192.0.2.61", Port: 8080}}
	saved, err := InsertAndGetProxiesWithWorkspace(input, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	probe, continueImport := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(continueImport) }) }
	if err := db.Callback().Create().After("gorm:create").Register("test:reimport_delete_between_statements", func(tx *gorm.DB) {
		conflict, ok := tx.Statement.Clauses["ON CONFLICT"].Expression.(clause.OnConflict)
		if tx.Statement.Table == "user_proxies" && ok && conflict.DoNothing {
			close(probe)
			<-continueImport
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Callback().Create().Remove("test:reimport_delete_between_statements") })
	done := make(chan error, 1)
	go func() { _, err := InsertAndGetProxiesWithWorkspace(input, w.ID); done <- err }()
	joined := false
	defer func() {
		unblock()
		if !joined {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("reimport did not join")
			}
		}
	}()
	select {
	case <-probe:
	case <-time.After(5 * time.Second):
		t.Fatal("import did not reach the conflict probe")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("workspace_id = ? AND proxy_id = ?", w.ID, saved[0].ID).Delete(&domain.ManagedProxy{}).Error; err != nil {
			return err
		}
		return refreshWorkspaceUsageActiveRoutes(tx, w.ID)
	}); err != nil {
		t.Fatal(err)
	}
	unblock()
	select {
	case err := <-done:
		joined = true
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("reimport did not finish")
	}
	var count int64
	if err := db.Model(&domain.ManagedProxy{}).Where("workspace_id = ?", w.ID).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if usage := reimportUsagePeriod(t, db, w.ID); count != 0 || usage.ActiveRoutes != 0 || usage.PeakActiveRoutes != 1 {
		t.Fatal("credential update resurrected deleted management or changed capacity")
	}
}
