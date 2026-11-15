package dashboard

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// afterFirstFullRead runs change once, right after the request's first full
// read returns, so the request continues from a state that is already old.
type afterFirstFullRead struct {
	logger.Interface
	once   sync.Once
	change func()
}

func (l *afterFirstFullRead) Trace(_ context.Context, _ time.Time, fc func() (string, int64), _ error) {
	if sql, _ := fc(); strings.Contains(sql, "s.payload AS payload") {
		l.once.Do(l.change)
	}
}

// A request whose claim is refused because another instance published in the
// meantime serves that publish, not the state it read before admission.
func TestRefusedAdmissionServesAPublishFromElsewherePostgres(t *testing.T) {
	db, workspaces := snapshotFixture(t)
	owner := workspaces[0].ID
	other := newTestService(db, Config{})
	defer other.Stop()
	hook := &afterFirstFullRead{Interface: logger.Default.LogMode(logger.Silent), change: func() {
		if view, err := other.View(context.Background(), owner); err != nil || view.Status != StatusFresh {
			t.Errorf("competing publish: %+v %v", view, err)
		}
	}}
	service := newTestService(db.Session(&gorm.Session{Logger: hook}), Config{})
	defer service.Stop()
	view, err := service.View(context.Background(), owner)
	if err != nil {
		t.Fatal(err)
	}
	if view.Status != StatusFresh || view.AliveCount() == nil || view.Rebuilding {
		t.Fatalf("a refused claim served the state from before admission: %+v", view)
	}
}

// A generation change committed while a refused admission runs hides the
// health read before it.
func TestGenerationChangeDuringRefusedAdmissionHidesHealthPostgres(t *testing.T) {
	db, workspaces := snapshotFixture(t)
	owner := workspaces[0].ID
	if _, err := newTestService(db, Config{}).View(context.Background(), owner); err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("UPDATE dashboard_snapshots SET computed_at = computed_at - interval '2 minutes', next_eligible_at = NULL WHERE workspace_id = ?", owner).Error; err != nil {
		t.Fatal(err)
	}
	hook := &afterFirstFullRead{Interface: logger.Default.LogMode(logger.Silent), change: func() {
		// Another instance commits a projection change and claims first.
		for _, statement := range []string{
			"UPDATE workspaces SET checker_generation = checker_generation + 1 WHERE id = ?",
			"UPDATE dashboard_snapshots SET next_eligible_at = now() + interval '1 minute' WHERE workspace_id = ?",
		} {
			if err := db.Exec(statement, owner).Error; err != nil {
				t.Error(err)
			}
		}
	}}
	service := newTestService(db.Session(&gorm.Session{Logger: hook}), Config{})
	defer service.Stop()
	view, err := service.View(context.Background(), owner)
	if err != nil {
		t.Fatal(err)
	}
	if view.AliveCount() != nil || view.FastestAliveProxies(5) != nil || view.Status != StatusPending || view.TotalScraped() == nil {
		t.Fatalf("health validated before the admission wait was served: %+v", view)
	}
}
