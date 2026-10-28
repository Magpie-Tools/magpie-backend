package checker

import (
	"testing"
	"time"

	"magpie/internal/domain"
)

func TestFailureResetOnlyUpdatesNonzeroStreaks(t *testing.T) {
	db := setupCheckerTestDB(t)
	workspace := domain.Workspace{Name: "Failure reset mutations"}
	if err := db.Create(&workspace).Error; err != nil {
		t.Fatal(err)
	}
	proxies := []domain.Proxy{{IP: "192.0.2.1", Port: 8080}, {IP: "192.0.2.2", Port: 8080}}
	if err := db.Create(&proxies).Error; err != nil {
		t.Fatal(err)
	}
	old := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	for i, proxy := range proxies {
		if err := db.Exec(`INSERT INTO user_proxies (workspace_id,proxy_id,consecutive_failures,state,updated_at)
VALUES (?,?,?,'active',?)`, workspace.ID, proxy.ID, i*3, old).Error; err != nil {
			t.Fatal(err)
		}
	}
	for _, sql := range []string{
		`CREATE TABLE observed_failure_updates (proxy_id bigint)`,
		`CREATE TRIGGER observe_failure_update AFTER UPDATE ON user_proxies BEGIN
INSERT INTO observed_failure_updates (proxy_id) VALUES (NEW.proxy_id); END`,
	} {
		if err := db.Exec(sql).Error; err != nil {
			t.Fatal(err)
		}
	}
	pairs := []failurePair{{workspace.ID, proxies[0].ID}, {workspace.ID, proxies[1].ID}}
	for i := 0; i < 2; i++ {
		if err := execResetChunk(pairs); err != nil {
			t.Fatal(err)
		}
	}
	var updated []uint64
	if err := db.Table("observed_failure_updates").Pluck("proxy_id", &updated).Error; err != nil {
		t.Fatal(err)
	}
	if len(updated) != 1 || updated[0] != proxies[1].ID {
		t.Fatal("successful zero streaks caused physical updates", updated)
	}
	var unchanged struct {
		ConsecutiveFailures uint16
		UpdatedAt           time.Time
	}
	if err := db.Table("user_proxies").Where("workspace_id=? AND proxy_id=?", workspace.ID, proxies[0].ID).Scan(&unchanged).Error; err != nil {
		t.Fatal(err)
	}
	if unchanged.ConsecutiveFailures != 0 || !unchanged.UpdatedAt.Equal(old) {
		t.Fatal("zero streak changed its mutation timestamp", unchanged)
	}
}
