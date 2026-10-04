package settings

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"magpie/internal/api/dto"
	"magpie/internal/checkerconfig"
	"magpie/internal/database"
	"magpie/internal/domain"
)

func TestColumnOnlySaveSkipsWorkspaceMutationAndCheckerRefresh(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:checker_preference_save?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	conn, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := db.AutoMigrate(&domain.User{}, &domain.Workspace{}, &domain.WorkspaceMemberPreference{}); err != nil {
		t.Fatal(err)
	}
	oldDB := database.DB
	database.DB = db
	t.Cleanup(func() { database.DB = oldDB })
	workspace := domain.Workspace{Name: "Preferences", HTTPProtocol: true, CheckerGeneration: 42, CheckerRevision: 9, CheckerProjectedRevision: 9}
	if err := db.Create(&workspace).Error; err != nil {
		t.Fatal(err)
	}
	user := domain.User{Email: "preferences@example.test", Password: "hash"}
	if err := db.Create(&user).Error; err != nil {
		t.Fatal(err)
	}
	var loads atomic.Int64
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := checkerconfig.Initialize(ctx, nil, func(context.Context, uint) (*checkerconfig.Workspace, error) { loads.Add(1); return nil, nil }, func(context.Context) ([]uint, error) { return nil, nil }); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		stopped, stop := context.WithCancel(context.Background())
		stop()
		_ = checkerconfig.Initialize(stopped, nil, nil, func(context.Context) ([]uint, error) { return nil, nil })
	})
	for _, field := range []string{"proxy_list_columns", "scrape_source_proxy_columns", "scrape_source_list_columns"} {
		var value dto.UserSettings
		if err := json.Unmarshal([]byte(`{"`+field+`":["ip_port","country"]}`), &value); err != nil {
			t.Fatal(err)
		}
		if err := SaveWorkspace(workspace.ID, user.ID, value); err != nil {
			t.Fatal(err)
		}
	}
	var saved domain.Workspace
	if err := db.First(&saved, workspace.ID).Error; err != nil {
		t.Fatal(err)
	}
	if loads.Load() != 0 || saved.CheckerDirty || saved.CheckerGeneration != 42 || saved.CheckerRevision != 9 || !saved.UpdatedAt.Equal(workspace.UpdatedAt) {
		t.Fatal("preference save touched checker workspace", loads.Load(), saved)
	}
	var preference domain.WorkspaceMemberPreference
	if err := db.Where("workspace_id=? AND user_id=?", workspace.ID, user.ID).First(&preference).Error; err != nil {
		t.Fatal(err)
	}
	if len(preference.ProxyListColumns) == 0 || len(preference.ScrapeSourceProxyColumns) == 0 || len(preference.ScrapeSourceListColumns) == 0 {
		t.Fatal("preferences not saved", preference)
	}
}
