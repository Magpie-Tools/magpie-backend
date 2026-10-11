package database

import (
	"context"
	"strings"
	"testing"
	"time"

	"magpie/internal/domain"
	"magpie/internal/security"
	"magpie/internal/support"

	"gorm.io/gorm"
)

func developmentSeedTestOptions() DevelopmentSeedOptions {
	return DevelopmentSeedOptions{Emails: []string{"first@example.invalid", "second@example.invalid", "third@example.invalid"},
		ProxiesPerWorkspace: 20, HistoryDays: 3, Seed: 42, Now: time.Now().UTC().Truncate(time.Minute)}
}

func setupDevelopmentSeedTest(t *testing.T) (*gorm.DB, DevelopmentSeedOptions, []domain.User) {
	t.Helper()
	db := setupProxyIngestionPostgresTest(t)
	if err := seedDefaults(db); err != nil {
		t.Fatal(err)
	}
	options := developmentSeedTestOptions()
	password, err := support.HashPassword("DevelopmentPassword123")
	if err != nil {
		t.Fatal(err)
	}
	users := make([]domain.User, 0, 3)
	for i, email := range options.Emails {
		user := domain.User{Email: email, Password: password, Role: "user"}
		if i == 0 {
			user.Role = "admin"
		}
		if err := db.Create(&user).Error; err != nil {
			t.Fatal(err)
		}
		if _, err := CreatePersonalWorkspaceForUser(db, user); err != nil {
			t.Fatal(err)
		}
		users = append(users, user)
	}
	return db, options, users
}

func TestDevelopmentSeedResetsAndPreservesUnrelatedDataPostgres(t *testing.T) {
	db, options, users := setupDevelopmentSeedTest(t)
	outsider := domain.User{Email: "unrelated@example.invalid", Password: "unchanged", Role: "user"}
	if err := db.Create(&outsider).Error; err != nil {
		t.Fatal(err)
	}
	outsideWorkspace, err := CreatePersonalWorkspaceForUser(db, outsider)
	if err != nil {
		t.Fatal(err)
	}
	shared := domain.Proxy{IP: "203.0.113.254", Port: 3128, Username: "outside-user", Password: "outside-password", Country: "Austria", EstimatedType: "ISP"}
	if err := db.Create(&shared).Error; err != nil {
		t.Fatal(err)
	}
	var oldMembership domain.WorkspaceMembership
	if err := db.Where("user_id = ?", users[0].ID).First(&oldMembership).Error; err != nil {
		t.Fatal(err)
	}
	for _, id := range []uint{oldMembership.WorkspaceID, outsideWorkspace.ID} {
		if err := db.Create(&domain.ManagedProxy{WorkspaceID: id, ProxyID: shared.ID, Username: shared.Username, Password: shared.Password}).Error; err != nil {
			t.Fatal(err)
		}
	}
	for run := 0; run < 2; run++ {
		plan, err := PlanDevelopmentSeed(context.Background(), db, options)
		if err != nil {
			t.Fatal(err)
		}
		if len(plan.MissingAccounts) != 0 || plan.NewRoutes != 100 {
			t.Fatalf("unexpected plan: %+v", plan)
		}
		result, err := SeedDevelopmentData(context.Background(), db, options)
		if err != nil {
			t.Fatal(err)
		}
		if result.Workspaces != 5 || result.Proxies != 100 || result.Sources != 60 || result.Tags != 40 || result.Rotators != 20 || result.AlertRules != 30 || result.Incidents != 90 {
			t.Fatalf("unexpected seed totals: %+v", result)
		}
		for _, count := range []struct {
			model any
			want  int64
		}{{&domain.User{}, 4}, {&domain.Workspace{}, 6}, {&domain.ManagedProxy{}, 101}, {&domain.Proxy{}, 101}, {&domain.WorkspaceProxyFilterIndex{}, 100}, {&domain.ScrapeSite{}, 60}, {&domain.WorkspaceInvitation{}, 3}} {
			var got int64
			if err := db.Model(count.model).Count(&got).Error; err != nil || got != count.want {
				t.Fatalf("%T count=%d want=%d error=%v", count.model, got, count.want, err)
			}
		}
		for _, original := range append(users, outsider) {
			var current domain.User
			if err := db.First(&current, original.ID).Error; err != nil {
				t.Fatal(err)
			}
			if current.Password != original.Password || current.Role != original.Role || current.Email != original.Email {
				t.Fatal("account identity changed")
			}
		}
		var outsideRoute domain.ManagedProxy
		if err := db.Where("workspace_id = ? AND proxy_id = ?", outsideWorkspace.ID, shared.ID).First(&outsideRoute).Error; err != nil {
			t.Fatal(err)
		}
		if outsideRoute.Username != shared.Username || outsideRoute.Password != shared.Password {
			t.Fatal("unrelated route credentials changed")
		}
		var unsafe int64
		if err := db.Table("user_proxies").Where("username <> '' AND username NOT LIKE 'enc:%'").Count(&unsafe).Error; err != nil || unsafe != 0 {
			t.Fatalf("unencrypted credentials=%d error=%v", unsafe, err)
		}
		if err := db.Model(&domain.AlertDestination{}).Where("enabled = ?", true).Count(&unsafe).Error; err != nil || unsafe != 0 {
			t.Fatalf("fixture destinations enabled=%d error=%v", unsafe, err)
		}
		var statisticCount, dailyCount int64
		if err := db.Model(&domain.ProxyStatistic{}).Count(&statisticCount).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Model(&domain.ProxyDailyCheck{}).Select("COALESCE(SUM(checks_count),0)").Scan(&dailyCount).Error; err != nil {
			t.Fatal(err)
		}
		if statisticCount != int64(result.Statistics) || dailyCount != statisticCount {
			t.Fatalf("history counters disagree: statistics=%d daily=%d result=%d", statisticCount, dailyCount, result.Statistics)
		}
		var workspace domain.Workspace
		if err := db.Where("name = ?", "Personal lab").First(&workspace).Error; err != nil {
			t.Fatal(err)
		}
		if _, err := LoadCheckerWorkspace(context.Background(), workspace.ID); err != nil {
			t.Fatal(err)
		}
		var reloaded domain.Workspace
		if err := db.First(&reloaded, workspace.ID).Error; err != nil {
			t.Fatal(err)
		}
		if reloaded.CheckerGeneration != workspace.CheckerGeneration {
			t.Fatal("startup invalidated seeded checker evidence")
		}
		var alive, dead, unknown int64
		for _, state := range []struct {
			where string
			count *int64
		}{{"health_overall > 0", &alive}, {"health_overall = 0", &dead}, {"health_overall IS NULL", &unknown}} {
			if err := db.Model(&domain.WorkspaceProxyFilterIndex{}).Where("workspace_id = ?", workspace.ID).Where(state.where).Count(state.count).Error; err != nil {
				t.Fatal(err)
			}
		}
		if alive == 0 || dead == 0 || unknown == 0 || alive+dead+unknown != 20 {
			t.Fatalf("fixture health missing: alive=%d dead=%d unknown=%d", alive, dead, unknown)
		}
	}
}

func TestDevelopmentSeedRefusesOutsideMembersWithoutChangingDataPostgres(t *testing.T) {
	db, options, users := setupDevelopmentSeedTest(t)
	outsider := domain.User{Email: "outside@example.invalid", Password: "unchanged", Role: "user"}
	if err := db.Create(&outsider).Error; err != nil {
		t.Fatal(err)
	}
	var membership domain.WorkspaceMembership
	if err := db.Where("user_id = ?", users[0].ID).First(&membership).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&domain.WorkspaceMembership{WorkspaceID: membership.WorkspaceID, UserID: outsider.ID, Role: domain.WorkspaceRoleViewer}).Error; err != nil {
		t.Fatal(err)
	}
	for _, action := range []func() error{
		func() error { _, err := PlanDevelopmentSeed(context.Background(), db, options); return err },
		func() error { _, err := SeedDevelopmentData(context.Background(), db, options); return err },
	} {
		if err := action(); err == nil || !strings.Contains(err.Error(), "unselected user") {
			t.Fatalf("unsafe reset accepted: %v", err)
		}
	}
	var workspaces int64
	if err := db.Model(&domain.Workspace{}).Count(&workspaces).Error; err != nil || workspaces != 3 {
		t.Fatalf("workspaces changed on refusal: %d, %v", workspaces, err)
	}
}

func TestDevelopmentSeedRollsBackAfterFixtureInsertFailurePostgres(t *testing.T) {
	db, options, users := setupDevelopmentSeedTest(t)
	if err := db.Exec(`CREATE FUNCTION reject_fixture_insert() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'forced fixture failure'; END $$`).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`CREATE TRIGGER reject_fixture_insert BEFORE INSERT ON proxies FOR EACH ROW EXECUTE FUNCTION reject_fixture_insert()`).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := SeedDevelopmentData(context.Background(), db, options); err == nil || !strings.Contains(err.Error(), "forced fixture failure") {
		t.Fatalf("expected fixture failure, got %v", err)
	}
	for _, user := range users {
		var membership domain.WorkspaceMembership
		if err := db.Where("user_id = ? AND is_default = ?", user.ID, true).First(&membership).Error; err != nil {
			t.Fatalf("old default workspace was not restored: %v", err)
		}
	}
	var workspaces, sources int64
	if err := db.Model(&domain.Workspace{}).Count(&workspaces).Error; err != nil || workspaces != 3 {
		t.Fatalf("old workspaces not restored: %d, %v", workspaces, err)
	}
	if err := db.Model(&domain.ScrapeSite{}).Count(&sources).Error; err != nil || sources != 0 {
		t.Fatalf("partial fixtures committed: %d, %v", sources, err)
	}
}

func TestDevelopmentSeedCreatesMissingAccountsAndRequiresPasswordPostgres(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	if err := seedDefaults(db); err != nil {
		t.Fatal(err)
	}
	options := developmentSeedTestOptions()
	if _, err := SeedDevelopmentData(context.Background(), db, options); err == nil {
		t.Fatal("missing password accepted for new accounts")
	}
	options.Password = "DevelopmentPassword123"
	if _, err := SeedDevelopmentData(context.Background(), db, options); err != nil {
		t.Fatal(err)
	}
	var users []domain.User
	if err := db.Order("id").Find(&users).Error; err != nil {
		t.Fatal(err)
	}
	if len(users) != 3 || users[0].Role != "admin" || users[1].Role != "user" || users[2].Role != "user" {
		t.Fatal("fresh installation roles incorrect")
	}
	for _, user := range users {
		if !support.CheckPasswordHash(options.Password, user.Password) {
			t.Fatal("new account password is invalid")
		}
	}
	var route domain.ManagedProxy
	if err := db.Where("username <> ''").First(&route).Error; err != nil {
		t.Fatal(err)
	}
	if !security.IsProxySecretEncrypted(route.UsernameEncrypted) || route.Username == "" || route.Password != "FixtureProxy12345" {
		t.Fatal("fixture credentials did not use encrypted storage")
	}
}
