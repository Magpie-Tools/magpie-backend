package testdb

import (
	"testing"

	"magpie/internal/domain"
	"magpie/internal/security"
)

func TestOpenSQLiteMigratesEveryModelAndIsolatesTests(t *testing.T) {
	for _, name := range []string{"first", "second"} {
		t.Run(name, func(t *testing.T) {
			db := OpenSQLite(t)
			for _, model := range []any{
				&domain.User{}, &domain.Workspace{}, &domain.ManagedProxy{}, &domain.Proxy{},
				&domain.ProxyStatistic{}, &domain.AlertRule{}, &domain.EmailOutbox{},
			} {
				if !db.Migrator().HasTable(model) {
					t.Fatalf("%T has no table", model)
				}
			}
			user := domain.User{Email: "kit@example.test", Password: "hash"}
			if err := db.Create(&user).Error; err != nil {
				t.Fatalf("each test starts with an empty database: %v", err)
			}
			workspace := CreateWorkspaceForUser(t, db, user)
			proxy := domain.Proxy{IP: "192.0.2.7", Port: 8080}
			if err := db.Create(&proxy).Error; err != nil {
				t.Fatal(err)
			}
			managed := domain.ManagedProxy{WorkspaceID: workspace.ID, ProxyID: proxy.ID, Username: "user", Password: "secret"}
			if err := db.Create(&managed).Error; err != nil {
				t.Fatal(err)
			}
			var stored string
			if err := db.Raw("SELECT password FROM user_proxies WHERE proxy_id = ?", proxy.ID).Scan(&stored).Error; err != nil {
				t.Fatal(err)
			}
			if stored == "secret" || !security.IsProxySecretEncrypted(stored) {
				t.Fatalf("stored credential is not encrypted: %q", stored)
			}
		})
	}
}

func TestOpenPostgresUsesAnIsolatedMigratedSchema(t *testing.T) {
	first := OpenPostgres(t)
	second := OpenPostgres(t)
	var firstSchema, secondSchema string
	if err := first.Raw("SELECT current_schema()").Scan(&firstSchema).Error; err != nil {
		t.Fatal(err)
	}
	if err := second.Raw("SELECT current_schema()").Scan(&secondSchema).Error; err != nil {
		t.Fatal(err)
	}
	if firstSchema == secondSchema || firstSchema == "public" || secondSchema == "public" {
		t.Fatalf("schemas are not isolated: %q and %q", firstSchema, secondSchema)
	}
	if !first.Migrator().HasTable(&domain.ProxyLatestStatistic{}) {
		t.Fatal("the schema is not migrated")
	}
	if err := first.Create(&domain.User{Email: "kit@example.test", Password: "hash"}).Error; err != nil {
		t.Fatal(err)
	}
	var users int64
	if err := second.Model(&domain.User{}).Count(&users).Error; err != nil || users != 0 {
		t.Fatalf("second schema sees %d users: %v", users, err)
	}
}

// Inside integration mode an unreachable server must fail the test. The kit
// may neither skip nor fall back to SQLite.
func TestOpenPostgresFailsWhenTheServerIsUnavailable(t *testing.T) {
	probe := &recordingTB{TB: t}
	func() {
		defer func() { _ = recover() }()
		t.Setenv(EnvPostgresDSN, "host=127.0.0.1 port=1 user=nobody dbname=nothing sslmode=disable connect_timeout=1")
		OpenPostgres(probe)
	}()
	if !probe.failed || probe.skipped {
		t.Fatalf("unavailable server: failed=%v skipped=%v, want a failure", probe.failed, probe.skipped)
	}
}

func TestOpenPostgresSkipsOutsideIntegrationMode(t *testing.T) {
	probe := &recordingTB{TB: t}
	func() {
		defer func() { _ = recover() }()
		t.Setenv(EnvPostgresDSN, "")
		OpenPostgres(probe)
	}()
	if !probe.skipped || probe.failed {
		t.Fatalf("no DSN: failed=%v skipped=%v, want a skip", probe.failed, probe.skipped)
	}
}

// recordingTB records Skip and Fatal instead of ending the real test.
type recordingTB struct {
	testing.TB
	failed, skipped bool
}

func (r *recordingTB) Helper()                  {}
func (r *recordingTB) Setenv(key, value string) {}
func (r *recordingTB) Cleanup(func())           {}
func (r *recordingTB) Skip(args ...any)         { r.skipped = true; panic("skip") }
func (r *recordingTB) Fatalf(string, ...any)    { r.failed = true; panic("fatal") }
func (r *recordingTB) Fatal(args ...any)        { r.failed = true; panic("fatal") }
