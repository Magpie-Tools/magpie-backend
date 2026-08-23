package database

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"magpie/internal/api/dto"
	"magpie/internal/domain"
	"magpie/internal/security"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func setupProxyTagTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	t.Setenv("PROXY_ENCRYPTION_KEY", "proxy-tag-test-key")
	security.ResetProxyCipherForTests()
	t.Cleanup(security.ResetProxyCipherForTests)

	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared&_fk=1", t.Name())
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open tag test database: %v", err)
	}
	if err := db.AutoMigrate(
		&domain.User{},
		&domain.Proxy{},
		&domain.UserProxy{},
		&domain.ProxyTag{},
		&domain.ProxyTagAssignment{},
		&domain.UserProxyFilterIndex{},
	); err != nil {
		t.Fatalf("migrate tag schema: %v", err)
	}

	previousDB := DB
	DB = db
	t.Cleanup(func() { DB = previousDB })
	return db
}

func TestProxyTagFiltersUseAnyMatchAndSearchIncludesNumericNames(t *testing.T) {
	db := setupProxyTagTestDB(t)
	user := domain.User{Email: "tag-filters@example.test", Password: "hash", Role: "user"}
	if err := db.Create(&user).Error; err != nil {
		t.Fatalf("create user: %v", err)
	}

	proxies := []domain.Proxy{
		{Port: 8080, Country: "DE", EstimatedType: "residential"},
		{Port: 3128, Country: "US", EstimatedType: "datacenter"},
	}
	for index, address := range []string{"192.0.2.51", "192.0.2.52"} {
		if err := proxies[index].SetIP(address); err != nil {
			t.Fatalf("set proxy %d address: %v", index, err)
		}
		if err := db.Create(&proxies[index]).Error; err != nil {
			t.Fatalf("create proxy %d: %v", index, err)
		}
		if err := db.Create(&domain.UserProxy{UserID: user.ID, ProxyID: proxies[index].ID}).Error; err != nil {
			t.Fatalf("create proxy %d access: %v", index, err)
		}
		if err := db.Create(&domain.UserProxyFilterIndex{
			UserID:          user.ID,
			ProxyID:         proxies[index].ID,
			Host:            proxies[index].IP,
			IPAddress:       proxies[index].IPAddress,
			Port:            proxies[index].Port,
			Country:         proxies[index].Country,
			CountryKey:      strings.ToLower(proxies[index].Country),
			EstimatedType:   proxies[index].EstimatedType,
			TypeKey:         strings.ToLower(proxies[index].EstimatedType),
			AnonymityLevel:  "N/A",
			AnonymityKey:    "n/a",
			ReputationLabel: "unknown",
		}).Error; err != nil {
			t.Fatalf("create proxy %d filter row: %v", index, err)
		}
	}

	numericTag, err := CreateProxyTag(user.ID, "1234", "#0EA5E9")
	if err != nil {
		t.Fatalf("create numeric tag: %v", err)
	}
	providerTag, err := CreateProxyTag(user.ID, "Provider B", "#F97316")
	if err != nil {
		t.Fatalf("create provider tag: %v", err)
	}
	if _, err := ReplaceProxyTags(user.ID, proxies[0].ID, []uint64{numericTag.ID}); err != nil {
		t.Fatalf("tag first proxy: %v", err)
	}
	if _, err := ReplaceProxyTags(user.ID, proxies[1].ID, []uint64{providerTag.ID}); err != nil {
		t.Fatalf("tag second proxy: %v", err)
	}

	rows, total := GetProxyInfoPageWithFiltersAndOptions(user.ID, 1, 40, "", dto.ProxyListFilters{
		TagIDs: []uint64{numericTag.ID, providerTag.ID},
	}, ProxyPageQueryOptions{})
	if total != 2 || len(rows) != 2 {
		t.Fatalf("ANY tag filter returned %d rows with total %d, want 2", len(rows), total)
	}

	rows, total = GetProxyInfoPageWithFiltersAndOptions(user.ID, 1, 40, "", dto.ProxyListFilters{
		TagIDs: []uint64{numericTag.ID},
	}, ProxyPageQueryOptions{})
	if total != 1 || len(rows) != 1 || uint64(rows[0].Id) != proxies[0].ID {
		t.Fatalf("single tag filter returned %#v with total %d, want proxy %d", rows, total, proxies[0].ID)
	}

	rows, total = GetProxyInfoPageWithFiltersAndOptions(user.ID, 1, 40, "1234", dto.ProxyListFilters{}, ProxyPageQueryOptions{})
	if total != 1 || len(rows) != 1 || uint64(rows[0].Id) != proxies[0].ID {
		t.Fatalf("numeric tag search returned %#v with total %d, want proxy %d", rows, total, proxies[0].ID)
	}
}

func TestProxyTagsAreUserOwnedAndManyToMany(t *testing.T) {
	db := setupProxyTagTestDB(t)
	users := []domain.User{
		{Email: "first-tags@example.test", Password: "hash", Role: "user"},
		{Email: "second-tags@example.test", Password: "hash", Role: "user"},
	}
	if err := db.Create(&users).Error; err != nil {
		t.Fatalf("create users: %v", err)
	}

	proxy := domain.Proxy{Port: 8080, Country: "N/A", EstimatedType: "N/A"}
	if err := proxy.SetIP("192.0.2.44"); err != nil {
		t.Fatalf("set proxy IP: %v", err)
	}
	if err := db.Create(&proxy).Error; err != nil {
		t.Fatalf("create proxy: %v", err)
	}
	for _, user := range users {
		if err := db.Create(&domain.UserProxy{UserID: user.ID, ProxyID: proxy.ID}).Error; err != nil {
			t.Fatalf("create proxy access: %v", err)
		}
	}

	provider, err := CreateProxyTag(users[0].ID, "Provider A", "#0ea5e9")
	if err != nil {
		t.Fatalf("create provider tag: %v", err)
	}
	residential, err := CreateProxyTag(users[0].ID, "Residential", "#22c55e")
	if err != nil {
		t.Fatalf("create residential tag: %v", err)
	}
	otherUserTag, err := CreateProxyTag(users[1].ID, "Provider A", "#f97316")
	if err != nil {
		t.Fatalf("create same-name tag for other user: %v", err)
	}

	tags, err := ReplaceProxyTags(users[0].ID, proxy.ID, []uint64{provider.ID})
	if err != nil {
		t.Fatalf("assign provider tag: %v", err)
	}
	assertProxyTagIDs(t, tags, provider.ID)

	if err := AddProxyTagsToProxies(users[0].ID, []uint64{proxy.ID}, []uint64{residential.ID}); err != nil {
		t.Fatalf("add import tags: %v", err)
	}
	tags, err = getProxyTagsForProxy(users[0].ID, proxy.ID)
	if err != nil {
		t.Fatalf("load first user's tags: %v", err)
	}
	assertProxyTagIDs(t, tags, provider.ID, residential.ID)

	if _, err := ReplaceProxyTags(users[0].ID, proxy.ID, []uint64{otherUserTag.ID}); !errors.Is(err, ErrProxyTagNotFound) {
		t.Fatalf("cross-user assignment error = %v, want ErrProxyTagNotFound", err)
	}
	tags, err = getProxyTagsForProxy(users[0].ID, proxy.ID)
	if err != nil {
		t.Fatalf("reload tags after rejected assignment: %v", err)
	}
	assertProxyTagIDs(t, tags, provider.ID, residential.ID)

	otherTags, err := ReplaceProxyTags(users[1].ID, proxy.ID, []uint64{otherUserTag.ID})
	if err != nil {
		t.Fatalf("assign other user's tag: %v", err)
	}
	assertProxyTagIDs(t, otherTags, otherUserTag.ID)

	infos := []dto.ProxyInfo{{Id: int(proxy.ID)}}
	if err := AttachProxyTagsToInfos(users[0].ID, infos); err != nil {
		t.Fatalf("attach tags to proxy info: %v", err)
	}
	assertProxyTagIDs(t, infos[0].Tags, provider.ID, residential.ID)

	if err := DeleteProxyTag(users[0].ID, provider.ID); err != nil {
		t.Fatalf("delete provider tag: %v", err)
	}
	tags, err = getProxyTagsForProxy(users[0].ID, proxy.ID)
	if err != nil {
		t.Fatalf("load tags after deletion: %v", err)
	}
	assertProxyTagIDs(t, tags, residential.ID)

	if err := db.Where("user_id = ? AND proxy_id = ?", users[0].ID, proxy.ID).
		Delete(&domain.UserProxy{}).Error; err != nil {
		t.Fatalf("delete first user's proxy access: %v", err)
	}
	var firstUserAssignments int64
	if err := db.Model(&domain.ProxyTagAssignment{}).
		Where("user_id = ? AND proxy_id = ?", users[0].ID, proxy.ID).
		Count(&firstUserAssignments).Error; err != nil {
		t.Fatalf("count first user's assignments: %v", err)
	}
	if firstUserAssignments != 0 {
		t.Fatalf("first user's assignments after access deletion = %d, want 0", firstUserAssignments)
	}

	otherTags, err = getProxyTagsForProxy(users[1].ID, proxy.ID)
	if err != nil {
		t.Fatalf("load other user's tags after first access deletion: %v", err)
	}
	assertProxyTagIDs(t, otherTags, otherUserTag.ID)
}

func assertProxyTagIDs(t *testing.T, tags []dto.ProxyTag, expected ...uint64) {
	t.Helper()
	if len(tags) != len(expected) {
		t.Fatalf("tag count = %d, want %d: %#v", len(tags), len(expected), tags)
	}
	seen := make(map[uint64]struct{}, len(tags))
	for _, tag := range tags {
		seen[tag.ID] = struct{}{}
	}
	for _, id := range expected {
		if _, ok := seen[id]; !ok {
			t.Fatalf("missing tag ID %d in %#v", id, tags)
		}
	}
}
