package database

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"magpie/internal/api/dto"
	"magpie/internal/domain"

	"gorm.io/gorm"
)

func setupScrapeSourceTags(t *testing.T) (*gorm.DB, domain.ScrapeSite, []domain.Proxy, []uint64) {
	t.Helper()
	db := setupProxyTagTestDB(t)
	if err := db.AutoMigrate(&domain.WorkspaceScrapeSite{}, &domain.ScrapeSourceTag{}); err != nil {
		t.Fatal(err)
	}
	return seedScrapeSourceTags(t, db)
}

func seedScrapeSourceTags(t *testing.T, db *gorm.DB) (*gorm.DB, domain.ScrapeSite, []domain.Proxy, []uint64) {
	t.Helper()
	for id := uint(1); id <= 3; id++ {
		if err := db.Create(&domain.Workspace{ID: id, Name: fmt.Sprintf("Workspace %d", id)}).Error; err != nil {
			t.Fatal(err)
		}
	}
	site := domain.ScrapeSite{URL: "https://example.test/proxies"}
	if err := db.Create(&site).Error; err != nil {
		t.Fatal(err)
	}
	for _, id := range []uint{1, 2} {
		if err := db.Create(&domain.WorkspaceScrapeSite{WorkspaceID: id, ScrapeSiteID: site.ID, FetchMode: domain.ScrapeFetchHTTP}).Error; err != nil {
			t.Fatal(err)
		}
	}
	proxies := make([]domain.Proxy, 2)
	for i := range proxies {
		proxies[i].Port = 8080
		if err := proxies[i].SetIP(fmt.Sprintf("192.0.2.%d", i+1)); err != nil {
			t.Fatal(err)
		}
		if err := db.Create(&proxies[i]).Error; err != nil {
			t.Fatal(err)
		}
		for _, id := range []uint{1, 2, 3} {
			if err := db.Create(&domain.ManagedProxy{WorkspaceID: id, ProxyID: proxies[i].ID, State: domain.ManagedProxyStatePaused}).Error; err != nil {
				t.Fatal(err)
			}
		}
	}
	var tagIDs []uint64
	for i, workspaceID := range []uint{1, 1, 1, 2, 3} {
		tag, err := CreateProxyTag(workspaceID, fmt.Sprintf("Tag %d", i+1), "")
		if err != nil {
			t.Fatal(err)
		}
		tagIDs = append(tagIDs, tag.ID)
	}
	return db, site, proxies, tagIDs
}

func setSourceTags(t *testing.T, workspaceID uint, siteID uint64, ids ...uint64) {
	t.Helper()
	found, err := UpdateScrapeSourceSettings(context.Background(), workspaceID, siteID, dto.ScrapeSourceSettings{AutoTagIDs: &ids})
	if err != nil || !found {
		t.Fatalf("update source tags: found=%v err=%v", found, err)
	}
}

func assertProxyTags(t *testing.T, workspaceID uint, proxyID uint64, ids ...uint64) {
	t.Helper()
	tags, err := getProxyTagsForProxy(workspaceID, proxyID)
	if err != nil {
		t.Fatal(err)
	}
	want := make(map[uint64]bool)
	for _, id := range ids {
		want[id] = true
	}
	if len(tags) != len(want) {
		t.Fatalf("workspace %d proxy %d tags=%v want=%v", workspaceID, proxyID, tags, ids)
	}
	for _, tag := range tags {
		if !want[tag.ID] {
			t.Fatalf("unexpected tag %v", tag)
		}
	}
}

func TestSourceTagsAreFutureOnlyAdditiveAndWorkspaceScoped(t *testing.T) {
	db, site, proxies, tags := setupScrapeSourceTags(t)
	ctx := context.Background()
	if err := AddProxyTagsToProxies(1, []uint64{proxies[0].ID}, []uint64{tags[0]}); err != nil {
		t.Fatal(err)
	}
	setSourceTags(t, 1, site.ID, tags[1], tags[2], tags[1])
	setSourceTags(t, 2, site.ID, tags[3])
	assertProxyTags(t, 1, proxies[0].ID, tags[0])
	assertProxyTags(t, 1, proxies[1].ID)
	// Only the observed route and original participating workspace receive tags.
	for range 2 {
		if err := ApplyScrapeSourceTags(ctx, site.ID, domain.ScrapeFetchHTTP, []uint{1}, proxies[:1]); err != nil {
			t.Fatal(err)
		}
	}
	assertProxyTags(t, 1, proxies[0].ID, tags[0], tags[1], tags[2])
	assertProxyTags(t, 1, proxies[1].ID)
	assertProxyTags(t, 2, proxies[0].ID)
	assertProxyTags(t, 3, proxies[0].ID)
	var managed domain.ManagedProxy
	if err := db.Where("workspace_id = ? AND proxy_id = ?", 1, proxies[0].ID).First(&managed).Error; err != nil || managed.State != domain.ManagedProxyStatePaused {
		t.Fatalf("tagging changed lifecycle: %+v %v", managed, err)
	}
	if _, err := ReplaceProxyTags(1, proxies[0].ID, []uint64{tags[0]}); err != nil {
		t.Fatal(err)
	}
	if err := ApplyScrapeSourceTags(ctx, site.ID, domain.ScrapeFetchHTTP, []uint{1}, proxies[:1]); err != nil {
		t.Fatal(err)
	}
	assertProxyTags(t, 1, proxies[0].ID, tags[0], tags[1], tags[2])
	// A second source accumulates tags without replacing the first source's tags.
	other := domain.ScrapeSite{URL: "https://other.test/proxies"}
	if err := db.Create(&other).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&domain.WorkspaceScrapeSite{WorkspaceID: 1, ScrapeSiteID: other.ID, FetchMode: domain.ScrapeFetchHTTP}).Error; err != nil {
		t.Fatal(err)
	}
	setSourceTags(t, 1, other.ID, tags[0])
	if err := ApplyScrapeSourceTags(ctx, other.ID, domain.ScrapeFetchHTTP, []uint{1}, proxies[:1]); err != nil {
		t.Fatal(err)
	}
	assertProxyTags(t, 1, proxies[0].ID, tags[0], tags[1], tags[2])
}

func TestSourceTagSettingsAreAtomicAndDoNotChangeScrapeStatus(t *testing.T) {
	db, site, proxies, tags := setupScrapeSourceTags(t)
	ctx := context.Background()
	now := time.Now().UTC()
	if err := RecordScrapeResult(ctx, site.ID, []uint{1}, domain.ScrapeFetchHTTP, "success", 9, nil, now); err != nil {
		t.Fatal(err)
	}
	setSourceTags(t, 1, site.ID, tags[1])
	var source domain.WorkspaceScrapeSite
	if err := db.Where("workspace_id = ? AND scrape_site_id = ?", 1, site.ID).First(&source).Error; err != nil || source.LastScrapeStatus != "success" || source.FetchMode != domain.ScrapeFetchHTTP {
		t.Fatalf("tag-only update changed mode/status: %+v %v", source, err)
	}
	mode := domain.ScrapeFetchBrowser
	foreignTags := []uint64{tags[3]}
	if _, err := UpdateScrapeSourceSettings(ctx, 1, site.ID, dto.ScrapeSourceSettings{FetchMode: &mode, AutoTagIDs: &foreignTags}); !errors.Is(err, ErrProxyTagNotFound) {
		t.Fatalf("foreign tag accepted: %v", err)
	}
	configured, err := GetScrapeSourceTags(1, site.ID)
	if err != nil || len(configured) != 1 || configured[0].ID != tags[1] {
		t.Fatalf("failed update changed rules: %v %v", configured, err)
	}
	if found, err := UpdateScrapeSourceSettings(ctx, 3, site.ID, dto.ScrapeSourceSettings{FetchMode: &mode}); err != nil || found {
		t.Fatalf("unsubscribed workspace updated source: %v %v", found, err)
	}
	if err := ApplyScrapeSourceTags(ctx, site.ID, domain.ScrapeFetchHTTP, []uint{1}, proxies[:1]); err != nil {
		t.Fatal(err)
	}
	assertProxyTags(t, 1, proxies[0].ID, tags[1])
	setSourceTags(t, 1, site.ID)
	assertProxyTags(t, 1, proxies[0].ID, tags[1])
	if err := ApplyScrapeSourceTags(ctx, site.ID, domain.ScrapeFetchHTTP, []uint{1}, proxies[1:]); err != nil {
		t.Fatal(err)
	}
	assertProxyTags(t, 1, proxies[1].ID)
}

func TestSourceTagsUseLiveRulesAndCascadeWithoutRemovingAssignments(t *testing.T) {
	db, site, proxies, tags := setupScrapeSourceTags(t)
	ctx := context.Background()
	setSourceTags(t, 1, site.ID, tags[0])
	// Simulate a running fetch followed by a change before ingestion.
	setSourceTags(t, 1, site.ID, tags[1])
	if err := ApplyScrapeSourceTags(ctx, site.ID, domain.ScrapeFetchHTTP, []uint{1}, proxies[:1]); err != nil {
		t.Fatal(err)
	}
	assertProxyTags(t, 1, proxies[0].ID, tags[1])
	mode := domain.ScrapeFetchBrowser
	if _, err := UpdateScrapeSourceSettings(ctx, 1, site.ID, dto.ScrapeSourceSettings{FetchMode: &mode}); err != nil {
		t.Fatal(err)
	}
	if err := ApplyScrapeSourceTags(ctx, site.ID, domain.ScrapeFetchHTTP, []uint{1}, proxies[1:]); err != nil {
		t.Fatal(err)
	}
	assertProxyTags(t, 1, proxies[1].ID)
	if err := DeleteProxyTag(1, tags[1]); err != nil {
		t.Fatal(err)
	}
	var count int64
	if err := db.Model(&domain.ScrapeSourceTag{}).Where("proxy_tag_id = ?", tags[1]).Count(&count).Error; err != nil || count != 0 {
		t.Fatalf("deleted tag retained rules: %d %v", count, err)
	}
	setSourceTags(t, 1, site.ID, tags[2])
	if err := ApplyScrapeSourceTags(ctx, site.ID, mode, []uint{1}, proxies[:1]); err != nil {
		t.Fatal(err)
	}
	if err := db.Where("workspace_id = ? AND scrape_site_id = ?", 1, site.ID).Delete(&domain.WorkspaceScrapeSite{}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&domain.ScrapeSourceTag{}).Where("workspace_id = ? AND scrape_site_id = ?", 1, site.ID).Count(&count).Error; err != nil || count != 0 {
		t.Fatalf("deleted subscription retained rules: %d %v", count, err)
	}
	assertProxyTags(t, 1, proxies[0].ID, tags[2])
	if err := ApplyScrapeSourceTags(ctx, site.ID, mode, []uint{1}, proxies[1:]); err != nil {
		t.Fatal(err)
	}
	assertProxyTags(t, 1, proxies[1].ID)
}

func TestSourceImportsShareTagsOnlyWithNewSubscriptions(t *testing.T) {
	_, site, _, tags := setupScrapeSourceTags(t)
	setSourceTags(t, 1, site.ID, tags[0])
	urls := []string{site.URL, "https://first.test/proxies", "https://second.test/proxies"}
	sites, err := SaveScrapingSourcesWithSettings(1, urls, domain.ScrapeFetchHTTP, []uint64{tags[1], tags[2]})
	if err != nil || len(sites) != 2 {
		t.Fatalf("import: %v %v", sites, err)
	}
	for _, added := range sites {
		configured, err := GetScrapeSourceTags(1, added.ID)
		if err != nil || len(configured) != 2 {
			t.Fatalf("new source rules: %v %v", configured, err)
		}
	}
	existing, err := GetScrapeSourceTags(1, site.ID)
	if err != nil || len(existing) != 1 || existing[0].ID != tags[0] {
		t.Fatalf("reimport changed existing source: %v %v", existing, err)
	}
	if _, err := SaveScrapingSourcesWithSettings(1, []string{"https://invalid.test/proxies"}, domain.ScrapeFetchHTTP, []uint64{tags[3]}); !errors.Is(err, ErrProxyTagNotFound) {
		t.Fatalf("foreign import tag accepted: %v", err)
	}
}

func TestSourceTagAssignmentsUseBoundedBatches(t *testing.T) {
	db, site, _, tags := setupScrapeSourceTags(t)
	setSourceTags(t, 1, site.ID, tags[0])
	proxies := make([]domain.Proxy, 1001)
	for i := range proxies {
		proxies[i].Port = 8080
		if err := proxies[i].SetIP(fmt.Sprintf("198.18.%d.%d", i/256, i%256)); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.CreateInBatches(&proxies, 100).Error; err != nil {
		t.Fatal(err)
	}
	managed := make([]domain.ManagedProxy, len(proxies))
	for i, proxy := range proxies {
		managed[i] = domain.ManagedProxy{WorkspaceID: 1, ProxyID: proxy.ID}
	}
	if err := db.CreateInBatches(&managed, 100).Error; err != nil {
		t.Fatal(err)
	}
	workspaces := make([]uint, 501)
	for i := range workspaces {
		workspaces[i] = uint(i + 1)
	}
	queries := 0
	if err := db.Callback().Raw().Before("gorm:raw").Register("source_tag_bind_budget", func(tx *gorm.DB) {
		if strings.Contains(tx.Statement.SQL.String(), "INSERT INTO proxy_tag_assignments") {
			queries++
			if len(tx.Statement.Vars) > 2*scrapeTagBatchSize+3 {
				t.Errorf("unbounded assignment query: %d binds", len(tx.Statement.Vars))
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := ApplyScrapeSourceTags(context.Background(), site.ID, domain.ScrapeFetchHTTP, workspaces, proxies); err != nil {
		t.Fatal(err)
	}
	if queries != 6 {
		t.Fatalf("got %d assignment queries, want six bounded batches", queries)
	}
	var count int64
	if err := db.Model(&domain.ProxyTagAssignment{}).Where("workspace_id = ?", 1).Count(&count).Error; err != nil || count != 1001 {
		t.Fatalf("assignments=%d err=%v", count, err)
	}
}

func TestSourceTagsPostgresMigrationAndLifecycle(t *testing.T) {
	db := setupProxyIngestionPostgresTest(t)
	_, site, proxies, tags := seedScrapeSourceTags(t, db)
	setSourceTags(t, 1, site.ID, tags[0], tags[1])
	setSourceTags(t, 2, site.ID, tags[3])
	if err := ApplyScrapeSourceTags(context.Background(), site.ID, domain.ScrapeFetchHTTP, []uint{1}, proxies); err != nil {
		t.Fatal(err)
	}
	assertProxyTags(t, 1, proxies[0].ID, tags[0], tags[1])
	assertProxyTags(t, 2, proxies[0].ID)
	detail, err := GetScrapeSiteDetail(1, site.ID)
	if err != nil || detail == nil || len(detail.AutoTags) != 2 {
		t.Fatalf("detail rules=%+v err=%v", detail, err)
	}
	if err := DeleteProxyTag(1, tags[1]); err != nil {
		t.Fatal(err)
	}
	assertProxyTags(t, 1, proxies[0].ID, tags[0])
	configured, err := GetScrapeSourceTags(1, site.ID)
	if err != nil || len(configured) != 1 || configured[0].ID != tags[0] {
		t.Fatalf("tag cascade rules=%v err=%v", configured, err)
	}
	if _, _, err := DeleteScrapeSiteRelation(1, []int{int(site.ID)}); err != nil {
		t.Fatal(err)
	}
	assertProxyTags(t, 1, proxies[0].ID, tags[0])
	configured, err = GetScrapeSourceTags(1, site.ID)
	if err != nil || len(configured) != 0 {
		t.Fatalf("source cascade rules=%v err=%v", configured, err)
	}
	other, err := GetScrapeSourceTags(2, site.ID)
	if err != nil || len(other) != 1 || other[0].ID != tags[3] {
		t.Fatalf("source deletion changed other workspace rules=%v err=%v", other, err)
	}
}
