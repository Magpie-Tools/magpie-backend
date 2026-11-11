package scraper

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"magpie/internal/checkpace"
	"magpie/internal/domain"
	sitequeue "magpie/internal/jobs/queue/sites"
	"magpie/internal/platform/pg"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func scrapeScheduleFixture(t *testing.T, modes ...string) {
	t.Helper()
	t.Setenv("ALLOW_PRIVATE_NETWORK_EGRESS", "true")
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "scrape.db")), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(&domain.WorkspaceScrapeSite{}, &domain.WorkspaceUsagePeriod{}); err != nil {
		t.Fatal(err)
	}
	originalDB, originalQueue, originalBrowser, originalPace := pg.DB, postProcessQueue, sharedBrowser, checkpace.Current()
	pg.DB = db
	postProcessQueue = make(chan scrapedHTMLJob, 16)
	t.Cleanup(func() {
		checkpace.Publish(originalPace)
		pg.DB, postProcessQueue, sharedBrowser = originalDB, originalQueue, originalBrowser
		sqlDB, _ := db.DB()
		sqlDB.Close()
	})
	for i, mode := range modes {
		if err := db.Create(&domain.WorkspaceScrapeSite{WorkspaceID: uint(i + 1), ScrapeSiteID: 1, FetchMode: mode}).Error; err != nil {
			t.Fatal(err)
		}
	}
}

// The cadence now derives from subscriber scrape allowances; a uniform
// snapshot stands in for an allowance that yields each tested interval.
func TestCompletedScrapesAlwaysUseCurrentInterval(t *testing.T) {
	scrapeScheduleFixture(t, domain.ScrapeFetchHTTP)
	var hits atomic.Int64
	var status atomic.Int32
	status.Store(http.StatusOK)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(int(status.Load()))
		fmt.Fprint(w, "8.8.8.8:8080")
	}))
	defer server.Close()
	job := sitequeue.ScrapeSiteJob{Site: domain.ScrapeSite{ID: 1, URL: server.URL}}
	for _, interval := range []time.Duration{5 * time.Minute, time.Hour, 5 * time.Hour} {
		checkpace.Publish(checkpace.Uniform(time.Minute, interval))
		for _, code := range []int{200, 429, 502, 521, 503, 404, 200} {
			status.Store(int32(code))
			job.Schedule.Modes = nil // The next scheduled attempt has become due.
			before := time.Now()
			if !scrapeSite(context.Background(), &job) {
				t.Fatal("subscribed source was removed")
			}
			next, reason := job.Schedule.NextAttempt(domain.ScrapeFetchHTTP)
			if next.Before(before.Add(interval-time.Millisecond)) || next.After(time.Now().Add(interval)) || reason != "Configured scrape interval" {
				t.Fatalf("HTTP %d interval=%s next=%v reason=%s", code, interval, next, reason)
			}
			hitsBefore := hits.Load()
			scrapeSite(context.Background(), &job)
			if hits.Load() != hitsBefore {
				t.Fatal("completed mode fetched again before its configured interval")
			}
		}
	}
}

func TestBusyBrowserStaysPendingWithoutRefetchingCompletedHTTP(t *testing.T) {
	scrapeScheduleFixture(t, domain.ScrapeFetchHTTP, domain.ScrapeFetchBrowser)
	var hits atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		fmt.Fprint(w, "8.8.8.8:8080")
	}))
	defer server.Close()
	manager := newBrowserManager(1, func(context.Context) (*browserInstance, error) { return nil, fmt.Errorf("browser unavailable") })
	manager.slots <- struct{}{}
	sharedBrowser = func() *browserManager { return manager }
	job := sitequeue.ScrapeSiteJob{Site: domain.ScrapeSite{ID: 1, URL: server.URL}, Interval: 15 * time.Hour}
	scrapeSite(context.Background(), &job)
	if hits.Load() != 1 || len(postProcessQueue) != 1 {
		t.Fatalf("HTTP hits=%d queued results=%d", hits.Load(), len(postProcessQueue))
	}
	var browserSubscription domain.WorkspaceScrapeSite
	if err := pg.DB.First(&browserSubscription, "workspace_id = 2").Error; err != nil {
		t.Fatal(err)
	}
	if browserSubscription.LastScrapedAt != nil || browserSubscription.LastScrapeStatus != "" || browserSubscription.LastScrapeError != "" {
		t.Fatalf("busy browser recorded an upstream failure: %+v", browserSubscription)
	}
	httpNext := job.Schedule.Modes[domain.ScrapeFetchHTTP].NextAttemptMS
	// The browser's local defer becomes due, while HTTP's interval has not elapsed.
	job.Schedule.Modes[domain.ScrapeFetchBrowser] = sitequeue.ScrapeModeSchedule{}
	scrapeSite(context.Background(), &job)
	if hits.Load() != 1 || job.Schedule.Modes[domain.ScrapeFetchHTTP].NextAttemptMS != httpNext {
		t.Fatal("pending browser work repeated or postponed the completed HTTP attempt")
	}
	if _, reason := job.Schedule.NextAttempt(domain.ScrapeFetchBrowser); reason != "Waiting for browser capacity or availability" {
		t.Fatalf("browser defer reason=%s", reason)
	}
}

func TestRetryAfterDelaysBothModesWithoutMultiplyingInterval(t *testing.T) {
	scrapeScheduleFixture(t, domain.ScrapeFetchHTTP, domain.ScrapeFetchBrowser)
	sharedBrowser = func() *browserManager { t.Fatal("provider cooldown allowed browser fetch"); return nil }
	var hits atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Retry-After", "108000")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()
	job := sitequeue.ScrapeSiteJob{Site: domain.ScrapeSite{ID: 1, URL: server.URL}, Interval: 15 * time.Hour}
	scrapeSite(context.Background(), &job)
	for _, mode := range []string{domain.ScrapeFetchHTTP, domain.ScrapeFetchBrowser} {
		next, reason := job.Schedule.NextAttempt(mode)
		if next.Before(time.Now().Add(30*time.Hour-time.Second)) || reason != "Provider requested a delay with Retry-After" {
			t.Fatalf("mode=%s next=%v reason=%s", mode, next, reason)
		}
	}
	scrapeSite(context.Background(), &job)
	if hits.Load() != 1 {
		t.Fatal("provider cooldown allowed another HTTP fetch")
	}
}

func TestSourceLookupFailureDefersWithoutCompletingAnAttempt(t *testing.T) {
	scrapeScheduleFixture(t, domain.ScrapeFetchHTTP)
	if err := pg.DB.Migrator().DropTable(&domain.WorkspaceScrapeSite{}); err != nil {
		t.Fatal(err)
	}
	job := sitequeue.ScrapeSiteJob{Site: domain.ScrapeSite{ID: 1, URL: "https://example.com/list"}, Interval: 15 * time.Hour}
	before := time.Now()
	if !scrapeSite(context.Background(), &job) || len(job.Schedule.Modes) != 0 {
		t.Fatalf("lookup failure completed source work: %+v", job.Schedule)
	}
	next, reason := job.Schedule.NextAttempt(domain.ScrapeFetchHTTP)
	if next.Before(before.Add(29*time.Second)) || next.After(time.Now().Add(30*time.Second)) || reason != "Waiting for source settings to become available" {
		t.Fatalf("local deferral next=%v reason=%s", next, reason)
	}
}

func TestScrapeChargesOnlyDueSubscribersWithAllowance(t *testing.T) {
	scrapeScheduleFixture(t, domain.ScrapeFetchHTTP, domain.ScrapeFetchHTTP)
	var hits atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		fmt.Fprint(w, "8.8.8.8:8080")
	}))
	defer server.Close()
	now := time.Now()
	checkpace.Publish(checkpace.Compute(now, []checkpace.Allowance{
		{WorkspaceID: 1, MonthlyChecks: 1, MonthlyScrapes: 1_000_000, ScrapesPerCycle: 1},
		{WorkspaceID: 2, MonthlyChecks: 1, MonthlyScrapes: 3, UsedScrapes: 3, ScrapesPerCycle: 1},
	}))
	job := sitequeue.ScrapeSiteJob{Site: domain.ScrapeSite{ID: 1, URL: server.URL}}
	if !scrapeSite(context.Background(), &job) {
		t.Fatal("subscribed source was removed")
	}
	if hits.Load() != 1 {
		t.Fatalf("fetches = %d, want 1", hits.Load())
	}
	var usage []domain.WorkspaceUsagePeriod
	if err := pg.DB.Find(&usage).Error; err != nil {
		t.Fatal(err)
	}
	if len(usage) != 1 || usage[0].WorkspaceID != 1 || usage[0].Scrapes != 1 {
		t.Fatalf("scrape usage = %+v, want one scrape for workspace 1 only", usage)
	}
	if next, _ := job.Schedule.NextAttempt(domain.ScrapeFetchHTTP); next.Before(now.Add(checkpace.MinimumScrapeInterval - time.Second)) {
		t.Fatalf("next scrape %v is faster than the cadence floor", next)
	}

	// With every subscriber out of allowance, a due source is revisited
	// without fetching.
	checkpace.Publish(checkpace.Compute(now, []checkpace.Allowance{
		{WorkspaceID: 1, MonthlyScrapes: 1, UsedScrapes: 1, ScrapesPerCycle: 1},
		{WorkspaceID: 2, MonthlyScrapes: 1, UsedScrapes: 1, ScrapesPerCycle: 1},
	}))
	job.Schedule.Modes = nil
	if !scrapeSite(context.Background(), &job) {
		t.Fatal("subscribed source was removed")
	}
	if hits.Load() != 1 {
		t.Fatalf("an exhausted source was fetched: %d", hits.Load())
	}
	if next, _ := job.Schedule.NextAttempt(domain.ScrapeFetchHTTP); next.Before(now.Add(checkpace.MaxRouteInterval - time.Minute)) {
		t.Fatalf("exhausted source revisit %v is earlier than the revisit cap", next)
	}
}
