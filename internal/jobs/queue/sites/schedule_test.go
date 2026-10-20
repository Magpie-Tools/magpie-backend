package sitequeue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"magpie/internal/domain"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func scheduleQueue(t *testing.T) (*RedisScrapeSiteQueue, *redis.Client, domain.ScrapeSite) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { client.Close() })
	queue := NewRedisScrapeSiteQueue(client)
	site := domain.ScrapeSite{ID: 1, URL: "https://example.com/list"}
	if err := queue.AddToQueue([]domain.ScrapeSite{site}); err != nil {
		t.Fatal(err)
	}
	return queue, client, site
}

func claimScrape(t *testing.T, q *RedisScrapeSiteQueue) ScrapeSiteJob {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	job, err := q.GetNextScrapeSiteContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return job
}

func TestScrapeScheduleSharesModeProgressAndProviderDeadlineAcrossReplicas(t *testing.T) {
	queue, client, site := scheduleQueue(t)
	ctx := context.Background()
	if err := queue.Reschedule(15 * time.Hour); err != nil {
		t.Fatal(err)
	}
	job := claimScrape(t, queue)
	if job.Interval != 15*time.Hour {
		t.Fatalf("interval = %s", job.Interval)
	}
	payload, _ := client.Get(ctx, scrapesiteKeyPrefix+site.URL).Result()
	now := time.Now()
	job.Schedule = ScrapeSchedule{Modes: map[string]ScrapeModeSchedule{
		domain.ScrapeFetchHTTP:    {NextAttemptMS: now.Add(job.Interval).UnixMilli()},
		domain.ScrapeFetchBrowser: {NextAttemptMS: now.Add(30 * time.Second).UnixMilli(), DeferredReason: "Waiting for browser capacity or availability"},
	}}
	if completed, err := queue.CompleteScrapeSite(job); err != nil || !completed {
		t.Fatalf("complete=%v error=%v", completed, err)
	}
	if got, _ := client.Get(ctx, scrapesiteKeyPrefix+site.URL).Result(); got != payload {
		t.Fatal("scheduling rewrote the source payload")
	}
	other := NewRedisScrapeSiteQueue(client)
	next, reason, err := other.GetScrapeSchedule(ctx, site.URL, domain.ScrapeFetchHTTP)
	if err != nil || next.UnixMilli() != now.Add(job.Interval).UnixMilli() || reason != "Configured scrape interval" {
		t.Fatalf("HTTP next=%v reason=%s error=%v", next, reason, err)
	}
	// Let only the browser mode become due; the new replica must retain HTTP's wait.
	job.Schedule.Modes[domain.ScrapeFetchBrowser] = ScrapeModeSchedule{}
	raw, _ := json.Marshal(job.Schedule)
	if err := client.Set(ctx, scrapeScheduleKeyPrefix+site.URL, raw, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.ZAdd(ctx, job.queueKey, redis.Z{Member: site.URL, Score: 0}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := other.refreshQueueHeads(); err != nil {
		t.Fatal(err)
	}
	second := claimScrape(t, other)
	if second.Schedule.Modes[domain.ScrapeFetchHTTP].NextAttemptMS != now.Add(job.Interval).UnixMilli() {
		t.Fatal("replica lost completed HTTP progress")
	}
	second.Schedule.RetryAfterMS = now.Add(30 * time.Hour).UnixMilli()
	if completed, err := other.CompleteScrapeSite(second); err != nil || !completed {
		t.Fatalf("complete=%v error=%v", completed, err)
	}
	score, err := client.ZScore(ctx, job.queueKey, site.URL).Result()
	if err != nil || int64(score) != second.Schedule.RetryAfterMS {
		t.Fatalf("provider deadline score=%f error=%v", score, err)
	}
	for _, requeue := range []func() error{
		func() error { return queue.ForceScrapeSite(site) },
		func() error { _, err := queue.RequeueAll(); return err },
	} {
		if err := requeue(); err != nil {
			t.Fatal(err)
		}
		next, reason, err := queue.GetScrapeSchedule(ctx, site.URL, domain.ScrapeFetchBrowser)
		if err != nil || next.UnixMilli() != second.Schedule.RetryAfterMS || reason != "Provider requested a delay with Retry-After" {
			t.Fatalf("manual requeue lost deadline: %v %s %v", next, reason, err)
		}
		limited, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
		_, err = queue.GetNextScrapeSiteContext(limited)
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("provider cooldown allowed an early claim: %v", err)
		}
	}
}

func TestScrapeCompletionCannotOverwriteNewClaimOrRemovedSource(t *testing.T) {
	queue, client, site := scheduleQueue(t)
	first := claimScrape(t, queue)
	// Simulate lease expiry and reclaim, rather than an administrator joining
	// the in-flight fetch.
	if err := client.Del(context.Background(), scrapeLeaseKeyPrefix+site.URL).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.ZAdd(context.Background(), first.queueKey, redis.Z{Member: site.URL, Score: 0}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := queue.refreshQueueHeads(); err != nil {
		t.Fatal(err)
	}
	second := claimScrape(t, queue)
	first.Schedule.RetryAfterMS = time.Now().Add(60 * time.Hour).UnixMilli()
	if completed, err := queue.CompleteScrapeSite(first); err != nil || completed {
		t.Fatalf("old claim completed=%v error=%v", completed, err)
	}
	if err := queue.RemoveFromQueue([]domain.ScrapeSite{site}); err != nil {
		t.Fatal(err)
	}
	if completed, err := queue.CompleteScrapeSite(second); err != nil || completed {
		t.Fatalf("removed source completed=%v error=%v", completed, err)
	}
	if count, err := client.Exists(context.Background(), scrapeScheduleKeyPrefix+site.URL, scrapeLeaseKeyPrefix+site.URL).Result(); err != nil || count != 0 {
		t.Fatalf("removed schedule/lease count=%d error=%v", count, err)
	}
}

func TestStaleCompletionOnlyExtendsProviderDeadline(t *testing.T) {
	for _, staleFirst := range []bool{true, false} {
		t.Run(fmt.Sprint("stale_first=", staleFirst), func(t *testing.T) {
			queue, client, site := scheduleQueue(t)
			ctx := context.Background()
			first := claimScrape(t, queue)
			client.Del(ctx, scrapeLeaseKeyPrefix+site.URL)
			client.ZAdd(ctx, first.queueKey, redis.Z{Member: site.URL, Score: 0})
			if err := queue.refreshQueueHeads(); err != nil {
				t.Fatal(err)
			}
			second := claimScrape(t, queue)
			now := time.Now()
			first.Schedule = ScrapeSchedule{RetryAfterMS: now.Add(30 * time.Hour).UnixMilli(), Modes: map[string]ScrapeModeSchedule{
				domain.ScrapeFetchHTTP: {NextAttemptMS: now.Add(60 * time.Hour).UnixMilli()},
			}}
			second.Schedule = ScrapeSchedule{RetryAfterMS: now.Add(time.Hour).UnixMilli(), Modes: map[string]ScrapeModeSchedule{
				domain.ScrapeFetchHTTP: {NextAttemptMS: now.Add(15 * time.Hour).UnixMilli()},
			}}
			completeStale := func() {
				if completed, err := queue.CompleteScrapeSite(first); err != nil || completed {
					t.Fatalf("stale completed=%v error=%v", completed, err)
				}
			}
			if staleFirst {
				completeStale()
				if score, err := client.ZScore(ctx, second.queueKey, site.URL).Result(); err != nil || int64(score) != second.leaseScoreMS {
					t.Fatalf("stale completion disturbed newer lease: score=%f error=%v", score, err)
				}
			}
			if completed, err := queue.CompleteScrapeSite(second); err != nil || !completed {
				t.Fatalf("current completed=%v error=%v", completed, err)
			}
			if !staleFirst {
				completeStale()
			}
			raw, err := client.Get(ctx, scrapeScheduleKeyPrefix+site.URL).Bytes()
			if err != nil {
				t.Fatal(err)
			}
			var stored ScrapeSchedule
			if err := json.Unmarshal(raw, &stored); err != nil {
				t.Fatal(err)
			}
			if stored.RetryAfterMS != first.Schedule.RetryAfterMS || stored.Modes[domain.ScrapeFetchHTTP] != second.Schedule.Modes[domain.ScrapeFetchHTTP] {
				t.Fatalf("deadline or newer mode progress lost: %+v", stored)
			}
			if score, err := client.ZScore(ctx, second.queueKey, site.URL).Result(); err != nil || int64(score) != first.Schedule.RetryAfterMS {
				t.Fatalf("scheduled before provider deadline: %f %v", score, err)
			}
		})
	}
}

func TestScrapeLeaseRenewalRetainsProgressAfterLongWork(t *testing.T) {
	queue, client, site := scheduleQueue(t)
	queue.leaseDuration = time.Second
	if err := queue.Reschedule(15 * time.Hour); err != nil {
		t.Fatal(err)
	}
	job := claimScrape(t, queue)
	workCtx, stop := queue.MaintainScrapeSiteLease(context.Background(), job)
	defer stop()
	// Model a long fetch or a blocked HTML handoff, over twice the lease length.
	select {
	case <-workCtx.Done():
		t.Fatalf("lease lost while work was blocked: %v", context.Cause(workCtx))
	case <-time.After(2*queue.leasePeriod() + 100*time.Millisecond):
	}
	score, err := client.ZScore(context.Background(), job.queueKey, site.URL).Result()
	if err != nil || int64(score) <= time.Now().UnixMilli() || int64(score) <= job.leaseScoreMS {
		t.Fatalf("processing lease was not renewed: score=%f error=%v", score, err)
	}
	other := NewRedisScrapeSiteQueue(client)
	limited, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := other.GetNextScrapeSiteContext(limited); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("long work became claimable by another replica: %v", err)
	}
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	next := time.Now().Add(job.Interval).UnixMilli()
	job.Schedule.Modes = map[string]ScrapeModeSchedule{domain.ScrapeFetchHTTP: {NextAttemptMS: next}}
	if completed, err := queue.CompleteScrapeSite(job); err != nil || !completed {
		t.Fatalf("renewed lease rejected completed progress: completed=%v error=%v", completed, err)
	}
	if nextAt, _, err := other.GetScrapeSchedule(context.Background(), site.URL, domain.ScrapeFetchHTTP); err != nil || nextAt.UnixMilli() != next {
		t.Fatalf("completed interval lost after long work: next=%v error=%v", nextAt, err)
	}
}

func TestScrapeLeaseLossCancelsWorkAndDoesNotRenewNewClaim(t *testing.T) {
	queue, client, site := scheduleQueue(t)
	queue.leaseDuration = 300 * time.Millisecond
	first := claimScrape(t, queue)
	workCtx, stop := queue.MaintainScrapeSiteLease(context.Background(), first)
	defer stop()
	ctx := context.Background()
	client.Del(ctx, scrapeLeaseKeyPrefix+site.URL)
	client.ZAdd(ctx, first.queueKey, redis.Z{Member: site.URL, Score: 0})
	if err := queue.refreshQueueHeads(); err != nil {
		t.Fatal(err)
	}
	second := claimScrape(t, queue)
	select {
	case <-workCtx.Done():
		if !errors.Is(context.Cause(workCtx), ErrScrapeLeaseLost) {
			t.Fatalf("work cancellation cause=%v", context.Cause(workCtx))
		}
	case <-time.After(time.Second):
		t.Fatal("lost lease did not cancel blocked work")
	}
	if err := stop(); !errors.Is(err, ErrScrapeLeaseLost) {
		t.Fatalf("renewal error=%v", err)
	}
	if score, err := client.ZScore(ctx, second.queueKey, site.URL).Result(); err != nil || int64(score) != second.leaseScoreMS {
		t.Fatalf("old worker renewed newer claim: score=%f error=%v", score, err)
	}
	if err := queue.RemoveFromQueue([]domain.ScrapeSite{site}); err != nil {
		t.Fatal(err)
	}
	if err := queue.RenewScrapeSiteLease(ctx, second); !errors.Is(err, ErrScrapeLeaseLost) {
		t.Fatalf("removed source was renewed: %v", err)
	}
}

func TestLegacyScrapePayloadCanCompleteAtConfiguredInterval(t *testing.T) {
	queue, client, site := scheduleQueue(t)
	ctx := context.Background()
	client.ZRem(ctx, queue.queueKeyForMember(site.URL), site.URL)
	client.ZAdd(ctx, legacyScrapesiteQueueKey, redis.Z{Member: site.URL, Score: 0})
	if err := queue.refreshQueueHeads(); err != nil {
		t.Fatal(err)
	}
	if err := queue.Reschedule(time.Minute); err != nil {
		t.Fatal(err)
	}
	job := claimScrape(t, queue)
	if job.queueKey != legacyScrapesiteQueueKey || len(job.Schedule.Modes) != 0 {
		t.Fatalf("legacy job=%+v", job)
	}
	next := time.Now().Add(job.Interval).UnixMilli()
	job.Schedule.Modes = map[string]ScrapeModeSchedule{domain.ScrapeFetchHTTP: {NextAttemptMS: next}}
	if completed, err := queue.CompleteScrapeSite(job); err != nil || !completed {
		t.Fatalf("legacy complete=%v error=%v", completed, err)
	}
	if score, err := client.ZScore(ctx, legacyScrapesiteQueueKey, site.URL).Result(); err != nil || int64(score) != next {
		t.Fatalf("legacy due=%f error=%v", score, err)
	}
}

func TestManualRequeueDuringFetchPreservesIncomingRetryAfter(t *testing.T) {
	for _, all := range []bool{false, true} {
		t.Run(fmt.Sprint("all=", all), func(t *testing.T) {
			queue, client, site := scheduleQueue(t)
			job := claimScrape(t, queue)
			// The response has arrived, but the worker has not committed it yet.
			deadline := time.Now().Add(30 * time.Hour).UnixMilli()
			job.Schedule.RetryAfterMS = deadline
			if all {
				if _, err := queue.RequeueAll(); err != nil {
					t.Fatal(err)
				}
			} else if err := queue.ForceScrapeSite(site); err != nil {
				t.Fatal(err)
			}
			if claim, err := client.Get(context.Background(), scrapeLeaseKeyPrefix+site.URL).Int64(); err != nil || claim != job.claimID {
				t.Fatalf("requeue revoked the in-flight claim before its deadline was saved: claim=%d error=%v", claim, err)
			}
			if completed, err := queue.CompleteScrapeSite(job); err != nil || !completed {
				t.Fatalf("manual requeue discarded in-flight completion: completed=%v error=%v", completed, err)
			}
			next, _, err := queue.GetScrapeSchedule(context.Background(), site.URL, domain.ScrapeFetchHTTP)
			if err != nil || next.UnixMilli() != deadline {
				t.Fatalf("provider deadline lost: next=%v error=%v", next, err)
			}
			if score, err := client.ZScore(context.Background(), job.queueKey, site.URL).Result(); err != nil || int64(score) < deadline {
				t.Fatalf("source scheduled before provider deadline: %f %v", score, err)
			}
		})
	}
}

func TestStaleProviderDeadlineSurvivesLegacyQueueMigration(t *testing.T) {
	queue, client, site := scheduleQueue(t)
	ctx := context.Background()
	client.ZRem(ctx, queue.queueKeyForMember(site.URL), site.URL)
	client.ZAdd(ctx, legacyScrapesiteQueueKey, redis.Z{Member: site.URL, Score: 0})
	if err := queue.refreshQueueHeads(); err != nil {
		t.Fatal(err)
	}
	job := claimScrape(t, queue)
	client.Del(ctx, scrapeLeaseKeyPrefix+site.URL) // The old lease expired.
	if err := queue.ForceScrapeSite(site); err != nil {
		t.Fatal(err)
	}
	job.Schedule.RetryAfterMS = time.Now().Add(30 * time.Hour).UnixMilli()
	if completed, err := queue.CompleteScrapeSite(job); err != nil || completed {
		t.Fatalf("old claim completed=%v error=%v", completed, err)
	}
	if next, _, err := queue.GetScrapeSchedule(ctx, site.URL, domain.ScrapeFetchHTTP); err != nil || next.UnixMilli() != job.Schedule.RetryAfterMS {
		t.Fatalf("migrated provider deadline lost: next=%v error=%v", next, err)
	}
	if score, err := client.ZScore(ctx, queue.queueKeyForMember(site.URL), site.URL).Result(); err != nil || int64(score) != job.Schedule.RetryAfterMS {
		t.Fatalf("migrated source scheduled before provider deadline: %f %v", score, err)
	}
}
