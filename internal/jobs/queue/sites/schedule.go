package sitequeue

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"magpie/internal/domain"

	"github.com/redis/go-redis/v9"
)

const scrapeScheduleKeyPrefix = "scrapesite_schedule:"
const scrapeLeaseKeyPrefix = "scrapesite_lease:"
const scrapeClaimSequenceKey = "magpie:queue:scrape:claim_sequence"

//go:embed complete.lua
var luaScrapeCompleteScript string

//go:embed force.lua
var luaScrapeForceScript string

//go:embed renew.lua
var luaScrapeRenewScript string

var (
	scrapeCompleteScript = redis.NewScript(luaScrapeCompleteScript)
	scrapeForceScript    = redis.NewScript(luaScrapeForceScript)
	scrapeRenewScript    = redis.NewScript(luaScrapeRenewScript)
	ErrScrapeLeaseLost   = errors.New("scrape source processing lease lost")
)

type ScrapeModeSchedule struct {
	NextAttemptMS  int64  `json:"next_attempt_ms"`
	DeferredReason string `json:"deferred_reason,omitempty"`
}

// ScrapeSchedule is runtime state shared by all replicas. Completed fetch modes
// keep their next due time while another mode waits for local capacity.
type ScrapeSchedule struct {
	Modes           map[string]ScrapeModeSchedule `json:"modes,omitempty"`
	RetryAfterMS    int64                         `json:"retry_after_ms,omitempty"`
	DeferredUntilMS int64                         `json:"deferred_until_ms,omitempty"`
	DeferredReason  string                        `json:"deferred_reason,omitempty"`
}

type ScrapeSiteJob struct {
	Site     domain.ScrapeSite
	Interval time.Duration
	Schedule ScrapeSchedule

	queueKey     string
	leaseScoreMS int64
	claimID      int64
}

func (s ScrapeSchedule) NextAttempt(mode string) (time.Time, string) {
	state := s.Modes[mode]
	next := state.NextAttemptMS
	reason := state.DeferredReason
	if next > 0 && reason == "" {
		reason = "Configured scrape interval"
	}
	if s.DeferredUntilMS > next {
		next, reason = s.DeferredUntilMS, s.DeferredReason
	}
	if s.RetryAfterMS > next {
		next, reason = s.RetryAfterMS, "Provider requested a delay with Retry-After"
	}
	if next <= 0 {
		return time.Time{}, ""
	}
	return time.UnixMilli(next), reason
}

func (s ScrapeSchedule) nextDue() time.Time {
	var next int64
	first := true
	for _, mode := range s.Modes {
		if first || mode.NextAttemptMS < next {
			next = mode.NextAttemptMS
			first = false
		}
	}
	next = max(next, s.DeferredUntilMS, s.RetryAfterMS)
	return time.UnixMilli(next)
}

// CompleteScrapeSite saves progress and the next due time only while the job
// still owns its lease. A stale claim can only extend a provider deadline, never
// replace newer mode progress or recreate a removed source.
func (q *RedisScrapeSiteQueue) CompleteScrapeSite(job ScrapeSiteJob) (bool, error) {
	client, err := q.clientOrErr()
	if err != nil {
		return false, err
	}
	payload, err := json.Marshal(job.Schedule)
	if err != nil {
		return false, err
	}
	next := max(job.Schedule.nextDue().UnixMilli(), time.Now().UnixMilli())
	result, err := scrapeCompleteScript.Run(q.baseContext(), client,
		[]string{job.queueKey, scrapesiteQueueHeadKey, scrapesiteKeyPrefix + job.Site.URL, scrapeScheduleKeyPrefix + job.Site.URL, scrapeLeaseKeyPrefix + job.Site.URL, q.queueKeyForMember(job.Site.URL)},
		job.Site.URL, next, string(payload), job.claimID).Int()
	return result == 1, err
}

func (q *RedisScrapeSiteQueue) GetScrapeSchedule(ctx context.Context, url, mode string) (time.Time, string, error) {
	client, err := q.clientOrErr()
	if err != nil {
		return time.Time{}, "", err
	}
	raw, err := client.Get(ctx, scrapeScheduleKeyPrefix+url).Bytes()
	if errors.Is(err, redis.Nil) {
		return time.Time{}, "", nil
	}
	if err != nil {
		return time.Time{}, "", err
	}
	var schedule ScrapeSchedule
	if err := json.Unmarshal(raw, &schedule); err != nil {
		return time.Time{}, "", err
	}
	next, reason := schedule.NextAttempt(mode)
	return next, reason, nil
}

// ForceScrapeSite clears interval waits for an explicit administrator request.
// An active fetch fulfills the request; its claim stays intact. The provider's
// Retry-After deadline still applies.
func (q *RedisScrapeSiteQueue) ForceScrapeSite(site domain.ScrapeSite) error {
	client, err := q.clientOrErr()
	if err != nil {
		return err
	}
	if site.URL == "" {
		return errors.New("scrape source URL is empty")
	}
	payload, err := json.Marshal(site)
	if err != nil {
		return err
	}
	return scrapeForceScript.Run(q.baseContext(), client,
		q.forceKeys(q.queueKeyForMember(site.URL), site.URL), site.URL, time.Now().UnixMilli(), string(payload)).Err()
}

func (q *RedisScrapeSiteQueue) leasePeriod() time.Duration {
	if q != nil && q.leaseDuration > 0 {
		return q.leaseDuration
	}
	return processingLease
}

func (q *RedisScrapeSiteQueue) RenewScrapeSiteLease(ctx context.Context, job ScrapeSiteJob) error {
	client, err := q.clientOrErr()
	if err != nil {
		return err
	}
	period := q.leasePeriod()
	renewed, err := scrapeRenewScript.Run(ctx, client,
		[]string{job.queueKey, scrapesiteQueueHeadKey, scrapesiteKeyPrefix + job.Site.URL, scrapeLeaseKeyPrefix + job.Site.URL},
		job.Site.URL, job.claimID, time.Now().Add(period).UnixMilli(), period.Milliseconds()).Int()
	if err != nil {
		return err
	}
	if renewed != 1 {
		return ErrScrapeLeaseLost
	}
	return nil
}

// MaintainScrapeSiteLease renews ownership while fetching or waiting to hand off
// HTML. Short jobs send no renewal commands. Loss of ownership cancels work.
// Stop and join renewal before completing the schedule; it is safe to stop twice.
func (q *RedisScrapeSiteQueue) MaintainScrapeSiteLease(parent context.Context, job ScrapeSiteJob) (context.Context, func() error) {
	workCtx, cancelWork := context.WithCancelCause(parent)
	renewCtx, stopRenewal := context.WithCancel(workCtx)
	done := make(chan struct{})
	var renewalErr error
	go func() {
		defer close(done)
		every := max(time.Millisecond, q.leasePeriod()/3)
		ticker := time.NewTicker(every)
		defer ticker.Stop()
		for {
			select {
			case <-renewCtx.Done():
				return
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(renewCtx, min(5*time.Second, every))
				err := q.RenewScrapeSiteLease(ctx, job)
				cancel()
				if err != nil {
					if renewCtx.Err() == nil {
						renewalErr = fmt.Errorf("renew scrape source lease: %w", err)
						cancelWork(renewalErr)
					}
					return
				}
			}
		}
	}()
	return workCtx, func() error {
		stopRenewal()
		<-done
		cancelWork(nil)
		return renewalErr
	}
}

func (q *RedisScrapeSiteQueue) forceKeys(queueKey, url string) []string {
	return []string{queueKey, scrapesiteQueueHeadKey, scrapesiteKeyPrefix + url, scrapeScheduleKeyPrefix + url, legacyScrapesiteQueueKey, scrapeLeaseKeyPrefix + url}
}
