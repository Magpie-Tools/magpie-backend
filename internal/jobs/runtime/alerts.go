package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/charmbracelet/log"
	"gorm.io/gorm"
	"magpie/internal/api/dto"
	"magpie/internal/database"
	"magpie/internal/domain"
	"magpie/internal/security"
	"magpie/internal/support"
)

func StartAlertEvaluationRoutine(ctx context.Context) {
	err := support.RunWithLeader(ctx, "magpie:leader:alerts", support.DefaultLeadershipTTL, func(leaderCtx context.Context) {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			passCtx, cancel := context.WithTimeout(leaderCtx, 45*time.Second)
			now := time.Now().UTC()
			if err := database.EvaluateAlerts(passCtx, now); err != nil && leaderCtx.Err() == nil {
				log.Error("Alert evaluation failed", "error", err)
			}
			cancel()
			cleanupCtx, cancelCleanup := context.WithTimeout(leaderCtx, 10*time.Second)
			if err := database.CleanupAlertHistory(cleanupCtx, now); err != nil && leaderCtx.Err() == nil {
				log.Error("Alert history cleanup failed", "error", err)
			}
			cancelCleanup()
			select {
			case <-leaderCtx.Done():
				return
			case <-ticker.C:
			}
		}
	})
	if err != nil && ctx.Err() == nil {
		log.Error("Alert evaluator stopped", "error", err)
	}
}

func StartAlertDeliveryRoutine(ctx context.Context) {
	client := support.NewRestrictedOutboundHTTPClient(15 * time.Second)
	client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }
	defer client.CloseIdleConnections()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	runAlertDeliveryLoop(ctx, 10, ticker.C, database.ClaimAlertDeliveries, func(ctx context.Context, message domain.AlertDelivery) {
		deliverAlertMessage(ctx, client, message)
	})
}

func runAlertDeliveryLoop(ctx context.Context, concurrency int, poll <-chan time.Time,
	claim func(context.Context, int, time.Time) ([]domain.AlertDelivery, error),
	deliver func(context.Context, domain.AlertDelivery)) {
	jobs := make(chan domain.AlertDelivery, concurrency)
	completed := make(chan struct{}, concurrency)
	var workers sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case message, ok := <-jobs:
					if !ok || ctx.Err() != nil {
						return
					}
					deliver(ctx, message)
					select {
					case completed <- struct{}{}:
					case <-ctx.Done():
						return
					}
				}
			}
		}()
	}
	defer func() { close(jobs); workers.Wait() }()
	active, canClaim, claimFailed := 0, true, false
	for {
		if ctx.Err() != nil {
			return
		}
		if canClaim && active < concurrency {
			// Claim only free slots; no lease sits waiting behind a slow send.
			messages, err := claim(ctx, concurrency-active, time.Now().UTC())
			if err != nil {
				if ctx.Err() == nil {
					log.Error("Alert delivery claim failed", "error", err)
				}
				canClaim, claimFailed = false, true
			} else if len(messages) == 0 {
				canClaim = false
			} else {
				active += len(messages)
				for _, message := range messages {
					jobs <- message
				}
			}
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-completed:
			active--
			// Refill all slots already freed while the last claim was running.
		drainCompletions:
			for {
				select {
				case <-completed:
					active--
				default:
					break drainCompletions
				}
			}
			// Completion may unblock the next event for the same destination.
			// Database errors still wait for the polling backoff.
			if !claimFailed {
				canClaim = true
			}
		case <-poll:
			canClaim, claimFailed = true, false
		}
	}
}

func deliverAlertMessage(ctx context.Context, client *http.Client, message domain.AlertDelivery) {
	destination, err := database.GetAlertDeliveryDestination(ctx, message)
	if err != nil {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return
		}
		_ = database.FinishAlertDelivery(ctx, message, "canceled", "Destination disabled or deleted", time.Now().UTC(), time.Now().UTC())
		return
	}
	err = sendAlertMessage(ctx, client, message, destination)
	now := time.Now().UTC()
	status, reason := "sent", ""
	next := now
	if err != nil {
		status, reason = "pending", "Notification delivery failed"
		delay := 5 * time.Second * time.Duration(1<<min(message.Attempts, 8))
		var sendErr *support.AlertSendError
		if errors.As(err, &sendErr) {
			reason = sendErr.Message
			if sendErr.Permanent {
				status = "failed"
			}
			if sendErr.RetryAfter > delay {
				delay = sendErr.RetryAfter
			}
		}
		if message.Attempts >= database.AlertMaximumDeliveryAttempts {
			status = "failed"
		}
		next = now.Add(delay)
	}
	if err := database.FinishAlertDelivery(ctx, message, status, reason, next, now); err != nil && ctx.Err() == nil {
		log.Error("Alert delivery status could not be saved", "delivery_id", message.ID)
	}
}

func sendAlertMessage(ctx context.Context, client *http.Client, message domain.AlertDelivery, destination domain.AlertDestination) error {
	target, _, err := security.DecryptProxySecret(destination.TargetEncrypted)
	if err != nil {
		return &support.AlertSendError{Message: "Destination could not be decrypted", Permanent: true}
	}
	if err := database.ValidateAlertTarget(destination.Kind, target); err != nil {
		return &support.AlertSendError{Message: "Destination blocked by outbound policy", Permanent: true}
	}
	var event dto.AlertEvent
	if err := json.Unmarshal([]byte(message.Payload), &event); err != nil {
		return &support.AlertSendError{Message: "Invalid stored notification", Permanent: true}
	}
	if destination.Kind == "email" {
		cfg, err := support.ReadEmailConfig()
		if err != nil || !cfg.IsConfigured() {
			return &support.AlertSendError{Message: "SMTP is not configured"}
		}
		if err := support.SendEmailContext(ctx, cfg, target, "Magpie alert "+event.Event+": "+event.RuleName, support.AlertEmailBody(event)); err != nil {
			return &support.AlertSendError{Message: "SMTP delivery failed"}
		}
		return nil
	}
	secret := ""
	if destination.SigningSecretEncrypted != "" {
		secret, _, err = security.DecryptProxySecret(destination.SigningSecretEncrypted)
		if err != nil {
			return &support.AlertSendError{Message: "Signing secret could not be decrypted", Permanent: true}
		}
	}
	return support.SendAlertWebhook(ctx, client, destination.Kind, target, secret, message.ID, event, support.AlertMention{Mode: destination.MentionMode, ID: destination.MentionID})
}
