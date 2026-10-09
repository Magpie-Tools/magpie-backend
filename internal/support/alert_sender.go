package support

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"magpie/internal/api/dto"
)

type AlertSendError struct {
	Message    string
	Permanent  bool
	RetryAfter time.Duration
}

func (err *AlertSendError) Error() string { return err.Message }

type AlertMention struct {
	Mode string
	ID   string
}

var slackAlertGroupID = regexp.MustCompile(`^S[A-Z0-9]{1,31}$`)

func ValidateAlertMention(kind string, mention AlertMention) error {
	if mention.Mode == "" || mention.Mode == "none" {
		return nil
	}
	if (kind == "discord" || kind == "slack") && (mention.Mode == "here" || mention.Mode == "everyone") {
		return nil
	}
	if kind == "discord" && mention.Mode == "role" {
		id, err := strconv.ParseUint(mention.ID, 10, 64)
		if err == nil && id != 0 && strconv.FormatUint(id, 10) == mention.ID {
			return nil
		}
		return fmt.Errorf("enter a numeric Discord role ID")
	}
	if kind == "slack" && mention.Mode == "channel" {
		return nil
	}
	if kind == "slack" && mention.Mode == "user_group" {
		if slackAlertGroupID.MatchString(mention.ID) {
			return nil
		}
		return fmt.Errorf("enter a Slack user group ID beginning with S")
	}
	return fmt.Errorf("mention option is not supported for this channel")
}

func AlertMessage(event dto.AlertEvent) string {
	return alertMessageBody(event) + "\nTime: " + event.OccurredAt.UTC().Format("02 Jan 2006 15:04:05 MST") + "\nTimestamp: " + event.OccurredAt.UTC().Format(time.RFC3339)
}

func AlertEmailBody(event dto.AlertEvent) string {
	return "<html><body><p>" + strings.ReplaceAll(html.EscapeString(AlertMessage(event)), "\n", "<br>") + "</p></body></html>"
}

func alertMessageBody(event dto.AlertEvent) string {
	metric := "Usable routes"
	unit := ""
	comparison := "below"
	if event.Metric == "success_rate" {
		metric, unit = "Checker success rate", "%"
	}
	if event.Metric == "latency_ms" {
		metric, unit, comparison = "Average successful-check latency", " ms", "above"
	}
	state := "Incident opened"
	if event.Event == "recovered" {
		state, comparison = "Incident recovered", "threshold"
	}
	measurement := "Current routing eligibility"
	if event.Metric != "usable_routes" {
		measurement = "Workspace checker attempts, last 15 minutes"
		if event.MeasurementScope == "workspace_protocol_tcp_checks" {
			measurement = strings.ToUpper(event.Protocol) + " TCP workspace checker attempts, last 15 minutes"
		}
	}
	precision := 2
	if event.Metric == "usable_routes" {
		precision = 0
	}
	return fmt.Sprintf("Magpie: %s\nWorkspace: %s\nScope: %s\nMeasurement: %s\nRule: %s\n%s: %.*f%s, %s %.*f%s\nIncident: %d", state, event.WorkspaceName, event.ScopeName, measurement, event.RuleName, metric, precision, event.Value, unit, comparison, precision, event.Threshold, unit, event.IncidentID)
}

// Error messages deliberately omit secret webhook URLs and response bodies.
func SendAlertWebhook(ctx context.Context, client *http.Client, kind, target, signingSecret string, deliveryID uint64, event dto.AlertEvent, mention AlertMention) error {
	if err := ValidateAlertMention(kind, mention); err != nil {
		return &AlertSendError{Message: err.Error(), Permanent: true}
	}
	var payload any = event
	// Names never introduce mentions, even when the selected destination allows
	// a broadcast or role ping. Append only validated, explicit mention tokens.
	text := strings.ReplaceAll(alertMessageBody(event), "@", "@\u200b")
	isoTime := event.OccurredAt.UTC().Format(time.RFC3339)
	switch kind {
	case "slack":
		text = strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;").Replace(text)
		text += fmt.Sprintf("\nTime: <!date^%d^{date_long} at {time_secs}|%s>\nTimestamp: %s", event.OccurredAt.Unix(), isoTime, isoTime)
		switch mention.Mode {
		case "here", "channel", "everyone":
			text = "<!" + mention.Mode + ">\n" + text
		case "user_group":
			text = "<!subteam^" + mention.ID + ">\n" + text
		}
		payload = map[string]any{"text": text, "mrkdwn": true, "link_names": false, "parse": "none"}
	case "discord":
		text += fmt.Sprintf("\nTime: <t:%d:F>\nTimestamp: %s", event.OccurredAt.Unix(), isoTime)
		allowed := map[string]any{"parse": []string{}}
		switch mention.Mode {
		case "here", "everyone":
			text = "@" + mention.Mode + "\n" + text
			allowed["parse"] = []string{"everyone"}
		case "role":
			text = "<@&" + mention.ID + ">\n" + text
			allowed["roles"] = []string{mention.ID}
		}
		payload = map[string]any{"content": text, "allowed_mentions": allowed}
		parsed, err := url.Parse(target)
		if err != nil {
			return &AlertSendError{Message: "Invalid webhook URL", Permanent: true}
		}
		query := parsed.Query()
		query.Set("wait", "true")
		parsed.RawQuery = query.Encode()
		target = parsed.String()
	case "webhook":
	default:
		return &AlertSendError{Message: "Unsupported webhook kind", Permanent: true}
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return &AlertSendError{Message: "Could not encode notification", Permanent: true}
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		return &AlertSendError{Message: "Invalid webhook URL", Permanent: true}
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("User-Agent", "Magpie-Alerts/1")
	request.Header.Set("X-Magpie-Delivery-ID", strconv.FormatUint(deliveryID, 10))
	if kind == "webhook" && signingSecret != "" {
		mac := hmac.New(sha256.New, []byte(signingSecret))
		_, _ = mac.Write(body)
		request.Header.Set("X-Magpie-Signature", "sha256="+hex.EncodeToString(mac.Sum(nil)))
	}
	response, err := client.Do(request)
	if err != nil {
		return &AlertSendError{Message: "Webhook request failed"}
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		return nil
	}
	retry := response.StatusCode == 408 || response.StatusCode == 425 || response.StatusCode == 429 || response.StatusCode >= 500
	return &AlertSendError{Message: fmt.Sprintf("Webhook returned HTTP %d", response.StatusCode), Permanent: !retry, RetryAfter: AlertRetryAfter(response.Header.Get("Retry-After"), time.Now())}
}

func AlertRetryAfter(raw string, now time.Time) time.Duration {
	var delay time.Duration
	if seconds, err := strconv.ParseFloat(raw, 64); err == nil && seconds > 0 && seconds < 1e9 {
		delay = time.Duration(seconds * float64(time.Second))
	} else if deadline, err := http.ParseTime(raw); err == nil {
		delay = deadline.Sub(now)
	}
	if delay < 0 {
		return 0
	}
	if delay > 30*time.Minute {
		return 30 * time.Minute
	}
	return delay
}
