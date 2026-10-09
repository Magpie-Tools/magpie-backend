package support

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"magpie/internal/api/dto"
)

type alertRoundTripper func(*http.Request) (*http.Response, error)

func (f alertRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestAlertWebhookPayloadsSigningRateLimitAndSecretRedaction(t *testing.T) {
	event := dto.AlertEvent{Version: 1, IncidentID: 17, WorkspaceID: 3, RuleName: "<@everyone>", Event: "opened", Metric: "success_rate", Value: 50}
	for _, kind := range []string{"webhook", "slack", "discord"} {
		t.Run(kind, func(t *testing.T) {
			client := &http.Client{Transport: alertRoundTripper(func(r *http.Request) (*http.Response, error) {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Fatal(err)
				}
				if r.Header.Get("X-Magpie-Delivery-ID") != "42" {
					t.Fatal("missing delivery identity")
				}
				var payload map[string]any
				if err := json.Unmarshal(body, &payload); err != nil {
					t.Fatal(err)
				}
				if kind == "webhook" {
					mac := hmac.New(sha256.New, []byte("shared-secret"))
					_, _ = mac.Write(body)
					if r.Header.Get("X-Magpie-Signature") != "sha256="+hex.EncodeToString(mac.Sum(nil)) {
						t.Fatal("invalid signature")
					}
					if payload["incident_id"] != float64(17) {
						t.Fatal("missing structured incident")
					}
				}
				if kind == "slack" && (payload["mrkdwn"] != true || payload["parse"] != "none" || payload["link_names"] != false || strings.Contains(payload["text"].(string), "<@everyone>")) {
					t.Fatal("Slack text permits markup mentions")
				}
				if kind == "discord" {
					if r.URL.Query().Get("wait") != "true" || len(payload["allowed_mentions"].(map[string]any)["parse"].([]any)) != 0 {
						t.Fatal("Discord confirmation or mention restriction missing")
					}
				}
				return &http.Response{StatusCode: 429, Header: http.Header{"Retry-After": []string{"60"}}, Body: io.NopCloser(strings.NewReader("secret-url"))}, nil
			})}
			err := SendAlertWebhook(context.Background(), client, kind, "https://example.com/secret-url", "shared-secret", 42, event, AlertMention{})
			var sendErr *AlertSendError
			if !errors.As(err, &sendErr) || sendErr.Permanent || sendErr.RetryAfter != time.Minute || strings.Contains(err.Error(), "secret-url") {
				t.Fatalf("wrong rate-limit handling: %v", err)
			}
		})
	}
}

func TestAlertWebhookRejectsRedirectsAndBoundsRetryAfter(t *testing.T) {
	var forwarded bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/private" {
			forwarded = true
			w.WriteHeader(204)
			return
		}
		w.Header().Set("Location", "/private")
		w.WriteHeader(307)
	}))
	defer server.Close()
	client := server.Client()
	client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }
	err := SendAlertWebhook(context.Background(), client, "webhook", server.URL, "", 1, dto.AlertEvent{}, AlertMention{})
	var sendErr *AlertSendError
	if !errors.As(err, &sendErr) || !sendErr.Permanent || forwarded {
		t.Fatal("redirect forwarded a secret notification")
	}
	if AlertRetryAfter("99999999", time.Now()) != 30*time.Minute || AlertRetryAfter("garbage", time.Now()) != 0 {
		t.Fatal("invalid retry bounds")
	}
}

func captureAlertPayload(t *testing.T, kind string, event dto.AlertEvent, mention AlertMention) map[string]any {
	t.Helper()
	var payload map[string]any
	client := &http.Client{Transport: alertRoundTripper(func(r *http.Request) (*http.Response, error) {
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}
		return &http.Response{StatusCode: 204, Body: io.NopCloser(strings.NewReader(""))}, nil
	})}
	if err := SendAlertWebhook(context.Background(), client, kind, "https://example.com/hook", "", 42, event, mention); err != nil {
		t.Fatal(err)
	}
	return payload
}

func TestAlertMessagesUseWholeRouteCountsAndChannelTimestamps(t *testing.T) {
	when, err := time.Parse(time.RFC3339, "2026-10-09T18:33:56+02:00")
	if err != nil {
		t.Fatal(err)
	}
	event := dto.AlertEvent{Event: "opened", Metric: "usable_routes", Value: 932, Threshold: 10000, OccurredAt: when}
	wantCount := "Usable routes: 932, below 10000"
	wantISO := "\nTimestamp: 2026-10-09T16:33:56Z"
	for _, kind := range []string{"discord", "slack"} {
		payload := captureAlertPayload(t, kind, event, AlertMention{})
		field := "text"
		if kind == "discord" {
			field = "content"
		}
		text := payload[field].(string)
		if !strings.Contains(text, wantCount) || !strings.HasSuffix(text, wantISO) {
			t.Fatalf("%s count or ISO timestamp formatting: %s", kind, text)
		}
		wantTime := fmt.Sprintf("\nTime: <t:%d:F>", when.Unix())
		if kind == "slack" {
			wantTime = fmt.Sprintf("\nTime: <!date^%d^{date_long} at {time_secs}|2026-10-09T16:33:56Z>", when.Unix())
		}
		if !strings.Contains(text, wantTime+wantISO) {
			t.Fatalf("%s native timestamp missing: %s", kind, text)
		}
	}
	plainEmail := htmlToPlainText(AlertEmailBody(event))
	if !strings.Contains(plainEmail, wantCount) || !strings.HasSuffix(plainEmail, "\nTime: 09 Oct 2026 16:33:56 UTC"+wantISO) {
		t.Fatalf("email count or timestamp rows: %s", plainEmail)
	}
	event.Metric, event.Value, event.Threshold = "success_rate", 93.25, 99.5
	if !strings.Contains(AlertMessage(event), "Checker success rate: 93.25%, below 99.50%") {
		t.Fatal("percentage precision was lost")
	}
	event.Metric, event.Value, event.Threshold, event.Event = "latency_ms", 123.45, 100.5, "recovered"
	if !strings.Contains(AlertMessage(event), "Average successful-check latency: 123.45 ms, threshold 100.50 ms") {
		t.Fatal("latency precision or recovery formatting was lost")
	}
}

func TestAlertMentionsOnlyAllowTheConfiguredAudience(t *testing.T) {
	attack := "@here @everyone <@&165511591545143296> <@123> <!here> <!subteam^SAZ94GDB8>"
	event := dto.AlertEvent{RuleName: attack, WorkspaceName: attack, ScopeName: attack, Metric: "usable_routes", OccurredAt: time.Now()}
	for _, mode := range []string{"none", "here", "everyone", "role"} {
		t.Run("discord_"+mode, func(t *testing.T) {
			mention := AlertMention{Mode: mode, ID: "165511591545143296"}
			payload := captureAlertPayload(t, "discord", event, mention)
			text := payload["content"].(string)
			allowed := payload["allowed_mentions"].(map[string]any)
			parse := allowed["parse"].([]any)
			prefix := ""
			if mode == "here" || mode == "everyone" {
				prefix = "@" + mode + "\n"
				if len(parse) != 1 || parse[0] != "everyone" {
					t.Fatal("broadcast mention not explicitly allowed")
				}
			} else if len(parse) != 0 {
				t.Fatal("unconfigured mentions were allowed")
			}
			if mode == "role" {
				prefix = "<@&" + mention.ID + ">\n"
				roles := allowed["roles"].([]any)
				if len(roles) != 1 || roles[0] != mention.ID {
					t.Fatal("role mention is not limited to the configured ID")
				}
			} else if _, exists := allowed["roles"]; exists {
				t.Fatal("extra roles allowed")
			}
			if !strings.HasPrefix(text, prefix+"Magpie:") || strings.Contains(strings.TrimPrefix(text, prefix), "@everyone") || strings.Contains(strings.TrimPrefix(text, prefix), "<@&") || strings.Contains(strings.TrimPrefix(text, prefix), "@here") {
				t.Fatal("a rule or workspace name introduced an unintended Discord ping")
			}
		})
	}
	for _, mode := range []string{"none", "here", "everyone", "channel", "user_group"} {
		t.Run("slack_"+mode, func(t *testing.T) {
			mention := AlertMention{Mode: mode, ID: "SAZ94GDB8"}
			payload := captureAlertPayload(t, "slack", event, mention)
			text := payload["text"].(string)
			prefix := ""
			if mode == "user_group" {
				prefix = "<!subteam^" + mention.ID + ">\n"
			} else if mode != "none" {
				prefix = "<!" + mode + ">\n"
			}
			body := strings.TrimPrefix(text, prefix)
			if !strings.HasPrefix(text, prefix+"Magpie:") || strings.Contains(body, "<!here>") || strings.Contains(body, "<!subteam^") || strings.Contains(body, "@everyone") || payload["parse"] != "none" || payload["link_names"] != false {
				t.Fatal("a rule or workspace name introduced an unintended Slack ping")
			}
		})
	}
}

func TestInvalidAlertMentionNeverSendsARequest(t *testing.T) {
	for _, tc := range []struct{ kind, mode, id string }{
		{"discord", "role", "123> @everyone"}, {"discord", "role", "0"}, {"discord", "role", "18446744073709551616"},
		{"discord", "channel", ""}, {"discord", "user_group", "SAZ94GDB8"},
		{"slack", "user_group", "SAZ94GDB8> <!everyone>"}, {"slack", "user_group", "missing-prefix"}, {"slack", "role", "123"},
		{"webhook", "everyone", ""}, {"email", "here", ""},
	} {
		client := &http.Client{Transport: alertRoundTripper(func(r *http.Request) (*http.Response, error) {
			t.Fatal("sent invalid mention configuration")
			return nil, nil
		})}
		err := SendAlertWebhook(context.Background(), client, tc.kind, "https://example.com/hook", "", 42, dto.AlertEvent{}, AlertMention{Mode: tc.mode, ID: tc.id})
		var sendErr *AlertSendError
		if !errors.As(err, &sendErr) || !sendErr.Permanent {
			t.Fatalf("invalid %s mention was not rejected: %v", tc.kind, err)
		}
	}
}

func TestEmailContextBoundsStalledSMTPGreeting(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err == nil {
			defer conn.Close()
			_, _ = io.Copy(io.Discard, conn)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	started := time.Now()
	err = SendEmailContext(ctx, EmailConfig{FromAddress: "from@example.com", SMTPHost: "127.0.0.1", SMTPPort: portNumber}, "to@example.com", "Test", "Body")
	if err == nil || time.Since(started) > 2*time.Second {
		t.Fatal("stalled SMTP greeting did not respect cancellation")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("SMTP socket was not closed")
	}
}
