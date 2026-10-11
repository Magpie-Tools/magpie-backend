package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"magpie/internal/api/dto"
	"magpie/internal/database"
	"magpie/internal/domain"
	"magpie/internal/rotatingproxy"
	"magpie/internal/support"
)

func TestCreateRotatingProxyFixtureMode(t *testing.T) {
	for _, key := range []string{"APP_ENV", "ENVIRONMENT", "GO_ENV", "MAGPIE_ENV"} {
		t.Setenv(key, "development")
	}
	for _, test := range []struct {
		name      string
		fixtures  string
		protocol  string
		transport string
	}{
		{name: "default starts HTTP listener", protocol: "http", transport: "tcp"},
		{name: "fixture HTTP", fixtures: "true", protocol: "http", transport: "tcp"},
		{name: "fixture SOCKS", fixtures: "true", protocol: "socks5", transport: "tcp"},
		{name: "fixture HTTP3", fixtures: "true", protocol: "http", transport: "http3"},
		{name: "fixture QUIC", fixtures: "true", protocol: "http", transport: "quic"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("DEVELOPMENT_FIXTURES", test.fixtures)
			redis := miniredis.RunT(t)
			t.Setenv("REDIS_URL", "redis://"+redis.Addr())
			_ = support.CloseRedisClient()
			t.Cleanup(func() { _ = support.CloseRedisClient() })
			setupUserRegistrationTestDB(t)
			if err := database.DB.AutoMigrate(
				&domain.ManagedProxy{}, &domain.RotatingProxy{}, &domain.ProxyLatestStatistic{}, &domain.ProxyCheckerPlan{},
				&domain.AlertRule{}, &domain.AlertDestination{}, &domain.AlertIncident{}, &domain.AlertDelivery{},
			); err != nil {
				t.Fatal(err)
			}
			_, workspace := createWorkspaceMiddlewareTestUser(t, "rotator@example.test")
			if err := database.DB.Model(&workspace).Update("http_protocol", true).Error; err != nil {
				t.Fatal(err)
			}
			if err := database.DB.Create(&[]domain.Protocol{{Name: "http"}, {Name: "socks5"}}).Error; err != nil {
				t.Fatal(err)
			}

			previous := rotatingproxy.GlobalManager
			rotatingproxy.GlobalManager = rotatingproxy.NewManager()
			t.Cleanup(func() {
				rotatingproxy.GlobalManager.StopAll()
				rotatingproxy.GlobalManager = previous
			})
			body, err := json.Marshal(dto.RotatingProxyCreateRequest{
				Name: "Development rotator", InstanceID: support.GetInstanceID(),
				Protocol: "http", ListenProtocol: test.protocol, ListenTransportProtocol: test.transport,
			})
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(http.MethodPost, "/rotating-proxies", bytes.NewReader(body))
			request = request.WithContext(context.WithValue(request.Context(), workspaceContextKey{}, database.WorkspaceAccess{WorkspaceID: workspace.ID}))
			response := httptest.NewRecorder()
			createRotatingProxy(response, request)
			if response.Code != http.StatusCreated {
				t.Fatalf("create: status %d, body %s", response.Code, response.Body.String())
			}
			var created dto.RotatingProxy
			if err := json.Unmarshal(response.Body.Bytes(), &created); err != nil {
				t.Fatal(err)
			}
			if _, err := database.GetRotatingProxyByID(created.ID); err != nil {
				t.Fatalf("rotator configuration was not retained: %v", err)
			}
			address := net.JoinHostPort("127.0.0.1", strconv.Itoa(int(created.ListenPort)))
			if test.fixtures == "" {
				connection, err := net.DialTimeout("tcp", address, time.Second)
				if err != nil {
					t.Fatalf("default mode did not start listener: %v", err)
				}
				_ = connection.Close()
				return
			}

			// Reconciliation must leave API-created fixture rotators offline too.
			rotatingproxy.GlobalManager.Reconcile()
			listener, err := net.Listen("tcp", address)
			if err != nil {
				t.Fatalf("fixture rotator opened a TCP listener: %v", err)
			}
			defer listener.Close()
			packet, err := net.ListenPacket("udp", address)
			if err != nil {
				t.Fatalf("fixture rotator opened a UDP listener: %v", err)
			}
			defer packet.Close()
		})
	}
}

func TestFormatRotatingProxyListenAddress(t *testing.T) {
	testCases := map[string]string{
		"127.0.0.1":     "127.0.0.1:19000",
		"2001:db8::1":   "[2001:db8::1]:19000",
		"[2001:db8::1]": "[2001:db8::1]:19000",
		"":              "19000",
	}

	for host, expected := range testCases {
		if got := formatRotatingProxyListenAddress(host, 19000); got != expected {
			t.Errorf("formatRotatingProxyListenAddress(%q) = %q, want %q", host, got, expected)
		}
	}
}
