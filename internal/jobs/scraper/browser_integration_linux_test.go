package scraper

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"github.com/go-rod/rod"
	"github.com/go-rod/rod/lib/defaults"
	"github.com/go-rod/rod/lib/launcher"
	"github.com/go-rod/rod/lib/proto"
	"magpie/internal/support"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// Run with SCRAPER_TEST_BROWSER_BIN pointing at Chromium. These tests use only
// local fixture servers and their own browser process/profile.
func browserFixture(t *testing.T, resolverRules ...string) (*browserManager, *launcher.Launcher) {
	t.Helper()
	bin := os.Getenv("SCRAPER_TEST_BROWSER_BIN")
	if bin == "" {
		t.Skip("set SCRAPER_TEST_BROWSER_BIN to run browser integration tests")
	}
	t.Setenv("ALLOW_PRIVATE_NETWORK_EGRESS", "true")
	l := launcher.New().Bin(bin).UserDataDir(t.TempDir()).Leakless(false).NoSandbox(true)
	l.Set("ignore-certificate-errors") // Only the self-signed TLS fixture below.
	if len(resolverRules) != 0 {
		l.Set("host-resolver-rules", resolverRules[0])
	}
	url, err := l.Launch()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	b := rod.New().ControlURL(url).Context(ctx)
	if err := b.Connect(); err != nil {
		cancel()
		l.Kill()
		t.Fatal(err)
	}
	instance := &browserInstance{browser: b, stop: func() { cancel(); l.Kill(); l.Cleanup() }}
	manager := newBrowserManager(2, func(context.Context) (*browserInstance, error) { return nil, fmt.Errorf("unexpected launch") })
	manager.current = instance
	t.Cleanup(instance.close)
	return manager, l
}

func TestBrowserResolverDivergenceCannotReachPrivateService(t *testing.T) {
	manager, _ := browserFixture(t, "MAP example.com 127.0.0.1")
	t.Setenv("ALLOW_PRIVATE_NETWORK_EGRESS", "false")
	var hits atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		fmt.Fprint(w, "<html><body>private service</body></html>")
	}))
	defer server.Close()
	_, port, _ := net.SplitHostPort(strings.TrimPrefix(server.URL, "http://"))
	target := "http://example.com:" + port + "/private-action"
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := support.ValidateOutboundHTTPURLContext(ctx, target); err != nil {
		t.Fatalf("public DNS fixture failed: %v", err)
	}
	_, err := fetchBrowser(ctx, target, manager)
	if err == nil || hits.Load() != 0 {
		t.Fatalf("private requests=%d error=%v", hits.Load(), err)
	}
}

func TestBrowserRendersJavaScriptAndIsolatesCookies(t *testing.T) {
	manager, _ := browserFixture(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/app.js":
			w.Header().Set("Content-Type", "application/javascript")
			fmt.Fprint(w, `document.body.innerHTML = "8.8.8.8:8080"; document.cookie="secret=first";`)
		case "/cookies":
			fmt.Fprint(w, "cookie="+r.Header.Get("Cookie"))
		default:
			w.Header().Set("Content-Type", "text/html")
			fmt.Fprint(w, `<html><body><script src="/app.js"></script></body></html>`)
		}
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	html, err := fetchBrowser(ctx, server.URL, manager)
	if err != nil || !strings.Contains(html, "8.8.8.8:8080") {
		t.Fatalf("rendered HTML=%q err=%v", html, err)
	}
	html, err = fetchBrowser(ctx, server.URL+"/cookies", manager)
	if err != nil || strings.Contains(html, "secret=first") {
		t.Fatalf("cookie isolation: HTML=%q err=%v", html, err)
	}
}

func TestBrowserEgressSupportsHTTPSAndWebSockets(t *testing.T) {
	manager, _ := browserFixture(t)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/socket" {
			conn, buffered, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			defer conn.Close()
			accept := sha1.Sum([]byte(r.Header.Get("Sec-WebSocket-Key") + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
			fmt.Fprintf(buffered, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n", base64.StdEncoding.EncodeToString(accept[:]))
			message := "8.8.8.8:8080"
			_, _ = buffered.Write(append([]byte{0x81, byte(len(message))}, message...))
			_ = buffered.Flush()
			return
		}
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, `<html><body><script>new WebSocket(location.origin.replace('http','ws')+'/socket').onmessage = e => document.body.textContent = e.data;</script></body></html>`)
	})
	for _, tls := range []bool{false, true} {
		var server *httptest.Server
		if tls {
			server = httptest.NewTLSServer(handler)
		} else {
			server = httptest.NewServer(handler)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		html, err := fetchBrowser(ctx, server.URL, manager)
		cancel()
		server.Close()
		if err != nil || !strings.Contains(html, "8.8.8.8:8080") {
			t.Fatalf("TLS=%v WebSocket rendered HTML=%q err=%v", tls, html, err)
		}
	}
}

func TestBrowserCleanupRecoversFromUnresponsiveChromium(t *testing.T) {
	manager, l := browserFixture(t)
	paused := make(chan struct{})
	var once sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		once.Do(func() {
			if err := syscall.Kill(l.PID(), syscall.SIGSTOP); err != nil {
				t.Error(err)
			}
			close(paused)
		})
		fmt.Fprint(w, "<html><body>8.8.8.8:8080</body></html>")
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	finished := make(chan error, 1)
	go func() { _, err := fetchBrowser(ctx, server.URL, manager); finished <- err }()
	// Cancel only once navigation reached the fixture and Chromium is stopped.
	// A short fixed deadline can expire during page setup on a busy test host.
	select {
	case <-paused:
	case err := <-finished:
		t.Fatalf("fixture navigation did not reach server: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("fixture navigation did not reach server")
	}
	started := time.Now()
	cancel()
	select {
	case err := <-finished:
		if err == nil {
			t.Fatal("unresponsive browser reported success")
		}
	case <-time.After(4 * time.Second):
		t.Fatal("cleanup exceeded bound")
	}
	if manager.current != nil {
		t.Fatal("unresponsive browser was not invalidated")
	}
	if len(manager.slots) != 0 {
		t.Fatal("cleanup leaked browser slot")
	}
	t.Logf("unresponsive browser discarded and worker released after %v", time.Since(started))
}

func TestBrowserStartupContextDoesNotCancelRunningBrowser(t *testing.T) {
	bin := os.Getenv("SCRAPER_TEST_BROWSER_BIN")
	if bin == "" {
		t.Skip("set SCRAPER_TEST_BROWSER_BIN to run browser integration tests")
	}
	original := defaults.Bin
	defaults.Bin = bin
	t.Cleanup(func() { defaults.Bin = original })
	startup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	instance, err := launchScraperBrowser(startup)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.close()
	cancel()
	if _, err := (proto.BrowserGetVersion{}).Call(instance.browser.Timeout(time.Second)); err != nil {
		t.Fatalf("startup cancellation killed a running browser: %v", err)
	}
}
