package scraper

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-rod/rod"
	"github.com/go-rod/rod/lib/launcher"
	"magpie/internal/support"
)

const (
	defaultBrowserConcurrency = 4
	maxBrowserConcurrency     = 64
	browserStartupTimeout     = 20 * time.Second
	browserCleanupTimeout     = 2 * time.Second
	browserRetryDelay         = 30 * time.Second
	envScraperPagePoolMax     = "SCRAPER_PAGE_POOL_MAX_CAPACITY"
)

var errBrowserUnavailable = errors.New("browser unavailable")
var errBrowserBusy = errors.New("browser capacity is busy")

type browserInstance struct {
	browser  *rod.Browser
	stop     func()
	stopOnce sync.Once
}

func (b *browserInstance) close() { b.stopOnce.Do(b.stop) }

type browserManager struct {
	mu         sync.Mutex
	current    *browserInstance
	starting   chan struct{}
	retryAfter time.Time
	lastError  error
	launch     func(context.Context) (*browserInstance, error)
	slots      chan struct{}
}

func newBrowserManager(limit int, launch func(context.Context) (*browserInstance, error)) *browserManager {
	return &browserManager{slots: make(chan struct{}, max(1, min(limit, maxBrowserConcurrency))), launch: launch}
}

var sharedBrowser = sync.OnceValue(func() *browserManager {
	return newBrowserManager(support.GetEnvInt(envScraperPagePoolMax, defaultBrowserConcurrency), launchScraperBrowser)
})

// A busy browser never consumes all HTTP workers waiting for a page. Its source
// is retried later through the ordinary source queue.
func (m *browserManager) acquire(ctx context.Context) (*browserInstance, func(), error) {
	select {
	case m.slots <- struct{}{}:
	default:
		return nil, nil, errBrowserBusy
	}
	release := func() { <-m.slots }
	b, err := m.get(ctx)
	if err != nil {
		release()
		return nil, nil, err
	}
	return b, release, nil
}

func (m *browserManager) get(ctx context.Context) (*browserInstance, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		m.mu.Lock()
		if m.current != nil {
			b := m.current
			m.mu.Unlock()
			return b, nil
		}
		if time.Now().Before(m.retryAfter) {
			err := m.lastError
			m.mu.Unlock()
			return nil, fmt.Errorf("%w: %v", errBrowserUnavailable, err)
		}
		if m.starting == nil {
			m.starting = make(chan struct{})
			go m.start(m.starting)
		}
		ready := m.starting
		m.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("%w: %w", errBrowserUnavailable, ctx.Err())
		case <-ready:
		}
	}
}

func (m *browserManager) start(ready chan struct{}) {
	ctx, cancel := context.WithTimeout(context.Background(), browserStartupTimeout)
	defer cancel()
	b, err := m.launch(ctx)
	m.mu.Lock()
	defer m.mu.Unlock()
	if err != nil {
		m.lastError = err
		m.retryAfter = time.Now().Add(browserRetryDelay)
	} else {
		m.current = b
		m.lastError = nil
		m.retryAfter = time.Time{}
	}
	m.starting = nil
	close(ready)
}

// Errors from an old generation cannot restart a newly recovered browser.
func (m *browserManager) invalidate(b *browserInstance) {
	m.mu.Lock()
	if m.current == b {
		m.current = nil
	}
	m.mu.Unlock()
	b.close()
}

func launchScraperBrowser(ctx context.Context) (*browserInstance, error) {
	profile, err := os.MkdirTemp("", "magpie-chromium-")
	if err != nil {
		return nil, err
	}
	l := launcher.New().Context(ctx).UserDataDir(profile).Leakless(true).Headless(true).
		Set("disable-background-timer-throttling").Set("disable-backgrounding-occluded-windows").Set("disable-renderer-backgrounding")
	url, err := l.Launch()
	if err != nil {
		if l.PID() > 0 {
			l.Kill()
		}
		_ = os.RemoveAll(profile)
		return nil, fmt.Errorf("launch Chromium: %w", err)
	}
	lifetime, cancel := context.WithCancel(context.Background())
	// Startup has a deadline, while a successfully connected browser lives until
	// explicitly invalidated. Its event loop must not inherit the startup deadline.
	stopStartupCancel := context.AfterFunc(ctx, cancel)
	b := rod.New().ControlURL(url).Context(lifetime)
	err = b.Connect()
	stopped := stopStartupCancel()
	if err != nil || !stopped || ctx.Err() != nil {
		cancel()
		l.Kill()
		l.Cleanup()
		if err == nil {
			err = ctx.Err()
		}
		return nil, fmt.Errorf("connect Chromium: %w", err)
	}
	return &browserInstance{browser: b, stop: func() { cancel(); l.Kill(); l.Cleanup() }}, nil
}

func isConnClosed(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	s := err.Error()
	return strings.Contains(s, "use of closed network connection") || strings.Contains(s, "websocket: close") || strings.Contains(s, "read tcp") || strings.Contains(s, "write tcp")
}
