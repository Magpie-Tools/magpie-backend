package scraper

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"magpie/internal/config"
	"magpie/internal/support"
)

// Each browser context has its own proxy. Chromium never resolves an upstream
// hostname or bypasses the proxy for loopback, redirects, workers or subresources.
type browserEgressProxy struct {
	server    *http.Server
	transport *http.Transport
	address   string
	reject    func(error)
	mu        sync.Mutex
	closed    bool
	tunnels   map[net.Conn]struct{}
}

func startBrowserEgressProxy(ctx context.Context, reject func(error)) (*browserEgressProxy, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	p := &browserEgressProxy{
		address:   "http://" + listener.Addr().String(),
		transport: support.NewRestrictedOutboundHTTPClient(0).Transport.(*http.Transport),
		reject:    reject, tunnels: make(map[net.Conn]struct{}),
	}
	p.server = &http.Server{
		Handler: p, ReadHeaderTimeout: 5 * time.Second,
		BaseContext: func(net.Listener) context.Context { return ctx },
	}
	go func() { _ = p.server.Serve(listener) }()
	return p, nil
}

func (p *browserEgressProxy) close() {
	p.mu.Lock()
	p.closed = true
	for conn := range p.tunnels {
		_ = conn.Close()
	}
	p.mu.Unlock()
	_ = p.server.Close()
	p.transport.CloseIdleConnections()
}

func (p *browserEgressProxy) fail(w http.ResponseWriter, err error) {
	p.reject(err)
	http.Error(w, "browser destination denied", http.StatusForbidden)
}

func (p *browserEgressProxy) upstreamFailure(w http.ResponseWriter, err error) {
	if errors.Is(err, support.ErrUnsafeOutboundTarget) {
		p.fail(w, err)
		return
	}
	// Network failures belong to this resource. Only policy violations reject
	// the whole scrape; an upstream EOF does not mean DevTools disconnected.
	http.Error(w, "browser upstream unavailable", http.StatusBadGateway)
}

func (p *browserEgressProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodConnect {
		p.connect(w, r)
		return
	}
	if r.URL.Scheme != "http" || r.Header.Get("Upgrade") != "" {
		p.fail(w, fmt.Errorf("unsupported browser proxy request"))
		return
	}
	if _, err := support.ValidateOutboundHTTPLiteral(r.URL.String()); err != nil {
		p.fail(w, err)
		return
	}
	if config.IsWebsiteBlocked(r.URL.String()) {
		p.fail(w, fmt.Errorf("browser request blocked by website blacklist"))
		return
	}
	req := r.Clone(r.Context())
	req.RequestURI = ""
	removeBrowserProxyHeaders(req.Header)
	resp, err := p.transport.RoundTrip(req)
	if err != nil {
		p.upstreamFailure(w, err)
		return
	}
	defer resp.Body.Close()
	removeBrowserProxyHeaders(resp.Header)
	for name, values := range resp.Header {
		w.Header()[name] = values
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func (p *browserEgressProxy) connect(w http.ResponseWriter, r *http.Request) {
	if _, _, err := net.SplitHostPort(r.Host); err != nil {
		p.fail(w, fmt.Errorf("invalid browser CONNECT destination: %w", err))
		return
	}
	target := "https://" + r.Host + "/"
	if _, err := support.ValidateOutboundHTTPLiteral(target); err != nil {
		p.fail(w, err)
		return
	}
	if config.IsWebsiteBlocked(target) {
		p.fail(w, fmt.Errorf("browser CONNECT blocked by website blacklist"))
		return
	}
	upstream, err := support.DialRestrictedOutboundContext(r.Context(), "tcp", r.Host)
	if err != nil {
		p.upstreamFailure(w, err)
		return
	}
	defer upstream.Close()
	client, buffered, err := w.(http.Hijacker).Hijack()
	if err != nil {
		return
	}
	defer client.Close()
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.tunnels[client] = struct{}{}
	p.tunnels[upstream] = struct{}{}
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		delete(p.tunnels, client)
		delete(p.tunnels, upstream)
		p.mu.Unlock()
	}()
	if _, err := buffered.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
		return
	}
	if err := buffered.Flush(); err != nil {
		return
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = io.Copy(upstream, buffered)
		_ = upstream.Close()
	}()
	_, _ = io.Copy(client, upstream)
	_ = client.Close()
	<-done
}

func removeBrowserProxyHeaders(header http.Header) {
	for _, name := range strings.Split(header.Get("Connection"), ",") {
		header.Del(strings.TrimSpace(name))
	}
	for _, name := range []string{"Connection", "Proxy-Connection", "Proxy-Authorization", "Proxy-Authenticate", "Keep-Alive", "TE", "Trailer", "Transfer-Encoding", "Upgrade"} {
		header.Del(name)
	}
}
