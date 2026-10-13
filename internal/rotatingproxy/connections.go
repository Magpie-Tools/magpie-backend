package rotatingproxy

import (
	"context"
	"net"
	"sync"

	"magpie/internal/api/dto"
	"magpie/internal/support"
)

const envRotatingProxyMaxConcurrentTunnels = "ROTATING_PROXY_MAX_CONCURRENT_TUNNELS"

// Each rotator owns its accepted clients and upstream sockets, including
// hijacked HTTP connections that http.Server.Shutdown cannot close.
type rotatorConnections struct {
	mu        sync.Mutex
	closed    bool
	limit     int
	clients   map[net.Conn]struct{}
	upstreams map[net.Conn]struct{}
	ctx       context.Context
	cancel    context.CancelFunc
}

func newRotatorConnections() *rotatorConnections {
	ctx, cancel := context.WithCancel(context.Background())
	limit := support.GetEnvInt(envRotatingProxyMaxConcurrentTunnels, 1024)
	if limit <= 0 {
		limit = 1024
	}
	return &rotatorConnections{limit: limit, clients: make(map[net.Conn]struct{}), upstreams: make(map[net.Conn]struct{}), ctx: ctx, cancel: cancel}
}

func (c *rotatorConnections) add(conn net.Conn, upstream bool) bool {
	if c == nil {
		return true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || (!upstream && len(c.clients) >= c.limit) {
		_ = conn.Close()
		return false
	}
	if upstream {
		c.upstreams[conn] = struct{}{}
	} else {
		c.clients[conn] = struct{}{}
	}
	return true
}

func (c *rotatorConnections) remove(conn net.Conn) {
	if c == nil {
		return
	}
	c.mu.Lock()
	delete(c.clients, conn)
	delete(c.upstreams, conn)
	c.mu.Unlock()
}

func (c *rotatorConnections) stop() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.closed = true
	c.cancel()
	connections := make([]net.Conn, 0, len(c.clients)+len(c.upstreams))
	for conn := range c.clients {
		connections = append(connections, conn)
	}
	for conn := range c.upstreams {
		connections = append(connections, conn)
	}
	c.mu.Unlock()
	for _, conn := range connections {
		_ = conn.Close()
	}
}

func connectForRotator(c *rotatorConnections, target string, next *dto.RotatingProxyNext) (net.Conn, error) {
	ctx := context.Background()
	if c != nil {
		ctx = c.ctx
	}
	return connectThroughUpstreamWithContext(ctx, target, next, connectThroughUpstreamFunc)
}
