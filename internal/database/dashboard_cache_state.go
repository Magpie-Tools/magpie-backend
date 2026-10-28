package database

import (
	"context"
	"errors"
	"sync"
	"time"

	"magpie/internal/api/dto"
)

const (
	dashboardCacheFreshness = 30 * time.Second
	dashboardDemandIdle     = 2 * time.Minute
	dashboardRefreshBudget  = 30 * time.Second
)

type dashboardComponent uint8

const (
	dashboardInfoComponent dashboardComponent = iota
	dashboardRecentComponent
	dashboardFastestComponent
)

type dashboardCacheKey struct {
	workspaceID uint
	component   dashboardComponent
	limit       int
}

type dashboardCacheFlight struct {
	done  chan struct{}
	value any
	err   error
}

type dashboardCacheInterest struct {
	requestedAt time.Time
	usedAt      time.Time
	flight      *dashboardCacheFlight
	revision    uint64
}

type dashboardCacheCoordinator struct {
	mu      sync.Mutex
	entries map[dashboardCacheKey]*dashboardCacheInterest
	now     func() time.Time
	evict   func(dashboardCacheKey)
}

func newDashboardCacheCoordinator(now func() time.Time, evict func(dashboardCacheKey)) *dashboardCacheCoordinator {
	return &dashboardCacheCoordinator{entries: make(map[dashboardCacheKey]*dashboardCacheInterest), now: now, evict: evict}
}

var dashboardCaches = newDashboardCacheCoordinator(time.Now, evictDashboardCacheKey)

func evictDashboardCacheKey(key dashboardCacheKey) {
	switch key.component {
	case dashboardInfoComponent:
		dashboardInfoCache.Delete(key.workspaceID)
	case dashboardRecentComponent:
		dashboardRecentChecksCache.Delete(dashboardProxyListCacheKey{WorkspaceID: key.workspaceID, Limit: key.limit})
	case dashboardFastestComponent:
		dashboardFastestAliveCache.Delete(dashboardProxyListCacheKey{WorkspaceID: key.workspaceID, Limit: key.limit})
	}
}

func (c *dashboardCacheCoordinator) touch(key dashboardCacheKey) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := c.entries[key]
	if entry == nil {
		entry = &dashboardCacheInterest{}
		c.entries[key] = entry
	}
	entry.requestedAt = c.now()
}

func dashboardTimeWithin(now, then time.Time, limit time.Duration) bool {
	age := now.Sub(then)
	return !then.IsZero() && age >= 0 && age < limit
}

func (c *dashboardCacheCoordinator) fresh(completedAt time.Time) bool {
	return dashboardTimeWithin(c.now(), completedAt, dashboardCacheFreshness)
}

func (c *dashboardCacheCoordinator) revision(key dashboardCacheKey) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := c.entries[key]
	if entry == nil {
		entry = &dashboardCacheInterest{usedAt: c.now()}
		c.entries[key] = entry
	}
	return entry.revision
}

func (c *dashboardCacheCoordinator) publish(key dashboardCacheKey, revision uint64, store func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry := c.entries[key]; entry != nil && entry.revision == revision {
		store()
	}
}

func invalidateDashboardWorkspace(workspaceID uint) {
	dashboardCaches.mu.Lock()
	defer dashboardCaches.mu.Unlock()
	for key, entry := range dashboardCaches.entries {
		if key.workspaceID == workspaceID {
			entry.revision++
			dashboardCaches.evict(key)
		}
	}
	// Tests and integrations can seed a value without creating demand.
	dashboardInfoCache.Delete(workspaceID)
	for _, cache := range []*sync.Map{&dashboardRecentChecksCache, &dashboardFastestAliveCache} {
		cache.Range(func(key, _ any) bool {
			if key.(dashboardProxyListCacheKey).WorkspaceID == workspaceID {
				cache.Delete(key)
			}
			return true
		})
	}
}

func forgetDashboardWorkspace(workspaceID uint) {
	invalidateDashboardWorkspace(workspaceID)
	dashboardCaches.mu.Lock()
	defer dashboardCaches.mu.Unlock()
	for key, entry := range dashboardCaches.entries {
		if key.workspaceID == workspaceID {
			entry.requestedAt, entry.usedAt = time.Time{}, time.Time{}
			if entry.flight == nil {
				delete(dashboardCaches.entries, key)
			}
		}
	}
}

func (c *dashboardCacheCoordinator) requested(key dashboardCacheKey) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := c.entries[key]
	return entry != nil && dashboardTimeWithin(c.now(), entry.requestedAt, dashboardDemandIdle)
}

func (c *dashboardCacheCoordinator) requestedKeys() []dashboardCacheKey {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	keys := make([]dashboardCacheKey, 0, len(c.entries))
	for key, entry := range c.entries {
		if dashboardTimeWithin(now, entry.requestedAt, dashboardDemandIdle) {
			keys = append(keys, key)
			continue
		}
		// A forced prewarm has no request. Retain its value briefly without
		// registering demand, and never evict a leader while it publishes.
		prewarmGrace := entry.requestedAt.IsZero() && dashboardTimeWithin(now, entry.usedAt, dashboardDemandIdle)
		if entry.flight == nil && !prewarmGrace {
			c.evict(key)
			delete(c.entries, key)
		}
	}
	return keys
}

func (c *dashboardCacheCoordinator) run(ctx context.Context, key dashboardCacheKey, load func(context.Context) (any, error)) (any, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	// A waiter may retry once when another request's cancellation stopped the
	// leader. Its own cancellation never stops a leader owned by someone else.
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		c.mu.Lock()
		entry := c.entries[key]
		if entry == nil {
			entry = &dashboardCacheInterest{}
			c.entries[key] = entry
		}
		if flight := entry.flight; flight != nil {
			c.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-flight.done:
				if attempt == 0 && ctx.Err() == nil && (errors.Is(flight.err, context.Canceled) || errors.Is(flight.err, context.DeadlineExceeded)) {
					continue
				}
				return flight.value, flight.err
			}
		}
		flight := &dashboardCacheFlight{done: make(chan struct{})}
		entry.flight = flight
		c.mu.Unlock()
		func() {
			loadCtx, cancel := context.WithTimeout(ctx, dashboardRefreshBudget)
			returned := false
			defer func() {
				cancel()
				if !returned {
					// Preserve the panic for the caller's recovery, but release
					// followers so one failed request cannot pin this key forever.
					flight.err = errors.New("dashboard cache: refresh aborted")
				}
				c.mu.Lock()
				entry.flight = nil
				entry.usedAt = c.now()
				if !entry.requestedAt.IsZero() && !dashboardTimeWithin(entry.usedAt, entry.requestedAt, dashboardDemandIdle) {
					c.evict(key)
					delete(c.entries, key)
				}
				close(flight.done)
				c.mu.Unlock()
			}()
			flight.value, flight.err = load(loadCtx)
			if flight.err == nil {
				flight.err = loadCtx.Err()
			}
			returned = true
		}()
		return flight.value, flight.err
	}
}

func dashboardDBContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func refreshDashboardComponent(ctx context.Context, key dashboardCacheKey) (any, error) {
	return dashboardCaches.run(ctx, key, func(loadCtx context.Context) (any, error) {
		return loadDashboardComponent(loadCtx, key)
	})
}

func loadDashboardComponent(ctx context.Context, key dashboardCacheKey) (any, error) {
	if DB == nil {
		return nil, errors.New("dashboard cache: database connection was not initialised")
	}
	db := DB.WithContext(ctx)
	switch key.component {
	case dashboardInfoComponent:
		return refreshDashboardInfoCache(db, key.workspaceID)
	case dashboardRecentComponent:
		return refreshRecentProxyChecksCache(db, key.workspaceID, key.limit)
	default:
		return refreshFastestAliveProxiesCache(db, key.workspaceID, key.limit)
	}
}

func cachedDashboardComponent(key dashboardCacheKey, version checkerHealthVersion) (any, bool) {
	if version.Dirty {
		return nil, false
	}
	switch key.component {
	case dashboardInfoComponent:
		if value, ok := dashboardInfoCache.Load(key.workspaceID); ok {
			entry := value.(dashboardInfoCacheEntry)
			if entry.version == version && dashboardCaches.fresh(entry.completedAt) {
				return entry.info, true
			}
		}
	case dashboardRecentComponent:
		if value, ok := dashboardRecentChecksCache.Load(dashboardProxyListCacheKey{WorkspaceID: key.workspaceID, Limit: key.limit}); ok {
			entry := value.(dashboardHealthCacheEntry[[]dto.ProxyRecentCheck])
			if entry.version == version && dashboardCaches.fresh(entry.completedAt) {
				return entry.value, true
			}
		}
	case dashboardFastestComponent:
		if value, ok := dashboardFastestAliveCache.Load(dashboardProxyListCacheKey{WorkspaceID: key.workspaceID, Limit: key.limit}); ok {
			entry := value.(dashboardHealthCacheEntry[[]dto.ProxyFastestAlive])
			if entry.version == version && dashboardCaches.fresh(entry.completedAt) {
				return entry.value, true
			}
		}
	}
	return nil, false
}

func dashboardHealthVersionContext(ctx context.Context, workspaceID uint) (checkerHealthVersion, error) {
	if DB == nil {
		return checkerHealthVersion{}, nil
	}
	return currentCheckerHealthVersionWithDB(DB.WithContext(ctx), workspaceID)
}

func maskDashboardComponent(key dashboardCacheKey, value any) any {
	switch key.component {
	case dashboardInfoComponent:
		info := value.(dto.DashboardInfo)
		info.JudgeValidProxies = nil
		return info
	case dashboardRecentComponent:
		return []dto.ProxyRecentCheck{}
	default:
		return []dto.ProxyFastestAlive{}
	}
}

func getDashboardComponent(ctx context.Context, key dashboardCacheKey) (any, error) {
	ctx = dashboardDBContext(ctx)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dashboardCaches.touch(key)
	version, err := dashboardHealthVersionContext(ctx, key.workspaceID)
	if err != nil {
		return nil, err
	}
	if version.Dirty && key.component != dashboardInfoComponent {
		return maskDashboardComponent(key, nil), nil
	}
	if value, ok := cachedDashboardComponent(key, version); ok {
		return value, nil
	}
	value, err := dashboardCaches.run(ctx, key, func(loadCtx context.Context) (any, error) {
		// Another caller may have filled the cache between the initial miss
		// and acquiring this flight. Recheck without another database query.
		if current, ok := cachedDashboardComponent(key, version); ok {
			return current, nil
		}
		return loadDashboardComponent(loadCtx, key)
	})
	if err != nil {
		return nil, err
	}
	current, err := dashboardHealthVersionContext(ctx, key.workspaceID)
	if err != nil {
		return nil, err
	}
	if current.Dirty || current != version {
		return maskDashboardComponent(key, value), nil
	}
	return value, nil
}

// RefreshRequestedDashboardCaches refreshes only components recently read by
// an API caller. It never creates demand itself or enumerates all workspaces.
func RefreshRequestedDashboardCaches(ctx context.Context) error {
	ctx = dashboardDBContext(ctx)
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, key := range dashboardCaches.requestedKeys() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !dashboardCaches.requested(key) {
			continue
		}
		if _, err := refreshDashboardComponent(ctx, key); err != nil {
			return err
		}
	}
	return nil
}
