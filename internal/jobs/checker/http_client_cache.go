package checker

import (
	"container/list"
	"math/bits"
	"net/http"
	"strings"
	"sync"
	"time"

	"magpie/internal/config"
	"magpie/internal/domain"
	"magpie/internal/support"
)

const (
	checkerHTTPClientCacheTTL            = 5 * time.Minute
	checkerHTTPClientCacheMinEntries     = 2048
	checkerHTTPClientCacheMaxCap         = 16384
	checkerHTTPClientCacheDefaultEntries = 12288
)

type checkerHTTPClientCacheKey struct {
	proxyAddr         string
	proxyUsername     string
	proxyPassword     string
	judgeURL          string
	judgeHostname     string
	judgeIP           string
	protocol          string
	transportProtocol string
	timeout           uint32
}

type cachedCheckerHTTPClient struct {
	client   *http.Client
	closeFn  func()
	lastUsed time.Time
	position *list.Element
}

var (
	checkerHTTPClientCacheMu sync.Mutex
	checkerHTTPClientCache   = make(map[checkerHTTPClientCacheKey]*cachedCheckerHTTPClient)
	checkerHTTPClientLRU     = list.New()

	checkerTransportFactory = support.CreateTransport
)

func getCheckerHTTPClient(proxyToCheck domain.Proxy, judge *domain.Judge, protocol string, transportProtocol string, timeouts ...uint16) (*http.Client, error) {
	if transportProtocol == "" {
		transportProtocol = support.TransportTCP
	}

	timeout := config.GetConfig().Checker.Timeout
	if len(timeouts) > 0 {
		timeout = uint32(timeouts[0])
	}
	key := checkerHTTPClientCacheKey{
		timeout:           timeout,
		proxyAddr:         proxyToCheck.GetFullProxy(),
		proxyUsername:     proxyToCheck.Username,
		proxyPassword:     proxyToCheck.Password,
		judgeURL:          judge.FullString,
		judgeHostname:     judge.GetHostname(),
		judgeIP:           judge.GetIp(),
		protocol:          strings.ToLower(strings.TrimSpace(protocol)),
		transportProtocol: support.NormalizeTransportProtocol(transportProtocol),
	}

	now := time.Now()

	checkerHTTPClientCacheMu.Lock()
	now = time.Now()
	if entry, ok := checkerHTTPClientCache[key]; ok && now.Sub(entry.lastUsed) <= checkerHTTPClientCacheTTL {
		entry.lastUsed = now
		checkerHTTPClientLRU.MoveToBack(entry.position)
		client := entry.client
		checkerHTTPClientCacheMu.Unlock()
		return client, nil
	}
	checkerHTTPClientCacheMu.Unlock()

	transport, closeFn, err := checkerTransportFactory(proxyToCheck, judge, protocol, transportProtocol, timeouts...)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Transport: transport}

	checkerHTTPClientCacheMu.Lock()
	now = time.Now()
	if existing, ok := checkerHTTPClientCache[key]; ok && now.Sub(existing.lastUsed) <= checkerHTTPClientCacheTTL {
		existing.lastUsed = now
		checkerHTTPClientLRU.MoveToBack(existing.position)
		client = existing.client
		checkerHTTPClientCacheMu.Unlock()
		if closeFn != nil {
			closeFn()
		}
		return client, nil
	}
	var closeFns []func()
	if existing := checkerHTTPClientCache[key]; existing != nil {
		closeFns = append(closeFns, removeCheckerHTTPClientLocked(key))
	}
	closeFns = append(closeFns, runCheckerHTTPClientCacheMaintenanceLocked(now)...)
	checkerHTTPClientCache[key] = &cachedCheckerHTTPClient{
		client: client, closeFn: closeFn, lastUsed: now,
		position: checkerHTTPClientLRU.PushBack(key),
	}
	checkerHTTPClientCacheMu.Unlock()
	closeCheckerClients(closeFns)

	return client, nil
}

func removeCheckerHTTPClientLocked(key checkerHTTPClientCacheKey) func() {
	entry := checkerHTTPClientCache[key]
	delete(checkerHTTPClientCache, key)
	if entry == nil {
		return nil
	}
	checkerHTTPClientLRU.Remove(entry.position)
	return entry.closeFn
}

func runCheckerHTTPClientCacheMaintenanceLocked(now time.Time) []func() {
	var closeFns []func()
	// Expiration follows LRU order. Limit cleanup so one insertion never
	// scans the entire cache while other checker workers wait for the lock.
	for removed := 0; removed < 16; removed++ {
		key, ok := oldestCheckerHTTPClientCacheKeyLocked()
		if !ok || now.Sub(checkerHTTPClientCache[key].lastUsed) <= checkerHTTPClientCacheTTL {
			break
		}
		closeFns = append(closeFns, removeCheckerHTTPClientLocked(key))
	}
	maxEntries := checkerHTTPClientCacheMaxEntries()
	for len(checkerHTTPClientCache) >= maxEntries {
		key, ok := oldestCheckerHTTPClientCacheKeyLocked()
		if !ok {
			break
		}
		closeFns = append(closeFns, removeCheckerHTTPClientLocked(key))
	}
	return closeFns
}

func checkerHTTPClientCacheMaxEntries() int {
	threads := currentThreads.Load()
	if threads == 0 {
		threads = config.GetConfig().Checker.Threads
	}
	if threads == 0 {
		return checkerHTTPClientCacheDefaultEntries
	}
	return checkerHTTPClientCacheEntriesForThreads(threads)
}

func checkerHTTPClientCacheEntriesForThreads(threads uint32) int {
	target := nextPow2(uint64(threads) * 3)
	if target < checkerHTTPClientCacheMinEntries {
		target = checkerHTTPClientCacheMinEntries
	}
	if target > checkerHTTPClientCacheMaxCap {
		target = checkerHTTPClientCacheMaxCap
	}
	return int(target)
}

func nextPow2(value uint64) uint64 {
	if value <= 1 {
		return 1
	}
	return uint64(1) << bits.Len64(value-1)
}

func oldestCheckerHTTPClientCacheKeyLocked() (checkerHTTPClientCacheKey, bool) {
	oldest := checkerHTTPClientLRU.Front()
	if oldest == nil {
		return checkerHTTPClientCacheKey{}, false
	}
	return oldest.Value.(checkerHTTPClientCacheKey), true
}

func closeCheckerClients(closeFns []func()) {
	for _, closeFn := range closeFns {
		if closeFn != nil {
			closeFn()
		}
	}
}

func resetCheckerHTTPClientCacheForTests() {
	checkerHTTPClientCacheMu.Lock()
	closeFns := make([]func(), 0, len(checkerHTTPClientCache))
	for _, entry := range checkerHTTPClientCache {
		if entry != nil && entry.closeFn != nil {
			closeFns = append(closeFns, entry.closeFn)
		}
	}
	checkerHTTPClientCache = make(map[checkerHTTPClientCacheKey]*cachedCheckerHTTPClient)
	checkerHTTPClientLRU.Init()
	checkerHTTPClientCacheMu.Unlock()

	closeCheckerClients(closeFns)
}
