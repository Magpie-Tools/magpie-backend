package blacklist

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/charmbracelet/log"
	"golang.org/x/sync/singleflight"

	"magpie/internal/config"
	"magpie/internal/database"
	"magpie/internal/domain"
	proxyqueue "magpie/internal/jobs/queue/proxy"
	"magpie/internal/support"
)

const (
	maxResponseBytes       = 10 << 20 // 10 MiB safety cap
	refreshLockKey         = "magpie:leader:blacklist_refresh"
	defaultRefreshInterval = 6 * time.Hour
	maxCIDRAddresses       = 1 << 16 // guardrail to prevent runaway expansion
)

var (
	cache       atomicMap
	refreshOnce singleflight.Group
	httpClient  = &http.Client{Timeout: 30 * time.Second}
	ipRegex     = regexp.MustCompile(`\b\d{1,3}(?:\.\d{1,3}){3}(?:/\d{1,2})?\b`)
)

type atomicMap struct {
	val atomic.Value
}

func (a *atomicMap) Load() map[string]struct{} {
	raw, ok := a.val.Load().(map[string]struct{})
	if !ok || raw == nil {
		empty := make(map[string]struct{})
		a.val.Store(empty)
		return empty
	}
	return raw
}

func (a *atomicMap) Store(m map[string]struct{}) {
	a.val.Store(m)
}

type RefreshOutcome struct {
	Sources          int
	TotalFromSources int
	NewIPs           int
	TotalCachedIPs   int
	RelationsRemoved int64
	OrphanedProxies  []domain.Proxy
}

func init() {
	cache.Store(make(map[string]struct{}))
}

// Initialize hydrates the in-memory blacklist cache and backfills missing proxy hashes.
func Initialize(ctx context.Context) error {
	if updated, err := database.BackfillProxyIPHashes(ctx); err != nil {
		return fmt.Errorf("backfill proxy ip hashes: %w", err)
	} else if updated > 0 {
		log.Info("Backfilled proxy IP hashes", "count", updated)
	}
	return LoadCache(ctx)
}

// LoadCache refreshes the in-memory blacklist IP set from the database.
func LoadCache(ctx context.Context) error {
	ips, err := database.ListBlacklistedIPs(ctx)
	if err != nil {
		return err
	}
	cache.Store(toSet(ips))
	return nil
}

func toSet(ips []string) map[string]struct{} {
	m := make(map[string]struct{}, len(ips))
	for _, ip := range ips {
		m[ip] = struct{}{}
	}
	return m
}

func cloneSet(m map[string]struct{}) map[string]struct{} {
	cp := make(map[string]struct{}, len(m))
	for k := range m {
		cp[k] = struct{}{}
	}
	return cp
}

// IsIPBlacklisted checks the in-memory cache for the given IP.
func IsIPBlacklisted(ip string) bool {
	normalized := normalizeIPv4(ip)
	if normalized == "" {
		return false
	}
	_, found := cache.Load()[normalized]
	return found
}

// FilterProxies separates allowed proxies from those using blacklisted IPs.
func FilterProxies(proxies []domain.Proxy) (allowed []domain.Proxy, blocked []domain.Proxy) {
	if len(proxies) == 0 {
		return nil, nil
	}

	set := cache.Load()
	allowed = make([]domain.Proxy, 0, len(proxies))

	for _, proxy := range proxies {
		ip := normalizeIPv4(proxy.GetIp())
		if ip == "" {
			continue
		}
		if _, found := set[ip]; found {
			blocked = append(blocked, proxy)
			continue
		}
		allowed = append(allowed, proxy)
	}

	return allowed, blocked
}

// StartRefreshRoutine runs the blacklist refresh loop with dynamic rescheduling.
func StartRefreshRoutine(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}

	var intervalValue atomic.Value
	initial := config.GetBlacklistRefreshInterval()
	if initial <= 0 {
		initial = defaultRefreshInterval
	}
	intervalValue.Store(initial)

	updateSignal := make(chan struct{}, 1)
	updates := config.BlacklistIntervalUpdates()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case newInterval := <-updates:
				if newInterval <= 0 {
					newInterval = defaultRefreshInterval
				}
				intervalValue.Store(newInterval)
				select {
				case updateSignal <- struct{}{}:
				default:
				}
			}
		}
	}()

	err := support.RunWithLeader(ctx, refreshLockKey, support.DefaultLeadershipTTL, func(leaderCtx context.Context) {
		runRefreshLoop(leaderCtx, &intervalValue, updateSignal)
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Error("Blacklist refresh routine stopped", "error", err)
	}
}

// RunRefresh triggers a refresh immediately (outside of the scheduled loop).
func RunRefresh(ctx context.Context, reason string, force bool) {
	triggerRefresh(ctx, reason, force)
}

func runRefreshLoop(ctx context.Context, intervalValue *atomic.Value, updateSignal <-chan struct{}) {
	current := intervalValue.Load().(time.Duration)
	if current <= 0 {
		current = defaultRefreshInterval
	}

	ticker := time.NewTicker(current)
	defer ticker.Stop()

	triggerRefresh(ctx, "startup", true)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			triggerRefresh(ctx, "scheduled", false)
		case <-updateSignal:
			newInterval := intervalValue.Load().(time.Duration)
			if newInterval <= 0 {
				newInterval = defaultRefreshInterval
			}
			if newInterval == current {
				continue
			}
			drainTicker(ticker)
			current = newInterval
			ticker.Reset(current)
		}
	}
}

func triggerRefresh(ctx context.Context, reason string, force bool) {
	outcome, err := Refresh(ctx, reason, force)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			log.Info("Blacklist refresh canceled", "reason", reason)
		} else {
			log.Error("Blacklist refresh failed", "reason", reason, "error", err)
		}
		return
	}
	if outcome == nil {
		return
	}

	if len(outcome.OrphanedProxies) > 0 {
		if err := proxyqueue.PublicProxyQueue.RemoveFromQueue(outcome.OrphanedProxies); err != nil {
			log.Warn("Failed to purge blacklisted proxies from queue", "error", err)
		}
	}

	log.Info("Blacklist refresh completed",
		"reason", reason,
		"sources", outcome.Sources,
		"new_ips", outcome.NewIPs,
		"cached_ips", outcome.TotalCachedIPs,
		"relations_removed", outcome.RelationsRemoved,
	)
}

func drainTicker(ticker *time.Ticker) {
	for {
		select {
		case <-ticker.C:
		default:
			return
		}
	}
}

// Refresh downloads all configured blacklist sources, persists the IPs, refreshes the cache,
// and removes any newly blacklisted proxies from user inventories.
func Refresh(ctx context.Context, reason string, force bool) (*RefreshOutcome, error) {
	result, err, _ := refreshOnce.Do("refresh", func() (interface{}, error) {
		return doRefresh(ctx, reason, force)
	})
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, nil
	}
	outcome, _ := result.(*RefreshOutcome)
	return outcome, nil
}

func doRefresh(ctx context.Context, reason string, force bool) (*RefreshOutcome, error) {
	cfg := config.GetConfig()
	sources := append([]string(nil), cfg.BlacklistSources...)

	before := cloneSet(cache.Load())

	if len(sources) == 0 {
		if err := LoadCache(ctx); err != nil {
			return nil, err
		}
		return &RefreshOutcome{
			Sources:        0,
			NewIPs:         0,
			TotalCachedIPs: len(cache.Load()),
		}, nil
	}

	var totalFromSources int

	for _, src := range sources {
		ips, fetchErr := fetchBlacklist(ctx, src)
		if fetchErr != nil {
			if errors.Is(fetchErr, context.Canceled) {
				return nil, fetchErr
			}
			log.Warn("Blacklist fetch failed", "source", src, "error", fetchErr)
			continue
		}

		totalFromSources += len(ips)

		if _, err := database.UpsertBlacklistedIPs(ctx, src, ips); err != nil {
			log.Error("Persisting blacklist entries failed", "source", src, "error", err)
		}
	}

	if err := LoadCache(ctx); err != nil {
		return nil, err
	}

	current := cache.Load()
	newIPs := diffSets(current, before)

	var (
		removed int64
		orphans []domain.Proxy
	)

	if len(newIPs) > 0 || force {
		var err error
		removed, orphans, err = database.RemoveProxiesByIPs(ctx, newIPs)
		if err != nil {
			return nil, err
		}
	}

	return &RefreshOutcome{
		Sources:          len(sources),
		TotalFromSources: totalFromSources,
		NewIPs:           len(newIPs),
		TotalCachedIPs:   len(current),
		RelationsRemoved: removed,
		OrphanedProxies:  orphans,
	}, nil
}

func diffSets(after, before map[string]struct{}) []string {
	if len(after) == 0 {
		return nil
	}
	added := make([]string, 0, len(after))
	for ip := range after {
		if _, found := before[ip]; found {
			continue
		}
		added = append(added, ip)
	}
	return added
}

func fetchBlacklist(ctx context.Context, source string) ([]string, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, source, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	limited := io.LimitReader(resp.Body, maxResponseBytes)
	content, err := io.ReadAll(limited)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	return parseIPs(content), nil
}

func parseIPs(payload []byte) []string {
	scanner := bufio.NewScanner(bytes.NewReader(payload))
	scanner.Buffer(make([]byte, 1024), 1024*1024)

	seen := make(map[string]struct{})

	for scanner.Scan() {
		line := scanner.Bytes()
		matches := ipRegex.FindAll(line, -1)
		for _, match := range matches {
			candidates := expandCIDROrIP(string(match))
			for _, ip := range candidates {
				seen[ip] = struct{}{}
			}
		}
	}

	if err := scanner.Err(); err != nil {
		log.Warn("Blacklist scanner warning", "error", err)
	}

	out := make([]string, 0, len(seen))
	for ip := range seen {
		out = append(out, ip)
	}
	return out
}

func normalizeIPv4(raw string) string {
	parsed := net.ParseIP(raw)
	if parsed == nil {
		return ""
	}
	v4 := parsed.To4()
	if v4 == nil {
		return ""
	}
	return v4.String()
}

func expandCIDROrIP(raw string) []string {
	if !strings.Contains(raw, "/") {
		ip := normalizeIPv4(raw)
		if ip == "" {
			return nil
		}
		return []string{ip}
	}

	_, ipnet, err := net.ParseCIDR(raw)
	if err != nil || ipnet == nil {
		return nil
	}

	base := ipnet.IP.To4()
	if base == nil {
		return nil
	}

	ones, bits := ipnet.Mask.Size()
	if bits != 32 || ones < 0 || ones > 32 {
		return nil
	}

	hostCount := 1 << (bits - ones)
	if hostCount > maxCIDRAddresses {
		log.Warn("Skipping CIDR expansion: too many addresses", "cidr", raw, "count", hostCount)
		return nil
	}

	baseInt := ipToUint32(base)
	ips := make([]string, 0, hostCount)
	for i := 0; i < hostCount; i++ {
		ip := uint32ToIP(baseInt + uint32(i))
		if ipnet.Contains(ip) {
			ips = append(ips, ip.String())
		}
	}
	return ips
}

func ipToUint32(ip net.IP) uint32 {
	ip = ip.To4()
	if ip == nil {
		return 0
	}
	return uint32(ip[0])<<24 | uint32(ip[1])<<16 | uint32(ip[2])<<8 | uint32(ip[3])
}

func uint32ToIP(i uint32) net.IP {
	return net.IPv4(byte(i>>24), byte(i>>16), byte(i>>8), byte(i))
}
