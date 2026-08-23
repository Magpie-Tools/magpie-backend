package blacklist

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"regexp"
	"sort"
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
)

var (
	cache       atomicMap
	rangeCache  atomicRangeCache
	refreshOnce singleflight.Group
	httpClient  = support.NewRestrictedOutboundHTTPClient(30 * time.Second)
	ipRegex     = regexp.MustCompile(`[0-9A-Fa-f:.]+(?:/[0-9]{1,3})?`)
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

type ipRange struct {
	start netip.Addr
	end   netip.Addr
}

type blacklistRangeCache struct {
	spans   []ipRange
	entries []domain.BlacklistedRange
}

type atomicRangeCache struct {
	val atomic.Value
}

func (a *atomicRangeCache) Load() blacklistRangeCache {
	raw, ok := a.val.Load().(blacklistRangeCache)
	if !ok {
		empty := blacklistRangeCache{}
		a.val.Store(empty)
		return empty
	}
	return raw
}

func (a *atomicRangeCache) Store(r blacklistRangeCache) {
	a.val.Store(r)
}

type RefreshOutcome struct {
	Sources          int
	TotalFromSources int
	NewIPs           int
	NewRanges        int
	TotalCachedIPs   int
	TotalRanges      int
	RelationsRemoved int64
	OrphanedProxies  []domain.Proxy
}

func init() {
	cache.Store(make(map[string]struct{}))
	rangeCache.Store(blacklistRangeCache{})
}

// Initialize hydrates the in-memory blacklist cache.
func Initialize(ctx context.Context) error {
	return LoadCache(ctx)
}

// LoadCache refreshes the in-memory blacklist IP set from the database.
func LoadCache(ctx context.Context) error {
	ips, err := database.ListBlacklistedIPs(ctx)
	if err != nil {
		return err
	}
	cache.Store(toSet(ips))
	ranges, err := database.ListBlacklistedRanges(ctx)
	if err != nil {
		return err
	}
	compiled, invalidCount := buildBlacklistRangeCache(ranges)
	if invalidCount > 0 {
		log.Warn("Blacklist range parse failures", "count", invalidCount)
	}
	rangeCache.Store(compiled)
	return nil
}

func toSet(ips []string) map[string]struct{} {
	m := make(map[string]struct{}, len(ips))
	for _, ip := range ips {
		if normalized := normalizeIP(ip); normalized != "" {
			m[normalized] = struct{}{}
		}
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

// FilterProxies separates allowed proxies from those using blacklisted IPs.
func FilterProxies(proxies []domain.Proxy) (allowed []domain.Proxy, blocked []domain.Proxy) {
	if len(proxies) == 0 {
		return nil, nil
	}

	set := cache.Load()
	ranges := rangeCache.Load().spans
	allowed = make([]domain.Proxy, 0, len(proxies))

	for _, proxy := range proxies {
		ip := normalizeIP(proxy.GetIp())
		if ip == "" {
			allowed = append(allowed, proxy)
			continue
		}
		if _, found := set[ip]; found {
			blocked = append(blocked, proxy)
			continue
		}
		if inRange(ip, ranges) {
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
		"cached_ranges", outcome.TotalRanges,
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
	beforeRanges := rangeCache.Load().entries

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

	var (
		totalFromSources int
		totalRanges      int
		allIPs           []domain.BlacklistedIP
		allRanges        []domain.BlacklistedRange
	)

	for _, src := range sources {
		ips, ranges, fetchErr := fetchBlacklist(ctx, src)
		if fetchErr != nil {
			if errors.Is(fetchErr, context.Canceled) {
				return nil, fetchErr
			}
			log.Warn("Blacklist fetch failed", "source", src, "error", fetchErr)
			continue
		}

		totalFromSources += len(ips)
		totalRanges += len(ranges)

		for _, ip := range ips {
			allIPs = append(allIPs, domain.BlacklistedIP{IP: ip, Source: src})
		}
		for _, r := range ranges {
			r.Source = src
			allRanges = append(allRanges, r)
		}
	}

	if _, _, err := database.ReplaceBlacklistData(ctx, allIPs, allRanges); err != nil {
		return nil, err
	}

	if err := LoadCache(ctx); err != nil {
		return nil, err
	}

	current := cache.Load()
	currentRanges := rangeCache.Load().entries
	newIPs := diffSets(current, before)
	newRanges := diffRanges(currentRanges, beforeRanges)

	var (
		removed int64
		orphans []domain.Proxy
	)

	if len(newIPs) > 0 || len(newRanges) > 0 || force {
		var err error
		removed, orphans, err = database.RemoveProxiesByIPs(ctx, newIPs)
		if err != nil {
			return nil, err
		}
		var rangeRemoved int64
		var rangeOrphans []domain.Proxy
		rangeRemoved, rangeOrphans, err = database.RemoveProxiesByRanges(ctx, newRanges)
		if err != nil {
			return nil, err
		}
		removed += rangeRemoved
		orphans = append(orphans, rangeOrphans...)
	}

	if err := broadcastRefreshUpdate(ctx, reason); err != nil {
		log.Warn("Blacklist refresh: failed to publish update", "reason", reason, "error", err)
	}

	return &RefreshOutcome{
		Sources:          len(sources),
		TotalFromSources: totalFromSources,
		NewIPs:           len(newIPs),
		NewRanges:        len(newRanges),
		TotalCachedIPs:   len(current),
		TotalRanges:      len(currentRanges),
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

func diffRanges(after, before []domain.BlacklistedRange) []domain.BlacklistedRange {
	if len(after) == 0 {
		return nil
	}

	beforeSet := make(map[string]struct{}, len(before))
	for _, r := range before {
		beforeSet[r.CIDR] = struct{}{}
	}

	added := make([]domain.BlacklistedRange, 0, len(after))
	for _, r := range after {
		if _, found := beforeSet[r.CIDR]; found {
			continue
		}
		added = append(added, r)
	}

	return added
}

func fetchBlacklist(ctx context.Context, source string) ([]string, []domain.BlacklistedRange, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	if config.IsWebsiteBlocked(source) {
		return nil, nil, fmt.Errorf("blacklist source blocked: %s", source)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, source, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("User-Agent", "magpie-blacklist-fetcher/1.0")
	req.Header.Set("Accept", "text/plain, */*;q=0.9")

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return nil, nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	limited := io.LimitReader(resp.Body, maxResponseBytes)
	content, err := io.ReadAll(limited)
	if err != nil {
		return nil, nil, fmt.Errorf("read response: %w", err)
	}

	ips, ranges := parseIPs(content)
	if len(ips) == 0 && len(ranges) == 0 {
		preview := string(bytes.TrimSpace(bytes.SplitN(content, []byte{'\n'}, 2)[0]))
		if preview == "" {
			preview = string(bytes.TrimSpace(content))
		}
		log.Warn("Blacklist source returned no entries",
			"source", source,
			"content_type", resp.Header.Get("Content-Type"),
			"preview", preview,
		)
	}
	return ips, ranges, nil
}

func parseIPs(payload []byte) ([]string, []domain.BlacklistedRange) {
	scanner := bufio.NewScanner(bytes.NewReader(payload))
	scanner.Buffer(make([]byte, 1024), 1024*1024)

	seen := make(map[string]struct{})
	var ranges []domain.BlacklistedRange

	for scanner.Scan() {
		line := scanner.Bytes()
		matches := ipRegex.FindAll(line, -1)
		for _, match := range matches {
			ipStr := string(match)
			cidrs, ips := parseCIDROrIP(ipStr)
			if len(cidrs) == 0 && len(ips) == 0 {
				trimmed := strings.TrimRight(ipStr, ".,;:")
				if trimmed != ipStr {
					cidrs, ips = parseCIDROrIP(trimmed)
				}
			}
			for _, ip := range ips {
				seen[ip] = struct{}{}
			}
			if len(cidrs) > 0 {
				ranges = append(ranges, cidrs...)
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
	return out, ranges
}

func normalizeIP(raw string) string {
	parsed, err := netip.ParseAddr(strings.TrimSpace(raw))
	if err != nil {
		return ""
	}
	return parsed.Unmap().String()
}

func parseCIDROrIP(raw string) ([]domain.BlacklistedRange, []string) {
	if !strings.Contains(raw, "/") {
		ip := normalizeIP(raw)
		if ip == "" {
			return nil, nil
		}
		return nil, []string{ip}
	}

	prefix, err := netip.ParsePrefix(raw)
	if err != nil || prefix.Addr().Is4In6() {
		return nil, nil
	}
	prefix = prefix.Masked()

	return []domain.BlacklistedRange{{
		CIDR: prefix.String(),
	}}, nil
}

func buildBlacklistRangeCache(ranges []domain.BlacklistedRange) (blacklistRangeCache, int) {
	entries := make([]domain.BlacklistedRange, 0, len(ranges))
	spans := make([]ipRange, 0, len(ranges))
	invalidCount := 0

	for _, entry := range ranges {
		prefix, err := netip.ParsePrefix(strings.TrimSpace(entry.CIDR))
		if err != nil || prefix.Addr().Is4In6() {
			invalidCount++
			continue
		}
		prefix = prefix.Masked()
		entry.CIDR = prefix.String()
		entries = append(entries, entry)
		spans = append(spans, ipRange{
			start: prefix.Addr(),
			end:   lastAddressInPrefix(prefix),
		})
	}

	sort.Slice(spans, func(i, j int) bool {
		if comparison := spans[i].start.Compare(spans[j].start); comparison != 0 {
			return comparison < 0
		}
		return spans[i].end.Compare(spans[j].end) > 0
	})

	merged := make([]ipRange, 0, len(spans))
	for _, span := range spans {
		if len(merged) == 0 {
			merged = append(merged, span)
			continue
		}

		last := &merged[len(merged)-1]
		if last.start.BitLen() == span.start.BitLen() && span.start.Compare(last.end) <= 0 {
			if span.end.Compare(last.end) > 0 {
				last.end = span.end
			}
			continue
		}
		merged = append(merged, span)
	}

	return blacklistRangeCache{spans: merged, entries: entries}, invalidCount
}

func lastAddressInPrefix(prefix netip.Prefix) netip.Addr {
	prefix = prefix.Masked()
	address := prefix.Addr()

	if address.Is4() {
		bytes := address.As4()
		for bit := prefix.Bits(); bit < 32; bit++ {
			bytes[bit/8] |= 1 << uint(7-bit%8)
		}
		return netip.AddrFrom4(bytes)
	}

	bytes := address.As16()
	for bit := prefix.Bits(); bit < 128; bit++ {
		bytes[bit/8] |= 1 << uint(7-bit%8)
	}
	return netip.AddrFrom16(bytes)
}

func inRange(ip string, ranges []ipRange) bool {
	if len(ranges) == 0 {
		return false
	}

	address, err := netip.ParseAddr(strings.TrimSpace(ip))
	if err != nil {
		return false
	}
	address = address.Unmap()

	index := sort.Search(len(ranges), func(i int) bool {
		return ranges[i].start.Compare(address) > 0
	})
	if index == 0 {
		return false
	}

	span := ranges[index-1]
	return span.start.BitLen() == address.BitLen() && address.Compare(span.end) <= 0
}
