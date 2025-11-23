package config

import (
	"net/url"
	"strings"
	"sync/atomic"
)

// websiteBlocklistSet holds normalized hostnames that should never be contacted.
var websiteBlocklistSet atomic.Value

func init() {
	websiteBlocklistSet.Store(make(map[string]struct{}))
}

// NormalizeWebsiteBlacklist trims, lowercases, and deduplicates host entries.
func NormalizeWebsiteBlacklist(entries []string) []string {
	return normalizeWebsiteEntries(entries)
}

// NewWebsiteBlocklistSet builds a lookup set from the provided entries.
func NewWebsiteBlocklistSet(entries []string) map[string]struct{} {
	return buildWebsiteBlocklist(normalizeWebsiteEntries(entries))
}

// updateWebsiteBlocklist refreshes the in-memory set from the persisted config.
func updateWebsiteBlocklist(entries []string) {
	normalized := normalizeWebsiteEntries(entries)
	websiteBlocklistSet.Store(buildWebsiteBlocklist(normalized))
}

// IsWebsiteBlocked reports whether the given URL or hostname matches the configured blacklist.
func IsWebsiteBlocked(rawURL string) bool {
	return isWebsiteBlocked(rawURL, websiteBlocklistSet.Load().(map[string]struct{}))
}

// FindBlockedURLs returns the subset of urls whose host is present in the given blocklist set.
func FindBlockedURLs(urls []string, blockedSet map[string]struct{}) []string {
	if len(urls) == 0 || len(blockedSet) == 0 {
		return nil
	}

	var blocked []string
	for _, raw := range urls {
		if isWebsiteBlocked(raw, blockedSet) {
			blocked = append(blocked, raw)
		}
	}
	return blocked
}

func isWebsiteBlocked(rawURL string, blockedSet map[string]struct{}) bool {
	if len(blockedSet) == 0 {
		return false
	}

	host := normalizeHostname(rawURL)
	if host == "" {
		return false
	}

	return isHostBlocked(host, blockedSet)
}

func buildWebsiteBlocklist(entries []string) map[string]struct{} {
	set := make(map[string]struct{}, len(entries))
	for _, host := range entries {
		if host == "" {
			continue
		}
		set[host] = struct{}{}
	}
	return set
}

func normalizeWebsiteEntries(entries []string) []string {
	unique := make(map[string]struct{}, len(entries))
	normalized := make([]string, 0, len(entries))

	for _, raw := range entries {
		host := normalizeHostname(raw)
		if host == "" {
			continue
		}
		if _, exists := unique[host]; exists {
			continue
		}
		unique[host] = struct{}{}
		normalized = append(normalized, host)
	}

	return normalized
}

func normalizeHostname(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}

	// Allow bare hostnames by prefixing a scheme for URL parsing.
	if !strings.Contains(trimmed, "://") {
		trimmed = "https://" + trimmed
	}

	parsed, err := url.Parse(trimmed)
	if err != nil {
		return ""
	}

	host := strings.ToLower(parsed.Hostname())
	return strings.Trim(host, ".")
}

func isHostBlocked(host string, blockedSet map[string]struct{}) bool {
	if host == "" || len(blockedSet) == 0 {
		return false
	}

	if _, ok := blockedSet[host]; ok {
		return true
	}

	for blocked := range blockedSet {
		if blocked == "" {
			continue
		}
		if strings.HasSuffix(host, "."+blocked) {
			return true
		}
	}

	return false
}
