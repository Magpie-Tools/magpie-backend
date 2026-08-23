package database

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"strings"

	"github.com/charmbracelet/log"

	"github.com/jackc/pgx/v5"

	"magpie/internal/domain"

	"gorm.io/gorm"
)

const (
	blacklistInsertBatchSize     = 500
	blacklistRangeQueryBatchSize = 100
)

// ListBlacklistedIPs returns all stored blacklist entries as normalized IP strings.
func ListBlacklistedIPs(ctx context.Context) ([]string, error) {
	if DB == nil {
		return nil, errors.New("database not initialised")
	}

	db := DB
	if ctx != nil {
		db = db.WithContext(ctx)
	}

	var ips []string
	if err := db.Model(&domain.BlacklistedIP{}).Pluck("ip", &ips).Error; err != nil {
		return nil, err
	}
	return ips, nil
}

// ListBlacklistedRanges returns all stored ranges in normalized CIDR form.
func ListBlacklistedRanges(ctx context.Context) ([]domain.BlacklistedRange, error) {
	if DB == nil {
		return nil, errors.New("database not initialised")
	}

	db := DB
	if ctx != nil {
		db = db.WithContext(ctx)
	}

	var rows []struct {
		ID     uint64 `gorm:"column:id"`
		CIDR   string `gorm:"column:cidr"`
		Source string `gorm:"column:source"`
	}
	if err := db.Raw("SELECT id, cidr::text AS cidr, source FROM blacklisted_ranges ORDER BY cidr ASC").Scan(&rows).Error; err != nil {
		return nil, err
	}
	ranges := make([]domain.BlacklistedRange, 0, len(rows))
	invalidCount := 0
	for _, row := range rows {
		cidr := strings.TrimSpace(row.CIDR)
		normalized, err := normalizeCIDR(cidr)
		if err != nil {
			invalidCount++
			continue
		}
		ranges = append(ranges, domain.BlacklistedRange{
			ID:     row.ID,
			CIDR:   normalized,
			Source: row.Source,
		})
	}
	if invalidCount > 0 {
		log.Warn("Blacklist range parse failures", "count", invalidCount)
	}

	return ranges, nil
}

// ReplaceBlacklistData truncates and bulk-loads blacklist entries using COPY (or a batch fallback).
func ReplaceBlacklistData(ctx context.Context, ips []domain.BlacklistedIP, ranges []domain.BlacklistedRange) (int, int, error) {
	if DB == nil {
		return 0, 0, errors.New("database not initialised")
	}

	cleanIPs := dedupeIPs(ips)
	cleanRanges := dedupeRanges(ranges)

	if len(cleanIPs) == 0 && len(cleanRanges) == 0 {
		// Still clear existing entries to align with "replace" semantics.
		db := DB
		if ctx != nil {
			db = db.WithContext(ctx)
		}
		if err := db.Exec("TRUNCATE TABLE blacklisted_ips, blacklisted_ranges RESTART IDENTITY").Error; err != nil {
			return 0, 0, err
		}
		return 0, 0, nil
	}

	if len(cleanRanges) == 0 {
		if dsn := getDSN(); dsn != "" {
			if ipCount, rangeCount, err := replaceBlacklistWithCopy(ctx, dsn, cleanIPs, cleanRanges); err == nil {
				return ipCount, rangeCount, nil
			}
		}
	}

	ipCount, rangeCount, err := replaceBlacklistWithBatches(ctx, cleanIPs, cleanRanges)
	return ipCount, rangeCount, err
}

func replaceBlacklistWithCopy(ctx context.Context, dsn string, ips []domain.BlacklistedIP, ranges []domain.BlacklistedRange) (int, int, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return 0, 0, err
	}
	defer conn.Close(ctx)

	tx, err := conn.Begin(ctx)
	if err != nil {
		return 0, 0, err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, "TRUNCATE TABLE blacklisted_ips, blacklisted_ranges RESTART IDENTITY"); err != nil {
		return 0, 0, err
	}

	if len(ips) > 0 {
		rows := make([][]any, len(ips))
		for i := range ips {
			rows[i] = []any{ips[i].IP, ips[i].Source}
		}
		if _, err := tx.CopyFrom(ctx, pgx.Identifier{"blacklisted_ips"}, []string{"ip", "source"}, pgx.CopyFromRows(rows)); err != nil {
			return 0, 0, err
		}
	}

	rangeInserted := 0
	if len(ranges) > 0 {
		rows := make([][]any, 0, len(ranges))
		emptyCIDR := 0
		for i := range ranges {
			cidr := strings.TrimSpace(ranges[i].CIDR)
			if cidr == "" {
				emptyCIDR++
				continue
			}
			rows = append(rows, []any{cidr, ranges[i].Source})
		}
		if emptyCIDR > 0 {
			log.Warn("Blacklist ranges missing CIDR", "count", emptyCIDR)
		}
		if len(rows) > 0 {
			if _, err := tx.CopyFrom(ctx, pgx.Identifier{"blacklisted_ranges"}, []string{"cidr", "source"}, pgx.CopyFromRows(rows)); err != nil {
				return 0, 0, err
			}
			rangeInserted = len(rows)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, 0, err
	}

	return len(ips), rangeInserted, nil
}

func replaceBlacklistWithBatches(ctx context.Context, ips []domain.BlacklistedIP, ranges []domain.BlacklistedRange) (int, int, error) {
	db := DB
	if ctx != nil {
		db = db.WithContext(ctx)
	}

	if err := db.Exec("TRUNCATE TABLE blacklisted_ips, blacklisted_ranges RESTART IDENTITY").Error; err != nil {
		return 0, 0, err
	}

	if len(ips) > 0 {
		if err := db.CreateInBatches(ips, blacklistInsertBatchSize).Error; err != nil {
			return 0, 0, err
		}
	}

	if len(ranges) > 0 {
		filtered := make([]domain.BlacklistedRange, 0, len(ranges))
		for _, r := range ranges {
			if r.CIDR == "" {
				continue
			}
			filtered = append(filtered, r)
		}
		if len(filtered) > 0 {
			if err := insertBlacklistRanges(db, filtered, blacklistInsertBatchSize); err != nil {
				return len(ips), 0, err
			}
			return len(ips), len(filtered), nil
		}
	}

	return len(ips), 0, nil
}

func insertBlacklistRanges(db *gorm.DB, ranges []domain.BlacklistedRange, batchSize int) error {
	if len(ranges) == 0 {
		return nil
	}
	if batchSize <= 0 {
		batchSize = blacklistInsertBatchSize
	}

	for i := 0; i < len(ranges); i += batchSize {
		end := i + batchSize
		if end > len(ranges) {
			end = len(ranges)
		}

		batch := ranges[i:end]
		placeholders := make([]string, 0, len(batch))
		args := make([]any, 0, len(batch)*2)

		for _, r := range batch {
			cidr := strings.TrimSpace(r.CIDR)
			if cidr == "" {
				continue
			}
			placeholders = append(placeholders, "(?::cidr, ?)")
			args = append(args, cidr, r.Source)
		}

		if len(placeholders) == 0 {
			continue
		}

		stmt := fmt.Sprintf(
			"INSERT INTO blacklisted_ranges (cidr, source) VALUES %s",
			strings.Join(placeholders, ","),
		)

		if err := db.Exec(stmt, args...).Error; err != nil {
			return err
		}
	}

	return nil
}

func dedupeIPs(ips []domain.BlacklistedIP) []domain.BlacklistedIP {
	if len(ips) == 0 {
		return nil
	}

	seen := make(map[string]domain.BlacklistedIP, len(ips))
	for _, ip := range ips {
		normalized := normalizeIP(ip.IP)
		if normalized == "" {
			continue
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		ip.IP = normalized
		seen[normalized] = ip
	}

	out := make([]domain.BlacklistedIP, 0, len(seen))
	for _, ip := range seen {
		out = append(out, ip)
	}
	return out
}

func dedupeRanges(ranges []domain.BlacklistedRange) []domain.BlacklistedRange {
	if len(ranges) == 0 {
		return nil
	}

	seenCIDR := make(map[string]string, len(ranges))
	for _, r := range ranges {
		cidr, err := normalizeCIDR(r.CIDR)
		if err != nil {
			continue
		}
		if _, ok := seenCIDR[cidr]; ok {
			continue
		}
		seenCIDR[cidr] = r.Source
	}

	if len(seenCIDR) == 0 {
		return nil
	}

	result := make([]domain.BlacklistedRange, 0, len(seenCIDR))
	for cidr, source := range seenCIDR {
		result = append(result, domain.BlacklistedRange{
			CIDR:   cidr,
			Source: source,
		})
	}

	return result
}

// RemoveProxiesByIPs removes proxy/user associations for proxies whose IP is in the given list.
// It returns the number of user-proxy relations removed and any orphaned proxies that can be purged from queues.
func RemoveProxiesByIPs(ctx context.Context, ips []string) (int64, []domain.Proxy, error) {
	if DB == nil {
		return 0, nil, errors.New("database not initialised")
	}

	normalized := normalizeIPList(ips)
	if len(normalized) == 0 {
		return 0, nil, nil
	}

	db := DB
	if ctx != nil {
		db = db.WithContext(ctx)
	}

	var proxies []domain.Proxy
	if err := db.Preload("Users").
		Where("ip_address IN ?", normalized).
		Find(&proxies).Error; err != nil {
		return 0, nil, err
	}

	if len(proxies) == 0 {
		return 0, nil, nil
	}

	perUser := make(map[uint][]int)
	orphanSet := make(map[uint64]domain.Proxy)
	for _, proxy := range proxies {
		if len(proxy.Users) == 0 {
			orphanSet[proxy.ID] = proxy
			continue
		}
		for _, user := range proxy.Users {
			perUser[user.ID] = append(perUser[user.ID], int(proxy.ID))
		}
	}

	var (
		totalRemoved int64
	)

	for userID, proxyIDs := range perUser {
		removed, orphans, err := DeleteProxyRelation(userID, proxyIDs)
		if err != nil {
			return totalRemoved, nil, err
		}
		totalRemoved += removed
		for _, orphan := range orphans {
			orphanSet[orphan.ID] = orphan
		}
	}

	orphaned := make([]domain.Proxy, 0, len(orphanSet))
	for _, proxy := range orphanSet {
		orphaned = append(orphaned, proxy)
	}

	return totalRemoved, orphaned, nil
}

func normalizeCIDR(raw string) (string, error) {
	prefix, err := netip.ParsePrefix(strings.TrimSpace(raw))
	if err != nil {
		return "", err
	}
	if prefix.Addr().Is4In6() {
		return "", fmt.Errorf("IPv4-mapped IPv6 CIDR is not supported: %s", raw)
	}
	prefix = prefix.Masked()
	return prefix.String(), nil
}

func normalizeIPList(ips []string) []string {
	if len(ips) == 0 {
		return nil
	}

	seen := make(map[string]struct{}, len(ips))
	out := make([]string, 0, len(ips))

	for _, raw := range ips {
		ip := normalizeIP(raw)
		if ip == "" {
			continue
		}
		if _, ok := seen[ip]; ok {
			continue
		}
		seen[ip] = struct{}{}
		out = append(out, ip)
	}

	return out
}

func normalizeIP(raw string) string {
	parsed, err := netip.ParseAddr(strings.TrimSpace(raw))
	if err != nil {
		return ""
	}
	return parsed.Unmap().String()
}

// RemoveProxiesByRanges removes proxies whose IP falls inside any of the provided ranges.
func RemoveProxiesByRanges(ctx context.Context, ranges []domain.BlacklistedRange) (int64, []domain.Proxy, error) {
	if DB == nil {
		return 0, nil, errors.New("database not initialised")
	}
	if len(ranges) == 0 {
		return 0, nil, nil
	}

	db := DB
	if ctx != nil {
		db = db.WithContext(ctx)
	}

	prefixes := make([]netip.Prefix, 0, len(ranges))
	seenPrefixes := make(map[string]struct{}, len(ranges))
	for _, r := range ranges {
		normalized, err := normalizeCIDR(r.CIDR)
		if err != nil {
			continue
		}
		if _, exists := seenPrefixes[normalized]; exists {
			continue
		}
		seenPrefixes[normalized] = struct{}{}
		prefix, _ := netip.ParsePrefix(normalized)
		prefixes = append(prefixes, prefix)
	}
	if len(prefixes) == 0 {
		return 0, nil, nil
	}

	proxies, err := findProxiesInIPRanges(db, prefixes)
	if err != nil {
		return 0, nil, err
	}

	if len(proxies) == 0 {
		return 0, nil, nil
	}

	perUser := make(map[uint][]int)
	orphanSet := make(map[uint64]domain.Proxy)
	for _, proxy := range proxies {
		if len(proxy.Users) == 0 {
			orphanSet[proxy.ID] = proxy
			continue
		}
		for _, user := range proxy.Users {
			perUser[user.ID] = append(perUser[user.ID], int(proxy.ID))
		}
	}

	var totalRemoved int64
	for userID, proxyIDs := range perUser {
		removed, orphans, err := DeleteProxyRelation(userID, proxyIDs)
		if err != nil {
			return totalRemoved, nil, err
		}
		totalRemoved += removed
		for _, orphan := range orphans {
			orphanSet[orphan.ID] = orphan
		}
	}

	orphaned := make([]domain.Proxy, 0, len(orphanSet))
	for _, proxy := range orphanSet {
		orphaned = append(orphaned, proxy)
	}

	return totalRemoved, orphaned, nil
}

func findProxiesInIPRanges(db *gorm.DB, prefixes []netip.Prefix) ([]domain.Proxy, error) {
	if isPostgresDialect(db) {
		proxyByID := make(map[uint64]domain.Proxy)
		for start := 0; start < len(prefixes); start += blacklistRangeQueryBatchSize {
			end := start + blacklistRangeQueryBatchSize
			if end > len(prefixes) {
				end = len(prefixes)
			}

			conditions := make([]string, 0, end-start)
			args := make([]any, 0, end-start)
			for _, prefix := range prefixes[start:end] {
				conditions = append(conditions, "ip_address <<= ?::cidr")
				args = append(args, prefix.String())
			}

			var batch []domain.Proxy
			if err := db.Preload("Users").
				Where("("+strings.Join(conditions, " OR ")+")", args...).
				Find(&batch).Error; err != nil {
				return nil, err
			}
			for _, proxy := range batch {
				proxyByID[proxy.ID] = proxy
			}
		}

		proxies := make([]domain.Proxy, 0, len(proxyByID))
		for _, proxy := range proxyByID {
			proxies = append(proxies, proxy)
		}
		return proxies, nil
	}

	var candidates []domain.Proxy
	if err := db.Preload("Users").Find(&candidates).Error; err != nil {
		return nil, err
	}

	proxies := make([]domain.Proxy, 0)
	for _, proxy := range candidates {
		address, err := netip.ParseAddr(proxy.GetIp())
		if err != nil {
			continue
		}
		address = address.Unmap()
		for _, prefix := range prefixes {
			if prefix.Contains(address) {
				proxies = append(proxies, proxy)
				break
			}
		}
	}
	return proxies, nil
}
