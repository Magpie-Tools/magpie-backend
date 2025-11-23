package database

import (
	"context"
	"crypto/sha256"
	"errors"
	"net"
	"sort"
	"time"

	"magpie/internal/domain"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	blacklistInsertBatchSize = 500
)

// ListBlacklistedIPs returns all stored blacklist entries as plain IPv4 strings.
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

// ListBlacklistedRanges returns all stored ranges ordered by start IP.
func ListBlacklistedRanges(ctx context.Context) ([]domain.BlacklistedRange, error) {
	if DB == nil {
		return nil, errors.New("database not initialised")
	}

	db := DB
	if ctx != nil {
		db = db.WithContext(ctx)
	}

	var ranges []domain.BlacklistedRange
	if err := db.Order("start_ip ASC").Find(&ranges).Error; err != nil {
		return nil, err
	}
	return ranges, nil
}

// UpsertBlacklistedIPs stores (or refreshes) the provided IPs for the given source.
// IPs are normalised and deduplicated before being persisted.
func UpsertBlacklistedIPs(ctx context.Context, source string, ips []string) (int, error) {
	if DB == nil {
		return 0, errors.New("database not initialised")
	}

	normalized := normalizeIPList(ips)
	if len(normalized) == 0 {
		return 0, nil
	}

	now := time.Now().UTC()
	records := make([]domain.BlacklistedIP, 0, len(normalized))
	for _, ip := range normalized {
		records = append(records, domain.BlacklistedIP{
			IP:         ip,
			Source:     source,
			LastSeenAt: now,
		})
	}

	db := DB
	if ctx != nil {
		db = db.WithContext(ctx)
	}

	err := db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "ip"}},
		DoUpdates: clause.Assignments(map[string]any{
			"source":       gorm.Expr("EXCLUDED.source"),
			"last_seen_at": gorm.Expr("EXCLUDED.last_seen_at"),
		}),
	}).CreateInBatches(&records, blacklistInsertBatchSize).Error
	if err != nil {
		return 0, err
	}

	return len(normalized), nil
}

// UpsertBlacklistedRanges stores or refreshes start/end pairs for a source.
func UpsertBlacklistedRanges(ctx context.Context, source string, ranges []domain.BlacklistedRange) (int, error) {
	if DB == nil {
		return 0, errors.New("database not initialised")
	}
	if len(ranges) == 0 {
		return 0, nil
	}

	now := time.Now().UTC()
	for i := range ranges {
		ranges[i].Source = source
		ranges[i].LastSeenAt = now
		if ranges[i].FirstSeenAt.IsZero() {
			ranges[i].FirstSeenAt = now
		}
		if ranges[i].StartIP > ranges[i].EndIP {
			ranges[i].StartIP, ranges[i].EndIP = ranges[i].EndIP, ranges[i].StartIP
		}
	}

	db := DB
	if ctx != nil {
		db = db.WithContext(ctx)
	}

	err := db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "start_ip"}, {Name: "end_ip"}},
		DoUpdates: clause.Assignments(map[string]any{
			"source":       gorm.Expr("EXCLUDED.source"),
			"last_seen_at": gorm.Expr("EXCLUDED.last_seen_at"),
		}),
	}).CreateInBatches(&ranges, blacklistInsertBatchSize).Error
	if err != nil {
		return 0, err
	}
	return len(ranges), nil
}

// BackfillProxyIPHashes fills missing proxy IP hashes for legacy records.
func BackfillProxyIPHashes(ctx context.Context) (int64, error) {
	if DB == nil {
		return 0, errors.New("database not initialised")
	}

	db := DB
	if ctx != nil {
		db = db.WithContext(ctx)
	}

	var (
		updated int64
		batch   []domain.Proxy
	)

	result := db.
		Where("COALESCE(octet_length(ip_hash), 0) = 0").
		FindInBatches(&batch, maxParamsPerBatch, func(tx *gorm.DB, _ int) error {
			if len(batch) == 0 {
				return nil
			}

			for i := range batch {
				// AfterFind already decrypted IP; ensure hash exists for persistence.
				batch[i].GenerateHash()
			}

			for i := range batch {
				if len(batch[i].IPHash) == 0 {
					continue
				}
				if err := tx.Model(&domain.Proxy{}).
					Where("id = ?", batch[i].ID).
					Update("ip_hash", batch[i].IPHash).Error; err != nil {
					return err
				}
				updated++
			}

			// Clear slice for next batch
			batch = batch[:0]
			return nil
		})

	if result.Error != nil {
		return updated, result.Error
	}

	return updated, nil
}

// BackfillProxyIPMetadata fills missing IP hashes and IP ints for legacy rows.
func BackfillProxyIPMetadata(ctx context.Context) (int64, int64, error) {
	if DB == nil {
		return 0, 0, errors.New("database not initialised")
	}

	db := DB
	if ctx != nil {
		db = db.WithContext(ctx)
	}

	var (
		hashUpdated int64
		intUpdated  int64
		batch       []domain.Proxy
	)

	result := db.
		Where("COALESCE(octet_length(ip_hash), 0) = 0 OR ip_int = 0").
		FindInBatches(&batch, maxParamsPerBatch, func(tx *gorm.DB, _ int) error {
			if len(batch) == 0 {
				return nil
			}

			for i := range batch {
				batch[i].GenerateHash()
			}

			for i := range batch {
				if len(batch[i].IPHash) > 0 {
					if err := tx.Model(&domain.Proxy{}).
						Where("id = ?", batch[i].ID).
						Updates(map[string]any{
							"ip_hash": batch[i].IPHash,
							"ip_int":  batch[i].IPInt,
						}).Error; err != nil {
						return err
					}
					hashUpdated++
					if batch[i].IPInt != 0 {
						intUpdated++
					}
				}
			}

			batch = batch[:0]
			return nil
		})

	if result.Error != nil {
		return hashUpdated, intUpdated, result.Error
	}

	return hashUpdated, intUpdated, nil
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

	hashes := make([][]byte, 0, len(normalized))
	for _, ip := range normalized {
		sum := sha256.Sum256([]byte(ip))
		hash := make([]byte, len(sum))
		copy(hash, sum[:])
		hashes = append(hashes, hash)
	}

	db := DB
	if ctx != nil {
		db = db.WithContext(ctx)
	}

	var proxies []domain.Proxy
	if err := db.Preload("Users").
		Where("ip_hash IN ?", hashes).
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

func normalizeIPList(ips []string) []string {
	if len(ips) == 0 {
		return nil
	}

	seen := make(map[string]struct{}, len(ips))
	out := make([]string, 0, len(ips))

	for _, raw := range ips {
		ip := normalizeIPv4(raw)
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

func normalizeIPv4(raw string) string {
	parsed := net.ParseIP(raw)
	if parsed == nil {
		return ""
	}
	ipv4 := parsed.To4()
	if ipv4 == nil {
		return ""
	}
	return ipv4.String()
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

	type span struct {
		start uint32
		end   uint32
	}
	spans := make([]span, 0, len(ranges))
	for _, r := range ranges {
		start := r.StartIP
		end := r.EndIP
		if start > end {
			start, end = end, start
		}
		spans = append(spans, span{start: start, end: end})
	}

	// Sort and merge to reduce queries
	sort.Slice(spans, func(i, j int) bool { return spans[i].start < spans[j].start })
	merged := make([]span, 0, len(spans))
	for _, s := range spans {
		if len(merged) == 0 {
			merged = append(merged, s)
			continue
		}
		last := &merged[len(merged)-1]
		if s.start <= last.end+1 {
			if s.end > last.end {
				last.end = s.end
			}
			continue
		}
		merged = append(merged, s)
	}

	var proxies []domain.Proxy
	for _, s := range merged {
		var batch []domain.Proxy
		if err := db.Preload("Users").
			Where("ip_int BETWEEN ? AND ?", s.start, s.end).
			Find(&batch).Error; err != nil {
			return 0, nil, err
		}
		proxies = append(proxies, batch...)
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
