package database

import (
	"context"
	"crypto/sha256"
	"errors"
	"net"
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
