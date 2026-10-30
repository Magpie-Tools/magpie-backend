package database

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"magpie/internal/config"
	"magpie/internal/domain"
	"magpie/internal/ipgeolocation"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const ipGeolocationBatchSize = 256

func IPGeolocationPolicy(cfg config.IPGeolocationPluginConfig) uint32 {
	policy := cfg.CacheHours * 2
	if cfg.IncludeSecurity {
		policy++
	}
	return policy
}

func GetIPGeolocationState(ctx context.Context) (domain.IPGeolocationState, error) {
	state := domain.IPGeolocationState{ID: 1}
	if DB == nil {
		return state, errors.New("database not initialized")
	}
	err := DB.WithContext(ctx).First(&state, 1).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		err = DB.WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).Create(&state).Error
	}
	return state, err
}

func enqueueIPGeolocationRows(tx *gorm.DB, proxies []domain.Proxy, cfg config.IPGeolocationPluginConfig, now time.Time) error {
	seen := make(map[string]bool)
	rows := make([]domain.IPGeolocationLookup, 0, len(proxies))
	for _, proxy := range proxies {
		ip, ok := ipgeolocation.PublicIP(proxy.GetIPAddress())
		if !ok || seen[ip] {
			continue
		}
		seen[ip] = true
		rows = append(rows, domain.IPGeolocationLookup{IP: ip, NextLookupAt: now, Policy: IPGeolocationPolicy(cfg)})
	}
	if len(rows) == 0 {
		return nil
	}
	return tx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "ip"}},
		DoUpdates: clause.AssignmentColumns([]string{"policy", "next_lookup_at"}),
		Where:     clause.Where{Exprs: []clause.Expression{clause.Neq{Column: clause.Column{Table: "ipgeolocation_lookups", Name: "policy"}, Value: clause.Column{Table: "excluded", Name: "policy"}}}},
	}).CreateInBatches(rows, ipGeolocationBatchSize).Error
}

// Discover existing inventory in bounded primary-key pages, never a GROUP BY
// over the whole inventory. A policy change restarts this background cursor.
func DiscoverIPGeolocationIPs(ctx context.Context, cfg config.IPGeolocationPluginConfig, now time.Time) (int, error) {
	count := 0
	err := DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var state domain.IPGeolocationState
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&state, 1).Error; err != nil {
			return err
		}
		policy := IPGeolocationPolicy(cfg)
		if state.Policy != policy {
			state.DiscoveryCursor = 0
			state.Policy = policy
		}
		var proxies []domain.Proxy
		if err := tx.Select("id", "host", "ip_address").Where("id > ?", state.DiscoveryCursor).
			Order("id").Limit(ipGeolocationBatchSize).Find(&proxies).Error; err != nil {
			return err
		}
		count = len(proxies)
		ids := make([]uint64, 0, len(proxies))
		for _, proxy := range proxies {
			ids = append(ids, proxy.ID)
		}
		managed := make(map[uint64]bool)
		if len(ids) > 0 {
			var linked []uint64
			if err := tx.Table("user_proxies").Distinct("proxy_id").Where("proxy_id IN ?", ids).Pluck("proxy_id", &linked).Error; err != nil {
				return err
			}
			for _, id := range linked {
				managed[id] = true
			}
		}
		candidates := make([]domain.Proxy, 0, len(proxies))
		for _, proxy := range proxies {
			if managed[proxy.ID] {
				candidates = append(candidates, proxy)
			}
		}
		if err := enqueueIPGeolocationRows(tx, candidates, cfg, now); err != nil {
			return err
		}
		for _, proxy := range proxies {
			state.DiscoveryCursor = proxy.ID
		}
		return tx.Model(&domain.IPGeolocationState{}).Where("id = 1").Updates(map[string]any{"discovery_cursor": state.DiscoveryCursor, "policy": policy, "next_discovery_at": now.Add(time.Minute)}).Error
	})
	return count, err
}

// Called only after route ingestion, and only while the plugin is active.
func QueueIPGeolocationIPs(ctx context.Context, proxies []domain.Proxy) error {
	cfg := config.GetConfig().Plugins.IPGeolocation
	if !cfg.Active() || DB == nil {
		return nil
	}
	queueCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return enqueueIPGeolocationRows(DB.WithContext(queueCtx), proxies, cfg, time.Now().UTC())
}

func NextIPGeolocationLookup(ctx context.Context, now time.Time) (*domain.IPGeolocationLookup, error) {
	var row domain.IPGeolocationLookup
	err := DB.WithContext(ctx).Where("next_lookup_at <= ?", now).Order("next_lookup_at, ip").First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	return &row, err
}

func IPGeolocationIPIsActive(ctx context.Context, ip string) (bool, error) {
	var id uint64
	err := DB.WithContext(ctx).Model(&domain.Proxy{}).Select("proxies.id").
		Where("ip_address = ?", ip).
		Where("EXISTS (SELECT 1 FROM user_proxies up WHERE up.proxy_id = proxies.id AND up.state = ?)", domain.ManagedProxyStateActive).
		Limit(1).Scan(&id).Error
	return id != 0, err
}

func RemoveOrphanIPGeolocationLookup(ctx context.Context, ip string) (bool, error) {
	var id uint64
	if err := DB.WithContext(ctx).Model(&domain.Proxy{}).Select("proxies.id").Where("ip_address = ?", ip).
		Where("EXISTS (SELECT 1 FROM user_proxies up WHERE up.proxy_id = proxies.id)").Limit(1).Scan(&id).Error; err != nil {
		return false, err
	}
	if id != 0 {
		return false, nil
	}
	return true, DB.WithContext(ctx).Where("ip = ?", ip).Delete(&domain.IPGeolocationLookup{}).Error
}

func DeferIPGeolocationLookup(ctx context.Context, ip string, until time.Time) error {
	return DB.WithContext(ctx).Model(&domain.IPGeolocationLookup{}).Where("ip = ?", ip).Update("next_lookup_at", until).Error
}

func ReserveIPGeolocationCredits(ctx context.Context, ip string, now time.Time, cfg config.IPGeolocationPluginConfig) (bool, time.Duration, error) {
	reserved := false
	wait := time.Minute
	err := DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var state domain.IPGeolocationState
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&state, 1).Error; err != nil {
			return err
		}
		day := now.UTC().Format("2006-01-02")
		if state.BudgetDay != day {
			state.BudgetDay = day
			state.UsedCredits = 0
		}
		cost := ipgeolocation.Cost(cfg.IncludeSecurity)
		if state.UsedCredits+cost > int(cfg.DailyCreditLimit) {
			wait = now.UTC().Truncate(24 * time.Hour).Add(24 * time.Hour).Sub(now)
			return nil
		}
		if state.NextRequestAt != nil && state.NextRequestAt.After(now) {
			wait = state.NextRequestAt.Sub(now)
			return nil
		}
		var lookup domain.IPGeolocationLookup
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("ip = ?", ip).First(&lookup).Error; err != nil {
			return err
		}
		if lookup.NextLookupAt.After(now) {
			wait = lookup.NextLookupAt.Sub(now)
			return nil
		}
		// Claim the IP beyond the HTTP timeout so a replacement leader cannot
		// immediately repeat an in-flight request for the same address.
		if err := tx.Model(&lookup).Update("next_lookup_at", now.Add(time.Minute)).Error; err != nil {
			return err
		}
		wait = max(5*time.Second, 24*time.Hour*time.Duration(cost)/time.Duration(max(1, cfg.DailyCreditLimit)))
		next := now.Add(wait)
		midnight := now.UTC().Truncate(24 * time.Hour).Add(24 * time.Hour)
		if next.After(midnight) {
			next = midnight
			wait = next.Sub(now)
		}
		if err := tx.Model(&domain.IPGeolocationState{}).Where("id = 1").Updates(map[string]any{
			"budget_day": day, "used_credits": state.UsedCredits + cost, "next_request_at": next,
		}).Error; err != nil {
			return err
		}
		reserved = true
		return nil
	})
	return reserved, wait, err
}

func CompleteIPGeolocationLookup(ctx context.Context, ip string, cfg config.IPGeolocationPluginConfig, response ipgeolocation.Response, lookupErr error, now time.Time) error {
	return DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var state domain.IPGeolocationState
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&state, 1).Error; err != nil {
			return err
		}
		cost := ipgeolocation.Cost(cfg.IncludeSecurity)
		charged := response.Credits
		if response.Status == 0 {
			charged = cost
		} // An interrupted request may have been charged.
		if state.BudgetDay == now.UTC().Format("2006-01-02") {
			state.UsedCredits = max(0, state.UsedCredits+charged-cost)
		} else {
			state.BudgetDay = now.UTC().Format("2006-01-02")
			state.UsedCredits = charged
		}
		state.LastCheckedAt = &now
		state.LastError = ""
		updates := map[string]any{}
		if lookupErr != nil {
			state.LastError = lookupErr.Error()
			delay := 15 * time.Minute
			if response.Status == 401 || response.Status == 403 {
				delay = time.Hour
			}
			if response.RetryAfter > 0 {
				delay = max(5*time.Second, response.RetryAfter)
			}
			next := now.Add(delay)
			if response.Status != 0 && response.Status != 200 || state.NextRequestAt == nil || next.After(*state.NextRequestAt) {
				state.NextRequestAt = &next
			}
			updates["next_lookup_at"] = next
		} else {
			payload, err := json.Marshal(response.Result)
			if err != nil {
				return err
			}
			expires := now.Add(time.Duration(cfg.CacheHours) * time.Hour)
			country := response.Result.Location.CountryName
			if len(country) > 56 || country == "" {
				country = response.Result.Location.CountryCode
			}
			updates = map[string]any{
				"country": country, "estimated_type": response.Result.EstimatedType(), "payload": string(payload),
				"security_included": cfg.IncludeSecurity, "checked_at": now, "expires_at": expires,
				"next_lookup_at": expires, "policy": IPGeolocationPolicy(cfg), "apply_pending": true, "apply_cursor": 0,
			}
		}
		if err := tx.Model(&domain.IPGeolocationLookup{}).Where("ip = ?", ip).Updates(updates).Error; err != nil {
			return err
		}
		return tx.Model(&domain.IPGeolocationState{}).Where("id = 1").Updates(map[string]any{
			"budget_day": state.BudgetDay, "used_credits": state.UsedCredits, "last_checked_at": state.LastCheckedAt, "last_error": state.LastError, "next_request_at": state.NextRequestAt,
		}).Error
	})
}

func freshIPGeolocation(row domain.IPGeolocationLookup, cfg config.IPGeolocationPluginConfig, now time.Time) bool {
	return row.CheckedAt != nil && row.ExpiresAt != nil && row.ExpiresAt.After(now) && row.CheckedAt.After(now.Add(-time.Duration(cfg.CacheHours)*time.Hour)) && (!cfg.IncludeSecurity || row.SecurityIncluded)
}

// Each local enrichment batch adds at most one cache query; disabled plugins
// never touch this table. There is no per-check or proxy-list cache query.
func loadIPGeolocationOverrides(ctx context.Context, proxies []domain.Proxy) (map[string]domain.IPGeolocationLookup, error) {
	cfg := config.GetConfig().Plugins.IPGeolocation
	rows := make(map[string]domain.IPGeolocationLookup)
	if !cfg.Active() || DB == nil || len(proxies) == 0 {
		return rows, nil
	}
	ips := make([]string, 0, len(proxies))
	for _, proxy := range proxies {
		if ip := proxy.GetIPAddress(); ip != "" {
			ips = append(ips, ip)
		}
	}
	if len(ips) == 0 {
		return rows, nil
	}
	var cached []domain.IPGeolocationLookup
	now := time.Now().UTC()
	lookupCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := DB.WithContext(lookupCtx).Select("ip", "country", "estimated_type", "checked_at", "expires_at", "security_included").
		Where("ip IN ? AND expires_at > ?", ips, now).Find(&cached).Error; err != nil {
		return nil, err
	}
	for _, row := range cached {
		if freshIPGeolocation(row, cfg, now) {
			rows[row.IP] = row
		}
	}
	return rows, nil
}

// Apply no more than 256 routes per transaction, and queue projections only for
// rows whose country/type actually changed. Shared-IP fanout cannot hold one
// transaction over an entire inventory.
func ApplyIPGeolocationBatch(ctx context.Context, cfg config.IPGeolocationPluginConfig) (bool, error) {
	var ids []uint64
	worked := false
	err := DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var row domain.IPGeolocationLookup
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("apply_pending = ?", true).Order("ip").First(&row).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		worked = true
		if !freshIPGeolocation(row, cfg, time.Now().UTC()) {
			return tx.Model(&row).Update("apply_pending", false).Error
		}
		var proxies []domain.Proxy
		if err := tx.Select("id", "country", "estimated_type").Where("ip_address = ? AND id > ?", row.IP, row.ApplyCursor).
			Order("id").Limit(ipGeolocationBatchSize).Find(&proxies).Error; err != nil {
			return err
		}
		changed := make([]domain.Proxy, 0, len(proxies))
		for _, proxy := range proxies {
			row.ApplyCursor = proxy.ID
			country, proxyType := proxy.Country, proxy.EstimatedType
			if row.Country != "" {
				country = row.Country
			}
			if row.EstimatedType != "" {
				proxyType = row.EstimatedType
			}
			if country == proxy.Country && proxyType == proxy.EstimatedType {
				continue
			}
			proxy.Country, proxy.EstimatedType = country, proxyType
			changed = append(changed, proxy)
			ids = append(ids, proxy.ID)
		}
		// The standard metadata writer is PostgreSQL-specific and takes the DB
		// connection explicitly here to keep the cursor and writes atomic.
		if len(changed) > 0 {
			values := make([]string, 0, len(changed))
			args := make([]any, 0, len(changed)*3)
			for _, proxy := range changed {
				values = append(values, "(?::bigint, ?::text, ?::text)")
				args = append(args, proxy.ID, proxy.Country, proxy.EstimatedType)
			}
			if err := tx.Exec(fmt.Sprintf("UPDATE proxies AS p SET country=v.country, estimated_type=v.estimated_type FROM (VALUES %s) AS v(id,country,estimated_type) WHERE p.id=v.id AND (p.country,p.estimated_type) IS DISTINCT FROM (v.country,v.estimated_type)", strings.Join(values, ",")), args...).Error; err != nil {
				return err
			}
		}
		return tx.Model(&row).Updates(map[string]any{"apply_cursor": row.ApplyCursor, "apply_pending": len(proxies) == ipGeolocationBatchSize}).Error
	})
	if err == nil && len(ids) > 0 {
		QueueReadModelRefreshForProxyIDs(ids)
	}
	return worked, err
}

// Protect a fresh provider result against a local lookup that started before
// the provider request completed. Used only in the two metadata writers.
func ipGeolocationMetadataGuard(alias string) (string, []any) {
	cfg := config.GetConfig().Plugins.IPGeolocation
	if !cfg.Active() {
		return "", nil
	}
	now := time.Now().UTC()
	security := ""
	if cfg.IncludeSecurity {
		security = " AND ig.security_included = TRUE"
	}
	return " AND NOT EXISTS (SELECT 1 FROM ipgeolocation_lookups ig WHERE ig.ip = p.host AND ig.expires_at > ? AND ig.checked_at > ?" + security +
		" AND ((ig.estimated_type <> '' AND ig.estimated_type <> " + alias + ".estimated_type) OR (ig.country <> '' AND ig.country <> " + alias + ".country)))", []any{now, now.Add(-time.Duration(cfg.CacheHours) * time.Hour)}
}
