package database

import (
	"context"
	"encoding/json"
	"errors"
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
	for start := 0; start < len(rows); start += ipGeolocationBatchSize {
		batch := rows[start:min(start+ipGeolocationBatchSize, len(rows))]
		if err := tx.Transaction(func(tx *gorm.DB) error {
			ips := make([]string, 0, len(batch))
			for _, row := range batch {
				ips = append(ips, row.IP)
			}
			var cached []domain.IPGeolocationLookup
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Select("ip", "checked_at", "security_included").
				Where("ip IN ? AND policy <> ?", ips, IPGeolocationPolicy(cfg)).Find(&cached).Error; err != nil {
				return err
			}
			schedules := make(map[string]domain.IPGeolocationLookup, len(cached))
			for _, row := range cached {
				schedules[row.IP] = row
			}
			for i := range batch {
				if cached, ok := schedules[batch[i].IP]; ok && cached.CheckedAt != nil {
					expires := cached.CheckedAt.Add(time.Duration(cfg.CacheHours) * time.Hour)
					batch[i].ExpiresAt = &expires
					if freshIPGeolocation(cached, cfg, now) {
						batch[i].NextLookupAt = expires
					}
				}
			}
			return tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "ip"}},
				DoUpdates: clause.AssignmentColumns([]string{"policy", "next_lookup_at", "expires_at"}),
				Where:     clause.Where{Exprs: []clause.Expression{clause.Neq{Column: clause.Column{Table: "ipgeolocation_lookups", Name: "policy"}, Value: clause.Column{Table: "excluded", Name: "policy"}}}},
			}).CreateInBatches(batch, ipGeolocationBatchSize).Error
		}); err != nil {
			return err
		}
	}
	return nil
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

// Drain local cleanup in bounded transactions without making the worker sleep
// for each paused, orphaned, or still-fresh IP ahead of an active lookup.
func NextIPGeolocationLookup(ctx context.Context, cfg config.IPGeolocationPluginConfig, now time.Time) (*domain.IPGeolocationLookup, error) {
	for {
		var next *domain.IPGeolocationLookup
		scanned := 0
		err := DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			ip := "ipgeolocation_lookups.ip"
			if tx.Dialector.Name() == "postgres" {
				ip += "::inet"
			}
			var rows []struct {
				domain.IPGeolocationLookup
				Managed bool
				Active  bool
			}
			if err := tx.Model(&domain.IPGeolocationLookup{}).Clauses(clause.Locking{Strength: "UPDATE"}).
				Select("ip, checked_at, security_included, "+
					"EXISTS (SELECT 1 FROM proxies p JOIN user_proxies up ON up.proxy_id = p.id WHERE p.ip_address = "+ip+") AS managed, "+
					"EXISTS (SELECT 1 FROM proxies p JOIN user_proxies up ON up.proxy_id = p.id WHERE p.ip_address = "+ip+" AND up.state = ?) AS active", domain.ManagedProxyStateActive).
				Where("next_lookup_at <= ?", now).Order("next_lookup_at, ip").Limit(ipGeolocationBatchSize).Find(&rows).Error; err != nil {
				return err
			}
			scanned = len(rows)
			var orphaned, paused []string
			var fresh []domain.IPGeolocationLookup
			for _, row := range rows {
				switch {
				case !row.Managed:
					orphaned = append(orphaned, row.IP)
				case !row.Active:
					paused = append(paused, row.IP)
				case freshIPGeolocation(row.IPGeolocationLookup, cfg, now):
					expires := row.CheckedAt.Add(time.Duration(cfg.CacheHours) * time.Hour)
					fresh = append(fresh, domain.IPGeolocationLookup{IP: row.IP, NextLookupAt: expires, ExpiresAt: &expires, Policy: IPGeolocationPolicy(cfg)})
				case next == nil:
					lookup := row.IPGeolocationLookup
					next = &lookup
				}
			}
			if len(orphaned) > 0 {
				if err := tx.Where("ip IN ?", orphaned).Delete(&domain.IPGeolocationLookup{}).Error; err != nil {
					return err
				}
			}
			if len(paused) > 0 {
				if err := tx.Model(&domain.IPGeolocationLookup{}).Where("ip IN ?", paused).Update("next_lookup_at", now.Add(time.Hour)).Error; err != nil {
					return err
				}
			}
			if len(fresh) > 0 {
				return tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "ip"}},
					DoUpdates: clause.AssignmentColumns([]string{"next_lookup_at", "expires_at", "policy"}),
				}).CreateInBatches(fresh, ipGeolocationBatchSize).Error
			}
			return nil
		})
		if err != nil || next != nil || scanned < ipGeolocationBatchSize {
			return next, err
		}
	}
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
		if freshIPGeolocation(lookup, cfg, now) {
			expires := lookup.CheckedAt.Add(time.Duration(cfg.CacheHours) * time.Hour)
			wait = expires.Sub(now)
			return tx.Model(&lookup).Updates(map[string]any{"next_lookup_at": expires, "expires_at": expires, "policy": IPGeolocationPolicy(cfg)}).Error
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

// Stored expiry can reflect an older cache duration until discovery catches up.
// The current duration and the data actually fetched determine freshness.
func freshIPGeolocation(row domain.IPGeolocationLookup, cfg config.IPGeolocationPluginConfig, now time.Time) bool {
	return row.CheckedAt != nil && row.CheckedAt.After(now.Add(-time.Duration(cfg.CacheHours)*time.Hour)) && (!cfg.IncludeSecurity || row.SecurityIncluded)
}

// Query each distinct IP once, with bounded parameter lists even for large
// imports. Disabled plugins never touch this table.
func loadIPGeolocationOverrides(ctx context.Context, proxies []domain.Proxy) (map[string]domain.IPGeolocationLookup, error) {
	cfg := config.GetConfig().Plugins.IPGeolocation
	rows := make(map[string]domain.IPGeolocationLookup)
	if !cfg.Active() || DB == nil || len(proxies) == 0 {
		return rows, nil
	}
	ips := make([]string, 0, len(proxies))
	seen := make(map[string]bool)
	for _, proxy := range proxies {
		if ip := proxy.GetIPAddress(); ip != "" && !seen[ip] {
			seen[ip] = true
			ips = append(ips, ip)
		}
	}
	if len(ips) == 0 {
		return rows, nil
	}
	now := time.Now().UTC()
	lookupCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for start := 0; start < len(ips); start += ipGeolocationBatchSize {
		var cached []domain.IPGeolocationLookup
		if err := DB.WithContext(lookupCtx).Select("ip", "country", "estimated_type", "checked_at", "expires_at", "security_included").
			Where("ip IN ? AND checked_at > ?", ips[start:min(start+ipGeolocationBatchSize, len(ips))], now.Add(-time.Duration(cfg.CacheHours)*time.Hour)).Find(&cached).Error; err != nil {
			return nil, err
		}
		for _, row := range cached {
			if freshIPGeolocation(row, cfg, now) {
				rows[row.IP] = row
			}
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
		changed := make([]proxyMetadataUpdate, 0, len(proxies))
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
			changed = append(changed, proxyMetadataUpdate{ID: proxy.ID, Country: country, EstimatedType: proxyType})
			ids = append(ids, proxy.ID)
		}
		if err := updateProxyMetadataBatch(tx, changed, ipGeolocationProxyMetadata); err != nil {
			return err
		}
		return tx.Model(&row).Updates(map[string]any{"apply_cursor": row.ApplyCursor, "apply_pending": len(proxies) == ipGeolocationBatchSize}).Error
	})
	if err == nil && len(ids) > 0 {
		QueueReadModelRefreshForProxyIDs(ids)
	}
	return worked, err
}

// Protect a fresh provider result against a local lookup that started before
// the provider request completed.
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
	return " AND NOT EXISTS (SELECT 1 FROM ipgeolocation_lookups ig WHERE ig.ip = p.host AND ig.checked_at > ?" + security +
		" AND ((ig.estimated_type <> '' AND ig.estimated_type <> " + alias + ".estimated_type) OR (ig.country <> '' AND ig.country <> " + alias + ".country)))", []any{now.Add(-time.Duration(cfg.CacheHours) * time.Hour)}
}
