package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"magpie/internal/api/dto"
	"magpie/internal/domain"

	"github.com/charmbracelet/log"
	"gorm.io/gorm"
)

const proxyExportBatchSize = 2000

func StreamProxiesForExport(ctx context.Context, workspaceID uint, settings dto.ExportSettings, batchSize int, consume func([]domain.Proxy) error) error {
	if DB == nil {
		return fmt.Errorf("database connection was not initialised")
	}
	if consume == nil {
		return fmt.Errorf("export consumer callback is nil")
	}
	if batchSize <= 0 {
		batchSize = proxyExportBatchSize
	}

	tx := DB.WithContext(ctx).Begin(&sql.TxOptions{
		ReadOnly:  true,
		Isolation: sql.LevelRepeatableRead,
	})
	if tx.Error != nil {
		return tx.Error
	}
	defer func() {
		if err := tx.Rollback().Error; err != nil && !errors.Is(err, gorm.ErrInvalidTransaction) && !errors.Is(err, sql.ErrTxDone) {
			log.Error("failed to rollback export transaction", "error", err)
		}
	}()

	idQuery := tx.Model(&domain.Proxy{}).
		Select("DISTINCT proxies.id").
		Joins("JOIN user_proxies ON user_proxies.proxy_id = proxies.id").
		Where("user_proxies.workspace_id = ?", workspaceID)

	if len(settings.Proxies) > 0 {
		idQuery = idQuery.Where("proxies.id IN ?", settings.Proxies)
	}

	if settings.Filter {
		filterQuery := buildProxyListFilterQuery(workspaceID, proxyListFiltersForExport(settings))
		if filterQuery != nil {
			idQuery = idQuery.Where("proxies.id IN (?)", filterQuery)
		}
	}

	var lastID uint64
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		var ids []uint64
		query := idQuery.Session(&gorm.Session{})
		if lastID > 0 {
			query = query.Where("proxies.id > ?", lastID)
		}
		if err := query.Order("proxies.id ASC").Limit(batchSize).Pluck("proxies.id", &ids).Error; err != nil {
			return err
		}
		if len(ids) == 0 {
			break
		}

		proxies, err := loadExportProxyBatch(tx, workspaceID, ids)
		if err != nil {
			return err
		}

		filtered := filterProxiesForExport(proxies, settings)
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := consume(filtered); err != nil {
			return err
		}

		lastID = ids[len(ids)-1]
	}

	if err := tx.Commit().Error; err != nil {
		return err
	}

	return nil
}

func proxyListFiltersForExport(settings dto.ExportSettings) dto.ProxyListFilters {
	protocols := make([]string, 0, 4)
	if settings.Http {
		protocols = append(protocols, "http")
	}
	if settings.Https {
		protocols = append(protocols, "https")
	}
	if settings.Socks4 {
		protocols = append(protocols, "socks4")
	}
	if settings.Socks5 {
		protocols = append(protocols, "socks5")
	}

	return dto.ProxyListFilters{
		States:           dto.NormalizeProxyStateFilters(settings.States),
		State:            dto.NormalizeProxyStateFilter(settings.State),
		Status:           settings.ProxyStatus,
		Protocols:        protocols,
		MinHealthOverall: int(settings.MinHealthOverall),
		MinHealthHTTP:    int(settings.MinHealthHTTP),
		MinHealthHTTPS:   int(settings.MinHealthHTTPS),
		MinHealthSOCKS4:  int(settings.MinHealthSOCKS4),
		MinHealthSOCKS5:  int(settings.MinHealthSOCKS5),
		Countries:        normalizeFilterValues(settings.Countries),
		Types:            normalizeFilterValues(settings.Types),
		AnonymityLevels:  normalizeFilterValues(settings.AnonymityLevels),
		MaxTimeout:       int(settings.MaxTimeout),
		MaxRetries:       int(settings.MaxRetries),
		ReputationLabels: settings.ReputationLabels,
		TagIDs:           settings.TagIDs,
	}
}

func normalizeFilterValues(values []string) []string {
	if len(values) == 0 {
		return nil
	}

	seen := make(map[string]struct{}, len(values))
	normalized := make([]string, 0, len(values))
	for _, value := range values {
		item := strings.ToLower(strings.TrimSpace(value))
		if item == "" {
			continue
		}
		if _, exists := seen[item]; exists {
			continue
		}
		seen[item] = struct{}{}
		normalized = append(normalized, item)
	}

	return normalized
}

func loadExportProxyBatch(tx *gorm.DB, workspaceID uint, ids []uint64) ([]domain.Proxy, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	var proxies []domain.Proxy
	if err := tx.
		Where("id IN ?", ids).
		Find(&proxies).Error; err != nil {
		return nil, err
	}
	if len(proxies) == 0 {
		return nil, nil
	}
	if err := hydrateProxyCredentials(tx, proxies, workspaceID); err != nil {
		return nil, err
	}

	indexByID := make(map[uint64]int, len(proxies))
	for i := range proxies {
		indexByID[proxies[i].ID] = i
	}

	var statRows []struct {
		ProxyID      uint64
		StatisticID  uint64
		Alive        bool
		Attempt      uint8
		ResponseTime uint16
		ProtocolID   int
		ProtocolName string
		CreatedAt    time.Time
	}
	if err := currentLatestStatistics(tx, workspaceID).
		Select(
			"pls.proxy_id AS proxy_id, "+
				"ps.id AS statistic_id, pls.alive AS alive, ps.attempt AS attempt, "+
				"ps.response_time AS response_time, ps.protocol_id AS protocol_id, "+
				"COALESCE(proto.name, '') AS protocol_name, ps.created_at AS created_at",
		).
		Joins("JOIN proxy_statistics ps ON ps.id = pls.statistic_id").
		Joins("LEFT JOIN protocols proto ON proto.id = ps.protocol_id").
		Where("pls.proxy_id IN ?", ids).
		Scan(&statRows).Error; err != nil {
		return nil, err
	}

	for _, row := range statRows {
		idx, ok := indexByID[row.ProxyID]
		if !ok {
			continue
		}
		proxies[idx].Statistics = append(proxies[idx].Statistics, domain.ProxyStatistic{
			ID:           row.StatisticID,
			Alive:        row.Alive,
			Attempt:      row.Attempt,
			ResponseTime: row.ResponseTime,
			ProtocolID:   row.ProtocolID,
			Protocol: domain.Protocol{
				ID:   row.ProtocolID,
				Name: row.ProtocolName,
			},
			CreatedAt: row.CreatedAt,
		})
	}

	var reputations []domain.ProxyReputation
	if err := tx.
		Where("proxy_id IN ?", ids).
		Find(&reputations).Error; err != nil {
		return nil, err
	}

	for _, rep := range reputations {
		idx, ok := indexByID[rep.ProxyID]
		if !ok {
			continue
		}
		proxies[idx].Reputations = append(proxies[idx].Reputations, rep)
	}

	ordered := make([]domain.Proxy, 0, len(ids))
	for _, id := range ids {
		idx, ok := indexByID[id]
		if !ok {
			continue
		}
		ordered = append(ordered, proxies[idx])
	}

	return ordered, nil
}

func filterProxiesForExport(proxies []domain.Proxy, settings dto.ExportSettings) []domain.Proxy {
	if len(settings.ReputationLabels) == 0 {
		return proxies
	}

	allowedLabels, includeUnknown := normalizeReputationLabels(settings.ReputationLabels)
	selectedProtocols := protocolsForExport(settings)

	filtered := make([]domain.Proxy, 0, len(proxies))
	for _, proxy := range proxies {
		if proxyMatchesReputationFilters(proxy, allowedLabels, includeUnknown, selectedProtocols) {
			filtered = append(filtered, proxy)
		}
	}

	return filtered
}
