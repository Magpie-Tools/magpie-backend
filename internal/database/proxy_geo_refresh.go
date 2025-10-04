package database

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/charmbracelet/log"
	"golang.org/x/sync/errgroup"
	"gorm.io/gorm"
	"magpie/internal/domain"
)

const (
	proxyGeoRefreshInterval    = 24 * time.Hour
	proxyGeoRefreshBatchSize   = 2000
	proxyGeoRefreshUpdateChunk = 500
	proxyGeoRefreshWorkerLimit = 16
)

type proxyGeoUpdate struct {
	ID            uint64 `gorm:"primaryKey"`
	Country       string
	EstimatedType string
}

func (proxyGeoUpdate) TableName() string {
	return "proxies"
}

func StartProxyGeoRefreshRoutine(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}

	ticker := time.NewTicker(proxyGeoRefreshInterval)
	defer ticker.Stop()

	runProxyGeoRefresh(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			runProxyGeoRefresh(ctx)
		}
	}
}

func runProxyGeoRefresh(ctx context.Context) {
	if DB == nil {
		log.Warn("Proxy geo refresh skipped: database not initialized")
		return
	}

	if !initSuccess {
		log.Warn("Proxy geo refresh skipped: GeoLite databases unavailable")
		return
	}

	start := time.Now()

	scanned, updated, err := refreshProxyGeoData(ctx, proxyGeoRefreshBatchSize)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			log.Info("Proxy geo refresh canceled", "duration", time.Since(start))
			return
		}
		log.Error("Proxy geo refresh failed", "error", err)
		return
	}

	log.Info("Proxy geo refresh completed", "scanned", scanned, "updated", updated, "duration", time.Since(start))
}

func refreshProxyGeoData(ctx context.Context, batchSize int) (int64, int64, error) {
	if DB == nil {
		return 0, 0, errors.New("database not initialized")
	}

	if batchSize <= 0 {
		batchSize = proxyGeoRefreshBatchSize
	}

	var (
		scanned int64
		updated int64
	)

	proxies := make([]domain.Proxy, 0, batchSize)

	result := DB.WithContext(ctx).
		Model(&domain.Proxy{}).
		Select("id", "ip1", "ip2", "ip3", "ip4", "country", "estimated_type").
		FindInBatches(&proxies, batchSize, func(tx *gorm.DB, batch int) error {
			if len(proxies) == 0 {
				return nil
			}

			currentBatch := make([]domain.Proxy, len(proxies))
			copy(currentBatch, proxies)

			updates, err := buildProxyGeoUpdates(ctx, currentBatch)
			scanned += int64(len(currentBatch))
			if err != nil {
				return err
			}
			if len(updates) == 0 {
				return nil
			}
			if err := applyProxyGeoUpdates(ctx, updates); err != nil {
				return err
			}
			updated += int64(len(updates))
			return nil
		})

	if result.Error != nil {
		return scanned, updated, result.Error
	}

	return scanned, updated, nil
}

func buildProxyGeoUpdates(ctx context.Context, proxies []domain.Proxy) ([]proxyGeoUpdate, error) {
	if len(proxies) == 0 {
		return nil, nil
	}

	updates := make([]proxyGeoUpdate, 0, len(proxies))
	var mu sync.Mutex

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(proxyGeoRefreshWorkerLimit)

	for _, proxy := range proxies {
		proxy := proxy
		g.Go(func() error {
			if err := gctx.Err(); err != nil {
				return err
			}

			ip := proxy.GetIp()
			country := GetCountryCode(ip)
			if country == "" {
				country = "N/A"
			}

			proxyType := DetermineProxyType(ip)
			if proxyType == "" {
				proxyType = "N/A"
			}

			if country == proxy.Country && proxyType == proxy.EstimatedType {
				return nil
			}

			mu.Lock()
			updates = append(updates, proxyGeoUpdate{
				ID:            proxy.ID,
				Country:       country,
				EstimatedType: proxyType,
			})
			mu.Unlock()
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return updates, nil
}

func applyProxyGeoUpdates(ctx context.Context, updates []proxyGeoUpdate) error {
	if len(updates) == 0 {
		return nil
	}

	for start := 0; start < len(updates); start += proxyGeoRefreshUpdateChunk {
		end := start + proxyGeoRefreshUpdateChunk
		if end > len(updates) {
			end = len(updates)
		}
		if err := ctx.Err(); err != nil {
			return err
		}

		batch := updates[start:end]
		placeholders := make([]string, len(batch))
		args := make([]any, 0, len(batch)*3)

		for i, u := range batch {
			placeholders[i] = "(?::bigint, ?::text, ?::text)"
			args = append(args, u.ID, u.Country, u.EstimatedType)
		}

		query := fmt.Sprintf(
			`UPDATE proxies AS p SET country = data.country, estimated_type = data.estimated_type `+
				`FROM (VALUES %s) AS data(id, country, estimated_type) WHERE p.id = data.id`,
			strings.Join(placeholders, ","),
		)

		if err := DB.WithContext(ctx).Exec(query, args...).Error; err != nil {
			return err
		}
	}

	return nil
}
