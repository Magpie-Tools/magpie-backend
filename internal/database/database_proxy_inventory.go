package database

import (
	"errors"

	"magpie/internal/domain"

	"gorm.io/gorm"
)

const proxyIterationBatchSize = 2000

func ForEachProxyBatch(batchSize int, fn func([]domain.Proxy) error) error {
	if fn == nil {
		return errors.New("for each proxy batch callback is nil")
	}
	if batchSize <= 0 {
		batchSize = proxyIterationBatchSize
	}

	var batchProxies []domain.Proxy
	result := DB.
		Model(&domain.Proxy{}).
		Distinct("proxies.*").
		Joins("JOIN user_proxies up ON up.proxy_id = proxies.id AND up.state = ?", domain.ManagedProxyStateActive).
		Order("proxies.id").
		FindInBatches(&batchProxies, batchSize, func(tx *gorm.DB, _ int) error {
			if len(batchProxies) == 0 {
				return nil
			}
			if err := hydrateActiveProxyWorkspaces(tx, batchProxies); err != nil {
				return err
			}
			if err := hydrateProxyCredentials(tx, batchProxies, 0); err != nil {
				return err
			}

			currentBatch := make([]domain.Proxy, len(batchProxies))
			copy(currentBatch, batchProxies)
			return fn(currentBatch)
		})

	return result.Error
}
