package database

import (
	"fmt"

	"magpie/internal/domain"

	"gorm.io/gorm"
)

func hydrateProxyCredentials(tx *gorm.DB, proxies []domain.Proxy, userID uint) error {
	if tx == nil {
		return fmt.Errorf("hydrate proxy credentials: nil database")
	}
	if len(proxies) == 0 {
		return nil
	}

	proxyIDs := make([]uint64, 0, len(proxies))
	for _, proxy := range proxies {
		if proxy.ID != 0 {
			proxyIDs = append(proxyIDs, proxy.ID)
		}
	}
	if len(proxyIDs) == 0 {
		return nil
	}

	query := tx.Session(&gorm.Session{NewDB: true}).
		Where("proxy_id IN ?", proxyIDs).
		Order("proxy_id ASC, user_id ASC")
	if userID != 0 {
		query = query.Where("user_id = ?", userID)
	}

	var accesses []domain.UserProxy
	if err := query.Find(&accesses).Error; err != nil {
		return err
	}

	credentials := make(map[uint64]domain.UserProxy, len(accesses))
	for _, access := range accesses {
		if _, exists := credentials[access.ProxyID]; !exists {
			credentials[access.ProxyID] = access
		}
	}

	for index := range proxies {
		access, ok := credentials[proxies[index].ID]
		if !ok {
			continue
		}
		proxies[index].Username = access.Username
		proxies[index].Password = access.Password
	}

	return nil
}

func hydrateProxyCredential(tx *gorm.DB, proxy *domain.Proxy, userID uint) error {
	if proxy == nil {
		return nil
	}
	items := []domain.Proxy{*proxy}
	if err := hydrateProxyCredentials(tx, items, userID); err != nil {
		return err
	}
	*proxy = items[0]
	return nil
}
