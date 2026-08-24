package database

import (
	"magpie/internal/domain"

	"gorm.io/gorm"
)

var checkerWorkspaceSelectColumns = []string{
	"id",
	"http_protocol",
	"http_s_protocol",
	"socks4_protocol",
	"socks5_protocol",
	"timeout",
	"retries",
	"use_https_for_socks",
	"transport_protocol",
	"auto_remove_failing_proxies",
	"auto_remove_failure_threshold",
}

var checkerUserSelectColumns = checkerWorkspaceSelectColumns

func preloadCheckerWorkspaces(db *gorm.DB) *gorm.DB {
	return db.Select(checkerWorkspaceSelectColumns)
}

func preloadWorkspaceIDsOnly(db *gorm.DB) *gorm.DB {
	return db.Select("id")
}

func preloadCheckerUsers(db *gorm.DB) *gorm.DB { return preloadCheckerWorkspaces(db) }
func preloadUserIDsOnly(db *gorm.DB) *gorm.DB  { return preloadWorkspaceIDsOnly(db) }

type activeProxyWorkspaceRow struct {
	ProxyID                    uint64 `gorm:"column:proxy_id"`
	WorkspaceID                uint   `gorm:"column:workspace_id"`
	HTTPProtocol               bool
	HTTPSProtocol              bool `gorm:"column:http_s_protocol"`
	SOCKS4Protocol             bool
	SOCKS5Protocol             bool
	Timeout                    uint16
	Retries                    uint8
	UseHttpsForSocks           bool
	TransportProtocol          string
	AutoRemoveFailingProxies   bool
	AutoRemoveFailureThreshold uint8
}

func hydrateActiveProxyWorkspaces(tx *gorm.DB, proxies []domain.Proxy) error {
	if tx == nil || len(proxies) == 0 {
		return nil
	}
	proxyIDs := make([]uint64, 0, len(proxies))
	indexes := make(map[uint64][]int, len(proxies))
	for index := range proxies {
		proxies[index].Workspaces = nil
		if proxies[index].ID == 0 {
			continue
		}
		if _, exists := indexes[proxies[index].ID]; !exists {
			proxyIDs = append(proxyIDs, proxies[index].ID)
		}
		indexes[proxies[index].ID] = append(indexes[proxies[index].ID], index)
	}
	if len(proxyIDs) == 0 {
		return nil
	}

	var rows []activeProxyWorkspaceRow
	err := tx.Session(&gorm.Session{NewDB: true}).
		Table("user_proxies up").
		Select(`
			up.proxy_id,
			w.id AS workspace_id,
			w.http_protocol,
			w.http_s_protocol,
			w.socks4_protocol,
			w.socks5_protocol,
			w.timeout,
			w.retries,
			w.use_https_for_socks,
			w.transport_protocol,
			w.auto_remove_failing_proxies,
			w.auto_remove_failure_threshold
		`).
		Joins("JOIN workspaces w ON w.id = up.workspace_id").
		Where("up.proxy_id IN ? AND up.state = ?", proxyIDs, domain.ManagedProxyStateActive).
		Order("up.proxy_id, up.workspace_id").
		Scan(&rows).Error
	if err != nil {
		return err
	}

	for _, row := range rows {
		workspace := domain.Workspace{
			ID:                         row.WorkspaceID,
			HTTPProtocol:               row.HTTPProtocol,
			HTTPSProtocol:              row.HTTPSProtocol,
			SOCKS4Protocol:             row.SOCKS4Protocol,
			SOCKS5Protocol:             row.SOCKS5Protocol,
			Timeout:                    row.Timeout,
			Retries:                    row.Retries,
			UseHttpsForSocks:           row.UseHttpsForSocks,
			TransportProtocol:          row.TransportProtocol,
			AutoRemoveFailingProxies:   row.AutoRemoveFailingProxies,
			AutoRemoveFailureThreshold: row.AutoRemoveFailureThreshold,
		}
		for _, index := range indexes[row.ProxyID] {
			proxies[index].Workspaces = append(proxies[index].Workspaces, workspace)
		}
	}
	return nil
}
