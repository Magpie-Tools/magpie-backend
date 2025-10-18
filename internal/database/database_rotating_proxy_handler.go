package database

import (
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"magpie/internal/api/dto"
	"magpie/internal/domain"
	"magpie/internal/support"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	ErrRotatingProxyNotFound           = errors.New("rotating proxy not found")
	ErrRotatingProxyNameRequired       = errors.New("rotating proxy name is required")
	ErrRotatingProxyNameTooLong        = errors.New("rotating proxy name is too long")
	ErrRotatingProxyNameConflict       = errors.New("rotating proxy name already exists")
	ErrRotatingProxyProtocolMissing    = errors.New("rotating proxy protocol is required")
	ErrRotatingProxyProtocolDenied     = errors.New("protocol is not enabled for this user")
	ErrRotatingProxyNoAliveProxies     = errors.New("no alive proxies are available for the selected protocol")
	ErrRotatingProxyAuthUsernameNeeded = errors.New("authentication username is required when authentication is enabled")
	ErrRotatingProxyAuthPasswordNeeded = errors.New("authentication password is required when authentication is enabled")
	ErrRotatingProxyPortExhausted      = errors.New("no available ports for rotating proxies")
)

const rotatingProxyNameMaxLength = 120

func CreateRotatingProxy(userID uint, payload dto.RotatingProxyCreateRequest) (*dto.RotatingProxy, error) {
	if DB == nil {
		return nil, fmt.Errorf("rotating proxy: database connection was not initialised")
	}

	name := strings.TrimSpace(payload.Name)
	if name == "" {
		return nil, ErrRotatingProxyNameRequired
	}
	if len(name) > rotatingProxyNameMaxLength {
		return nil, ErrRotatingProxyNameTooLong
	}

	protocolName := strings.ToLower(strings.TrimSpace(payload.Protocol))
	if protocolName == "" {
		return nil, ErrRotatingProxyProtocolMissing
	}

	if payload.AuthRequired {
		if strings.TrimSpace(payload.AuthUsername) == "" {
			return nil, ErrRotatingProxyAuthUsernameNeeded
		}
		if strings.TrimSpace(payload.AuthPassword) == "" {
			return nil, ErrRotatingProxyAuthPasswordNeeded
		}
	}

	var result *dto.RotatingProxy

	err := DB.Transaction(func(tx *gorm.DB) error {
		var user domain.User
		if err := tx.First(&user, userID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("rotating proxy: user %d not found", userID)
			}
			return err
		}

		protocol, err := fetchProtocolByName(tx, protocolName)
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrRotatingProxyProtocolDenied
			}
			return err
		}
		if !isProtocolEnabledForUser(user, protocolName) {
			return ErrRotatingProxyProtocolDenied
		}

		entity := domain.RotatingProxy{
			UserID:       userID,
			Name:         name,
			ProtocolID:   protocol.ID,
			AuthRequired: payload.AuthRequired,
			AuthUsername: strings.TrimSpace(payload.AuthUsername),
			AuthPassword: payload.AuthPassword,
		}

		listenPort, err := allocateListenPort(tx)
		if err != nil {
			return err
		}
		entity.ListenPort = listenPort

		if err := tx.Create(&entity).Error; err != nil {
			if isUniqueConstraintError(err) {
				return ErrRotatingProxyNameConflict
			}
			return err
		}

		aliveProxies, err := aliveProxiesForProtocol(tx, userID, protocol.ID)
		if err != nil {
			return err
		}

		result = &dto.RotatingProxy{
			ID:              entity.ID,
			Name:            entity.Name,
			Protocol:        protocol.Name,
			AliveProxyCount: len(aliveProxies),
			ListenPort:      entity.ListenPort,
			AuthRequired:    entity.AuthRequired,
			AuthUsername:    entity.AuthUsername,
			AuthPassword:    strings.TrimSpace(payload.AuthPassword),
			CreatedAt:       entity.CreatedAt,
		}

		entity.AuthPassword = ""

		return nil
	})

	if err != nil {
		return nil, err
	}

	return result, nil
}

func ListRotatingProxies(userID uint) ([]dto.RotatingProxy, error) {
	if DB == nil {
		return nil, fmt.Errorf("rotating proxy: database connection was not initialised")
	}

	var rows []domain.RotatingProxy
	if err := DB.
		Preload("Protocol").
		Where("user_id = ?", userID).
		Order("created_at DESC").
		Find(&rows).Error; err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		return []dto.RotatingProxy{}, nil
	}

	protocolCache := make(map[int][]domain.Proxy)
	lastProxyCache := make(map[uint64]string)
	result := make([]dto.RotatingProxy, 0, len(rows))

	for _, row := range rows {
		protocolName := row.Protocol.Name
		proxies, err := getAliveProxiesCached(userID, row.ProtocolID, protocolCache)
		if err != nil {
			return nil, err
		}

		lastProxy := ""
		if row.LastProxyID != nil {
			lastProxy, err = getProxyAddressCached(userID, *row.LastProxyID, lastProxyCache)
			if err != nil {
				return nil, err
			}
		}

		result = append(result, dto.RotatingProxy{
			ID:              row.ID,
			Name:            row.Name,
			Protocol:        protocolName,
			AliveProxyCount: len(proxies),
			ListenPort:      row.ListenPort,
			AuthRequired:    row.AuthRequired,
			AuthUsername:    row.AuthUsername,
			AuthPassword:    row.AuthPassword,
			LastRotationAt:  row.LastRotationAt,
			LastServedProxy: lastProxy,
			CreatedAt:       row.CreatedAt,
		})
	}

	return result, nil
}

func DeleteRotatingProxy(userID uint, rotatingProxyID uint64) error {
	if DB == nil {
		return fmt.Errorf("rotating proxy: database connection was not initialised")
	}

	res := DB.Where("user_id = ? AND id = ?", userID, rotatingProxyID).Delete(&domain.RotatingProxy{})
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return ErrRotatingProxyNotFound
	}
	return nil
}

func GetNextRotatingProxy(userID uint, rotatingProxyID uint64) (*dto.RotatingProxyNext, error) {
	if DB == nil {
		return nil, fmt.Errorf("rotating proxy: database connection was not initialised")
	}

	var result *dto.RotatingProxyNext

	err := DB.Transaction(func(tx *gorm.DB) error {
		var entity domain.RotatingProxy
		if err := tx.
			Preload("Protocol").
			Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("user_id = ? AND id = ?", userID, rotatingProxyID).
			First(&entity).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrRotatingProxyNotFound
			}
			return err
		}

		proxies, err := aliveProxiesForProtocol(tx, userID, entity.ProtocolID)
		if err != nil {
			return err
		}

		if len(proxies) == 0 {
			return ErrRotatingProxyNoAliveProxies
		}

		selected := selectNextProxy(proxies, entity.LastProxyID)

		now := time.Now()

		updatePayload := map[string]interface{}{
			"last_proxy_id":    selected.ID,
			"last_rotation_at": now,
		}

		if err := tx.Model(&domain.RotatingProxy{}).
			Where("id = ?", entity.ID).
			Updates(updatePayload).Error; err != nil {
			return err
		}

		result = &dto.RotatingProxyNext{
			ProxyID:  selected.ID,
			IP:       selected.GetIp(),
			Port:     selected.Port,
			Username: selected.Username,
			Password: selected.Password,
			HasAuth:  selected.HasAuth(),
			Protocol: entity.Protocol.Name,
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return result, nil
}

func getAliveProxiesCached(userID uint, protocolID int, cache map[int][]domain.Proxy) ([]domain.Proxy, error) {
	if proxies, ok := cache[protocolID]; ok {
		return proxies, nil
	}

	proxies, err := aliveProxiesForProtocol(DB, userID, protocolID)
	if err != nil {
		return nil, err
	}

	cache[protocolID] = proxies
	return proxies, nil
}

func getProxyAddressCached(userID uint, proxyID uint64, cache map[uint64]string) (string, error) {
	if cached, ok := cache[proxyID]; ok {
		return cached, nil
	}

	proxy, err := fetchUserProxyByID(DB, userID, proxyID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			cache[proxyID] = ""
			return "", nil
		}
		return "", err
	}

	address := proxy.GetFullProxy()
	cache[proxyID] = address
	return address, nil
}

func aliveProxiesForProtocol(tx *gorm.DB, userID uint, protocolID int) ([]domain.Proxy, error) {
	subQuery := tx.
		Model(&domain.ProxyStatistic{}).
		Select("proxy_id, MAX(created_at) AS created_at").
		Where("protocol_id = ?", protocolID).
		Group("proxy_id")

	var proxies []domain.Proxy
	err := tx.
		Model(&domain.Proxy{}).
		Select("proxies.*").
		Joins("JOIN user_proxies up ON up.proxy_id = proxies.id AND up.user_id = ?", userID).
		Joins("JOIN (?) latest_stats ON latest_stats.proxy_id = proxies.id", subQuery).
		Joins("JOIN proxy_statistics ps ON ps.proxy_id = proxies.id AND ps.created_at = latest_stats.created_at AND ps.protocol_id = ?", protocolID).
		Where("ps.alive = ?", true).
		Order("proxies.id").
		Find(&proxies).Error
	if err != nil {
		return nil, err
	}

	return proxies, nil
}

func fetchUserProxyByID(tx *gorm.DB, userID uint, proxyID uint64) (*domain.Proxy, error) {
	var proxy domain.Proxy
	err := tx.
		Model(&domain.Proxy{}).
		Where("proxies.id = ?", proxyID).
		Joins("JOIN user_proxies up ON up.proxy_id = proxies.id AND up.user_id = ?", userID).
		First(&proxy).Error
	if err != nil {
		return nil, err
	}
	return &proxy, nil
}

func selectNextProxy(proxies []domain.Proxy, lastProxyID *uint64) domain.Proxy {
	if lastProxyID == nil {
		return proxies[0]
	}

	for idx := range proxies {
		if proxies[idx].ID == *lastProxyID {
			next := idx + 1
			if next >= len(proxies) {
				next = 0
			}
			return proxies[next]
		}
	}

	return proxies[0]
}

func fetchProtocolByName(tx *gorm.DB, name string) (domain.Protocol, error) {
	var protocol domain.Protocol
	err := tx.
		Model(&domain.Protocol{}).
		Where("LOWER(name) = ?", strings.ToLower(name)).
		First(&protocol).Error
	return protocol, err
}

func isProtocolEnabledForUser(user domain.User, protocolName string) bool {
	switch protocolName {
	case "http":
		return user.HTTPProtocol
	case "https":
		return user.HTTPSProtocol
	case "socks4":
		return user.SOCKS4Protocol
	case "socks5":
		return user.SOCKS5Protocol
	default:
		return false
	}
}

func isUniqueConstraintError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "duplicate key value violates unique constraint")
}

func allocateListenPort(tx *gorm.DB) (uint16, error) {
	start, end := support.GetRotatingProxyPortRange()
	if start <= 0 || end <= 0 {
		return 0, ErrRotatingProxyPortExhausted
	}

	count := end - start + 1
	if count <= 0 {
		return 0, ErrRotatingProxyPortExhausted
	}

	ports := make([]int, 0, count)
	for port := start; port <= end; port++ {
		ports = append(ports, port)
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	r.Shuffle(len(ports), func(i, j int) {
		ports[i], ports[j] = ports[j], ports[i]
	})

	for _, port := range ports {
		var existing int64
		if err := tx.Model(&domain.RotatingProxy{}).
			Where("listen_port = ?", port).
			Count(&existing).Error; err != nil {
			return 0, err
		}
		if existing == 0 {
			return uint16(port), nil
		}
	}

	return 0, ErrRotatingProxyPortExhausted
}

func GetAllRotatingProxies() ([]domain.RotatingProxy, error) {
	if DB == nil {
		return nil, fmt.Errorf("rotating proxy: database connection was not initialised")
	}

	var proxies []domain.RotatingProxy
	if err := DB.
		Preload("Protocol").
		Order("created_at ASC").
		Find(&proxies).Error; err != nil {
		return nil, err
	}

	return proxies, nil
}

func GetRotatingProxyByID(rotatorID uint64) (*domain.RotatingProxy, error) {
	if DB == nil {
		return nil, fmt.Errorf("rotating proxy: database connection was not initialised")
	}

	var proxy domain.RotatingProxy
	if err := DB.
		Preload("Protocol").
		Where("id = ?", rotatorID).
		First(&proxy).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrRotatingProxyNotFound
		}
		return nil, err
	}

	return &proxy, nil
}
