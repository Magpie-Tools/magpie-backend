package database

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"strings"
	"time"

	"magpie/internal/alerts"
	"magpie/internal/api/dto"
	"magpie/internal/domain"
	"magpie/internal/evidence"
	"magpie/internal/platform/pg"
	"magpie/internal/support"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	ErrRotatingProxyNotFound            = errors.New("rotating proxy not found")
	ErrRotatingProxyNameRequired        = errors.New("rotating proxy name is required")
	ErrRotatingProxyNameTooLong         = errors.New("rotating proxy name is too long")
	ErrRotatingProxyNameConflict        = errors.New("rotating proxy name already exists")
	ErrRotatingProxyProtocolMissing     = errors.New("rotating proxy protocol is required")
	ErrRotatingProxyProtocolUnsupported = errors.New("unsupported rotating proxy protocol")
	ErrRotatingProxyNoAliveProxies      = errors.New("no alive proxies are available for the selected protocol")
	ErrRotatingProxyAuthUsernameNeeded  = errors.New("authentication username is required when authentication is enabled")
	ErrRotatingProxyAuthPasswordNeeded  = errors.New("authentication password is required when authentication is enabled")
	ErrRotatingProxyPortExhausted       = errors.New("no available ports for rotating proxies")
	ErrRotatingProxyUptimeTypeInvalid   = errors.New("uptime filter type must be either min or max")
	ErrRotatingProxyUptimeTypeMissing   = errors.New("uptime filter type is required when uptime percentage is set")
	ErrRotatingProxyUptimeValueMissing  = errors.New("uptime percentage is required when uptime filter type is set")
	ErrRotatingProxyUptimeOutOfRange    = errors.New("uptime percentage must be between 0 and 100")
)

const (
	rotatingProxyNameMaxLength = 120
	defaultInstanceRegion      = "Unknown"
)

func CreateRotatingProxy(workspaceID uint, payload dto.RotatingProxyCreateRequest) (*dto.RotatingProxy, error) {
	if pg.DB == nil {
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
	listenProtocolName := strings.ToLower(strings.TrimSpace(payload.ListenProtocol))
	if listenProtocolName == "" {
		listenProtocolName = protocolName
	}
	if !slices.Contains(domain.CheckerProtocols[:], protocolName) || !slices.Contains(domain.CheckerProtocols[:], listenProtocolName) {
		return nil, ErrRotatingProxyProtocolUnsupported
	}
	transportProtocol := support.NormalizeTransportProtocol(payload.TransportProtocol)
	listenTransportProtocol := support.NormalizeTransportProtocol(payload.ListenTransportProtocol)
	if strings.TrimSpace(payload.ListenTransportProtocol) == "" {
		listenTransportProtocol = transportProtocol
	}
	instanceID := strings.TrimSpace(payload.InstanceID)
	if instanceID == "" {
		instanceID = support.GetInstanceID()
	}
	instanceName := strings.TrimSpace(payload.InstanceName)
	if instanceName == "" {
		instanceName = instanceID
	}
	instanceRegion := strings.TrimSpace(payload.InstanceRegion)
	if instanceRegion == "" {
		instanceRegion = defaultInstanceRegion
	}

	if payload.AuthRequired {
		if strings.TrimSpace(payload.AuthUsername) == "" {
			return nil, ErrRotatingProxyAuthUsernameNeeded
		}
		if strings.TrimSpace(payload.AuthPassword) == "" {
			return nil, ErrRotatingProxyAuthPasswordNeeded
		}
	}

	uptimeFilterType, uptimePercentage, err := validateRotatorUptimeFilter(payload.UptimeFilterType, payload.UptimePercentage)
	if err != nil {
		return nil, err
	}

	var result *dto.RotatingProxy

	err = pg.DB.Transaction(func(tx *gorm.DB) error {
		var workspace domain.Workspace
		if err := tx.First(&workspace, workspaceID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("rotating proxy: workspace %d not found", workspaceID)
			}
			return err
		}

		proxyProtocol, err := fetchProtocolByName(tx, protocolName)
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrRotatingProxyProtocolUnsupported
			}
			return err
		}
		listenProtocol, err := fetchProtocolByName(tx, listenProtocolName)
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrRotatingProxyProtocolUnsupported
			}
			return err
		}
		filters := evidence.SanitizeRotatorReputationLabels(payload.ReputationLabels)

		entity := domain.RotatingProxy{
			WorkspaceID:             workspaceID,
			Name:                    name,
			InstanceID:              instanceID,
			InstanceName:            instanceName,
			InstanceRegion:          instanceRegion,
			ProtocolID:              proxyProtocol.ID,
			ListenProtocolID:        listenProtocol.ID,
			TransportProtocol:       transportProtocol,
			ListenTransportProtocol: listenTransportProtocol,
			UptimeFilterType:        uptimeFilterType,
			UptimePercentage:        cloneFloat64Ptr(uptimePercentage),
			AuthRequired:            payload.AuthRequired,
			AuthUsername:            strings.TrimSpace(payload.AuthUsername),
			AuthPassword:            payload.AuthPassword,
			ReputationLabels:        domain.StringList(filters),
		}

		listenPort, err := allocateListenPort(tx, instanceID)
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

		aliveCount, err := evidence.CountAliveProxiesForProtocol(tx, workspaceID, proxyProtocol.ID, filters, uptimeFilterType, uptimePercentage)
		if err != nil {
			return err
		}

		result = &dto.RotatingProxy{
			ID:                      entity.ID,
			Name:                    entity.Name,
			InstanceID:              entity.InstanceID,
			InstanceName:            entity.InstanceName,
			InstanceRegion:          entity.InstanceRegion,
			Protocol:                proxyProtocol.Name,
			ListenProtocol:          listenProtocol.Name,
			TransportProtocol:       transportProtocol,
			ListenTransportProtocol: listenTransportProtocol,
			UptimeFilterType:        uptimeFilterType,
			UptimePercentage:        cloneFloat64Ptr(uptimePercentage),
			AliveProxyCount:         aliveCount,
			ListenPort:              entity.ListenPort,
			AuthRequired:            entity.AuthRequired,
			AuthUsername:            entity.AuthUsername,
			AuthPassword:            strings.TrimSpace(payload.AuthPassword),
			ReputationLabels:        filters,
			CreatedAt:               entity.CreatedAt,
		}

		entity.AuthPassword = ""

		return nil
	})

	if err != nil {
		return nil, err
	}

	return result, nil
}

func ListRotatingProxies(workspaceID uint) ([]dto.RotatingProxy, error) {
	if pg.DB == nil {
		return nil, fmt.Errorf("rotating proxy: database connection was not initialised")
	}

	var rows []domain.RotatingProxy
	if err := pg.DB.
		Preload("Protocol").
		Preload("ListenProtocol").
		Where("workspace_id = ?", workspaceID).
		Order("created_at DESC").
		Find(&rows).Error; err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		return []dto.RotatingProxy{}, nil
	}

	protocolCache := make(map[string]int)
	lastProxyCache := make(map[uint64]string)
	result := make([]dto.RotatingProxy, 0, len(rows))

	for _, row := range rows {
		normalizeRotatingProxyProtocols(&row)
		protocolName := row.Protocol.Name
		listenProtocol := row.ListenProtocol.Name
		transportProtocol := support.NormalizeTransportProtocol(row.TransportProtocol)
		listenTransportProtocol := support.NormalizeTransportProtocol(row.ListenTransportProtocol)
		if strings.TrimSpace(row.ListenTransportProtocol) == "" {
			listenTransportProtocol = transportProtocol
		}
		labels := evidence.SanitizeRotatorReputationLabels(row.ReputationLabels.Clone())
		uptimeFilterType, uptimePercentage := evidence.NormalizeRotatorUptimeFilter(row.UptimeFilterType, row.UptimePercentage)
		instanceID := strings.TrimSpace(row.InstanceID)
		if instanceID == "" {
			instanceID = support.GetInstanceID()
		}
		instanceName := strings.TrimSpace(row.InstanceName)
		if instanceName == "" {
			instanceName = instanceID
		}
		instanceRegion := strings.TrimSpace(row.InstanceRegion)
		if instanceRegion == "" {
			instanceRegion = defaultInstanceRegion
		}
		aliveCount, err := getAliveProxyCountCached(workspaceID, row.ProtocolID, labels, uptimeFilterType, uptimePercentage, protocolCache)
		if err != nil {
			return nil, err
		}

		lastProxy := ""
		if row.LastProxyID != nil {
			lastProxy, err = getProxyAddressCached(workspaceID, *row.LastProxyID, lastProxyCache)
			if err != nil {
				return nil, err
			}
		}

		result = append(result, dto.RotatingProxy{
			ID:                      row.ID,
			Name:                    row.Name,
			InstanceID:              instanceID,
			InstanceName:            instanceName,
			InstanceRegion:          instanceRegion,
			Protocol:                protocolName,
			ListenProtocol:          listenProtocol,
			TransportProtocol:       transportProtocol,
			ListenTransportProtocol: listenTransportProtocol,
			UptimeFilterType:        uptimeFilterType,
			UptimePercentage:        cloneFloat64Ptr(uptimePercentage),
			AliveProxyCount:         aliveCount,
			ListenPort:              row.ListenPort,
			AuthRequired:            row.AuthRequired,
			AuthUsername:            row.AuthUsername,
			AuthPassword:            row.AuthPassword,
			LastRotationAt:          row.LastRotationAt,
			LastServedProxy:         lastProxy,
			ReputationLabels:        labels,
			CreatedAt:               row.CreatedAt,
		})
	}

	return result, nil
}

func DeleteRotatingProxy(workspaceID uint, rotatingProxyID uint64) error {
	if pg.DB == nil {
		return fmt.Errorf("rotating proxy: database connection was not initialised")
	}

	return pg.DB.Transaction(func(tx *gorm.DB) error {
		var workspace domain.Workspace
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Select("id").First(&workspace, workspaceID).Error; err != nil {
			return err
		}
		var rules []domain.AlertRule
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("workspace_id = ? AND rotator_id = ?", workspaceID, rotatingProxyID).Find(&rules).Error; err != nil {
			return err
		}
		for _, rule := range rules {
			if err := alerts.CloseAlertForConfiguration(tx, &rule, time.Now().UTC()); err != nil {
				return err
			}
			if err := tx.Model(&rule).Updates(map[string]any{"enabled": false, "status": "disabled", "unknown_reason": "Rotator deleted", "active_incident_id": nil, "breach_since": nil, "recovery_since": nil, "revision": gorm.Expr("revision + 1")}).Error; err != nil {
				return err
			}
		}
		res := tx.Where("workspace_id = ? AND id = ?", workspaceID, rotatingProxyID).Delete(&domain.RotatingProxy{})
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			return ErrRotatingProxyNotFound
		}
		return nil
	})
}

// GetNextRotatingProxy selects a qualifying route for the workspace.
// Locking the rotator row serializes concurrent cursor updates.
// Selection uses committed health evidence and advances the cursor before the
// listener connects upstream; a failed connection does not undo the selection.
// This call neither performs a live check nor reserves the upstream exclusively.
func GetNextRotatingProxy(workspaceID uint, rotatingProxyID uint64) (*dto.RotatingProxyNext, error) {
	if pg.DB == nil {
		return nil, fmt.Errorf("rotating proxy: database connection was not initialised")
	}

	var result *dto.RotatingProxyNext

	err := pg.DB.Transaction(func(tx *gorm.DB) error {
		var entity domain.RotatingProxy
		if err := tx.
			Preload("Protocol").
			Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("workspace_id = ? AND id = ?", workspaceID, rotatingProxyID).
			First(&entity).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrRotatingProxyNotFound
			}
			return err
		}

		labels := evidence.SanitizeRotatorReputationLabels(entity.ReputationLabels.Clone())
		uptimeFilterType, uptimePercentage := evidence.NormalizeRotatorUptimeFilter(entity.UptimeFilterType, entity.UptimePercentage)
		selected, err := nextAliveProxyForProtocol(tx, workspaceID, entity.ProtocolID, labels, uptimeFilterType, uptimePercentage, entity.LastProxyID)
		if err != nil {
			return err
		}

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

func getAliveProxyCountCached(workspaceID uint, protocolID int, labels []string, uptimeFilterType string, uptimePercentage *float64, cache map[string]int) (int, error) {
	normLabels := evidence.SanitizeRotatorReputationLabels(labels)
	cacheKey := evidence.BuildAliveProxyCacheKey(protocolID, normLabels, uptimeFilterType, uptimePercentage)
	if count, ok := cache[cacheKey]; ok {
		return count, nil
	}
	count, err := evidence.CountAliveProxiesForProtocol(pg.DB, workspaceID, protocolID, normLabels, uptimeFilterType, uptimePercentage)
	if err != nil {
		return 0, err
	}
	cache[cacheKey] = count
	return count, nil
}

func getProxyAddressCached(workspaceID uint, proxyID uint64, cache map[uint64]string) (string, error) {
	if cached, ok := cache[proxyID]; ok {
		return cached, nil
	}

	proxy, err := fetchWorkspaceProxyByID(pg.DB, workspaceID, proxyID)
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

func aliveProxiesForProtocol(tx *gorm.DB, workspaceID uint, protocolID int, labels []string, uptimeFilterType string, uptimePercentage *float64) ([]domain.Proxy, error) {
	query := evidence.BuildAliveProxyQuery(tx, workspaceID, protocolID, labels, uptimeFilterType, uptimePercentage)

	var proxies []domain.Proxy
	err := query.
		Order("proxies.id").
		Find(&proxies).Error
	if err != nil {
		return nil, err
	}
	if err := hydrateProxyCredentials(tx, proxies, workspaceID); err != nil {
		return nil, err
	}

	return proxies, nil
}

func nextAliveProxyForProtocol(tx *gorm.DB, workspaceID uint, protocolID int, labels []string, uptimeFilterType string, uptimePercentage *float64, lastProxyID *uint64) (*domain.Proxy, error) {
	baseQuery := evidence.BuildAliveProxyQuery(tx, workspaceID, protocolID, labels, uptimeFilterType, uptimePercentage)

	if lastProxyID != nil {
		nextAfterCursor, err := fetchAliveProxyCandidate(baseQuery, *lastProxyID, true, workspaceID)
		if err == nil {
			return nextAfterCursor, nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, err
		}
	}

	first, err := fetchAliveProxyCandidate(baseQuery, 0, false, workspaceID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrRotatingProxyNoAliveProxies
		}
		return nil, err
	}
	return first, nil
}

func fetchAliveProxyCandidate(baseQuery *gorm.DB, cursorProxyID uint64, applyCursor bool, workspaceID uint) (*domain.Proxy, error) {
	query := baseQuery.Session(&gorm.Session{})
	if applyCursor {
		query = query.Where("proxies.id > ?", cursorProxyID)
	}

	var proxy domain.Proxy
	if err := query.Order("proxies.id").Limit(1).First(&proxy).Error; err != nil {
		return nil, err
	}
	if err := hydrateProxyCredential(query, &proxy, workspaceID); err != nil {
		return nil, err
	}

	return &proxy, nil
}

func fetchWorkspaceProxyByID(tx *gorm.DB, workspaceID uint, proxyID uint64) (*domain.Proxy, error) {
	var proxy domain.Proxy
	err := tx.
		Model(&domain.Proxy{}).
		Where("proxies.id = ?", proxyID).
		Joins("JOIN user_proxies up ON up.proxy_id = proxies.id AND up.workspace_id = ? AND up.state = ?", workspaceID, domain.ManagedProxyStateActive).
		First(&proxy).Error
	if err != nil {
		return nil, err
	}
	if err := hydrateProxyCredential(tx, &proxy, workspaceID); err != nil {
		return nil, err
	}
	return &proxy, nil
}

func fetchProtocolByName(tx *gorm.DB, name string) (domain.Protocol, error) {
	var protocol domain.Protocol
	err := tx.
		Model(&domain.Protocol{}).
		Where("LOWER(name) = ?", strings.ToLower(name)).
		First(&protocol).Error
	return protocol, err
}

func isUniqueConstraintError(err error) bool {
	if err == nil {
		return false
	}
	normalized := strings.ToLower(err.Error())
	return strings.Contains(normalized, "duplicate key value violates unique constraint") ||
		strings.Contains(normalized, "unique constraint failed")
}

func allocateListenPort(tx *gorm.DB, instanceID string) (uint16, error) {
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
			Where("instance_id = ? AND listen_port = ?", instanceID, port).
			Count(&existing).Error; err != nil {
			return 0, err
		}
		if existing == 0 {
			return uint16(port), nil
		}
	}

	return 0, ErrRotatingProxyPortExhausted
}

func validateRotatorUptimeFilter(rawType string, rawPercentage *float64) (string, *float64, error) {
	filterType := strings.ToLower(strings.TrimSpace(rawType))
	hasType := filterType != ""
	hasPercentage := rawPercentage != nil

	if !hasType && !hasPercentage {
		return "", nil, nil
	}
	if !hasType && hasPercentage {
		return "", nil, ErrRotatingProxyUptimeTypeMissing
	}
	if hasType && !hasPercentage {
		return "", nil, ErrRotatingProxyUptimeValueMissing
	}
	if filterType != evidence.UptimeFilterMin && filterType != evidence.UptimeFilterMax {
		return "", nil, ErrRotatingProxyUptimeTypeInvalid
	}

	value := *rawPercentage
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return "", nil, ErrRotatingProxyUptimeOutOfRange
	}
	if value < 0 || value > 100 {
		return "", nil, ErrRotatingProxyUptimeOutOfRange
	}

	return filterType, new(math.Round(value*10) / 10), nil
}

func cloneFloat64Ptr(value *float64) *float64 {
	if value == nil {
		return nil
	}
	return new(*value)
}

func GetAllRotatingProxies() ([]domain.RotatingProxy, error) {
	if pg.DB == nil {
		return nil, fmt.Errorf("rotating proxy: database connection was not initialised")
	}

	instanceID := support.GetInstanceID()

	var proxies []domain.RotatingProxy
	if err := pg.DB.
		Preload("Protocol").
		Preload("ListenProtocol").
		Where("instance_id = ?", instanceID).
		Order("created_at ASC").
		Find(&proxies).Error; err != nil {
		return nil, err
	}

	for idx := range proxies {
		normalizeRotatingProxyProtocols(&proxies[idx])
	}

	return proxies, nil
}

func CountRotatingProxiesByInstanceIDs(instanceIDs []string) (map[string]int, error) {
	if pg.DB == nil {
		return nil, fmt.Errorf("rotating proxy: database connection was not initialised")
	}

	cleanIDs := make([]string, 0, len(instanceIDs))
	seen := make(map[string]struct{}, len(instanceIDs))
	for _, raw := range instanceIDs {
		value := strings.TrimSpace(raw)
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		cleanIDs = append(cleanIDs, value)
	}
	if len(cleanIDs) == 0 {
		return map[string]int{}, nil
	}

	type instanceCount struct {
		InstanceID string `gorm:"column:instance_id"`
		Count      int64  `gorm:"column:count"`
	}
	var rows []instanceCount
	if err := pg.DB.
		Model(&domain.RotatingProxy{}).
		Select("instance_id, COUNT(*) AS count").
		Where("instance_id IN ?", cleanIDs).
		Group("instance_id").
		Scan(&rows).Error; err != nil {
		return nil, err
	}

	result := make(map[string]int, len(rows))
	for _, row := range rows {
		id := strings.TrimSpace(row.InstanceID)
		if id == "" {
			continue
		}
		result[id] = int(row.Count)
	}
	return result, nil
}

func GetRotatingProxyByID(rotatorID uint64) (*domain.RotatingProxy, error) {
	if pg.DB == nil {
		return nil, fmt.Errorf("rotating proxy: database connection was not initialised")
	}

	instanceID := support.GetInstanceID()

	var proxy domain.RotatingProxy
	if err := pg.DB.
		Preload("Protocol").
		Preload("ListenProtocol").
		Where("id = ? AND instance_id = ?", rotatorID, instanceID).
		First(&proxy).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrRotatingProxyNotFound
		}
		return nil, err
	}

	normalizeRotatingProxyProtocols(&proxy)

	return &proxy, nil
}

func normalizeRotatingProxyProtocols(rotator *domain.RotatingProxy) {
	if rotator == nil {
		return
	}

	if rotator.ListenProtocolID == 0 {
		rotator.ListenProtocolID = rotator.ProtocolID
	}

	if strings.TrimSpace(rotator.ListenProtocol.Name) == "" {
		rotator.ListenProtocol = rotator.Protocol
	}

	transportProtocol := support.NormalizeTransportProtocol(rotator.TransportProtocol)
	listenTransportProtocol := support.NormalizeTransportProtocol(rotator.ListenTransportProtocol)
	if strings.TrimSpace(rotator.ListenTransportProtocol) == "" {
		listenTransportProtocol = transportProtocol
	}
	uptimeFilterType, uptimePercentage := evidence.NormalizeRotatorUptimeFilter(rotator.UptimeFilterType, rotator.UptimePercentage)

	rotator.TransportProtocol = transportProtocol
	rotator.ListenTransportProtocol = listenTransportProtocol
	rotator.UptimeFilterType = uptimeFilterType
	rotator.UptimePercentage = cloneFloat64Ptr(uptimePercentage)
	if strings.TrimSpace(rotator.InstanceID) == "" {
		rotator.InstanceID = support.GetInstanceID()
	}
	if strings.TrimSpace(rotator.InstanceName) == "" {
		rotator.InstanceName = rotator.InstanceID
	}
	if strings.TrimSpace(rotator.InstanceRegion) == "" {
		rotator.InstanceRegion = defaultInstanceRegion
	}
}
