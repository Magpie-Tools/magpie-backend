package database

import (
	"slices"
	"sort"

	"magpie/internal/api/dto"
	"magpie/internal/domain"

	"gorm.io/gorm"
)

// proxyProtocolRecentChecks is how many past verdicts each protocol carries.
const proxyProtocolRecentChecks = 20

// proxyProtocolHealth turns the workspace's current latest statistics for one
// proxy into one entry per protocol, ordered by protocol, each with the
// verdicts of its most recent checks under the same settings.
func proxyProtocolHealth(db *gorm.DB, workspaceID uint, proxyID uint64, current []domain.ProxyLatestStatistic) ([]dto.ProxyProtocolHealth, error) {
	health := make([]dto.ProxyProtocolHealth, 0, len(current))
	if len(current) == 0 {
		return health, nil
	}
	entries := slices.Clone(current)
	sort.Slice(entries, func(i, j int) bool { return entries[i].ProtocolID < entries[j].ProtocolID })

	protocolIDs := make([]int, 0, len(entries))
	levelIDs := make([]int, 0, len(entries))
	for _, entry := range entries {
		protocolIDs = append(protocolIDs, entry.ProtocolID)
		if entry.LevelID != nil {
			levelIDs = append(levelIDs, *entry.LevelID)
		}
	}
	var protocols []domain.Protocol
	if err := db.Where("id IN ?", protocolIDs).Find(&protocols).Error; err != nil {
		return nil, err
	}
	protocolNames := make(map[int]string, len(protocols))
	for _, protocol := range protocols {
		protocolNames[protocol.ID] = protocol.Name
	}
	levelNames := map[int]string{}
	if len(levelIDs) > 0 {
		var levels []domain.AnonymityLevel
		if err := db.Where("id IN ?", levelIDs).Find(&levels).Error; err != nil {
			return nil, err
		}
		for _, level := range levels {
			levelNames[level.ID] = level.Name
		}
	}

	sqlite := db.Dialector.Name() == "sqlite"
	for _, entry := range entries {
		recent, err := recentProtocolVerdicts(db, sqlite, workspaceID, proxyID, entry)
		if err != nil {
			return nil, err
		}
		level := "Unknown"
		if entry.LevelID != nil {
			level = normaliseDisplayValue(levelNames[*entry.LevelID], "Unknown")
		}
		health = append(health, dto.ProxyProtocolHealth{
			Protocol:       normaliseDisplayValue(protocolNames[entry.ProtocolID], "Unknown"),
			Transport:      entry.TransportProtocol,
			Alive:          entry.Alive,
			ResponseTime:   entry.ResponseTime,
			Attempt:        entry.Attempt,
			AnonymityLevel: level,
			CheckedAt:      entry.CheckedAt,
			Recent:         recent,
		})
	}
	return health, nil
}

// recentProtocolVerdicts reads the workspace's verdicts for the protocol's
// latest checks, oldest first. Checks under other settings are skipped so the
// row matches the current result; legacy evidence without a configuration key
// falls back to every check this workspace can see.
func recentProtocolVerdicts(db *gorm.DB, sqlite bool, workspaceID uint, proxyID uint64, entry domain.ProxyLatestStatistic) ([]bool, error) {
	query := db.Table("proxy_statistics").
		Select("proxy_statistics.id", "proxy_statistics.alive", "proxy_statistics.check_evidence", "proxy_statistics.created_at").
		Where("proxy_statistics.proxy_id = ? AND proxy_statistics.protocol_id = ?", proxyID, entry.ProtocolID)
	if entry.ConfigKey != "" {
		expr, vars := statisticCurrentConfigExpr(sqlite, workspaceID, map[int]string{entry.ProtocolID: entry.ConfigKey})
		query = query.Where(expr, vars...)
	} else {
		query = filterWorkspaceStatisticEvidence(query, workspaceID)
	}
	var rows []domain.ProxyStatistic
	if err := query.Order("proxy_statistics.created_at DESC, proxy_statistics.id DESC").Limit(proxyProtocolRecentChecks).Find(&rows).Error; err != nil {
		return nil, err
	}
	verdicts := make([]bool, len(rows))
	for index, row := range rows {
		alive := row.Alive
		for _, evidence := range row.CheckEvidence {
			if evidence.WorkspaceID == workspaceID {
				alive = evidence.Alive
				break
			}
		}
		verdicts[len(rows)-1-index] = alive
	}
	return verdicts, nil
}
