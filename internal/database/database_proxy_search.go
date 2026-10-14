package database

import (
	"net"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"magpie/internal/api/dto"

	"gorm.io/gorm"
)

const proxyTagSearchExistsSQL = "EXISTS (SELECT 1 FROM proxy_tag_assignments pta JOIN proxy_tags pt ON pt.id = pta.proxy_tag_id AND pt.workspace_id = pta.workspace_id WHERE pta.workspace_id = ufi.workspace_id AND pta.proxy_id = ufi.proxy_id AND pt.name_key LIKE ?)"

func isLikelyProxyIPSearch(search string) bool {
	_, _, _, ok := buildIPSearchNetwork(search)
	return ok
}

func buildIPSearchNetwork(search string) (network string, fallbackPrefix string, exact bool, ok bool) {
	search = strings.TrimSpace(search)
	if search == "" {
		return "", "", false, false
	}

	if host, _, err := net.SplitHostPort(search); err == nil {
		search = host
	} else if strings.HasPrefix(search, "[") && strings.HasSuffix(search, "]") {
		search = strings.TrimSpace(search[1 : len(search)-1])
	}

	if prefix, err := netip.ParsePrefix(search); err == nil {
		prefix = prefix.Masked()
		address := prefix.Addr().Unmap()
		if address.Is4() && prefix.Addr().Is6() {
			return "", "", false, false
		}
		exact = prefix.Bits() == prefix.Addr().BitLen()
		fallbackPrefix = address.String()
		if !exact {
			fallbackPrefix = strings.TrimSuffix(fallbackPrefix, "::")
		}
		return prefix.String(), fallbackPrefix, exact, true
	}

	if address, err := netip.ParseAddr(search); err == nil {
		address = address.Unmap()
		return netip.PrefixFrom(address, address.BitLen()).String(), address.String(), true, true
	}

	parts := strings.Split(search, ".")
	for len(parts) > 0 && parts[len(parts)-1] == "" {
		parts = parts[:len(parts)-1]
	}
	if len(parts) == 0 || len(parts) > 4 {
		return "", "", false, false
	}

	octets := make([]int, 0, len(parts))
	for _, part := range parts {
		if part == "" {
			return "", "", false, false
		}
		value, err := strconv.Atoi(part)
		if err != nil || value < 0 || value > 255 {
			return "", "", false, false
		}
		octets = append(octets, value)
	}

	canonicalParts := make([]string, 4)
	for index := 0; index < 4; index++ {
		if index < len(octets) {
			canonicalParts[index] = strconv.Itoa(octets[index])
		} else {
			canonicalParts[index] = "0"
		}
	}

	fallbackParts := make([]string, len(octets))
	for index, octet := range octets {
		fallbackParts[index] = strconv.Itoa(octet)
	}
	exact = len(octets) == 4
	return strings.Join(canonicalParts, ".") + "/" + strconv.Itoa(len(octets)*8), strings.Join(fallbackParts, "."), exact, true
}

func buildProxySearchIDQuery(workspaceID uint, filterQuery *gorm.DB, lowerSearch string) *gorm.DB {
	query := DB.Table("user_proxy_filter_indexes ufi").
		Select("ufi.proxy_id AS id").
		Where("ufi.workspace_id = ?", workspaceID)

	if filterQuery != nil {
		query = query.Where("ufi.proxy_id IN (?)", filterQuery)
	}

	query = applyProxySearchQuery(query, lowerSearch)
	return query.Group("ufi.proxy_id")
}

func buildProxyIPSearchIDQuery(workspaceID uint, filterQuery *gorm.DB, network, fallbackPrefix, lowerSearch string, exact bool) *gorm.DB {
	query := DB.Table("user_proxy_filter_indexes ufi").
		Select("ufi.proxy_id AS id").
		Where("ufi.workspace_id = ?", workspaceID)
	tagPattern := "%" + strings.ToLower(strings.TrimSpace(lowerSearch)) + "%"
	if isPostgresDialect(DB) {
		query = query.Where("(ufi.ip_address <<= ?::cidr OR "+proxyTagSearchExistsSQL+")", network, tagPattern)
	} else if exact {
		query = query.Where("(ufi.ip_address = ? OR "+proxyTagSearchExistsSQL+")", fallbackPrefix, tagPattern)
	} else {
		query = query.Where("(ufi.ip_address LIKE ? OR "+proxyTagSearchExistsSQL+")", fallbackPrefix+"%", tagPattern)
	}

	if filterQuery != nil {
		query = query.Where("ufi.proxy_id IN (?)", filterQuery)
	}

	return query.Group("ufi.proxy_id")
}

func applyProxySearchQuery(query *gorm.DB, lowerSearch string) *gorm.DB {
	whereSQL, args := buildProxySearchPredicate(lowerSearch)
	if whereSQL == "" || len(args) == 0 {
		return query
	}
	return query.Where(whereSQL, args...)
}

func buildProxySearchPredicate(lowerSearch string) (string, []interface{}) {
	lowerSearch = strings.ToLower(strings.TrimSpace(lowerSearch))
	if lowerSearch == "" {
		return "", nil
	}

	pattern := "%" + lowerSearch + "%"
	conditions := []string{
		"LOWER(ufi.host) LIKE ?",
		"ufi.type_key LIKE ?",
		"ufi.country_key LIKE ?",
		"ufi.anonymity_key LIKE ?",
		"ufi.reputation_label LIKE ?",
		proxyTagSearchExistsSQL,
	}
	args := []interface{}{
		pattern,
		pattern,
		pattern,
		pattern,
		pattern,
		pattern,
	}

	switch lowerSearch {
	case "alive":
		conditions = append(conditions, "ufi.alive = ?")
		args = append(args, true)
	case "dead":
		conditions = append(conditions, "ufi.alive = ?")
		args = append(args, false)
	}

	if numericValue, err := strconv.ParseUint(lowerSearch, 10, 16); err == nil {
		conditions = append(conditions, "ufi.port = ?")
		args = append(args, uint16(numericValue))
		conditions = append(conditions, "ufi.response_time = ?")
		args = append(args, uint16(numericValue))
	}

	return "(" + strings.Join(conditions, " OR ") + ")", args
}

func filterProxiesBySearch(proxies []dto.ProxyInfo, search string) []dto.ProxyInfo {
	if search == "" {
		return proxies
	}

	filtered := make([]dto.ProxyInfo, 0, len(proxies))
	for _, proxy := range proxies {
		if proxyMatchesSearch(proxy, search) {
			filtered = append(filtered, proxy)
		}
	}

	return filtered
}

func proxyMatchesSearch(proxy dto.ProxyInfo, search string) bool {
	lowerSearch := strings.ToLower(strings.TrimSpace(search))
	if lowerSearch == "" {
		return true
	}

	ipLower := strings.ToLower(proxy.IP)
	if strings.Contains(ipLower, lowerSearch) || strings.Contains(lowerSearch, ipLower) {
		return true
	}

	portStr := strconv.FormatUint(uint64(proxy.Port), 10)
	if strings.Contains(portStr, lowerSearch) || strings.Contains(lowerSearch, portStr) {
		return true
	}

	fields := []string{
		strings.ToLower(proxy.EstimatedType),
		strings.ToLower(proxy.Country),
		strings.ToLower(proxy.AnonymityLevel),
	}
	for _, tag := range proxy.Tags {
		name := strings.ToLower(strings.TrimSpace(tag.Name))
		if name != "" && (strings.Contains(name, lowerSearch) || strings.Contains(lowerSearch, name)) {
			return true
		}
	}

	for _, field := range fields {
		if field == "" {
			continue
		}
		if strings.Contains(field, lowerSearch) || strings.Contains(lowerSearch, field) {
			return true
		}
	}

	responseStr := strconv.Itoa(int(proxy.ResponseTime))
	if strings.Contains(responseStr, lowerSearch) || strings.Contains(lowerSearch, responseStr) {
		return true
	}

	aliveLabel := "dead"
	if proxy.Alive {
		aliveLabel = "alive"
	}
	if strings.Contains(aliveLabel, lowerSearch) || strings.Contains(lowerSearch, aliveLabel) {
		return true
	}

	if !proxy.LatestCheck.IsZero() {
		timestamp := strings.ToLower(proxy.LatestCheck.Format(time.RFC3339))
		if strings.Contains(timestamp, lowerSearch) || strings.Contains(lowerSearch, timestamp) {
			return true
		}
	}

	if proxy.Reputation != nil {
		if proxy.Reputation.Overall != nil {
			label := strings.ToLower(strings.TrimSpace(proxy.Reputation.Overall.Label))
			if label != "" && (strings.Contains(label, lowerSearch) || strings.Contains(lowerSearch, label)) {
				return true
			}

			score := strings.TrimSpace(strconv.FormatFloat(float64(proxy.Reputation.Overall.Score), 'f', -1, 32))
			if score != "" && (strings.Contains(score, lowerSearch) || strings.Contains(lowerSearch, score)) {
				return true
			}
		}

		for protocol, rep := range proxy.Reputation.Protocols {
			protocolLower := strings.ToLower(strings.TrimSpace(protocol))
			if protocolLower != "" && (strings.Contains(protocolLower, lowerSearch) || strings.Contains(lowerSearch, protocolLower)) {
				return true
			}

			label := strings.ToLower(strings.TrimSpace(rep.Label))
			if label != "" && (strings.Contains(label, lowerSearch) || strings.Contains(lowerSearch, label)) {
				return true
			}

			score := strings.TrimSpace(strconv.FormatFloat(float64(rep.Score), 'f', -1, 32))
			if score != "" && (strings.Contains(score, lowerSearch) || strings.Contains(lowerSearch, score)) {
				return true
			}
		}
	}

	return false
}
