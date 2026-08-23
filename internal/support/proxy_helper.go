package support

import (
	"fmt"
	"magpie/internal/config"
	"magpie/internal/domain"
	"net/netip"
	"regexp"
	"strconv"
	"strings"
)

func ParseTextToProxies(text string) []domain.Proxy {
	proxies, _ := parseTextToProxiesWithStats(text, proxyParseOptions{
		allowIPv6:       true,
		allowColonAuth:  true,
		allowSuffixAuth: true,
	})
	return proxies
}

type ProxyParseStats struct {
	SubmittedCount     int
	ParsedCount        int
	InvalidFormatCount int
	InvalidIPCount     int
	InvalidIPv4Count   int
	InvalidPortCount   int
}

func ParseTextToProxiesWithStats(text string) ([]domain.Proxy, ProxyParseStats) {
	return parseTextToProxiesWithStats(text, proxyParseOptions{
		allowIPv6:       true,
		allowColonAuth:  true,
		allowSuffixAuth: true,
	})
}

// ParseScrapedTextToIPv4Proxies keeps the proxy scraper on its current IPv4-only
// contract. Credentials are read only from user:pass@host:port entries so page
// text with extra colon-delimited fields is not mistaken for authentication.
func ParseScrapedTextToIPv4Proxies(text string) []domain.Proxy {
	proxies, _ := parseTextToProxiesWithStats(text, proxyParseOptions{
		allowIPv6: false,
	})
	return proxies
}

type proxyParseOptions struct {
	allowIPv6       bool
	allowColonAuth  bool
	allowSuffixAuth bool
}

func parseTextToProxiesWithStats(text string, options proxyParseOptions) ([]domain.Proxy, ProxyParseStats) {
	text = clearProxyString(text)

	lines := strings.Split(text, "\n")
	proxies := make([]domain.Proxy, 0, len(lines))
	stats := ProxyParseStats{}

	for _, raw := range lines {
		line := strings.TrimSpace(raw)
		if line == "" {
			continue
		}
		stats.SubmittedCount++

		parsedLine, ok := parseProxyLine(line, options)
		if !ok {
			stats.InvalidFormatCount++
			continue
		}

		parsedIP, err := parseProxyIP(parsedLine.host)
		if err != nil {
			stats.InvalidIPCount++
			continue
		}
		if parsedIP.Is6() && !options.allowIPv6 {
			stats.InvalidIPv4Count++
			continue
		}
		ip := parsedIP.String()

		port, err := strconv.Atoi(parsedLine.port)
		if err != nil || port < 1 || port > 65535 {
			stats.InvalidPortCount++
			continue
		}

		proxy := domain.Proxy{
			Port:     uint16(port),
			Username: parsedLine.username,
			Password: parsedLine.password,
		}

		if err := proxy.SetIP(ip); err != nil {
			stats.InvalidIPCount++
			continue
		}

		proxies = append(proxies, proxy)
		stats.ParsedCount++
	}

	return proxies, stats
}

type parsedProxyLine struct {
	host     string
	port     string
	username string
	password string
}

func parseProxyLine(line string, options proxyParseOptions) (parsedProxyLine, bool) {
	line = strings.TrimSpace(line)
	if line == "" {
		return parsedProxyLine{}, false
	}

	if at := strings.LastIndex(line, "@"); at >= 0 {
		left := strings.TrimSpace(line[:at])
		right := strings.TrimSpace(line[at+1:])

		if host, port, tail, ok := splitProxyHostPort(right); ok && tail == "" && isProxyIP(host) {
			username, password, credentialsOK := splitProxyCredentials(left)
			if !credentialsOK {
				return parsedProxyLine{}, false
			}
			return parsedProxyLine{host: host, port: port, username: username, password: password}, true
		}

		if options.allowSuffixAuth {
			if host, port, tail, ok := splitProxyHostPort(left); ok && tail == "" && isProxyIP(host) {
				username, password, credentialsOK := splitProxyCredentials(right)
				if !credentialsOK {
					return parsedProxyLine{}, false
				}
				return parsedProxyLine{host: host, port: port, username: username, password: password}, true
			}
		}

		return parsedProxyLine{}, false
	}

	host, port, tail, ok := splitProxyHostPort(line)
	if !ok {
		return parsedProxyLine{}, false
	}

	parsed := parsedProxyLine{host: host, port: port}
	if tail == "" || !options.allowColonAuth {
		return parsed, true
	}

	username, password, credentialsOK := splitProxyCredentials(tail)
	if !credentialsOK {
		return parsedProxyLine{}, false
	}
	parsed.username = username
	parsed.password = password
	return parsed, true
}

func splitProxyHostPort(value string) (host string, port string, tail string, ok bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", "", "", false
	}

	if strings.HasPrefix(value, "[") {
		closingBracket := strings.IndexByte(value, ']')
		if closingBracket <= 1 || closingBracket+1 >= len(value) || value[closingBracket+1] != ':' {
			return "", "", "", false
		}
		host = strings.TrimSpace(value[1:closingBracket])
		value = value[closingBracket+2:]
	} else {
		separator := strings.IndexByte(value, ':')
		if separator <= 0 || separator+1 >= len(value) {
			return "", "", "", false
		}
		host = strings.TrimSpace(value[:separator])
		value = value[separator+1:]
	}

	if next := strings.IndexByte(value, ':'); next >= 0 {
		port = strings.TrimSpace(value[:next])
		tail = strings.TrimSpace(value[next+1:])
	} else {
		port = strings.TrimSpace(value)
	}

	if host == "" || port == "" {
		return "", "", "", false
	}
	return host, port, tail, true
}

func splitProxyCredentials(value string) (username string, password string, ok bool) {
	parts := strings.SplitN(strings.TrimSpace(value), ":", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]), true
}

func clearProxyString(proxies string) string {
	proxies = strings.ReplaceAll(proxies, "\r", "")

	proxies = strings.ReplaceAll(proxies, "..", ".0.")
	proxies = strings.ReplaceAll(proxies, ".:", ".0:")

	return proxies
}

func normalizeIPv4(value string) string {
	parts := strings.Split(value, ".")
	if len(parts) != 4 {
		return value
	}

	normalized := make([]string, 0, 4)
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return value
		}
		num, err := strconv.Atoi(part)
		if err != nil || num < 0 || num > 255 {
			return value
		}
		normalized = append(normalized, strconv.Itoa(num))
	}

	return strings.Join(normalized, ".")
}

func parseProxyIP(value string) (netip.Addr, error) {
	value = normalizeIPv4(strings.TrimSpace(value))
	addr, err := netip.ParseAddr(value)
	if err != nil {
		return netip.Addr{}, err
	}
	return addr.Unmap(), nil
}

func isProxyIP(value string) bool {
	_, err := parseProxyIP(value)
	return err == nil
}

var ipCandidateRegex = regexp.MustCompile(`[0-9A-Fa-f:.]+`)

// FindIP identifies the first IP address (IPv4 or IPv6) in a given string.
func FindIP(input string) string {
	for _, candidate := range ipCandidateRegex.FindAllString(input, -1) {
		addr, err := parseProxyIP(candidate)
		if err == nil {
			return addr.String()
		}
	}
	return ""
}

func GetProxyLevel(html string) int {
	//When the headers contain UserIp proxy is transparent
	if strings.Contains(html, config.GetCurrentIp()) {
		return 3
	}

	//When containing one of these headers the proxy is anonymous
	cfg := config.GetConfig()
	for _, header := range cfg.Checker.ProxyHeader {
		if strings.Contains(html, header) {
			return 2
		}
	}

	//Proxy is elite
	return 1
}

func FormatProxy(proxy domain.Proxy, outputFormat string) string {
	protocolName := ""
	aliveValue := "false"
	timeValue := "0"

	if latestStat := latestStatistic(proxy.Statistics); latestStat != nil {
		protocolName = getProtocolName(latestStat)
		timeValue = strconv.Itoa(int(latestStat.ResponseTime))
	}

	aliveValue = strconv.FormatBool(overallAliveFromStatistics(proxy.Statistics))

	reputationLabel, reputationScore := resolveReputationForExport(proxy.Reputations, protocolName)

	replacements := []string{
		"ip:port", proxy.GetFullProxy(),
		"protocol", protocolName,
		"ip", proxy.GetIp(),
		"port", fmt.Sprintf("%d", proxy.Port),
		"username", proxy.Username,
		"password", proxy.Password,
		"country", proxy.Country,
		"alive", aliveValue,
		"type", proxy.EstimatedType,
		"time", timeValue,
		"reputation_score", reputationScore,
		"reputation_label", reputationLabel,
		"reputation", reputationLabel,
	}

	return strings.NewReplacer(replacements...).Replace(outputFormat)
}

// FormatProxies formats the list of proxies according to the specified output format
func FormatProxies(proxies []domain.Proxy, outputFormat string) string {
	var result strings.Builder

	for _, proxy := range proxies {
		result.WriteString(FormatProxy(proxy, outputFormat))
		result.WriteString("\n")
	}

	return result.String()
}

// Helper function to get protocol name from statistics
func getProtocolName(stat *domain.ProxyStatistic) string {
	if stat == nil || stat.Protocol.Name == "" {
		return ""
	}
	return stat.Protocol.Name
}

func latestStatistic(stats []domain.ProxyStatistic) *domain.ProxyStatistic {
	if len(stats) == 0 {
		return nil
	}

	latest := &stats[0]
	for i := 1; i < len(stats); i++ {
		candidate := &stats[i]
		if candidate.CreatedAt.After(latest.CreatedAt) {
			latest = candidate
			continue
		}
		if candidate.CreatedAt.Equal(latest.CreatedAt) && candidate.ID > latest.ID {
			latest = candidate
		}
	}
	return latest
}

func overallAliveFromStatistics(stats []domain.ProxyStatistic) bool {
	if len(stats) == 0 {
		return false
	}

	const maxProtocols = 4
	seen := make(map[string]struct{}, maxProtocols)

	for _, stat := range stats {
		key := protocolKeyForStat(stat)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		if stat.Alive {
			return true
		}
		if len(seen) >= maxProtocols {
			return false
		}
	}

	return false
}

func protocolKeyForStat(stat domain.ProxyStatistic) string {
	if stat.ProtocolID > 0 {
		return fmt.Sprintf("id:%d", stat.ProtocolID)
	}
	if stat.Protocol.Name != "" {
		return strings.ToLower(strings.TrimSpace(stat.Protocol.Name))
	}
	return "unknown"
}

func formatReputationScore(score float32) string {
	formatted := fmt.Sprintf("%.2f", score)
	formatted = strings.TrimRight(strings.TrimRight(formatted, "0"), ".")
	if formatted == "-0" {
		return "0"
	}
	return formatted
}

func resolveReputationForExport(reputations []domain.ProxyReputation, protocolName string) (string, string) {
	targetKinds := make([]string, 0, 2)
	if trimmed := strings.ToLower(strings.TrimSpace(protocolName)); trimmed != "" {
		targetKinds = append(targetKinds, trimmed)
	}
	targetKinds = append(targetKinds, domain.ProxyReputationKindOverall)

	for _, kind := range targetKinds {
		if rep, ok := findReputationByKind(reputations, kind); ok {
			label := strings.TrimSpace(rep.Label)
			if label == "" {
				label = "unknown"
			}
			return label, formatReputationScore(rep.Score)
		}
	}

	return "", ""
}

func findReputationByKind(reputations []domain.ProxyReputation, kind string) (domain.ProxyReputation, bool) {
	lowerKind := strings.ToLower(strings.TrimSpace(kind))
	for _, rep := range reputations {
		if strings.ToLower(rep.Kind) == lowerKind {
			return rep, true
		}
	}
	return domain.ProxyReputation{}, false
}
