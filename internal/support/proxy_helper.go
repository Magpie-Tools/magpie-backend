package support

import (
	"fmt"
	"magpie/internal/config"
	"magpie/internal/domain"
	"net"
	"regexp"
	"strconv"
	"strings"
)

func ParseTextToProxies(text string) []domain.Proxy {
	text = clearProxyString(text)

	lines := strings.Split(text, "\n")
	proxies := make([]domain.Proxy, 0, len(lines))

	for _, line := range lines {
		split := strings.Split(line, ":")
		count := len(split)

		// Validate ip
		ip := split[0]
		if len(ip) > 0 && ip[0] == '0' {
			ip = ip[1:] // Fix proxy if it leads with 0
		}
		if net.ParseIP(ip) == nil {
			continue
		}

		// Validate Port
		port, err := strconv.Atoi(split[1])
		if err != nil || port < 1 || port > 65535 {
			continue
		}

		proxy := domain.Proxy{
			Port: uint16(port),
		}

		err = proxy.SetIP(ip)
		if err != nil {
			continue
		}

		if count == 2 {
			proxies = append(proxies, proxy)
		} else if count == 4 {
			proxy.Username = split[2]
			proxy.Password = split[3]

			proxies = append(proxies, proxy)
		}
	}

	return proxies
}

func clearProxyString(proxies string) string {
	proxies = strings.ReplaceAll(proxies, "@", ";")
	proxies = strings.ReplaceAll(proxies, "\r", "")

	// Makes leading 0 proxies valid
	proxies = strings.ReplaceAll(proxies, ".0", ".")
	proxies = strings.ReplaceAll(proxies, "..", ".0.")
	proxies = strings.ReplaceAll(proxies, ".:", ".0:")

	return proxies
}

// FindIP identifies the first IP address (IPv4 or IPv6) in a given string.
func FindIP(input string) string {
	// Regular expression for matching IPv4 and IPv6 addresses
	ipRegex := `\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b|` + // IPv4
		`\b(?:[A-Fa-f0-9]{1,4}:){7}[A-Fa-f0-9]{1,4}\b` // IPv6

	return regexp.MustCompile(ipRegex).FindString(input)
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

// FormatProxies formats the list of proxies according to the specified output format
func FormatProxies(proxies []domain.Proxy, outputFormat string) string {
	var result strings.Builder

	for _, proxy := range proxies {
		line := outputFormat

		// Get the latest statistics for the proxy
		var latestStat domain.ProxyStatistic
		if len(proxy.Statistics) > 0 {
			latestStat = proxy.Statistics[0]
		}

		// Replace keywords with actual values
		line = strings.ReplaceAll(line, "protocol", getProtocolName(&latestStat))
		line = strings.ReplaceAll(line, "ip", proxy.GetIp())
		line = strings.ReplaceAll(line, "port", fmt.Sprintf("%d", proxy.Port))
		line = strings.ReplaceAll(line, "username", proxy.Username)
		line = strings.ReplaceAll(line, "password", proxy.Password)
		line = strings.ReplaceAll(line, "country", proxy.Country)
		line = strings.ReplaceAll(line, "alive", fmt.Sprintf("%t", latestStat.Alive))
		line = strings.ReplaceAll(line, "type", proxy.EstimatedType)
		line = strings.ReplaceAll(line, "time", fmt.Sprintf("%d", latestStat.ResponseTime))

		result.WriteString(line)
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
