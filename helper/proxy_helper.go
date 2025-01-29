package helper

import (
	"magpie/models"
	"magpie/settings"
	"net"
	"regexp"
	"strconv"
	"strings"
)

func ParseTextToProxies(text string) []models.Proxy {
	text = strings.ReplaceAll(text, "@", ";")

	lines := strings.Split(text, "\n")
	proxies := make([]models.Proxy, 0, len(lines))

	for _, line := range lines {
		split := strings.Split(line, ":")
		count := len(split)

		// Validate ip
		ip := split[0]
		if net.ParseIP(ip) == nil {
			continue
		}

		// Validate Port
		port, err := strconv.Atoi(split[1])
		if err != nil || port < 1 || port > 65535 {
			continue
		}

		if count == 2 {
			proxies = append(proxies, models.Proxy{
				IP:   ip,
				Port: port,
			})
		} else if count == 4 {
			proxies = append(proxies, models.Proxy{
				IP:       ip,
				Port:     port,
				Username: split[2],
				Password: split[3],
			})
		}
	}

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
	if strings.Contains(html, settings.GetCurrentIp()) {
		return 1
	}

	//When containing one of these headers the proxy is anonymous
	cfg := settings.GetConfig()
	for _, header := range cfg.Checker.ProxyHeader {
		if strings.Contains(html, header) {
			return 2
		}
	}

	//Proxy is elite
	return 3
}
