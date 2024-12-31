package helper

import (
	"magpie/models"
	"net"
	"strconv"
	"strings"
)

func parseTextToProxies(text string) []models.Proxy {
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
