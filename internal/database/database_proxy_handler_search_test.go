package database

import (
	"testing"

	dto "magpie/internal/api/dto"
)

func TestProxyMatchesSearch(t *testing.T) {
	proxy := dto.ProxyInfo{
		IP:             "198.51.100.42",
		Port:           3128,
		EstimatedType:  "HTTP",
		Country:        "Germany",
		AnonymityLevel: "High",
		ResponseTime:   450,
		Alive:          true,
		Reputation: &dto.ProxyReputationSummary{
			Overall: &dto.ProxyReputation{
				Kind:  "overall",
				Score: 87.2,
				Label: "Good",
			},
			Protocols: map[string]dto.ProxyReputation{
				"http": {
					Kind:  "http",
					Score: 75.5,
					Label: "Neutral",
				},
			},
		},
	}

	testCases := map[string]bool{
		"198.51.100":                                       true,
		"198.51.100.42":                                    true,
		"198.51.100.42:3128":                               true,
		"198.51.100.42 3128":                               true,
		"http://198.51.100.42:3128":                        true,
		"https://198.51.100.42:3128":                       true,
		"user:pass@198.51.100.42:3128":                     true,
		"http://user:pass@198.51.100.42:3128":              true,
		"http://user:pass@198.51.100.42:3128/path?foo=bar": true,
		"198.51.100.42/extra":                              true,
		"3128":                                             true,
		"http":                                             true,
		"germany":                                          true,
		"high":                                             true,
		"450":                                              true,
		"alive":                                            true,
		"good":                                             true,
		"87":                                               true,
		"neutral":                                          true,
		"75.5":                                             true,
		"notfound":                                         false,
		"bad":                                              false,
	}

	for term, expected := range testCases {
		result := proxyMatchesSearch(proxy, term)
		if result != expected {
			t.Errorf("proxyMatchesSearch(%q) = %v, want %v", term, result, expected)
		}
	}
}
