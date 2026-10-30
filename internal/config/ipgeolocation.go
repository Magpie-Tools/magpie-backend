package config

import (
	"errors"
	"strings"
)

type IPGeolocationPluginConfig struct {
	Enabled          bool   `json:"enabled"`
	APIKey           string `json:"api_key"`
	IncludeSecurity  bool   `json:"include_security"`
	DailyCreditLimit uint32 `json:"daily_credit_limit"`
	CacheHours       uint32 `json:"cache_hours"`
}

func (c IPGeolocationPluginConfig) Active() bool {
	return c.Enabled && strings.TrimSpace(c.APIKey) != ""
}

func (c IPGeolocationPluginConfig) Validate() error {
	if c.DailyCreditLimit > 100000 || c.CacheHours > 720 {
		return errors.New("IPGeolocation.io requires a daily credit limit from 1 to 100000 and a cache duration from 1 to 720 hours")
	}
	if c.Enabled && (c.DailyCreditLimit == 0 || c.CacheHours == 0) {
		return errors.New("IPGeolocation.io requires a positive daily credit limit and cache duration")
	}
	if c.IncludeSecurity && c.DailyCreditLimit > 0 && c.DailyCreditLimit < 3 {
		return errors.New("IPGeolocation.io security lookups require a daily budget of at least 3 credits")
	}
	return nil
}

func normalizeIPGeolocation(c *IPGeolocationPluginConfig) {
	c.APIKey = strings.TrimSpace(c.APIKey)
	if c.DailyCreditLimit == 0 {
		c.DailyCreditLimit = 300
	}
	if c.CacheHours == 0 {
		c.CacheHours = 168
	}
}
