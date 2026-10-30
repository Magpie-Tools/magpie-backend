package config

import "testing"

func TestIPGeolocationDefaultsAndValidation(t *testing.T) {
	cfg := IPGeolocationPluginConfig{}
	normalizeIPGeolocation(&cfg)
	if cfg.Enabled || cfg.DailyCreditLimit != 300 || cfg.CacheHours != 168 || cfg.IncludeSecurity {
		t.Fatal("unsafe defaults", cfg)
	}
	for _, invalid := range []IPGeolocationPluginConfig{
		{Enabled: true, DailyCreditLimit: 0, CacheHours: 24},
		{Enabled: true, DailyCreditLimit: 1, CacheHours: 0},
		{DailyCreditLimit: 100001, CacheHours: 24},
		{DailyCreditLimit: 300, CacheHours: 721},
		{DailyCreditLimit: 2, CacheHours: 24, IncludeSecurity: true},
	} {
		if invalid.Validate() == nil {
			t.Fatal("accepted invalid policy", invalid)
		}
	}
}
