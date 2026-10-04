package dto

import (
	"encoding/json"
	"strings"
)

type UserSettings struct {
	ProvidedFields             map[string]bool  `json:"-"`
	CheckerSettings            *CheckerSettings `json:"checker_settings,omitempty"`
	HTTPProtocol               bool             `json:"http_protocol"`
	HTTPSProtocol              bool             `json:"https_protocol"`
	SOCKS4Protocol             bool             `json:"socks4_protocol"`
	SOCKS5Protocol             bool             `json:"socks5_protocol"`
	Timeout                    uint16           `json:"timeout"`
	Retries                    uint8            `json:"retries"`
	UseHttpsForSocks           bool             `gorm:"not null;default:true"`
	TransportProtocol          string           `json:"transport_protocol"`
	AutoRemoveFailingProxies   bool             `json:"auto_remove_failing_proxies"`
	AutoRemoveFailureThreshold uint8            `json:"auto_remove_failure_threshold"`
	FailureAction              string           `json:"failure_action"`

	SimpleUserJudges []SimpleUserJudge `json:"judges"`

	ScrapingSources          []string `json:"scraping_sources"`
	ProxyListColumns         []string `json:"proxy_list_columns"`
	ScrapeSourceProxyColumns []string `json:"scrape_source_proxy_columns"`
	ScrapeSourceListColumns  []string `json:"scrape_source_list_columns"`
}

// Presence matters for legacy partial saves: missing protocol fields must not
// disable Default checks or discard newer per-protocol settings.
func (settings *UserSettings) UnmarshalJSON(data []byte) error {
	type plain UserSettings
	if err := json.Unmarshal(data, (*plain)(settings)); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	settings.ProvidedFields = make(map[string]bool, len(fields))
	for name := range fields {
		settings.ProvidedFields[strings.ToLower(name)] = true
	}
	return nil
}
func (settings UserSettings) HasField(name string) bool {
	return settings.ProvidedFields == nil || settings.ProvidedFields[strings.ToLower(name)]
}

// Preference-only writes never mutate workspace settings or reconcile checker
// snapshots. Direct DTO callers without presence metadata remain full saves.
func (settings UserSettings) ChangesWorkspaceSettings() bool {
	for _, name := range []string{"checker_settings", "http_protocol", "https_protocol", "socks4_protocol", "socks5_protocol", "timeout", "retries", "UseHttpsForSocks", "transport_protocol", "auto_remove_failing_proxies", "auto_remove_failure_threshold", "failure_action", "judges"} {
		if settings.HasField(name) {
			return true
		}
	}
	return false
}
func (settings UserSettings) ChangesLegacyCheckerDefaults() bool {
	for _, name := range []string{"http_protocol", "https_protocol", "socks4_protocol", "socks5_protocol", "timeout", "retries", "transport_protocol"} {
		if settings.HasField(name) {
			return true
		}
	}
	return false
}
