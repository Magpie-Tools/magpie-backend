package dto

import (
	"bytes"
	"encoding/json"
	"sort"
)

// A profile shares one transport and request budget across its selected protocols.
// Rules are ordered highest priority first. Nil fields inherit earlier values.
type CheckerSettings struct {
	Defaults        CheckerProfileSettings `json:"defaults"`
	Rules           []TagCheckerRule       `json:"rules"`
	LegacyProtocols bool                   `json:"-"`
}
type CheckerProfileSettings struct {
	Protocols []string `json:"protocols"`
	Transport string   `json:"transport"`
	Timeout   uint16   `json:"timeout"`
	Retries   uint8    `json:"retries"`
}
type TagCheckerRule struct {
	TagID     uint64   `json:"tag_id"`
	Mode      string   `json:"mode"`
	Protocols []string `json:"protocols"`
	Transport *string  `json:"transport,omitempty"`
	Timeout   *uint16  `json:"timeout,omitempty"`
	Retries   *uint8   `json:"retries,omitempty"`
}

// ProtocolCheckSettings is the compiled runtime check, rather than an editor profile.
type ProtocolCheckSettings struct {
	Enabled   bool   `json:"enabled"`
	Transport string `json:"transport"`
	Timeout   uint16 `json:"timeout"`
	Retries   uint8  `json:"retries"`
}

func (profile CheckerProfileSettings) Enabled(protocol string) bool {
	for _, name := range profile.Protocols {
		if name == protocol {
			return true
		}
	}
	return false
}

// Read the earlier per-protocol draft format outside the checker hot path.
// Its first enabled protocol supplies the shared defaults. Rule fields use the
// first explicit value in HTTP, HTTPS, SOCKS4, SOCKS5 order. New writes use profiles.
func (settings *CheckerSettings) UnmarshalJSON(data []byte) error {
	var raw struct {
		Defaults json.RawMessage  `json:"defaults"`
		Rules    []TagCheckerRule `json:"rules"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw.Defaults, &fields); err != nil {
		return err
	}
	var defaults CheckerProfileSettings
	_, current := fields["protocols"]
	if current {
		if err := json.Unmarshal(raw.Defaults, &defaults); err != nil {
			return err
		}
		if defaults.Protocols == nil {
			defaults.Protocols = []string{}
		}
	} else {
		var legacy map[string]struct {
			Enabled bool `json:"enabled"`
			CheckerProfileSettings
		}
		if err := json.Unmarshal(raw.Defaults, &legacy); err != nil {
			return err
		}
		defaults.Protocols = []string{}
		chosen := false
		for _, name := range orderedProtocolNames(fields) {
			entry := legacy[name]
			if entry.Enabled {
				defaults.Protocols = append(defaults.Protocols, name)
				if !chosen {
					defaults.Transport, defaults.Timeout, defaults.Retries = entry.Transport, entry.Timeout, entry.Retries
					chosen = true
				}
			}
		}
		if !chosen {
			entry := legacy["http"]
			defaults.Transport, defaults.Timeout, defaults.Retries = entry.Transport, entry.Timeout, entry.Retries
		}
	}
	*settings = CheckerSettings{Defaults: defaults, Rules: raw.Rules, LegacyProtocols: !current}
	return nil
}
func (rule *TagCheckerRule) UnmarshalJSON(data []byte) error {
	type plain TagCheckerRule
	var raw struct {
		TagID     uint64          `json:"tag_id"`
		Mode      string          `json:"mode"`
		Protocols json.RawMessage `json:"protocols"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	protocols := bytes.TrimSpace(raw.Protocols)
	if len(protocols) == 0 || protocols[0] != '{' {
		if err := json.Unmarshal(data, (*plain)(rule)); err != nil {
			return err
		}
		if rule.Protocols == nil {
			rule.Protocols = []string{}
		}
		return nil
	}
	var legacy map[string]json.RawMessage
	if err := json.Unmarshal(protocols, &legacy); err != nil {
		return err
	}
	*rule = TagCheckerRule{TagID: raw.TagID, Mode: raw.Mode, Protocols: orderedProtocolNames(legacy)}
	for _, name := range rule.Protocols {
		var fields plain
		if err := json.Unmarshal(legacy[name], &fields); err != nil {
			return err
		}
		if rule.Transport == nil {
			rule.Transport = fields.Transport
		}
		if rule.Timeout == nil {
			rule.Timeout = fields.Timeout
		}
		if rule.Retries == nil {
			rule.Retries = fields.Retries
		}
	}
	return nil
}
func orderedProtocolNames(fields map[string]json.RawMessage) []string {
	names := make([]string, 0, len(fields))
	for _, name := range []string{"http", "https", "socks4", "socks5"} {
		if _, exists := fields[name]; exists {
			names = append(names, name)
		}
	}
	unknown := []string{}
	for name := range fields {
		if name != "http" && name != "https" && name != "socks4" && name != "socks5" {
			unknown = append(unknown, name)
		}
	}
	sort.Strings(unknown)
	return append(names, unknown...)
}
