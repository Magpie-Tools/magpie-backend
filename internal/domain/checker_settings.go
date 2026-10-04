package domain

import (
	"fmt"
	"magpie/internal/api/dto"
)

type CheckerSettingsError struct{ Message string }

func (err *CheckerSettingsError) Error() string { return err.Message }
func invalidCheckerSettings(format string, args ...any) error {
	return &CheckerSettingsError{Message: fmt.Sprintf(format, args...)}
}

var CheckerProtocols = [4]string{"http", "https", "socks4", "socks5"}

func (workspace Workspace) DefaultCheckerSettings() *dto.CheckerSettings {
	if workspace.CheckerConfig != nil {
		// Earlier profiles could mix HTTP over QUIC and SOCKS over TCP. Their
		// shared conversion uses TCP if preserving the first transport would
		// make any tag combination unsupported. This runs at settings boundaries.
		value := workspace.CheckerConfig
		if value.LegacyProtocols && ValidateCheckerSettings(value) != nil {
			compatible := *value
			compatible.LegacyProtocols = false
			compatible.Defaults.Transport = "tcp"
			compatible.Rules = append([]dto.TagCheckerRule{}, value.Rules...)
			for i := range compatible.Rules {
				if compatible.Rules[i].Transport != nil {
					compatible.Rules[i].Transport = new(string("tcp"))
				}
			}
			if ValidateCheckerSettings(&compatible) == nil {
				return &compatible
			}
		}
		return workspace.CheckerConfig
	}
	transport := workspace.TransportProtocol
	if transport == "" {
		transport = "tcp"
	}
	profile := dto.CheckerProfileSettings{Protocols: []string{}, Transport: transport, Timeout: workspace.Timeout, Retries: workspace.Retries}
	enabled := [4]bool{workspace.HTTPProtocol, workspace.HTTPSProtocol, workspace.SOCKS4Protocol, workspace.SOCKS5Protocol}
	for i, protocol := range CheckerProtocols {
		if enabled[i] {
			profile.Protocols = append(profile.Protocols, protocol)
		}
	}
	return &dto.CheckerSettings{Defaults: profile, Rules: []dto.TagCheckerRule{}}
}
func ValidateCheckerSettings(value *dto.CheckerSettings) error {
	if value == nil {
		return nil
	}
	if value.Defaults.Timeout == 0 {
		return invalidCheckerSettings("timeout must be between 1 and 65535 milliseconds")
	}
	if err := validateCheckerTransport(value.Defaults.Transport); err != nil {
		return err
	}
	mask, err := checkerProtocolMask(value.Defaults.Protocols)
	if err != nil {
		return err
	}
	// Four protocol flags and three transports bound all reachable combinations
	// to 48 states. Validation runs on settings writes, never during dequeue.
	type selection struct {
		mask      uint8
		transport string
	}
	states := map[selection]bool{{mask, value.Defaults.Transport}: true}
	seen := make(map[uint64]bool, len(value.Rules))
	for n := len(value.Rules) - 1; n >= 0; n-- {
		rule := value.Rules[n]
		if rule.TagID == 0 || seen[rule.TagID] {
			return invalidCheckerSettings("checker rules require distinct, nonzero tag IDs")
		}
		seen[rule.TagID] = true
		if rule.Mode != "replace" && rule.Mode != "add" && rule.Mode != "remove" {
			return invalidCheckerSettings("checker rule mode must be replace, add or remove")
		}
		selected, err := checkerProtocolMask(rule.Protocols)
		if err != nil {
			return err
		}
		if rule.Mode == "remove" && (rule.Transport != nil || rule.Timeout != nil || rule.Retries != nil) {
			return invalidCheckerSettings("remove rules select protocols without field overrides")
		}
		if rule.Timeout != nil && *rule.Timeout == 0 {
			return invalidCheckerSettings("timeout must be between 1 and 65535 milliseconds")
		}
		if rule.Transport != nil {
			if err := validateCheckerTransport(*rule.Transport); err != nil {
				return err
			}
		}
		next := make(map[selection]bool, len(states)*2)
		for state := range states {
			next[state] = true // A proxy need not have this tag.
			if rule.Mode == "replace" {
				state.mask = selected
			}
			if rule.Mode == "add" {
				state.mask |= selected
			}
			if rule.Mode == "remove" {
				state.mask &^= selected
			}
			if rule.Transport != nil {
				state.transport = *rule.Transport
			}
			next[state] = true
		}
		states = next
	}
	for state := range states {
		if state.mask&12 != 0 && state.transport != "tcp" {
			return invalidCheckerSettings("SOCKS4 and SOCKS5 require TCP; a matching tag combination selects an unsupported transport")
		}
	}
	return nil
}
func validateCheckerTransport(transport string) error {
	if transport != "tcp" && transport != "quic" && transport != "http3" {
		return invalidCheckerSettings("unsupported checker transport %q", transport)
	}
	return nil
}
func checkerProtocolMask(protocols []string) (uint8, error) {
	var mask uint8
	for _, protocol := range protocols {
		var bit uint8
		for i, name := range CheckerProtocols {
			if name == protocol {
				bit = 1 << i
				break
			}
		}
		if bit == 0 {
			return 0, invalidCheckerSettings("unsupported proxy protocol %q", protocol)
		}
		if mask&bit != 0 {
			return 0, invalidCheckerSettings("duplicate checker protocol %q", protocol)
		}
		mask |= bit
	}
	return mask, nil
}

// Merge during snapshot construction. Every resulting protocol uses the same
// transport, timeout and retries. Workers continue reading compiled plans.
func ResolveCheckerSettings(value *dto.CheckerSettings, tagIDs map[uint64]bool) [4]dto.ProtocolCheckSettings {
	profile := value.Defaults
	mask, _ := checkerProtocolMask(profile.Protocols)
	for n := len(value.Rules) - 1; n >= 0; n-- {
		rule := value.Rules[n]
		if !tagIDs[rule.TagID] {
			continue
		}
		selected, _ := checkerProtocolMask(rule.Protocols)
		switch rule.Mode {
		case "replace":
			mask = selected
		case "add":
			mask |= selected
		case "remove":
			mask &^= selected
			continue
		}
		if rule.Transport != nil {
			profile.Transport = *rule.Transport
		}
		if rule.Timeout != nil {
			profile.Timeout = *rule.Timeout
		}
		if rule.Retries != nil {
			profile.Retries = *rule.Retries
		}
	}
	var plan [4]dto.ProtocolCheckSettings
	for i := range plan {
		plan[i] = dto.ProtocolCheckSettings{Enabled: mask&(1<<i) != 0, Transport: profile.Transport, Timeout: profile.Timeout, Retries: profile.Retries}
	}
	return plan
}
