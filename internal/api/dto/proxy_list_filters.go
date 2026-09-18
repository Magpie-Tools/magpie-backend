package dto

import "strings"

type ProxyListFilters struct {
	States           []string `json:"states,omitempty"`
	State            string   `json:"state,omitempty"`
	Status           string   `json:"status,omitempty"`
	Protocols        []string `json:"protocols,omitempty"`
	MinHealthOverall int      `json:"minHealthOverall,omitempty"`
	MinHealthHTTP    int      `json:"minHealthHttp,omitempty"`
	MinHealthHTTPS   int      `json:"minHealthHttps,omitempty"`
	MinHealthSOCKS4  int      `json:"minHealthSocks4,omitempty"`
	MinHealthSOCKS5  int      `json:"minHealthSocks5,omitempty"`
	Countries        []string `json:"countries,omitempty"`
	Types            []string `json:"types,omitempty"`
	AnonymityLevels  []string `json:"anonymityLevels,omitempty"`
	MaxTimeout       int      `json:"maxTimeout,omitempty"`
	MaxRetries       int      `json:"maxRetries,omitempty"`
	ReputationLabels []string `json:"reputationLabels,omitempty"`
	TagIDs           []uint64 `json:"tagIds,omitempty"`
}

// NormalizeProxyStateFilter leaves omitted and unsupported values unfiltered.
func NormalizeProxyStateFilter(state string) string {
	switch strings.ToLower(strings.TrimSpace(state)) {
	case "active":
		return "active"
	case "paused":
		return "paused"
	case "archived":
		return "archived"
	default:
		return ""
	}
}

// NormalizeProxyStateFilters accepts multiple lifecycle states with ANY matching.
func NormalizeProxyStateFilters(states []string) []string {
	var result []string
	seen := make(map[string]bool)
	for _, raw := range states {
		state := NormalizeProxyStateFilter(raw)
		if state != "" && !seen[state] {
			result = append(result, state)
			seen[state] = true
		}
	}
	return result
}

func (filters ProxyListFilters) LifecycleStates() []string {
	return NormalizeProxyStateFilters(append([]string{filters.State}, filters.States...))
}
