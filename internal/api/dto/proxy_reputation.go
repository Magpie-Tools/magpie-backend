package dto

type ProxyReputation struct {
	Kind  string  `json:"kind"`
	Score float32 `json:"score"`
	Label string  `json:"label"`
}

type ProxyReputationSummary struct {
	Overall   *ProxyReputation           `json:"overall,omitempty"`
	Protocols map[string]ProxyReputation `json:"protocols,omitempty"`
}

type ProxyReputationDetail struct {
	Kind    string         `json:"kind"`
	Score   float32        `json:"score"`
	Label   string         `json:"label"`
	Signals map[string]any `json:"signals,omitempty"`
}

type ProxyReputationBreakdown struct {
	Overall   *ProxyReputationDetail           `json:"overall,omitempty"`
	Protocols map[string]ProxyReputationDetail `json:"protocols,omitempty"`
}
