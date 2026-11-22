package dto

import (
	"magpie/internal/ipgeolocation"
	"time"
)

type IPGeolocationInfo struct {
	Result    ipgeolocation.Result `json:"result"`
	CheckedAt *time.Time           `json:"checked_at"`
	ExpiresAt *time.Time           `json:"expires_at"`
	Fresh     bool                 `json:"fresh"`
}

type ProxyDetail struct {
	Alive           *bool                     `json:"alive"`
	Id              int                       `json:"id"`
	IP              string                    `json:"ip"`
	Port            uint16                    `json:"port"`
	Username        string                    `json:"username"`
	Password        string                    `json:"password"`
	HasAuth         bool                      `json:"has_auth"`
	EstimatedType   string                    `json:"estimated_type"`
	Country         string                    `json:"country"`
	CreatedAt       time.Time                 `json:"created_at"`
	LatestCheck     *time.Time                `json:"latest_check,omitempty"`
	LatestStatistic *ProxyStatistic           `json:"latest_statistic,omitempty"`
	Protocols       []ProxyProtocolHealth     `json:"protocols"`
	Reputation      *ProxyReputationBreakdown `json:"reputation,omitempty"`
	Tags            []ProxyTag                `json:"tags"`
	State           string                    `json:"state"`
	PauseReason     string                    `json:"pause_reason,omitempty"`
	IPGeolocation   *IPGeolocationInfo        `json:"ipgeolocation,omitempty"`
}

// ProxyProtocolHealth is the current check result for one protocol of a proxy,
// as the workspace's current checker settings see it.
type ProxyProtocolHealth struct {
	Protocol       string    `json:"protocol"`
	Transport      string    `json:"transport,omitempty"`
	Alive          bool      `json:"alive"`
	ResponseTime   uint16    `json:"response_time"`
	Attempt        uint8     `json:"attempt"`
	AnonymityLevel string    `json:"anonymity_level"`
	CheckedAt      time.Time `json:"checked_at"`
	// Recent holds the verdicts of the latest checks with the current settings, oldest first.
	Recent []bool `json:"recent"`
}
