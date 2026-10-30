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
	Reputation      *ProxyReputationBreakdown `json:"reputation,omitempty"`
	Tags            []ProxyTag                `json:"tags"`
	State           string                    `json:"state"`
	PauseReason     string                    `json:"pause_reason,omitempty"`
	IPGeolocation   *IPGeolocationInfo        `json:"ipgeolocation,omitempty"`
}
