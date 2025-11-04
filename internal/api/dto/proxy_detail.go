package dto

import "time"

type ProxyDetail struct {
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
}
