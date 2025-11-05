package dto

import "time"

type ProxyInfo struct {
	Id             int                     `json:"id"`
	IP             string                  `json:"ip"`
	Port           uint16                  `json:"port"`
	EstimatedType  string                  `json:"estimated_type"`
	ResponseTime   uint16                  `json:"response_time"`
	Country        string                  `json:"country"`
	AnonymityLevel string                  `json:"anonymity_level"`
	Alive          bool                    `json:"alive"`
	LatestCheck    time.Time               `json:"latest_check"`
	Reputation     *ProxyReputationSummary `json:"reputation,omitempty"`
}

type ProxyPage struct {
	Proxies []ProxyInfo `json:"proxies"`
	Total   int64       `json:"total"`
}
