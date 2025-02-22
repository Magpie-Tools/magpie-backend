package routeModels

import "time"

type ProxyInfo struct {
	IP             string    `json:"ip"`
	EstimatedType  string    `json:"estimated_type"`
	ResponseTime   int16     `json:"response_time"`
	Country        string    `json:"country"`
	AnonymityLevel string    `json:"anonymity_level"`
	Protocol       string    `json:"protocol"`
	Alive          bool      `json:"alive"`
	LatestCheck    time.Time `json:"latest_check"`
}
