package dto

import "time"

type ProxyInfo struct {
	Id             int       `json:"id"`
	IP             string    `json:"ip"`
	Port           uint16    `json:"port"`
	EstimatedType  string    `json:"estimated_type"`
	ResponseTime   int16     `json:"response_time"`
	Country        string    `json:"country"`
	AnonymityLevel string    `json:"anonymity_level"`
	Protocol       string    `json:"protocol"`
	Alive          bool      `json:"alive"`
	LatestCheck    time.Time `json:"latest_check"`
}
