package dto

import "time"

type ProxyStatistic struct {
	Transport      string    `json:"transport,omitempty"`
	Timeout        uint16    `json:"timeout,omitempty"`
	Retries        uint8     `json:"retries"`
	ConfigKey      string    `json:"config_key,omitempty"`
	Current        bool      `json:"current"`
	Id             uint64    `json:"id"`
	Alive          bool      `json:"alive"`
	Attempt        uint8     `json:"attempt"`
	ResponseTime   uint16    `json:"response_time"`
	ResponseBody   string    `json:"response_body"`
	Protocol       string    `json:"protocol"`
	AnonymityLevel string    `json:"anonymity_level"`
	Judge          string    `json:"judge"`
	CreatedAt      time.Time `json:"created_at"`
}

type ProxyStatisticDetail struct {
	ResponseBody string `json:"response_body"`
	Regex        string `json:"regex"`
}

// ProxyStatisticHour summarises one UTC hour of a proxy's check history for
// one workspace. AvgResponseTime averages successful checks only and is zero
// when the hour has none.
type ProxyStatisticHour struct {
	Hour            time.Time `json:"hour"`
	Checks          int64     `json:"checks"`
	Failed          int64     `json:"failed"`
	AvgResponseTime int64     `json:"avg_response_time"`
}
