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

// ProxyStatisticsQuery selects one page of a proxy's check history. Zero values
// mean "no filter"; Sort defaults to the check time, newest first.
type ProxyStatisticsQuery struct {
	Page      int
	PageSize  int
	Sort      string // checked, response, attempt, protocol, anonymity, judge
	Order     string // asc, desc
	Status    string // alive, dead
	Protocol  string // http, https, socks4, socks5
	Transport string // tcp, quic
	JudgeID   uint
	Settings  string // current, previous (previous includes legacy rows)
	Hours     int    // checks from the last N hours; 0 means any time
}

// ProxyStatisticsPage is one page of check history plus the totals the
// filters need: Total matches every filter, Counts ignores the status filter so
// the status buttons can show how many alive and dead checks the other filters
// leave. Options lists the protocol/transport pairs and judges this proxy's
// visible history contains.
type ProxyStatisticsPage struct {
	Statistics []ProxyStatistic          `json:"statistics"`
	Total      int64                     `json:"total"`
	Page       int                       `json:"page"`
	PageSize   int                       `json:"page_size"`
	Counts     ProxyStatisticCounts      `json:"counts"`
	Options    ProxyStatisticFilterLists `json:"options"`
}

type ProxyStatisticCounts struct {
	Alive int64 `json:"alive"`
	Dead  int64 `json:"dead"`
}

type ProxyStatisticFilterLists struct {
	Protocols []ProxyStatisticProtocolOption `json:"protocols"`
	Judges    []ProxyStatisticJudgeOption    `json:"judges"`
}

type ProxyStatisticProtocolOption struct {
	Protocol  string `json:"protocol"`
	Transport string `json:"transport"`
}

type ProxyStatisticJudgeOption struct {
	ID  uint   `json:"id"`
	URL string `json:"url"`
}
