package dto

// DashboardReputationBreakdown counts active routes per reputation label.
type DashboardReputationBreakdown struct {
	Good    uint `json:"good"`
	Neutral uint `json:"neutral"`
	Poor    uint `json:"poor"`
	Unknown uint `json:"unknown"`
}

// DashboardCountryCount counts active routes in one country.
type DashboardCountryCount struct {
	Country string `json:"country"`
	Count   uint   `json:"count"`
}

// DashboardTopReputationProxy is the active route with the best reputation.
type DashboardTopReputationProxy struct {
	ProxyID uint64  `json:"proxy_id"`
	IP      string  `json:"ip"`
	Port    uint16  `json:"port"`
	Score   float32 `json:"score"`
	Label   string  `json:"label"`
}

// DashboardJudgeCount counts alive routes per anonymity level for one judge.
type DashboardJudgeCount struct {
	JudgeURL           string `json:"judge_url"`
	EliteProxies       uint   `json:"elite_proxies"`
	AnonymousProxies   uint   `json:"anonymous_proxies"`
	TransparentProxies uint   `json:"transparent_proxies"`
}
