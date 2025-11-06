package dto

type DashboardInfo struct {
	TotalChecks      int64 `json:"total_checks"`
	TotalScraped     int64 `json:"total_scraped"`
	TotalChecksWeek  int64 `json:"total_checks_week"`
	TotalScrapedWeek int64 `json:"total_scraped_week"`

	ReputationBreakdown struct {
		Good    uint `json:"good"`
		Neutral uint `json:"neutral"`
		Poor    uint `json:"poor"`
		Unknown uint `json:"unknown"`
	} `json:"reputation_breakdown"`

	TopReputationProxy *struct {
		ProxyID uint64  `json:"proxy_id"`
		IP      string  `json:"ip"`
		Port    uint16  `json:"port"`
		Score   float32 `json:"score"`
		Label   string  `json:"label"`
	} `json:"top_reputation_proxy"`

	CountryBreakdown []struct {
		Country string `json:"country"`
		Count   uint   `json:"count"`
	} `json:"country_breakdown"`

	JudgeValidProxies []struct {
		JudgeUrl           string `json:"judge_url"`
		EliteProxies       uint   `json:"elite_proxies"`
		AnonymousProxies   uint   `json:"anonymous_proxies"`
		TransparentProxies uint   `json:"transparent_proxies"`
	} `json:"judge_valid_proxies"`
}
