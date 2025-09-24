package dto

type DashboardInfo struct {
	TotalChecks      int64 `json:"total_checks"`
	TotalScraped     int64 `json:"total_scraped"`
	TotalChecksWeek  int64 `json:"total_checks_week"`
	TotalScrapedWeek int64 `json:"total_scraped_week"`

	JudgeValidProxies []struct {
		JudgeUrl           string `json:"judge_url"`
		EliteProxies       uint   `json:"elite_proxies"`
		AnonymousProxies   uint   `json:"anonymous_proxies"`
		TransparentProxies uint   `json:"transparent_proxies"`
	} `json:"judge_valid_proxies"`
}
