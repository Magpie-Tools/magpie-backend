package routeModels

import "time"

type ScrapeSiteInfo struct {
	Id         uint64    `json:"id"`
	Url        string    `json:"url"`
	ProxyCount uint      `json:"proxy_count"`
	AddedAt    time.Time `json:"added_at"`
}
