package dto

type ExportSettings struct {
	Proxies          []uint   `json:"proxies"`
	Filter           bool     `json:"filter"`
	Http             bool     `json:"http"`
	Https            bool     `json:"https"`
	Socks4           bool     `json:"socks4"`
	Socks5           bool     `json:"socks5"`
	MaxRetries       uint     `json:"maxRetries"`
	MaxTimeout       uint     `json:"maxTimeout"`
	ProxyStatus      string   `json:"proxyStatus"`
	ReputationLabels []string `json:"reputationLabels"`
	OutputFormat     string   `json:"outputFormat"`
}
