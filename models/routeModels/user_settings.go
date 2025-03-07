package routeModels

type UserSettings struct {
	HTTPProtocol     bool   `json:"http_protocol"`
	HTTPSProtocol    bool   `json:"https_protocol"`
	SOCKS4Protocol   bool   `json:"socks4_protocol"`
	SOCKS5Protocol   bool   `json:"socks5_protocol"`
	Timeout          uint16 `json:"timeout"`
	Retries          uint8  `json:"retries"`
	UseHttpsForSocks bool   `gorm:"not null;default:true"`
}
