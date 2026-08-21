package domain

import (
	"errors"
	"fmt"
	"net"
	"time"

	"magpie/internal/security"

	"gorm.io/gorm"
)

type Proxy struct {
	ID       uint64 `gorm:"primaryKey;autoIncrement"`
	IP       string `gorm:"column:ip_address;type:inet;index:idx_proxy_addr,priority:1" json:"ip"`
	Port     uint16 `gorm:"not null;index:idx_proxy_addr,priority:2"`
	Username string `gorm:"-"`
	Password string `gorm:"-" json:"password"`

	Country       string `gorm:"size:56;not null"` // Human-readable country name
	EstimatedType string `gorm:"size:20;not null"` // ISP, Datacenter, Residential

	// Relationships
	Statistics  []ProxyStatistic  `gorm:"foreignKey:ProxyID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	ScrapeSites []ScrapeSite      `gorm:"many2many:proxy_scrape_site;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	Reputations []ProxyReputation `gorm:"foreignKey:ProxyID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`

	Users []User `gorm:"many2many:user_proxies;"`

	Hash      []byte    `gorm:"type:bytea;uniqueIndex;size:32"` // Keyed fingerprint of exact IP|Port|Username|Password
	CreatedAt time.Time `gorm:"autoCreateTime"`
}

func (proxy *Proxy) BeforeSave(_ *gorm.DB) error {
	if proxy.IP != "" {
		if err := proxy.SetIP(proxy.IP); err != nil {
			return err
		}
	}

	if len(proxy.Hash) == 0 {
		return proxy.GenerateHash()
	}
	return nil
}

func (proxy *Proxy) GenerateHash() error {
	hash, err := security.FingerprintProxyRoute(
		proxy.GetIp(),
		proxy.Port,
		proxy.Username,
		proxy.Password,
	)
	if err != nil {
		return err
	}
	proxy.Hash = hash
	return nil
}

func (proxy *Proxy) SetIP(ip string) error {
	parsedIP := net.ParseIP(ip)
	if parsedIP == nil {
		return errors.New("invalid IP address")
	}
	ipv4 := parsedIP.To4()
	if ipv4 == nil {
		return errors.New("only IPv4 addresses are supported")
	}
	proxy.IP = ipv4.String()
	return nil
}

func (proxy *Proxy) GetFullProxy() string {
	return fmt.Sprintf("%s:%d", proxy.GetIp(), proxy.Port)
}

func (proxy *Proxy) GetIp() string {
	return proxy.IP
}

func (proxy *Proxy) HasAuth() bool {
	return proxy.Username != "" && proxy.Password != ""
}
