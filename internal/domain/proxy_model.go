package domain

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"gorm.io/gorm"
	"magpie/internal/security"
)

type Proxy struct {
	ID       uint64 `gorm:"primaryKey;autoIncrement"`
	IP1      uint8  `gorm:"not null;index:idx_proxy_addr,priority:1"`
	IP2      uint8  `gorm:"not null;index:idx_proxy_addr,priority:2"`
	IP3      uint8  `gorm:"not null;index:idx_proxy_addr,priority:3"`
	IP4      uint8  `gorm:"not null;index:idx_proxy_addr,priority:4"`
	Port     uint16 `gorm:"not null;index:idx_proxy_addr,priority:5"`
	Username string `gorm:"default:''"`
	Password string `gorm:"-" json:"password"`

	PasswordEncrypted string `gorm:"column:password;default:''" json:"-"`

	Country       string `gorm:"size:56;not null"` // Human-readable country name
	EstimatedType string `gorm:"size:20;not null"` // ISP, Datacenter, Residential

	// Relationships
	Statistics  []ProxyStatistic `gorm:"foreignKey:ProxyID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	ScrapeSites []ScrapeSite     `gorm:"many2many:proxy_scrape_site;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`

	Users []User `gorm:"many2many:user_proxies;"`

	Hash      []byte    `gorm:"type:bytea;uniqueIndex;size:32"` // SHA-256 of IP|Port|Username|Password|UserID
	CreatedAt time.Time `gorm:"autoCreateTime"`
}

func (proxy *Proxy) BeforeSave(_ *gorm.DB) error {
	if len(proxy.Hash) == 0 {
		proxy.GenerateHash()
	}

	if proxy.Password == "" {
		proxy.PasswordEncrypted = ""
		return nil
	}

	encrypted, err := security.EncryptProxySecret(proxy.Password)
	if err != nil {
		return err
	}

	proxy.PasswordEncrypted = encrypted
	return nil
}

func (proxy *Proxy) AfterFind(_ *gorm.DB) error {
	plain, _, err := security.DecryptProxySecret(proxy.PasswordEncrypted)
	if err != nil {
		return err
	}

	proxy.Password = plain
	return nil
}

func (proxy *Proxy) GenerateHash() {
	hash := sha256.Sum256([]byte(
		strings.ToLower( // having different upper/lowercase username/password would not make sense for the same proxy
			fmt.Sprintf("%s|%d|%s|%s",
				proxy.GetIp(),
				proxy.Port,
				proxy.Username,
				proxy.Password,
			))))
	proxy.Hash = hash[:]
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
	proxy.IP1 = ipv4[0]
	proxy.IP2 = ipv4[1]
	proxy.IP3 = ipv4[2]
	proxy.IP4 = ipv4[3]
	return nil
}

func (proxy *Proxy) GetFullProxy() string {
	return fmt.Sprintf("%s:%d", proxy.GetIp(), proxy.Port)
}

func (proxy *Proxy) GetIp() string {
	return fmt.Sprintf("%d.%d.%d.%d", proxy.IP1, proxy.IP2, proxy.IP3, proxy.IP4)
}

func (proxy *Proxy) HasAuth() bool {
	return proxy.Username != "" && proxy.Password != ""
}
