package models

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"gorm.io/gorm"
	"net"
	"strings"
	"time"
)

type Proxy struct {
	ID       uint64 `gorm:"primaryKey;autoIncrement"`
	IP1      uint8  `gorm:"not null;index:idx_proxy_addr,priority:1"`
	IP2      uint8  `gorm:"not null;index:idx_proxy_addr,priority:2"`
	IP3      uint8  `gorm:"not null;index:idx_proxy_addr,priority:3"`
	IP4      uint8  `gorm:"not null;index:idx_proxy_addr,priority:4"`
	Port     int    `gorm:"not null;index:idx_proxy_addr,priority:5"`
	Username string `gorm:"default:''"`
	Password string `gorm:"default:''"`

	// Relationships
	Statistics []ProxyStatistic `gorm:"foreignKey:ProxyID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`

	//UserID uint `gorm:"not null;index"` // Foreign key (indexed for performance)
	//User   User `gorm:"constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`

	Hash      []byte    `gorm:"type:bytea;uniqueIndex;size:32"` // SHA-256 of IP|Port|Username|Password|UserID
	CreatedAt time.Time `gorm:"autoCreateTime"`
}

func (proxy *Proxy) BeforeCreate(_ *gorm.DB) error {
	hash := sha256.Sum256([]byte(
		strings.ToLower( // having different upper/lowercase username/password would not make sense for the same proxy
			fmt.Sprintf("%s|%d|%s|%s|%d",
				proxy.GetIp(),
				proxy.Port,
				proxy.Username,
				proxy.Password,
				//proxy.UserID,
			))))
	proxy.Hash = hash[:]
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
