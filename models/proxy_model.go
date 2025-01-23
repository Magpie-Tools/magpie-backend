package models

import (
	"crypto/sha256"
	"fmt"
	"gorm.io/gorm"
	"strings"
)

type Proxy struct {
	ID       uint64 `gorm:"primaryKey;autoIncrement"`
	IP       string `gorm:"size:45;not null;index:idx_proxy_addr"` // IPv6 support + index
	Port     int    `gorm:"not null;index:idx_proxy_addr"`
	Username string `gorm:"default:''"`
	Password string `gorm:"default:''"`
	Hash     []byte `gorm:"type:bytea;uniqueIndex;size:32"` // SHA-256 of IP|Port|Username|Password
}

func (proxy *Proxy) BeforeCreate(_ *gorm.DB) error {
	hash := sha256.Sum256([]byte(
		strings.ToLower( // having different upper/lowercase username/password would not make sense for the same proxy
			fmt.Sprintf("%s|%d|%s|%s",
				proxy.IP,
				proxy.Port,
				proxy.Username,
				proxy.Password,
			))))
	proxy.Hash = hash[:]
	return nil
}

func (proxy *Proxy) GetFullProxy() string {
	return fmt.Sprintf("%s:%d", proxy.Username, proxy.Port)
}

func (proxy *Proxy) HasAuth() bool {
	return proxy.Username != "" && proxy.Password != ""
}
