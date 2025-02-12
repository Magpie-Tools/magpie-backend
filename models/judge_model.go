package models

import (
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
)

type Judge struct {
	ID         uint   `gorm:"primaryKey;autoIncrement"`
	FullString string `gorm:"size:512;not null;unique"`

	url       url.URL
	hostname  string
	ip        atomic.Value // Stores a string
	setupOnce sync.Once    // Ensures safe one-time initialization

	ProxyStatistics []ProxyStatistic `gorm:"foreignKey:JudgeID;constraint:OnUpdate:CASCADE,OnDelete:SET NULL;"`
	Users           []User           `gorm:"many2many:user_judges;"`

	CreatedAt time.Time `gorm:"autoCreateTime"`
}

func (judge *Judge) SetUp() error {
	var err error
	judge.setupOnce.Do(func() {
		var parsedURL *url.URL
		parsedURL, err = url.Parse(judge.FullString)
		if err != nil {
			return
		}

		// Extract hostname once during setup
		judge.url = *parsedURL
		judge.hostname = parsedURL.Hostname()
		judge.FullString = parsedURL.String()
		judge.ip.Store("")
	})
	return err
}

func (judge *Judge) UpdateIp() {
	hostname := judge.hostname

	if hostname == "" {
		judge.ip.Store("")
		return
	}

	addrs, err := net.LookupHost(hostname)
	if err != nil || len(addrs) == 0 {
		judge.ip.Store("")
		return
	}

	judge.ip.Store(addrs[0])
}

func (judge *Judge) GetIp() string {
	ip, _ := judge.ip.Load().(string)
	return ip
}

func (judge *Judge) GetHostname() string {
	return judge.hostname
}

func (judge *Judge) GetScheme() string {
	return judge.url.Scheme
}
