package models

import (
	"net"
	"net/url"
	"sync"
	"sync/atomic"
)

type Judge struct {
	url        url.URL
	hostname   string // Pre-extracted during setup
	fullString string
	ip         atomic.Value // Stores a string
	regex      string
	setupOnce  sync.Once // Ensures safe one-time initialization
}

func (judge *Judge) SetUp(urlStr, regex string) error {
	var err error
	judge.setupOnce.Do(func() {
		var parsedURL *url.URL
		parsedURL, err = url.Parse(urlStr)
		if err != nil {
			return
		}

		// Extract hostname once during setup
		judge.url = *parsedURL
		judge.hostname = parsedURL.Hostname()
		judge.fullString = parsedURL.String()
		judge.regex = regex
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

func (judge *Judge) GetFullString() string {
	return judge.fullString
}

func (judge *Judge) GetScheme() string {
	return judge.url.Scheme
}

func (judge *Judge) GetRegex() string {
	return judge.regex // Immutable after setup
}
