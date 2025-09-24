package database

import (
	_ "embed"
	"golang.org/x/sync/singleflight"
	"magpie/internal/domain"
	"net"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/oschwald/geoip2-golang"
)

//go:embed GeoLite2-ASN.mmdb
var geoLiteASNDB []byte

//go:embed GeoLite2-Country.mmdb
var geoLiteCountryDB []byte

type dnsCacheEntry struct {
	names   []string
	expires time.Time
}

var (
	countryDB   *geoip2.Reader
	asnDB       *geoip2.Reader
	initOnce    sync.Once
	initSuccess bool

	datacenterRegex     = regexp.MustCompile(`(?i)(amazon|google|microsoft|digitalocean|linode|hetzner|ovh|vultr|ibm|alibaba|tencent|cloudflare|rackspace|hostinger|upcloud|azure|gcp|aws)`)
	residentialKeywords = regexp.MustCompile(`(?i)(dyn|pool|dsl|cust|res|ip|adsl|ppp|user|mobile|static|dhcp)`)
	ispKeywords         = regexp.MustCompile(`(?i)(isp|broadband|telecom|communications|networks|carrier)`)

	dnsCache       sync.Map
	dnsLookupGroup singleflight.Group
	dnsCacheTTL    = 12 * time.Hour
)

func init() {
	initOnce.Do(func() {
		var err error
		if len(geoLiteCountryDB) > 0 {
			countryDB, err = geoip2.FromBytes(geoLiteCountryDB)
			if err != nil {
				countryDB = nil
			}
		}

		if len(geoLiteASNDB) > 0 {
			asnDB, err = geoip2.FromBytes(geoLiteASNDB)
			if err != nil {
				asnDB = nil
			}
		}

		initSuccess = countryDB != nil && asnDB != nil
	})
}

func getCachedDNS(ip string) []string {
	now := time.Now()
	if entry, ok := dnsCache.Load(ip); ok {
		cachedEntry := entry.(dnsCacheEntry)
		if now.Before(cachedEntry.expires) {
			return cachedEntry.names
		}
	}

	result, err, _ := dnsLookupGroup.Do(ip, func() (interface{}, error) {
		names, err := net.LookupAddr(ip)
		if err != nil {
			return []string{}, nil // Cache failures as empty results
		}
		return names, nil
	})

	if err != nil {
		result = []string{}
	}

	names := result.([]string)
	dnsCache.Store(ip, dnsCacheEntry{
		names:   names,
		expires: now.Add(dnsCacheTTL),
	})
	return names
}

func EnrichProxiesWithCountryAndType(proxies *[]domain.Proxy) {
	for i := range *proxies {
		ip := (*proxies)[i].GetIp()
		(*proxies)[i].Country = GetCountryCode(ip)
		(*proxies)[i].EstimatedType = DetermineProxyType(ip)
	}
}

func GetCountryCode(ipAddress string) string {
	if !initSuccess {
		return "N/A"
	}

	ip := net.ParseIP(ipAddress)
	if ip == nil {
		return "N/A"
	}

	record, err := countryDB.Country(ip)
	if err != nil {
		return "N/A"
	}

	return record.Country.IsoCode
}

func DetermineProxyType(ipAddress string) string {
	if !initSuccess {
		return "unknown"
	}

	ip := net.ParseIP(ipAddress)
	if ip == nil {
		return "unknown"
	}

	// Check cached reverse DNS results
	names := getCachedDNS(ipAddress)
	for _, name := range names {
		if residentialKeywords.MatchString(name) {
			return "Residential"
		}
	}

	// Check ASN information
	asnRecord, err := asnDB.ASN(ip)
	if err != nil {
		return "unknown"
	}

	org := strings.ToLower(asnRecord.AutonomousSystemOrganization)

	// Check for datacenter organizations
	if datacenterRegex.MatchString(org) {
		return "Datacenter"
	}

	// Check for ISP indicators in ASN organization
	if ispKeywords.MatchString(org) {
		return "ISP"
	}

	// Final check for common residential ASN patterns
	if strings.Contains(org, "customer") || strings.Contains(org, "residential") {
		return "Residential"
	}

	// Default to ISP for unknown organizations
	return "N/A"
}
