package database

import (
	"context"
	_ "embed"
	"fmt"
	"magpie/internal/domain"
	"net"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/charmbracelet/log"
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

type residentialOverrideCandidate struct {
	index int
	ip    string
}

var (
	countryDB   *geoip2.Reader
	asnDB       *geoip2.Reader
	initOnce    sync.Once
	initSuccess bool

	datacenterRegex     = regexp.MustCompile(`(?i)(amazon|google|microsoft|digitalocean|linode|hetzner|ovh|vultr|ibm|alibaba|tencent|cloudflare|rackspace|hostinger|upcloud|azure|gcp|aws)`)
	residentialKeywords = regexp.MustCompile(`(?i)(dyn|pool|dsl|cust|res|ip|adsl|ppp|user|mobile|static|dhcp)`)
	ispKeywords         = regexp.MustCompile(`(?i)(isp|broadband|telecom|communications|networks|carrier)`)

	dnsCache                      sync.Map
	dnsLookupGroup                singleflight.Group
	dnsCacheTTL                   = 12 * time.Hour
	dnsLookupTimeout              = 2 * time.Second
	maxEnrichmentWorkers          = 64
	maxResidentialOverrideWorkers = 32
	enrichmentUpdateBatchSize     = 512
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
		ctx, cancel := context.WithTimeout(context.Background(), dnsLookupTimeout)
		defer cancel()

		names, err := net.DefaultResolver.LookupAddr(ctx, ip)
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

func AsyncEnrichProxyMetadata(proxies []domain.Proxy) {
	if len(proxies) == 0 {
		return
	}

	copySlice := make([]domain.Proxy, len(proxies))
	copy(copySlice, proxies)

	go func(items []domain.Proxy) {
		candidates := EnrichProxiesWithCountryAndType(&items)
		if err := persistProxyMetadata(items); err != nil {
			log.Error("persist proxy metadata", "err", err)
			return
		}
		if len(candidates) == 0 {
			return
		}
		if err := applyResidentialOverrides(&items, candidates); err != nil {
			log.Error("apply residential overrides", "err", err)
		}
	}(copySlice)
}

func EnrichProxiesWithCountryAndType(proxies *[]domain.Proxy) []residentialOverrideCandidate {
	if proxies == nil || len(*proxies) == 0 {
		return nil
	}

	workerCount := runtime.NumCPU() * 4
	if workerCount > maxEnrichmentWorkers {
		workerCount = maxEnrichmentWorkers
	}
	if workerCount < 1 {
		workerCount = 1
	}
	if len(*proxies) < workerCount {
		workerCount = len(*proxies)
	}

	jobs := make(chan int, workerCount)
	candidateCh := make(chan residentialOverrideCandidate, len(*proxies))
	var wg sync.WaitGroup

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				proxy := &(*proxies)[idx]
				ip := proxy.GetIp()
				proxy.Country = GetCountryCode(ip)
				typeValue, needsDNS := determineProxyTypeByASN(ip)
				proxy.EstimatedType = typeValue
				if needsDNS {
					candidateCh <- residentialOverrideCandidate{index: idx, ip: ip}
				}
			}
		}()
	}

	for i := range *proxies {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	close(candidateCh)

	candidates := make([]residentialOverrideCandidate, 0, len(*proxies))
	for candidate := range candidateCh {
		candidates = append(candidates, candidate)
	}

	return candidates
}

func determineProxyTypeByASN(ipAddress string) (string, bool) {
	if !initSuccess {
		return "unknown", false
	}

	ip := net.ParseIP(ipAddress)
	if ip == nil {
		return "unknown", false
	}

	asnRecord, err := asnDB.ASN(ip)
	if err != nil {
		return "unknown", true
	}

	org := strings.ToLower(asnRecord.AutonomousSystemOrganization)
	switch {
	case strings.Contains(org, "customer") || strings.Contains(org, "residential"):
		return "Residential", false
	case datacenterRegex.MatchString(org):
		return "Datacenter", true
	case ispKeywords.MatchString(org):
		return "ISP", true
	default:
		return "N/A", true
	}
}

func applyResidentialOverrides(proxies *[]domain.Proxy, candidates []residentialOverrideCandidate) error {
	if proxies == nil || len(*proxies) == 0 || len(candidates) == 0 {
		return nil
	}

	ipToIndices := make(map[string][]int, len(candidates))
	for _, candidate := range candidates {
		if candidate.index < 0 || candidate.index >= len(*proxies) {
			continue
		}
		ipToIndices[candidate.ip] = append(ipToIndices[candidate.ip], candidate.index)
	}

	if len(ipToIndices) == 0 {
		return nil
	}

	type dnsJob struct {
		ip      string
		indices []int
	}

	jobs := make(chan dnsJob)
	var wg sync.WaitGroup
	var mu sync.Mutex
	overrides := make([]domain.Proxy, 0, len(candidates))

	workerCount := len(ipToIndices)
	if workerCount > maxResidentialOverrideWorkers {
		workerCount = maxResidentialOverrideWorkers
	}
	if workerCount < 1 {
		workerCount = 1
	}

	wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go func() {
			defer wg.Done()
			for job := range jobs {
				names := getCachedDNS(job.ip)
				var matchesResidential bool
				for _, name := range names {
					if residentialKeywords.MatchString(name) {
						matchesResidential = true
						break
					}
				}
				if !matchesResidential {
					continue
				}

				mu.Lock()
				for _, idx := range job.indices {
					if idx < 0 || idx >= len(*proxies) {
						continue
					}
					proxy := &(*proxies)[idx]
					if proxy.EstimatedType == "Residential" {
						continue
					}
					proxy.EstimatedType = "Residential"
					overrides = append(overrides, *proxy)
				}
				mu.Unlock()
			}
		}()
	}

	for ip, indices := range ipToIndices {
		jobs <- dnsJob{ip: ip, indices: indices}
	}
	close(jobs)
	wg.Wait()

	if len(overrides) == 0 {
		return nil
	}

	return persistProxyMetadata(overrides)
}

func persistProxyMetadata(proxies []domain.Proxy) error {
	for i := 0; i < len(proxies); i += enrichmentUpdateBatchSize {
		end := i + enrichmentUpdateBatchSize
		if end > len(proxies) {
			end = len(proxies)
		}
		batch := proxies[i:end]
		if err := updateProxyMetadataBatch(batch); err != nil {
			return err
		}
	}
	return nil
}

func updateProxyMetadataBatch(batch []domain.Proxy) error {
	if len(batch) == 0 {
		return nil
	}

	values := make([]string, len(batch))
	args := make([]interface{}, 0, len(batch)*3)
	for i, proxy := range batch {
		values[i] = "(?::bigint, ?::text, ?::text)"
		args = append(args, proxy.ID, proxy.Country, proxy.EstimatedType)
	}

	query := fmt.Sprintf(`UPDATE proxies AS p
SET country = tmp.country,
    estimated_type = tmp.estimated_type
FROM (VALUES %s) AS tmp(id, country, estimated_type)
WHERE p.id = tmp.id`, strings.Join(values, ","))

	return DB.Exec(query, args...).Error
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

	if name := record.Country.Names["en"]; name != "" {
		return name
	}

	if record.Country.IsoCode != "" {
		return strings.ToUpper(record.Country.IsoCode)
	}

	return "N/A"
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
