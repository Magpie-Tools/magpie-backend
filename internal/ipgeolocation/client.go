package ipgeolocation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const Endpoint = "https://api.ipgeolocation.io/v3/ipgeo"

type Result struct {
	IP       string `json:"ip"`
	Location struct {
		CountryCode string `json:"country_code2"`
		CountryName string `json:"country_name"`
	} `json:"location"`
	Network struct {
		ConnectionType string `json:"connection_type"`
	} `json:"network"`
	ASN struct {
		Number       string `json:"as_number"`
		Organization string `json:"organization"`
		Type         string `json:"type"`
	} `json:"asn"`
	Company struct {
		Name string `json:"name"`
		Type string `json:"type"`
	} `json:"company"`
	Security *struct {
		IsResidentialProxy *bool    `json:"is_residential_proxy"`
		IsProxy            *bool    `json:"is_proxy"`
		IsVPN              *bool    `json:"is_vpn"`
		IsTor              *bool    `json:"is_tor"`
		IsCloudProvider    *bool    `json:"is_cloud_provider"`
		ProxyConfidence    *int     `json:"proxy_confidence_score"`
		ProxyLastSeen      string   `json:"proxy_last_seen"`
		ProxyProviders     []string `json:"proxy_provider_names"`
	} `json:"security,omitempty"`
}

func (r Result) EstimatedType() string {
	company := strings.ToUpper(strings.TrimSpace(r.Company.Type))
	connection := strings.ToUpper(strings.TrimSpace(r.Network.ConnectionType))
	switch company {
	case "HOSTING", "DATACENTER", "DATA CENTER":
		return "Datacenter"
	case "MOBILE", "CELLULAR":
		return "Mobile"
	case "RESIDENTIAL":
		return "Residential"
	}
	switch connection {
	case "MOBILE", "CELLULAR", "3G", "4G", "5G", "LTE":
		return "Mobile"
	}
	if r.Security != nil && r.Security.IsResidentialProxy != nil && *r.Security.IsResidentialProxy {
		return "Residential"
	}
	switch connection {
	case "DSL", "ADSL", "VDSL", "CABLE", "FTTH", "FTTP":
		return "Residential"
	}
	if company == "ISP" {
		return "ISP"
	}
	switch strings.ToUpper(strings.TrimSpace(r.ASN.Type)) {
	case "HOSTING", "DATACENTER":
		return "Datacenter"
	case "ISP":
		return "ISP"
	}
	if r.Security != nil && r.Security.IsCloudProvider != nil && *r.Security.IsCloudProvider {
		return "Datacenter"
	}
	return ""
}

// PublicIP rejects local and special-use addresses before spending API credits.
func PublicIP(value string) (string, bool) {
	ip, err := netip.ParseAddr(value)
	if err != nil || ip.Zone() != "" {
		return "", false
	}
	ip = ip.Unmap()
	if !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
		return "", false
	}
	for _, prefix := range specialNetworks {
		if prefix.Contains(ip) {
			return "", false
		}
	}
	return ip.String(), true
}

var specialNetworks = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"), netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("192.0.0.0/24"), netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"), netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"), netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("2001:db8::/32"), netip.MustParsePrefix("2001:2::/48"),
}

type Response struct {
	Result     Result
	Credits    int
	RetryAfter time.Duration
	Status     int
}

type Client struct{ HTTPClient *http.Client }

func Cost(security bool) int {
	if security {
		return 3
	}
	return 1
}

func (c Client) Lookup(ctx context.Context, key, ip string, security bool) (Response, error) {
	var response Response
	canonical, ok := PublicIP(ip)
	if !ok || strings.TrimSpace(key) == "" {
		return response, errors.New("IPGeolocation.io requires a public IP and an API key")
	}
	endpoint, _ := url.Parse(Endpoint)
	query := endpoint.Query()
	query.Set("apiKey", strings.TrimSpace(key))
	query.Set("ip", canonical)
	query.Set("fields", "location,network,asn,company")
	if security {
		query.Set("include", "security")
		query.Set("fields", query.Get("fields")+",security")
	}
	endpoint.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return response, errors.New("could not create IPGeolocation.io request")
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "magpie-ipgeolocation/1.0")
	client := c.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 20 * time.Second}
	}
	// The key is in the URL. Never follow redirects or return transport errors
	// containing that URL, including when a custom client is used in tests.
	copyClient := *client
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := copyClient.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return response, ctx.Err()
		}
		return response, errors.New("IPGeolocation.io request failed; billing is unknown")
	}
	defer resp.Body.Close()
	response.Status = resp.StatusCode
	if seconds, err := strconv.ParseInt(resp.Header.Get("Retry-After"), 10, 64); err == nil && seconds > 0 {
		response.RetryAfter = time.Duration(min(seconds, int64((1<<63-1)/time.Second))) * time.Second
	} else if at, err := http.ParseTime(resp.Header.Get("Retry-After")); err == nil {
		response.RetryAfter = max(0, time.Until(at))
	}
	if resp.StatusCode != http.StatusOK {
		return response, fmt.Errorf("IPGeolocation.io returned HTTP %d; check the API key, plan access, and remaining credits", resp.StatusCode)
	}
	response.Credits = Cost(security)
	if raw := resp.Header.Get("X-Credits-Charged"); raw != "" {
		charged, err := strconv.Atoi(raw)
		if err != nil || charged < 0 || charged > 100000 {
			return response, errors.New("IPGeolocation.io returned an invalid credit charge header")
		}
		response.Credits = charged
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1024*1024+1))
	if err != nil || len(body) > 1024*1024 {
		return response, errors.New("could not read IPGeolocation.io response")
	}
	if err := json.Unmarshal(body, &response.Result); err != nil {
		return response, errors.New("invalid IPGeolocation.io JSON response")
	}
	returned, err := netip.ParseAddr(response.Result.IP)
	if err != nil || returned.Unmap().String() != canonical {
		return response, errors.New("IPGeolocation.io returned a different IP address")
	}
	if code := response.Result.Location.CountryCode; len(code) != 0 && (len(code) != 2 || code[0] < 'A' || code[0] > 'Z' || code[1] < 'A' || code[1] > 'Z') {
		return response, errors.New("IPGeolocation.io returned an invalid country code")
	}
	if security := response.Result.Security; security != nil {
		if security.ProxyProviders == nil {
			security.ProxyProviders = []string{}
		}
		if security.ProxyConfidence != nil && (*security.ProxyConfidence < 0 || *security.ProxyConfidence > 100) {
			return response, errors.New("IPGeolocation.io returned an invalid confidence score")
		}
	}
	return response, nil
}
