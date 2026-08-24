package domain

import (
	"errors"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"magpie/internal/security"

	"golang.org/x/net/idna"
	"gorm.io/gorm"
)

type Proxy struct {
	ID        uint64  `gorm:"primaryKey;autoIncrement"`
	IP        string  `gorm:"column:host;type:varchar(253);index:idx_proxy_addr,priority:1" json:"ip"`
	IPAddress *string `gorm:"column:ip_address;type:inet;index" json:"-"`
	Port      uint16  `gorm:"not null;index:idx_proxy_addr,priority:2"`
	Username  string  `gorm:"-"`
	Password  string  `gorm:"-" json:"password"`

	Country       string `gorm:"size:56;not null"` // Human-readable country name
	EstimatedType string `gorm:"size:20;not null"` // ISP, Datacenter, Residential

	// Relationships
	Statistics  []ProxyStatistic  `gorm:"foreignKey:ProxyID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	ScrapeSites []ScrapeSite      `gorm:"many2many:proxy_scrape_site;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	Reputations []ProxyReputation `gorm:"foreignKey:ProxyID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`

	Workspaces []Workspace `gorm:"many2many:user_proxies;joinForeignKey:ProxyID;joinReferences:WorkspaceID;"`

	Hash      []byte    `gorm:"type:bytea;uniqueIndex;size:32"` // Keyed fingerprint of exact Host|Port|Username|Password
	CreatedAt time.Time `gorm:"autoCreateTime"`
}

func (proxy *Proxy) BeforeSave(_ *gorm.DB) error {
	if proxy.IP != "" {
		if err := proxy.SetHost(proxy.IP); err != nil {
			return err
		}
	} else if proxy.IPAddress != nil {
		if err := proxy.SetIP(*proxy.IPAddress); err != nil {
			return err
		}
	}

	if len(proxy.Hash) == 0 {
		return proxy.GenerateHash()
	}
	return nil
}

func (proxy *Proxy) AfterFind(_ *gorm.DB) error {
	if proxy.IP == "" && proxy.IPAddress != nil {
		return proxy.SetIP(*proxy.IPAddress)
	}
	return nil
}

func (proxy *Proxy) GenerateHash() error {
	if proxy.IP != "" {
		if err := proxy.SetHost(proxy.IP); err != nil {
			return err
		}
	}
	hash, err := security.FingerprintProxyRoute(
		proxy.GetHost(),
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
	parsedIP, err := netip.ParseAddr(strings.TrimSpace(ip))
	if err != nil || parsedIP.Zone() != "" {
		return errors.New("invalid IP address")
	}
	canonical := parsedIP.Unmap().String()
	proxy.IP = canonical
	proxy.IPAddress = new(canonical)
	return nil
}

func (proxy *Proxy) SetHost(host string) error {
	canonical, ipAddress, err := canonicalizeProxyHost(host)
	if err != nil {
		return err
	}
	proxy.IP = canonical
	proxy.IPAddress = ipAddress
	return nil
}

func (proxy *Proxy) GetFullProxy() string {
	return net.JoinHostPort(proxy.GetHost(), strconv.Itoa(int(proxy.Port)))
}

func (proxy *Proxy) GetHost() string {
	if proxy.IP != "" {
		return proxy.IP
	}
	if proxy.IPAddress != nil {
		return *proxy.IPAddress
	}
	return ""
}

// GetIp retains the existing API name. It returns the route host, which may be
// an IP address or a DNS hostname.
func (proxy *Proxy) GetIp() string {
	return proxy.GetHost()
}

func (proxy *Proxy) GetIPAddress() string {
	if proxy.IPAddress != nil {
		return *proxy.IPAddress
	}
	parsed, err := netip.ParseAddr(strings.TrimSpace(proxy.IP))
	if err != nil {
		return ""
	}
	return parsed.Unmap().String()
}

func (proxy *Proxy) IsHostname() bool {
	return proxy.GetHost() != "" && proxy.GetIPAddress() == ""
}

func (proxy *Proxy) HasAuth() bool {
	return proxy.Username != "" && proxy.Password != ""
}

func canonicalizeProxyHost(raw string) (string, *string, error) {
	host := strings.TrimSpace(raw)
	if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		host = strings.TrimSpace(host[1 : len(host)-1])
	}
	if host == "" {
		return "", nil, errors.New("proxy host is required")
	}

	if parsed, err := netip.ParseAddr(host); err == nil && parsed.Zone() == "" {
		canonical := parsed.Unmap().String()
		return canonical, new(canonical), nil
	}
	if strings.Contains(host, ":") {
		return "", nil, errors.New("invalid proxy host")
	}

	host = strings.TrimSuffix(host, ".")
	ascii, err := idna.Lookup.ToASCII(host)
	if err != nil {
		return "", nil, errors.New("invalid proxy hostname")
	}
	ascii = strings.ToLower(strings.TrimSpace(ascii))
	if len(ascii) == 0 || len(ascii) > 253 {
		return "", nil, errors.New("invalid proxy hostname")
	}

	hasLetter := false
	for _, label := range strings.Split(ascii, ".") {
		if !validHostnameLabel(label) {
			return "", nil, errors.New("invalid proxy hostname")
		}
		for i := 0; i < len(label); i++ {
			if label[i] >= 'a' && label[i] <= 'z' {
				hasLetter = true
				break
			}
		}
	}
	if !hasLetter {
		return "", nil, errors.New("invalid proxy hostname")
	}

	return ascii, nil, nil
}

func validHostnameLabel(label string) bool {
	if len(label) == 0 || len(label) > 63 || !isHostnameAlphaNumeric(label[0]) || !isHostnameAlphaNumeric(label[len(label)-1]) {
		return false
	}
	for i := 1; i < len(label)-1; i++ {
		if !isHostnameAlphaNumeric(label[i]) && label[i] != '-' {
			return false
		}
	}
	return true
}

func isHostnameAlphaNumeric(value byte) bool {
	return value >= 'a' && value <= 'z' || value >= '0' && value <= '9'
}
