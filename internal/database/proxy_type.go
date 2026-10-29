package database

import (
	"regexp"
	"strings"

	"golang.org/x/net/publicsuffix"
)

// These hosting ASNs are checked before organization-name heuristics. Keep the
// list limited to known hosting networks; a consumer ISP's ASN alone does not
// distinguish its residential, business, and mobile customers. Holder names were
// checked against RIPEstat's as-overview endpoint on 2026-10-29:
// https://stat.ripe.net/data/as-overview/data.json?resource=AS14061
var datacenterASNs = map[uint]struct{}{
	8075:   {}, // Microsoft
	13335:  {}, // Cloudflare
	14061:  {}, // DigitalOcean
	14618:  {}, // Amazon
	15169:  {}, // Google
	16276:  {}, // OVH
	16509:  {}, // Amazon
	20473:  {}, // Vultr / The Constant Company
	24940:  {}, // Hetzner
	63949:  {}, // Akamai Connected Cloud / Linode
	396982: {}, // Google Cloud
}

var (
	datacenterOrganization = regexp.MustCompile(`(?i)\b(amazon|google[ -](?:llc|cloud|inc)|microsoft|digital[ -]?ocean|linode|hetzner|ovh(?:cloud)?|vultr|ibm|alibaba|tencent|cloudflare|rackspace|hostinger|upcloud|azure|gcp|aws)\b`)
	ispOrganization        = regexp.MustCompile(`(?i)\b(isp|broadband|telecom(?:munications)?|communications|carrier|fiber|fibre)\b`)
	// Match access-network markers in hostname labels, including common forms
	// such as "adsl123". Generic allocation words (ip, static, dynamic, pool,
	// customer, user) do not establish that an address is residential.
	residentialDNS = regexp.MustCompile(`(?i)(^|[.-])(adsl|vdsl|sdsl|xdsl|dsl|cable|ftth|fttp|pppoe|residential|broadband)[0-9]*([.-]|$)`)
	// Conflicting access or server markers leave the ASN estimate unchanged.
	nonResidentialDNS = regexp.MustCompile(`(?i)(^|[.-])(mobile|lte|umts|gprs|[345]g|vps|vds|server|dedicated|cloud|compute|ec2|colo|colocation|datacenter)([0-9.-]|$)`)
)

func proxyTypeFromASN(asn uint, organization string) string {
	if _, ok := datacenterASNs[asn]; ok {
		return "Datacenter"
	}
	if datacenterOrganization.MatchString(organization) {
		return "Datacenter"
	}
	if ispOrganization.MatchString(organization) {
		return "ISP"
	}
	return "N/A"
}

func canRefineProxyTypeWithDNS(proxyType string) bool {
	return proxyType == "ISP" || proxyType == "N/A" || proxyType == "unknown"
}

func refineProxyTypeWithDNS(proxyType string, names []string) string {
	if !canRefineProxyTypeWithDNS(proxyType) {
		return proxyType
	}

	residential := false
	for _, name := range names {
		name = strings.ToLower(strings.TrimSuffix(strings.TrimSpace(name), "."))
		domain, err := publicsuffix.EffectiveTLDPlusOne(name)
		if err != nil {
			continue
		}
		// Provider names such as "residential.example" or "dsl.co.uk" are
		// not evidence about the address. Public suffix data is local.
		host := strings.TrimSuffix(name, "."+domain)
		if host == name {
			continue
		}
		if nonResidentialDNS.MatchString(host) {
			return proxyType
		}
		if residentialDNS.MatchString(host) {
			residential = true
		}
	}
	if residential {
		return "Residential"
	}
	return proxyType
}
