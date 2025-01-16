package checker

import (
	"context"
	"crypto/tls"
	"golang.org/x/net/proxy"
	"io"
	"magpie/models"
	"magpie/settings"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

// CheckerRequest makes a request to the provided siteUrl with the provided proxy
func CheckerRequest(proxyToCheck models.Proxy, targetIp string, siteName *url.URL, regex string, protocol string) (string, int, error) {
	privateTransport := GetSharedTransport()
	isAuthProxy := false

	if proxyToCheck.Username != "" && proxyToCheck.Password != "" {
		isAuthProxy = true
	}

	switch protocol {
	case "http", "https":
		dialer := net.Dialer{
			Timeout: time.Millisecond * time.Duration(settings.GetConfig().Checker.Timeout),
		}

		proxyUrl := &url.URL{
			Scheme: strings.Replace(protocol, "https", "http", 1),
			Host:   proxyToCheck.GetFullProxy(),
		}

		if isAuthProxy {
			proxyUrl.User = url.UserPassword(proxyToCheck.Username, proxyToCheck.Password)
		}

		privateTransport.Proxy = http.ProxyURL(proxyUrl)

		privateTransport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			if strings.Contains(addr, siteName.Hostname()) {
				addr = net.JoinHostPort(targetIp, siteName.Port())
			}
			return dialer.DialContext(ctx, network, addr)
		}
	default:
		var proxyAuth *proxy.Auth

		if isAuthProxy {
			proxyAuth = &proxy.Auth{
				User:     proxyToCheck.Username,
				Password: proxyToCheck.Password,
			}
		}

		dialer, err := proxy.SOCKS5("tcp", proxyToCheck.GetFullProxy(), proxyAuth,
			&net.Dialer{
				Timeout: time.Millisecond * time.Duration(settings.GetConfig().Checker.Timeout),
			})

		if err != nil {
			return "Error creating SOCKS dialer", -1, err
		}

		privateTransport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			return dialer.Dial(network, addr)
		}

	}

	privateTransport.TLSClientConfig = &tls.Config{
		ServerName:         siteName.Hostname(),
		InsecureSkipVerify: false,
	}

	client := GetClientFromPool()
	client.Transport = privateTransport

	req, err := http.NewRequest("GET", siteName.String(), nil)
	if err != nil {
		ReturnClientToPool(client)
		return "Error creating HTTP request", -1, err
	}

	req.Header.Set("Connection", "close")

	resp, err := client.Do(req)
	ReturnClientToPool(client)
	if err != nil {
		return "Error making HTTP request", -1, err
	}
	defer resp.Body.Close()

	status := resp.StatusCode
	resBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "Error reading response body", -1, err
	}

	html := string(resBody)

	if !CheckForValidResponse(html, regex) {
		return "Invalid response", -1, nil
	}

	return html, status, nil
}

func CheckForValidResponse(html string, regex string) bool {
	if strings.EqualFold(regex, "default") {
		html = strings.ReplaceAll(html, "_", "-")
		html = strings.ToUpper(html)

		for _, header := range settings.GetConfig().Checker.StandardHeader {
			if !strings.Contains(html, header) {

				return false
			}
		}

		return true
	}

	re, err := regexp.Compile(regex)
	if err != nil {
		return false
	}

	return re.MatchString(html)
}
