package helper

import (
	"context"
	"crypto/tls"
	"golang.org/x/net/proxy"
	"magpie/models"
	"magpie/settings"
	"net"
	"net/http"
	"net/url"
	"time"
)

func CreateTransport(proxyToCheck models.Proxy, judge *models.Judge, protocol string) (*http.Transport, error) {
	// Base configuration with keep-alives disabled
	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   time.Duration(settings.GetConfig().Checker.Timeout) * time.Millisecond,
			KeepAlive: 0, // KeepAlive disabled
		}).DialContext,
		DisableKeepAlives:     true,
		MaxIdleConns:          0,
		MaxIdleConnsPerHost:   0,
		IdleConnTimeout:       0,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	switch protocol {
	case "http", "https":
		// Configure HTTP/HTTPS proxy
		proxyURL := &url.URL{
			Scheme: "http",
			Host:   proxyToCheck.GetFullProxy(),
		}
		if proxyToCheck.HasAuth() {
			proxyURL.User = url.UserPassword(proxyToCheck.Username, proxyToCheck.Password)
		}
		transport.Proxy = http.ProxyURL(proxyURL)

		// Override dialer to resolve judge's host to pre-defined IP
		dialer := &net.Dialer{Timeout: time.Duration(settings.GetConfig().Checker.Timeout) * time.Millisecond}
		transport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			if host, port, err := net.SplitHostPort(addr); err == nil && host == judge.GetHostname() {
				addr = net.JoinHostPort(judge.GetIp(), port)
			}
			return dialer.DialContext(ctx, network, addr)
		}

	default:
		// Handle SOCKS5 proxy
		var auth *proxy.Auth
		if proxyToCheck.HasAuth() {
			auth = &proxy.Auth{User: proxyToCheck.Username, Password: proxyToCheck.Password}
		}
		socksDialer, err := proxy.SOCKS5("tcp", proxyToCheck.GetFullProxy(), auth, &net.Dialer{
			Timeout: time.Duration(settings.GetConfig().Checker.Timeout) * time.Millisecond,
		})
		if err != nil {
			return nil, err
		}
		transport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			return socksDialer.Dial(network, addr)
		}
	}

	// Configure TLS to use judge's hostname
	transport.TLSClientConfig = &tls.Config{
		ServerName:         judge.GetHostname(),
		InsecureSkipVerify: false,
	}

	return transport, nil
}
