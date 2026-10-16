package scraper

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"magpie/internal/support"
)

func TestBrowserEgressSeparatesNetworkFailuresFromPolicyDenials(t *testing.T) {
	t.Setenv("ALLOW_PRIVATE_NETWORK_EGRESS", "false")
	for _, test := range []struct {
		name   string
		err    error
		status int
		denied bool
	}{
		{name: "EOF", err: io.EOF, status: http.StatusBadGateway},
		{name: "timeout", err: context.DeadlineExceeded, status: http.StatusBadGateway},
		{name: "DNS", err: &net.DNSError{Err: "no such host", Name: "source.invalid", IsNotFound: true}, status: http.StatusBadGateway},
		{name: "resolved private address", err: fmt.Errorf("dial: %w", &support.OutboundTargetError{IP: "127.0.0.1", Reason: "loopback"}), status: http.StatusForbidden, denied: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var rejected error
			proxy, err := startBrowserEgressProxy(context.Background(), func(err error) { rejected = err })
			if err != nil {
				t.Fatal(err)
			}
			defer proxy.close()
			proxy.transport.DialContext = func(context.Context, string, string) (net.Conn, error) { return nil, test.err }
			response := httptest.NewRecorder()
			proxy.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "http://source.example/image.png", nil))
			if response.Code != test.status || (rejected != nil) != test.denied {
				t.Fatalf("status=%d rejection=%v, want status=%d denied=%v", response.Code, rejected, test.status, test.denied)
			}
			if test.denied && !errors.Is(rejected, support.ErrUnsafeOutboundTarget) {
				t.Fatalf("lost policy error: %v", rejected)
			}
		})
	}
}

func TestBrowserEgressCONNECTFailureDoesNotRejectScrape(t *testing.T) {
	t.Setenv("ALLOW_PRIVATE_NETWORK_EGRESS", "true")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	proxy, err := startBrowserEgressProxy(context.Background(), func(err error) { t.Errorf("network failure rejected scrape: %v", err) })
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.close()
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodConnect, "http://"+address, nil)
	proxy.ServeHTTP(response, request)
	if response.Code != http.StatusBadGateway {
		t.Fatalf("CONNECT status=%d, want 502", response.Code)
	}
}

func TestBrowserEgressRejectsPrivateHTTPAndCONNECTBeforeDial(t *testing.T) {
	t.Setenv("ALLOW_PRIVATE_NETWORK_EGRESS", "false")
	var hits, denials atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1) }))
	defer target.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	proxy, err := startBrowserEgressProxy(ctx, func(error) { denials.Add(1) })
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.close()
	address, _ := url.Parse(proxy.address)
	transport := &http.Transport{Proxy: http.ProxyURL(address)}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: time.Second}
	for _, scheme := range []string{"http", "https"} {
		resp, err := client.Get(strings.Replace(target.URL, "http", scheme, 1))
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode != http.StatusForbidden {
				t.Fatalf("status=%d", resp.StatusCode)
			}
		}
	}
	if hits.Load() != 0 || denials.Load() != 2 {
		t.Fatalf("private requests=%d denied=%d", hits.Load(), denials.Load())
	}
}

func TestBrowserEgressCloseStopsEstablishedCONNECT(t *testing.T) {
	t.Setenv("ALLOW_PRIVATE_NETWORK_EGRESS", "true")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	upstreamDone := make(chan struct{})
	go func() {
		defer close(upstreamDone)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		var data [1]byte
		_, _ = conn.Read(data[:])
	}()
	proxy, err := startBrowserEgressProxy(context.Background(), func(err error) { t.Error(err) })
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.close()
	address, _ := url.Parse(proxy.address)
	conn, err := net.Dial("tcp", address.Host)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	fmt.Fprintf(conn, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", listener.Addr(), listener.Addr())
	response, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: http.MethodConnect})
	if err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("CONNECT response: %v %v", response, err)
	}
	proxy.close()
	select {
	case <-upstreamDone:
	case <-time.After(time.Second):
		t.Fatal("proxy close leaked an upstream tunnel")
	}
}
