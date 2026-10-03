package checker

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"magpie/internal/domain"
	"magpie/internal/support"
)

func BenchmarkProxyCheckRequest(b *testing.B) {
	for _, size := range []int{2048, 65536} {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			b.Setenv(envCheckerMaxResponseBody, "8192")
			resetCheckerHTTPClientCacheForTests()
			b.Cleanup(resetCheckerHTTPClientCacheForTests)
			body := strings.Repeat("x", size)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Length", strconv.Itoa(len(body)))
				_, _ = w.Write([]byte(body))
			}))
			b.Cleanup(server.Close)
			host, port, err := net.SplitHostPort(server.Listener.Addr().String())
			if err != nil {
				b.Fatal(err)
			}
			portNumber, err := strconv.Atoi(port)
			if err != nil {
				b.Fatal(err)
			}
			proxy := domain.Proxy{IP: host, Port: uint16(portNumber)}
			judge := &domain.Judge{FullString: "http://judge.example.test/"}
			b.ReportAllocs()
			b.SetBytes(int64(min(size, 8193)))
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					_, err := ProxyCheckRequest(proxy, judge, "http", support.TransportTCP, 1000)
					if size <= 8192 && err != nil {
						b.Error(err)
						return
					}
					if size > 8192 && (err == nil || !strings.Contains(err.Error(), "judge response body exceeded")) {
						b.Errorf("oversized response error = %v", err)
						return
					}
				}
			})
		})
	}
}
