package support

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"magpie/internal/domain"

	"github.com/quic-go/quic-go/http3"
)

func TestCreateHTTP3Transport_RoundTripUsesProxyAndPreservesJudgeAuthority(t *testing.T) {
	for _, protocol := range []string{TransportHTTP3, TransportQUIC} {
		t.Run(protocol, func(t *testing.T) {
			certificateServer := httptest.NewTLSServer(http.NotFoundHandler())
			certificates := certificateServer.TLS.Certificates
			roots := x509.NewCertPool()
			roots.AddCert(certificateServer.Certificate())
			certificateServer.Close()
			packetConn, err := net.ListenPacket("udp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			server := &http3.Server{
				TLSConfig:       &tls.Config{Certificates: certificates},
				EnableDatagrams: protocol == TransportQUIC,
				Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Host != "judge.example.test" || r.URL.RequestURI() != "/check?source=local" || r.ProtoMajor != 3 {
						t.Errorf("HTTP/3 request = %s %s %s", r.Host, r.URL.RequestURI(), r.Proto)
					}
					_, _ = io.WriteString(w, "judge response")
				}),
			}
			serveDone := make(chan error, 1)
			go func() { serveDone <- server.Serve(packetConn) }()
			t.Cleanup(func() {
				_ = server.Close()
				_ = packetConn.Close()
				select {
				case <-serveDone:
				case <-time.After(2 * time.Second):
					t.Error("HTTP/3 server did not stop")
				}
			})
			host, port, err := net.SplitHostPort(packetConn.LocalAddr().String())
			if err != nil {
				t.Fatal(err)
			}
			portNumber, err := strconv.Atoi(port)
			if err != nil {
				t.Fatal(err)
			}
			judge := &domain.Judge{FullString: "https://judge.example.test/check?source=local"}
			transport, closeTransport, err := CreateTransport(domain.Proxy{IP: host, Port: uint16(portNumber)}, judge, "http", protocol)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(closeTransport)
			transport.(*http3.Transport).TLSClientConfig.RootCAs = roots
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			request, err := http.NewRequestWithContext(ctx, http.MethodGet, judge.FullString, nil)
			if err != nil {
				t.Fatal(err)
			}
			response, err := (&http.Client{Transport: transport}).Do(request)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			if err != nil || string(body) != "judge response" {
				t.Fatalf("HTTP/3 response = %q, error=%v", body, err)
			}
		})
	}
}
