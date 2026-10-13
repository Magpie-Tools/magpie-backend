package rotatingproxy

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"magpie/internal/api/dto"
	"magpie/internal/domain"
)

func TestBackendLongevityRenewedRotatorCertificate(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := createSelfSignedRotatorTLSFiles(t, dir)
	t.Setenv(envRotatingProxyHTTP3TLSCertFile, certPath)
	t.Setenv(envRotatingProxyHTTP3TLSKeyFile, keyPath)
	original, err := rotatorTLSConfig()
	if err != nil {
		t.Fatal(err)
	}
	firstCert, err := original.GetCertificate(&tls.ClientHelloInfo{})
	if err != nil {
		t.Fatal(err)
	}
	first, err := x509.ParseCertificate(firstCert.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	createSelfSignedRotatorTLSFiles(t, dir)
	renewed, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		t.Fatal(err)
	}
	disk, err := x509.ParseCertificate(renewed.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	current := original
	currentCert, err := current.GetCertificate(&tls.ClientHelloInfo{})
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := x509.ParseCertificate(currentCert.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("first_serial=%s renewed_disk_serial=%s currently_served_serial=%s", first.SerialNumber, disk.SerialNumber, loaded.SerialNumber)
	if loaded.SerialNumber.Cmp(disk.SerialNumber) != 0 {
		t.Fatal("certificate renewal is ignored for the life of the backend process")
	}
}

func TestBackendLongevityStoppedRotatorClosesConnectTunnel(t *testing.T) {
	oldGet, oldConnect, oldTraffic := getNextRotatingProxyFunc, connectThroughUpstreamFunc, recordManagedTrafficFunc
	getNextRotatingProxyFunc = func(uint, uint64) (*dto.RotatingProxyNext, error) {
		return &dto.RotatingProxyNext{Protocol: "http"}, nil
	}
	serverEnd, upstreamEnd := net.Pipe()
	connectThroughUpstreamFunc = func(string, *dto.RotatingProxyNext) (net.Conn, error) { return serverEnd, nil }
	finished := make(chan struct{})
	var once sync.Once
	recordManagedTrafficFunc = func(_ uint, requests, _ uint64) {
		if requests == 0 {
			once.Do(func() { close(finished) })
		}
	}
	ps := newProxyServer(domain.RotatingProxy{ID: 1, WorkspaceID: 1, Protocol: domain.Protocol{Name: "http"}})
	if err := ps.startHTTPServer(); err != nil {
		t.Fatal(err)
	}
	client, err := net.Dial("tcp", ps.listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = client.Close()
		_ = upstreamEnd.Close()
		ps.Stop()
		select {
		case <-finished:
		case <-time.After(time.Second):
			t.Error("tunnel handler did not exit during test cleanup")
		}
		getNextRotatingProxyFunc, connectThroughUpstreamFunc, recordManagedTrafficFunc = oldGet, oldConnect, oldTraffic
	})
	if _, err := io.WriteString(client, "CONNECT example.invalid:443 HTTP/1.1\r\nHost: example.invalid:443\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	response, err := http.ReadResponse(bufio.NewReader(client), &http.Request{Method: http.MethodConnect})
	if err != nil || response.StatusCode != 200 {
		t.Fatalf("CONNECT setup: %v %v", response, err)
	}
	ps.Stop()
	_ = client.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	var b [1]byte
	_, err = client.Read(b[:])
	t.Logf("read_after_rotator_stop=%v", err)
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		t.Fatal("Stop leaves the established CONNECT tunnel and its handler alive")
	}
	if err == nil {
		t.Fatal("stopped listener still has a readable tunnel")
	}
}

func TestStoppedRotatorClosesIdleSocksClients(t *testing.T) {
	for _, protocol := range []string{"socks4", "socks5"} {
		t.Run(protocol, func(t *testing.T) {
			ps := newProxyServer(domain.RotatingProxy{ID: 1, WorkspaceID: 1, Protocol: domain.Protocol{Name: protocol}})
			if err := ps.startSocksServer(); err != nil {
				t.Fatal(err)
			}
			defer ps.Stop()
			client, err := net.Dial("tcp", ps.listener.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			deadline := time.Now().Add(time.Second)
			for {
				ps.connections.mu.Lock()
				n := len(ps.connections.clients)
				ps.connections.mu.Unlock()
				if n == 1 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("client not registered")
				}
				time.Sleep(time.Millisecond)
			}
			ps.Stop()
			client.SetReadDeadline(time.Now().Add(time.Second))
			var b [1]byte
			if _, err := client.Read(b[:]); err == nil {
				t.Fatal("idle client survived stop")
			} else if e, ok := err.(net.Error); ok && e.Timeout() {
				t.Fatal("idle client survived stop")
			}
		})
	}
}

func TestRotatorTunnelCapacityAndLateUpstream(t *testing.T) {
	t.Setenv(envRotatingProxyMaxConcurrentTunnels, "1")
	connections := newRotatorConnections()
	first, firstPeer := net.Pipe()
	defer firstPeer.Close()
	if !connections.add(first, false) {
		t.Fatal("first client rejected")
	}
	second, secondPeer := net.Pipe()
	defer secondPeer.Close()
	if connections.add(second, false) {
		t.Fatal("capacity exceeded")
	}
	connections.stop()
	late, latePeer := net.Pipe()
	defer latePeer.Close()
	if connections.add(late, true) {
		t.Fatal("upstream registered after stop")
	}
	latePeer.SetReadDeadline(time.Now().Add(time.Second))
	var b [1]byte
	if _, err := latePeer.Read(b[:]); err != io.EOF {
		t.Fatalf("late upstream was not closed: %v", err)
	}
}
