package rotatingproxy

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/charmbracelet/log"

	"magpie/internal/api/dto"
	"magpie/internal/database"
	"magpie/internal/domain"
)

const (
	connectEstablishedResponse = "HTTP/1.1 200 Connection Established\r\nProxy-Agent: Magpie Rotator\r\n\r\n"
)

var (
	getNextRotatingProxyFunc   = database.GetNextRotatingProxy
	dialUpstreamFunc           = dialUpstream
	performUpstreamConnectFunc = performUpstreamConnect
)

type proxyHandler struct {
	rotator domain.RotatingProxy
}

func (h *proxyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !h.authenticateClient(w, r) {
		return
	}

	switch strings.ToUpper(r.Method) {
	case http.MethodConnect:
		h.handleConnect(w, r)
	default:
		h.handleHTTP(w, r)
	}
}

func (h *proxyHandler) authenticateClient(w http.ResponseWriter, r *http.Request) bool {
	if !h.rotator.AuthRequired {
		return true
	}

	header := strings.TrimSpace(r.Header.Get("Proxy-Authorization"))
	if header == "" {
		writeProxyAuthRequired(w)
		return false
	}

	parts := strings.SplitN(header, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Basic") {
		writeProxyAuthRequired(w)
		return false
	}

	decoded, err := base64.StdEncoding.DecodeString(parts[1])
	if err != nil {
		writeProxyAuthRequired(w)
		return false
	}

	creds := strings.SplitN(string(decoded), ":", 2)
	if len(creds) != 2 {
		writeProxyAuthRequired(w)
		return false
	}

	if creds[0] != h.rotator.AuthUsername || creds[1] != h.rotator.AuthPassword {
		writeProxyAuthRequired(w)
		return false
	}

	return true
}

func writeProxyAuthRequired(w http.ResponseWriter) {
	w.Header().Set("Proxy-Authenticate", `Basic realm="Magpie Rotator"`)
	w.WriteHeader(http.StatusProxyAuthRequired)
	_, _ = w.Write([]byte("Proxy authentication required"))
}

func (h *proxyHandler) handleHTTP(w http.ResponseWriter, r *http.Request) {
	next, err := getNextRotatingProxyFunc(h.rotator.UserID, h.rotator.ID)
	if err != nil {
		http.Error(w, "failed to acquire upstream proxy", http.StatusBadGateway)
		return
	}

	if !supportedUpstream(next.Protocol) {
		http.Error(w, "upstream protocol not supported by rotator", http.StatusBadGateway)
		return
	}

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read request body", http.StatusBadRequest)
		return
	}
	_ = r.Body.Close()

	targetURL := r.URL
	if !targetURL.IsAbs() {
		scheme := "http"
		if strings.HasPrefix(strings.ToLower(r.Proto), "https") {
			scheme = "https"
		}
		targetURL = &url.URL{
			Scheme:   scheme,
			Host:     r.Host,
			Path:     r.URL.Path,
			RawQuery: r.URL.RawQuery,
		}
	}

	newReq, err := http.NewRequestWithContext(r.Context(), r.Method, targetURL.String(), bytes.NewReader(bodyBytes))
	if err != nil {
		http.Error(w, "failed to build upstream request", http.StatusInternalServerError)
		return
	}

	newReq.Header = r.Header.Clone()
	newReq.Header.Del("Proxy-Authorization")

	transport := buildHTTPTransport(next)
	resp, err := transport.RoundTrip(newReq)
	if err != nil {
		http.Error(w, "upstream proxy request failed", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	copyHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	if _, err := io.Copy(w, resp.Body); err != nil {
		log.Warn("rotating proxy: failed to copy response body", "rotator_id", h.rotator.ID, "error", err)
	}
}

func (h *proxyHandler) handleConnect(w http.ResponseWriter, r *http.Request) {
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "hijacking not supported", http.StatusInternalServerError)
		return
	}

	clientConn, buf, err := hijacker.Hijack()
	if err != nil {
		http.Error(w, "failed to hijack connection", http.StatusInternalServerError)
		return
	}

	defer func() {
		if err := clientConn.Close(); err != nil {
			log.Debug("rotating proxy: client connection close", "error", err)
		}
	}()

	next, err := getNextRotatingProxyFunc(h.rotator.UserID, h.rotator.ID)
	if err != nil {
		writeHijackedResponse(buf, http.StatusBadGateway, "Failed to acquire upstream proxy")
		return
	}

	if !supportedUpstream(next.Protocol) {
		writeHijackedResponse(buf, http.StatusBadGateway, "Upstream protocol not supported by rotator")
		return
	}

	upConn, err := dialUpstreamFunc(next)
	if err != nil {
		writeHijackedResponse(buf, http.StatusBadGateway, "Failed to connect to upstream proxy")
		return
	}

	if err := performUpstreamConnectFunc(upConn, r.Host, next); err != nil {
		_ = upConn.Close()
		writeHijackedResponse(buf, http.StatusBadGateway, "Upstream CONNECT failed")
		return
	}

	if _, err := clientConn.Write([]byte(connectEstablishedResponse)); err != nil {
		_ = upConn.Close()
		return
	}

	pipeConnections(clientConn, upConn)
}

func writeHijackedResponse(buf *bufio.ReadWriter, status int, message string) {
	fmt.Fprintf(buf, "HTTP/1.1 %d %s\r\nContent-Type: text/plain\r\nContent-Length: %d\r\n\r\n%s",
		status,
		http.StatusText(status),
		len(message),
		message,
	)
	_ = buf.Flush()
}

func copyHeaders(dst, src http.Header) {
	for key, values := range src {
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

func supportedUpstream(protocol string) bool {
	switch strings.ToLower(protocol) {
	case "http", "https":
		return true
	default:
		return false
	}
}

func buildHTTPTransport(next *dto.RotatingProxyNext) *http.Transport {
	proxyURL := &url.URL{
		Scheme: strings.ToLower(next.Protocol),
		Host:   fmt.Sprintf("%s:%d", next.IP, next.Port),
	}
	if next.HasAuth {
		proxyURL.User = url.UserPassword(next.Username, next.Password)
	}

	transport := &http.Transport{
		Proxy:               http.ProxyURL(proxyURL),
		DisableKeepAlives:   true,
		MaxIdleConns:        0,
		IdleConnTimeout:     0,
		TLSHandshakeTimeout: 10 * time.Second,
	}

	if strings.ToLower(next.Protocol) == "https" {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}

	return transport
}

func dialUpstream(next *dto.RotatingProxyNext) (net.Conn, error) {
	address := fmt.Sprintf("%s:%d", next.IP, next.Port)
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	conn, err := dialer.Dial("tcp", address)
	if err != nil {
		return nil, err
	}

	if strings.ToLower(next.Protocol) == "https" {
		tlsConn := tls.Client(conn, &tls.Config{InsecureSkipVerify: true})
		if err := tlsConn.Handshake(); err != nil {
			conn.Close()
			return nil, err
		}
		return tlsConn, nil
	}

	return conn, nil
}

func performUpstreamConnect(conn net.Conn, targetHost string, next *dto.RotatingProxyNext) error {
	request := fmt.Sprintf("CONNECT %s HTTP/1.1\r\nHost: %s\r\nProxy-Connection: Keep-Alive\r\n", targetHost, targetHost)
	if next.HasAuth {
		auth := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", next.Username, next.Password)))
		request += fmt.Sprintf("Proxy-Authorization: Basic %s\r\n", auth)
	}
	request += "\r\n"

	if _, err := conn.Write([]byte(request)); err != nil {
		return err
	}

	reader := bufio.NewReader(conn)
	resp, err := http.ReadResponse(reader, &http.Request{Method: http.MethodConnect})
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, resp.Body)
		return errors.New("upstream returned non-200 response")
	}

	return nil
}

func pipeConnections(left, right net.Conn) {
	errCh := make(chan error, 2)

	go func() {
		_, err := io.Copy(left, right)
		errCh <- err
	}()

	go func() {
		_, err := io.Copy(right, left)
		errCh <- err
	}()

	<-errCh
	left.Close()
	right.Close()
}
