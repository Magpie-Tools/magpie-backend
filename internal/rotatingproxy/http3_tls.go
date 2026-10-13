package rotatingproxy

import (
	"crypto/tls"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"magpie/internal/support"

	"github.com/charmbracelet/log"
)

const (
	envRotatingProxyHTTP3TLSCertFile = "ROTATING_PROXY_HTTP3_TLS_CERT_FILE"
	envRotatingProxyHTTP3TLSKeyFile  = "ROTATING_PROXY_HTTP3_TLS_KEY_FILE"
)

func rotatorTLSConfig() (*tls.Config, error) {
	certFile := strings.TrimSpace(support.GetEnv(envRotatingProxyHTTP3TLSCertFile, ""))
	keyFile := strings.TrimSpace(support.GetEnv(envRotatingProxyHTTP3TLSKeyFile, ""))
	if certFile == "" || keyFile == "" {
		return nil, fmt.Errorf("HTTP/3 rotators require both %s and %s", envRotatingProxyHTTP3TLSCertFile, envRotatingProxyHTTP3TLSKeyFile)
	}
	source := &rotatorCertificate{certFile: certFile, keyFile: keyFile}
	if err := source.reload(); err != nil {
		return nil, fmt.Errorf("failed to load HTTP/3 rotator TLS cert/key pair: %w", err)
	}
	return &tls.Config{MinVersion: tls.VersionTLS13, GetCertificate: source.getCertificate}, nil
}

type rotatorCertificate struct {
	mu                sync.Mutex
	certFile, keyFile string
	certInfo, keyInfo os.FileInfo
	cert              *tls.Certificate
	lastErrorLog      time.Time
}

func (c *rotatorCertificate) reload() error {
	certInfo, err := os.Stat(c.certFile)
	if err != nil {
		return err
	}
	keyInfo, err := os.Stat(c.keyFile)
	if err != nil {
		return err
	}
	if sameCertificateFile(c.certInfo, certInfo) && sameCertificateFile(c.keyInfo, keyInfo) {
		return nil
	}
	cert, err := tls.LoadX509KeyPair(c.certFile, c.keyFile)
	if err != nil {
		return err
	}
	c.cert, c.certInfo, c.keyInfo = &cert, certInfo, keyInfo
	return nil
}

func (c *rotatorCertificate) getCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.reload(); err != nil {
		// A renewal can briefly expose a mismatched pair. Keep the last valid
		// certificate and retry on the next handshake.
		if time.Since(c.lastErrorLog) >= time.Minute {
			log.Warn("HTTP/3 certificate reload failed; keeping previous certificate", "error", err)
			c.lastErrorLog = time.Now()
		}
	}
	return c.cert, nil
}

func sameCertificateFile(old, current os.FileInfo) bool {
	return old != nil && os.SameFile(old, current) && old.Size() == current.Size() && old.ModTime().Equal(current.ModTime())
}
