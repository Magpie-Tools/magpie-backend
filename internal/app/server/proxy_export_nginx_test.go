package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"magpie/internal/domain"
)

// Run from the sibling workspace on Linux with Docker available:
// MAGPIE_EXPORT_NGINX_IMAGE=nginx:alpine go test ./internal/app/server -run TestProxyExportThroughNginx -count=1
func TestProxyExportThroughNginx(t *testing.T) {
	image := os.Getenv("MAGPIE_EXPORT_NGINX_IMAGE")
	if image == "" {
		t.Skip("set MAGPIE_EXPORT_NGINX_IMAGE to run the 31-second nginx integration test")
	}
	releaseFailure := make(chan struct{})
	canceled := make(chan error, 1)
	backend := httptest.NewUnstartedServer(withRequestID(withAccessLog(withPanicRecovery(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mode := r.URL.Query().Get("mode")
		timeout := 40 * time.Second
		if mode == "timeout" {
			timeout = 50 * time.Millisecond
		}
		writeProxyExport(w, r, timeout, "ip:port", func(ctx context.Context, consume func([]domain.Proxy) error) error {
			switch mode {
			case "slow":
				select {
				case <-time.After(31 * time.Second):
					return consume(exportTestBatch)
				case <-ctx.Done():
					return ctx.Err()
				}
			case "timeout":
				<-ctx.Done()
				return ctx.Err()
			case "broken", "cancel":
				if err := consume(exportTestBatch); err != nil {
					return err
				}
				if mode == "cancel" {
					<-ctx.Done()
					canceled <- ctx.Err()
					return ctx.Err()
				}
				select {
				case <-releaseFailure:
					return errors.New("injected database failure")
				case <-ctx.Done():
					return ctx.Err()
				}
			default:
				return consume(exportTestBatch)
			}
		})
	})))))
	backend.Config.WriteTimeout = 30 * time.Second
	backend.Start()
	defer backend.Close()

	config, err := os.ReadFile("../../../../magpie-frontend/nginx.conf")
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	nginxConfig := strings.ReplaceAll(string(config), "backend:5656", strings.TrimPrefix(backend.URL, "http://"))
	nginxConfig = strings.ReplaceAll(nginxConfig, "listen       80;", fmt.Sprintf("listen       %d;", port))
	nginxConfig = strings.ReplaceAll(nginxConfig, "listen  [::]:80;", fmt.Sprintf("listen  [::]:%d;", port))
	configPath := filepath.Join(t.TempDir(), "default.conf.template")
	if err := os.WriteFile(configPath, []byte(nginxConfig), 0644); err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("magpie-export-test-%d", time.Now().UnixNano())
	cmd := exec.Command("docker", "run", "--detach", "--rm", "--name", name, "--network", "host",
		"-e", "EXPORT_PROXY_READ_TIMEOUT_SECONDS=310", "-v", configPath+":/etc/nginx/templates/default.conf.template:ro", image)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("start nginx: %s %v", output, err)
	}
	defer func() {
		if t.Failed() {
			output, _ := exec.Command("docker", "logs", name).CombinedOutput()
			t.Logf("nginx logs: %s", output)
		}
		_ = exec.Command("docker", "rm", "-f", name).Run()
	}()
	baseURL := fmt.Sprintf("http://127.0.0.1:%d", port)
	client := &http.Client{Timeout: 45 * time.Second}
	ready := false
	for until := time.Now().Add(10 * time.Second); time.Now().Before(until); {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 100*time.Millisecond)
		if err == nil {
			conn.Close()
			ready = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !ready {
		t.Fatal("nginx did not start")
	}
	for _, mode := range []string{"slow", "timeout", "broken", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			res, err := client.Post(baseURL+"/api/user/export?mode="+mode, "application/json", strings.NewReader("{}"))
			if err != nil {
				t.Fatal(err)
			}
			defer res.Body.Close()
			reader := bufio.NewReader(res.Body)
			switch mode {
			case "broken", "cancel":
				line, err := reader.ReadString('\n')
				if err != nil || line != "192.0.2.1:8080\n" {
					t.Fatalf("batch not flushed: %q %v", line, err)
				}
				if mode == "cancel" {
					res.Body.Close()
					select {
					case err := <-canceled:
						if !errors.Is(err, context.Canceled) {
							t.Fatalf("not canceled: %v", err)
						}
					case <-time.After(3 * time.Second):
						t.Fatal("nginx did not propagate client disconnect")
					}
					return
				}
				close(releaseFailure)
				if _, err := io.ReadAll(reader); !errors.Is(err, io.ErrUnexpectedEOF) {
					t.Fatalf("nginx must preserve interrupted transfer: %v", err)
				}
			default:
				body, err := io.ReadAll(reader)
				if err != nil {
					t.Fatal(err)
				}
				if mode == "slow" && (res.StatusCode != 200 || string(body) != "192.0.2.1:8080\n") {
					t.Fatalf("slow export: %d %q", res.StatusCode, body)
				}
				if mode == "timeout" && (res.StatusCode != 504 || !strings.Contains(string(body), "Export timed out")) {
					t.Fatalf("timeout response: %d %q", res.StatusCode, body)
				}
			}
		})
	}
}
