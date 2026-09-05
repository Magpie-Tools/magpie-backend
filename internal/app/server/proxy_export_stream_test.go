package server

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"magpie/internal/domain"
)

var exportTestBatch = []domain.Proxy{{IP: "192.0.2.1", Port: 8080}}

func exportTestServer(t *testing.T, timeout time.Duration, stream func(context.Context, func([]domain.Proxy) error) error) *httptest.Server {
	t.Helper()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeProxyExport(w, r, timeout, "ip:port", stream)
	})
	s := httptest.NewUnstartedServer(withRequestID(withAccessLog(withPanicRecovery(handler))))
	s.Config.WriteTimeout = 25 * time.Millisecond
	s.Start()
	t.Cleanup(s.Close)
	return s
}

func TestProxyExportOutlivesNormalWriteDeadline(t *testing.T) {
	s := exportTestServer(t, time.Second, func(ctx context.Context, consume func([]domain.Proxy) error) error {
		select {
		case <-time.After(100 * time.Millisecond):
			return consume(exportTestBatch)
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	res, err := s.Client().Get(s.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil || res.StatusCode != 200 || string(body) != "192.0.2.1:8080\n" {
		t.Fatalf("status=%d body=%q error=%v", res.StatusCode, body, err)
	}
}

func TestProxyExportTimeoutBeforeFirstBatchReturnsJSON(t *testing.T) {
	s := exportTestServer(t, 50*time.Millisecond, func(ctx context.Context, _ func([]domain.Proxy) error) error {
		<-ctx.Done()
		return ctx.Err()
	})
	res, err := s.Client().Get(s.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil || res.StatusCode != 504 || !strings.Contains(string(body), "Export timed out") {
		t.Fatalf("status=%d body=%q error=%v", res.StatusCode, body, err)
	}
	if res.Header.Get("Content-Disposition") != "" || res.Header.Get("Content-Type") != "application/json" {
		t.Fatalf("unexpected error headers: %v", res.Header)
	}
}

func TestProxyExportFlushesBatchesAndAbortsIncompleteResponse(t *testing.T) {
	release := make(chan struct{})
	s := exportTestServer(t, time.Second, func(ctx context.Context, consume func([]domain.Proxy) error) error {
		if err := consume(exportTestBatch); err != nil {
			return err
		}
		select {
		case <-release:
			return errors.New("database failed after first batch")
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	res, err := s.Client().Get(s.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	reader := bufio.NewReader(res.Body)
	line, err := reader.ReadString('\n')
	close(release)
	if err != nil || line != "192.0.2.1:8080\n" {
		t.Fatalf("first batch not flushed: %q %v", line, err)
	}
	remaining, err := io.ReadAll(reader)
	if !errors.Is(err, io.ErrUnexpectedEOF) || len(remaining) != 0 {
		t.Fatalf("expected interrupted transfer, got %q %v", remaining, err)
	}
}

func TestProxyExportClientDisconnectCancelsWork(t *testing.T) {
	canceled := make(chan error, 1)
	s := exportTestServer(t, time.Second, func(ctx context.Context, consume func([]domain.Proxy) error) error {
		if err := consume(exportTestBatch); err != nil {
			return err
		}
		<-ctx.Done()
		canceled <- ctx.Err()
		return ctx.Err()
	})
	res, err := s.Client().Get(s.URL)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	select {
	case err := <-canceled:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("want client cancellation, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("export work did not cancel")
	}
}

func TestProxyExportTimeoutAfterBatchAbortsResponse(t *testing.T) {
	s := exportTestServer(t, 100*time.Millisecond, func(ctx context.Context, consume func([]domain.Proxy) error) error {
		if err := consume(exportTestBatch); err != nil {
			return err
		}
		<-ctx.Done()
		return ctx.Err()
	})
	res, err := s.Client().Get(s.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if !errors.Is(err, io.ErrUnexpectedEOF) || string(body) != "192.0.2.1:8080\n" {
		t.Fatalf("expected partial body with transfer error: %q %v", body, err)
	}
}
