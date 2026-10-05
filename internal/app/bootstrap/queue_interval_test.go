package bootstrap

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"magpie/internal/config"
	"magpie/internal/domain"
	proxyqueue "magpie/internal/jobs/queue/proxy"
	sitequeue "magpie/internal/jobs/queue/sites"
	"magpie/internal/support"

	"github.com/alicebob/miniredis/v2"
)

// The child starts a fresh process so queue package initialization runs against
// this disposable Redis, just as it does in a test, migration, or new replica.
func TestQueueImportsPreserveConfiguredIntervals(t *testing.T) {
	server := miniredis.RunT(t)
	intervals := map[string]string{
		"magpie:queue:proxy:interval_ms":  "3900000",
		"magpie:queue:scrape:interval_ms": "7200000",
	}
	for key, value := range intervals {
		if err := server.Set(key, value); err != nil {
			t.Fatal(err)
		}
	}
	runQueueIntervalProcess(t, server, "imports", "")
	for key, want := range intervals {
		if got, err := server.Get(key); err != nil || got != want {
			t.Errorf("queue import changed %s: got %q, want %q, error %v", key, got, want, err)
		}
	}
}

func TestQueueIntervalStartupRepairsStateAndHonorsConfiguredDelay(t *testing.T) {
	server := miniredis.RunT(t)
	for _, key := range []string{"magpie:queue:proxy:interval_ms", "magpie:queue:scrape:interval_ms"} {
		if err := server.Set(key, "1000"); err != nil {
			t.Fatal(err)
		}
	}
	directory := t.TempDir()
	if err := os.Mkdir(filepath.Join(directory, "data"), 0o700); err != nil {
		t.Fatal(err)
	}
	settings := `{"checker":{"checker_timer":{"hours":1,"minutes":5}},"scraper":{"scraper_timer":{"hours":2}}}`
	if err := os.WriteFile(filepath.Join(directory, "data", "settings.json"), []byte(settings), 0o600); err != nil {
		t.Fatal(err)
	}
	runQueueIntervalProcess(t, server, "loaded", directory)
}

func runQueueIntervalProcess(t *testing.T, server *miniredis.Miniredis, mode, directory string) {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, executable, "-test.run=^TestQueueIntervalHelper$")
	command.Dir = directory
	for _, value := range os.Environ() {
		key, _, _ := strings.Cut(value, "=")
		switch key {
		case "REDIS_URL", "redisUrl", "REDIS_MODE", "REDIS_PASSWORD", "MAGPIE_TEST_QUEUE_IMPORT_HELPER":
			continue
		}
		command.Env = append(command.Env, value)
	}
	command.Env = append(command.Env,
		"REDIS_URL=redis://"+server.Addr(), "REDIS_MODE=single", "REDIS_PASSWORD=",
		"MAGPIE_TEST_QUEUE_IMPORT_HELPER="+mode,
	)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("queue import subprocess: %v\n%s", err, output)
	}
}

func TestQueueIntervalHelper(t *testing.T) {
	switch os.Getenv("MAGPIE_TEST_QUEUE_IMPORT_HELPER") {
	case "imports":
		// Allow the old package-init background writers to finish before exit.
		time.Sleep(200 * time.Millisecond)
		return
	case "loaded":
	default:
		return
	}
	if err := config.ReadSettings(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	proxyDone, scrapeDone := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(proxyDone)
		proxyqueue.PublicProxyQueue.StartIntervalUpdates(ctx)
	}()
	go func() {
		defer close(scrapeDone)
		sitequeue.PublicScrapeSiteQueue.StartIntervalUpdates(ctx)
	}()
	defer func() {
		cancel()
		<-proxyDone
		<-scrapeDone
	}()
	client, err := support.GetRedisClient()
	if err != nil {
		t.Fatal(err)
	}
	for {
		values, err := client.MGet(ctx, "magpie:queue:proxy:interval_ms", "magpie:queue:scrape:interval_ms").Result()
		if err != nil {
			t.Fatal(err)
		}
		if values[0] == "3900000" && values[1] == "7200000" {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("loaded queue intervals were not applied: %v", values)
		case <-time.After(10 * time.Millisecond):
		}
	}

	queue := &proxyqueue.PublicProxyQueue
	proxy := domain.Proxy{ID: 42, IP: "192.0.2.42", Port: 40005, Hash: []byte("interval-regression-route"), Workspaces: []domain.Workspace{{ID: 1}}}
	if err := queue.AddToQueue([]domain.Proxy{proxy}); err != nil {
		t.Fatal(err)
	}
	worker, scheduled, err := queue.GetNextProxyContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := client.Get(ctx, "proxy:"+string(worker.Hash)).Result()
	if err != nil {
		t.Fatal(err)
	}
	before := time.Now().Add(65 * time.Minute).UnixMilli()
	if err := queue.RequeueProxy(worker, scheduled); err != nil {
		t.Fatal(err)
	}
	if score, err := client.ZScore(ctx, worker.QueueLease.QueueKey, string(worker.Hash)).Result(); err != nil || score < float64(before) {
		t.Fatalf("normal requeue ignored loaded interval: score=%f, minimum=%d, error=%v", score, before, err)
	}
	if after, err := client.Get(ctx, "proxy:"+string(worker.Hash)).Result(); err != nil || after != payload {
		t.Fatalf("normal requeue rewrote payload: error=%v", err)
	}
	limited, stop := context.WithTimeout(ctx, 50*time.Millisecond)
	defer stop()
	if _, _, err := queue.GetNextProxyContext(limited); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("completed proxy became immediately available: %v", err)
	}
}
