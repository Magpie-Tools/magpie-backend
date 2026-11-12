package bootstrap

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"magpie/internal/checkpace"
	"magpie/internal/domain"
	proxyqueue "magpie/internal/jobs/queue/proxy"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// The child starts a fresh process so queue package initialization runs against
// this disposable Redis, just as it does in a test, migration, or new replica.
// Cadence now derives from workspace allowances, so no queue interval state
// may be written to Redis at all.
func TestQueueImportsWriteNoIntervalState(t *testing.T) {
	server := miniredis.RunT(t)
	runQueueIntervalProcess(t, server, "imports", "")
	for _, key := range []string{"magpie:queue:proxy:interval_ms", "magpie:queue:scrape:interval_ms"} {
		if server.Exists(key) {
			t.Errorf("queue import wrote obsolete interval state %s", key)
		}
	}
}

// A normal requeue uses the owning workspace's allowance cadence from the
// in-memory snapshot, issues no extra command and keeps the payload intact.
func TestRequeueUsesWorkspaceAllowanceCadence(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	pacer := checkpace.NewPacer()

	now := time.Now()
	_, periodEnd := checkpace.UsagePeriod(now)
	const perCycle = 3
	cycles := uint64(periodEnd.Sub(now)/(65*time.Minute)) + 1
	pacer.Publish(checkpace.Compute(now, []checkpace.Allowance{{
		WorkspaceID: 1, MonthlyChecks: cycles * perCycle, ChecksPerCycle: perCycle,
		MonthlyScrapes: 1, ScrapesPerCycle: 1,
	}}))
	pace, _ := pacer.Current().Pace(1)
	if pace.CheckInterval < 60*time.Minute || pace.CheckInterval > 65*time.Minute {
		t.Fatalf("derived cadence = %s, want about 65m", pace.CheckInterval)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	queue := proxyqueue.NewRedisProxyQueue(client, pacer)
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
	minimum := time.Now().Add(pace.CheckInterval - time.Second).UnixMilli()
	if err := queue.RequeueProxy(worker, scheduled); err != nil {
		t.Fatal(err)
	}
	if score, err := client.ZScore(ctx, worker.QueueLease.QueueKey, string(worker.Hash)).Result(); err != nil || score < float64(minimum) {
		t.Fatalf("normal requeue ignored the allowance cadence: score=%f, minimum=%d, error=%v", score, minimum, err)
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
	if os.Getenv("MAGPIE_TEST_QUEUE_IMPORT_HELPER") == "imports" {
		// Give any package-init background writer time to run before exit.
		time.Sleep(200 * time.Millisecond)
	}
}
