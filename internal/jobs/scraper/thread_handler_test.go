package scraper

import (
	"runtime"
	"testing"
	"time"
)

func TestResolvePostProcessWorkers_DefaultAndClamp(t *testing.T) {
	t.Setenv(envPostProcessWorkers, "")
	if got := resolvePostProcessWorkers(); got != defaultPostProcessWorkers {
		t.Fatalf("workers = %d, want %d", got, defaultPostProcessWorkers)
	}

	t.Setenv(envPostProcessWorkers, "0")
	if got := resolvePostProcessWorkers(); got != 1 {
		t.Fatalf("workers = %d, want 1", got)
	}

	t.Setenv(envPostProcessWorkers, "999")
	if got := resolvePostProcessWorkers(); got != maxPostProcessWorkers {
		t.Fatalf("workers = %d, want %d", got, maxPostProcessWorkers)
	}
}

func TestResolvePostProcessQueueSize_DefaultAndClamp(t *testing.T) {
	t.Setenv(envPostProcessQueueSize, "")
	if got := resolvePostProcessQueueSize(); got != defaultPostProcessQueueSize {
		t.Fatalf("queue size = %d, want %d", got, defaultPostProcessQueueSize)
	}

	t.Setenv(envPostProcessQueueSize, "0")
	if got := resolvePostProcessQueueSize(); got != 1 {
		t.Fatalf("queue size = %d, want 1", got)
	}

	t.Setenv(envPostProcessQueueSize, "999999")
	if got := resolvePostProcessQueueSize(); got != maxPostProcessQueueSize {
		t.Fatalf("queue size = %d, want %d", got, maxPostProcessQueueSize)
	}
}

func TestShouldEmitScrapePopErrorLog_RateLimitAndSuppressedCount(t *testing.T) {
	resetScrapePopErrorLogStateForTest()
	t.Cleanup(resetScrapePopErrorLogStateForTest)

	base := time.Date(2026, time.March, 4, 12, 0, 0, 0, time.UTC)

	emit, suppressed := shouldEmitScrapePopErrorLog(base)
	if !emit || suppressed != 0 {
		t.Fatalf("first error emit=%t suppressed=%d, want true/0", emit, suppressed)
	}

	emit, suppressed = shouldEmitScrapePopErrorLog(base.Add(5 * time.Second))
	if emit || suppressed != 0 {
		t.Fatalf("within interval emit=%t suppressed=%d, want false/0", emit, suppressed)
	}

	emit, suppressed = shouldEmitScrapePopErrorLog(base.Add(10 * time.Second))
	if emit || suppressed != 0 {
		t.Fatalf("within interval emit=%t suppressed=%d, want false/0", emit, suppressed)
	}

	emit, suppressed = shouldEmitScrapePopErrorLog(base.Add(scrapePopErrorLogInterval))
	if !emit || suppressed != 2 {
		t.Fatalf("interval boundary emit=%t suppressed=%d, want true/2", emit, suppressed)
	}

	emit, suppressed = shouldEmitScrapePopErrorLog(base.Add(scrapePopErrorLogInterval + time.Second))
	if emit || suppressed != 0 {
		t.Fatalf("post-emit immediate repeat emit=%t suppressed=%d, want false/0", emit, suppressed)
	}
}

func resetScrapePopErrorLogStateForTest() {
	scrapePopErrorLogState.mu.Lock()
	defer scrapePopErrorLogState.mu.Unlock()
	scrapePopErrorLogState.lastLogAt = time.Time{}
	scrapePopErrorLogState.suppressed = 0
}

func TestRequestScraperWorkerStop_DoesNotBlockWithoutListener(t *testing.T) {
	originalStopThread := stopThread
	stopThread = make(chan struct{})
	t.Cleanup(func() {
		stopThread = originalStopThread
	})

	done := make(chan bool, 1)
	go func() {
		done <- requestScraperWorkerStop()
	}()

	select {
	case ok := <-done:
		if ok {
			t.Fatal("requestScraperWorkerStop should return false when no worker is listening")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("requestScraperWorkerStop blocked without listener")
	}
}

func TestRequestScraperWorkerStop_SucceedsWithListener(t *testing.T) {
	originalStopThread := stopThread
	stopThread = make(chan struct{})
	t.Cleanup(func() {
		stopThread = originalStopThread
	})

	stopped := make(chan struct{})
	go func() {
		<-stopThread
		close(stopped)
	}()

	deadline := time.Now().Add(100 * time.Millisecond)
	delivered := false
	for time.Now().Before(deadline) {
		if requestScraperWorkerStop() {
			delivered = true
			break
		}
		runtime.Gosched()
	}
	if !delivered {
		t.Fatal("requestScraperWorkerStop should return true when a worker listener is available")
	}

	select {
	case <-stopped:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected stop signal to be delivered")
	}
}
