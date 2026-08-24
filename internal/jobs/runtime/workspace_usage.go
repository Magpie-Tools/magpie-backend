package runtime

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/charmbracelet/log"

	"magpie/internal/database"
)

const (
	workspaceTrafficFlushInterval = 5 * time.Second
	workspaceTrafficFlushTimeout  = 5 * time.Second
)

type workspaceTrafficKey struct {
	WorkspaceID uint
	PeriodStart int64
}

type workspaceTrafficCounter struct {
	Requests atomic.Uint64
	Bytes    atomic.Uint64
}

var workspaceTrafficCounters sync.Map
var recordWorkspaceManagedTrafficBatch = database.RecordWorkspaceManagedTrafficBatch

// AddWorkspaceManagedTraffic records rotator usage without blocking on the
// database. Counters are keyed by usage month so a flush across a month
// boundary cannot attribute traffic to the wrong billing period.
func AddWorkspaceManagedTraffic(workspaceID uint, requests, bytes uint64) {
	if workspaceID == 0 || (requests == 0 && bytes == 0) {
		return
	}
	now := time.Now().UTC()
	periodStart := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	key := workspaceTrafficKey{WorkspaceID: workspaceID, PeriodStart: periodStart.Unix()}
	value, _ := workspaceTrafficCounters.LoadOrStore(key, &workspaceTrafficCounter{})
	counter := value.(*workspaceTrafficCounter)
	if requests > 0 {
		counter.Requests.Add(requests)
	}
	if bytes > 0 {
		counter.Bytes.Add(bytes)
	}
}

func StartWorkspaceUsageRoutine(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	ticker := time.NewTicker(workspaceTrafficFlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			flushCtx, cancel := context.WithTimeout(context.Background(), workspaceTrafficFlushTimeout)
			if err := flushWorkspaceManagedTraffic(flushCtx); err != nil {
				log.Warn("workspace traffic usage final flush failed", "error", err)
			}
			cancel()
			return
		case <-ticker.C:
			flushCtx, cancel := context.WithTimeout(ctx, workspaceTrafficFlushTimeout)
			if err := flushWorkspaceManagedTraffic(flushCtx); err != nil && !errors.Is(err, context.Canceled) {
				log.Warn("workspace traffic usage flush failed", "error", err)
			}
			cancel()
		}
	}
}

func flushWorkspaceManagedTraffic(ctx context.Context) error {
	type pendingSample struct {
		counter  *workspaceTrafficCounter
		requests uint64
		bytes    uint64
	}
	pending := make([]pendingSample, 0)
	samples := make([]database.WorkspaceManagedTrafficSample, 0)
	workspaceTrafficCounters.Range(func(rawKey, rawValue any) bool {
		key, ok := rawKey.(workspaceTrafficKey)
		if !ok || key.WorkspaceID == 0 {
			return true
		}
		counter, ok := rawValue.(*workspaceTrafficCounter)
		if !ok || counter == nil {
			return true
		}

		requests := counter.Requests.Swap(0)
		bytes := counter.Bytes.Swap(0)
		if requests == 0 && bytes == 0 {
			return true
		}
		pending = append(pending, pendingSample{counter: counter, requests: requests, bytes: bytes})
		samples = append(samples, database.WorkspaceManagedTrafficSample{
			WorkspaceID: key.WorkspaceID,
			Requests:    requests,
			Bytes:       bytes,
			RecordedAt:  time.Unix(key.PeriodStart, 0).UTC(),
		})
		return true
	})
	if len(samples) == 0 {
		return nil
	}
	if err := recordWorkspaceManagedTrafficBatch(ctx, samples); err != nil {
		for _, sample := range pending {
			// Preserve every delta for the next pass. Atomic adds compose with
			// traffic recorded while the database call was in flight.
			sample.counter.Requests.Add(sample.requests)
			sample.counter.Bytes.Add(sample.bytes)
		}
		return err
	}
	return nil
}
