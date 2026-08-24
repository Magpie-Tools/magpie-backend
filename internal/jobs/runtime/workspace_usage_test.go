package runtime

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"magpie/internal/database"
)

func TestWorkspaceManagedTrafficAggregatesBeforeFlush(t *testing.T) {
	workspaceTrafficCounters = sync.Map{}
	originalRecord := recordWorkspaceManagedTrafficBatch
	t.Cleanup(func() {
		recordWorkspaceManagedTrafficBatch = originalRecord
		workspaceTrafficCounters = sync.Map{}
	})

	type sample struct {
		workspaceID uint
		requests    uint64
		bytes       uint64
		recordedAt  time.Time
	}
	var samples []sample
	recordWorkspaceManagedTrafficBatch = func(_ context.Context, batch []database.WorkspaceManagedTrafficSample) error {
		for _, item := range batch {
			samples = append(samples, sample{workspaceID: item.WorkspaceID, requests: item.Requests, bytes: item.Bytes, recordedAt: item.RecordedAt})
		}
		return nil
	}

	AddWorkspaceManagedTraffic(42, 1, 100)
	AddWorkspaceManagedTraffic(42, 2, 350)
	if err := flushWorkspaceManagedTraffic(context.Background()); err != nil {
		t.Fatalf("flush managed traffic: %v", err)
	}
	if len(samples) != 1 {
		t.Fatalf("flush samples = %d, want 1", len(samples))
	}
	if samples[0].workspaceID != 42 || samples[0].requests != 3 || samples[0].bytes != 450 {
		t.Fatalf("flush sample = %#v", samples[0])
	}
	if samples[0].recordedAt.Day() != 1 || samples[0].recordedAt.Hour() != 0 || samples[0].recordedAt.Location() != time.UTC {
		t.Fatalf("usage period start = %v, want first day UTC", samples[0].recordedAt)
	}

	if err := flushWorkspaceManagedTraffic(context.Background()); err != nil {
		t.Fatalf("second managed traffic flush: %v", err)
	}
	if len(samples) != 1 {
		t.Fatalf("empty second flush emitted %d samples, want 1 total", len(samples))
	}
}

func TestWorkspaceManagedTrafficRestoresCountersAfterFailedFlush(t *testing.T) {
	workspaceTrafficCounters = sync.Map{}
	originalRecord := recordWorkspaceManagedTrafficBatch
	t.Cleanup(func() {
		recordWorkspaceManagedTrafficBatch = originalRecord
		workspaceTrafficCounters = sync.Map{}
	})

	flushes := 0
	recordWorkspaceManagedTrafficBatch = func(_ context.Context, batch []database.WorkspaceManagedTrafficSample) error {
		flushes++
		if flushes == 1 {
			return errors.New("temporary database failure")
		}
		if len(batch) != 1 || batch[0].WorkspaceID != 7 || batch[0].Requests != 2 || batch[0].Bytes != 512 {
			t.Fatalf("restored batch = %#v", batch)
		}
		return nil
	}

	AddWorkspaceManagedTraffic(7, 2, 512)
	if err := flushWorkspaceManagedTraffic(context.Background()); err == nil {
		t.Fatal("first flush error = nil, want temporary database failure")
	}
	if err := flushWorkspaceManagedTraffic(context.Background()); err != nil {
		t.Fatalf("retry managed traffic flush: %v", err)
	}
	if flushes != 2 {
		t.Fatalf("flush calls = %d, want 2", flushes)
	}
}
