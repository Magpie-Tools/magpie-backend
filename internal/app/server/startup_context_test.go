package server

import (
	"context"
	"testing"
	"time"

	"magpie/internal/checkpace"
)

func TestOpenRoutesHonorsCancelledStartupContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := OpenRoutes(ctx, -1, checkpace.NewPacer()); err != context.Canceled {
		t.Fatalf("API initialized after startup cancellation: %v", err)
	}
}

func TestOpenRoutesHonorsExpiredStartupContext(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	if err := OpenRoutes(ctx, -1, checkpace.NewPacer()); err != context.DeadlineExceeded {
		t.Fatalf("API initialized after startup deadline: %v", err)
	}
}

// The API reads allowance usage and check usage health from the process's
// Pacer; it has none of its own to fall back on.
func TestOpenRoutesRequiresTheProcessPacer(t *testing.T) {
	if err := OpenRoutes(context.Background(), -1, nil); err == nil {
		t.Fatal("API opened without a Pacer")
	}
}
