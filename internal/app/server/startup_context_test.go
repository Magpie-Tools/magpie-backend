package server

import (
	"context"
	"testing"
	"time"
)

func TestOpenRoutesHonorsCancelledStartupContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := OpenRoutes(ctx, -1); err != context.Canceled {
		t.Fatalf("API initialized after startup cancellation: %v", err)
	}
}

func TestOpenRoutesHonorsExpiredStartupContext(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	if err := OpenRoutes(ctx, -1); err != context.DeadlineExceeded {
		t.Fatalf("API initialized after startup deadline: %v", err)
	}
}
