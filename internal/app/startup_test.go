package app

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestServeAfterSetupHonorsStartupCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, setupErr := range []error{nil, context.Canceled} {
		// An API start with this port would fail. A normal startup signal must
		// bypass API initialization and proceed to the existing runtime drain.
		if err := serveAfterSetup(ctx, -1, setupErr); err != nil {
			t.Fatalf("normal startup cancellation returned an error: %v", err)
		}
	}
}

func TestServeAfterSetupPreservesSeparateFailures(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	failure := errors.New("required setup failed")
	for _, setupErr := range []error{failure, fmt.Errorf("database: %w", failure), errors.Join(context.Canceled, failure)} {
		if err := serveAfterSetup(ctx, -1, setupErr); err != setupErr {
			t.Fatalf("signal hid a required startup failure: got=%v want=%v", err, setupErr)
		}
	}
	if err := serveAfterSetup(context.Background(), -1, context.Canceled); err != context.Canceled {
		t.Fatalf("unrelated setup cancellation was hidden: %v", err)
	}
}
