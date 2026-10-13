package maintenance

import (
	"context"
	"testing"
	"time"
)

func TestOrphanCleanupRetriesInterruptedPassBeforeHourlyInterval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	passes := 0
	runOrphanCleanupPasses(ctx, time.Hour, 5*time.Millisecond, func(context.Context) error {
		passes++
		if passes == 1 {
			return context.DeadlineExceeded
		}
		cancel()
		return nil
	})
	if passes != 2 {
		t.Fatalf("interrupted pass did not retry before normal interval: passes=%d", passes)
	}
}

func TestOrphanCleanupInvalidDurationsUseDefaults(t *testing.T) {
	for _, raw := range []string{"0s", "-1s", "invalid"} {
		t.Setenv(envCleanupMaxRunDuration, raw)
		t.Setenv(envCleanupRetryInterval, raw)
		if got := resolveCleanupDuration(envCleanupMaxRunDuration, defaultCleanupBudget); got != 45*time.Second {
			t.Fatalf("invalid budget %q became %v", raw, got)
		}
		if got := resolveCleanupDuration(envCleanupRetryInterval, defaultCleanupRetry); got != time.Minute {
			t.Fatalf("invalid retry %q became %v", raw, got)
		}
	}
}
