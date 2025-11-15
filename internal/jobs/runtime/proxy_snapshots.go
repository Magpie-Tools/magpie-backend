package runtime

import (
	"context"
	"errors"
	"time"

	"magpie/internal/database"
	"magpie/internal/support"

	"github.com/charmbracelet/log"
)

const (
	proxySnapshotInterval = 10 * time.Minute
	proxySnapshotLockKey  = "magpie:leader:proxy_snapshots"
)

func StartProxySnapshotRoutine(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}

	err := support.RunWithLeader(ctx, proxySnapshotLockKey, support.DefaultLeadershipTTL, func(leaderCtx context.Context) {
		runProxySnapshotLoop(leaderCtx)
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Error("Proxy snapshot routine stopped", "error", err)
	}
}

func runProxySnapshotLoop(ctx context.Context) {
	runProxySnapshotsOnce(ctx)

	ticker := time.NewTicker(proxySnapshotInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			runProxySnapshotsOnce(ctx)
		}
	}
}

func runProxySnapshotsOnce(ctx context.Context) {
	start := time.Now()
	if err := database.SaveProxySnapshots(ctx); err != nil {
		log.Error("Failed to persist proxy metric snapshot", "error", err)
		return
	}
	log.Info("Proxy metric snapshots stored", "duration", time.Since(start))
}
