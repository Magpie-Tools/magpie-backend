package runtime

import (
	"context"
	"time"

	"magpie/internal/database"

	"github.com/charmbracelet/log"
)

const proxySnapshotInterval = 10 * time.Minute

func StartProxySnapshotRoutine(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}

	runProxySnapshots(ctx)

	ticker := time.NewTicker(proxySnapshotInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			runProxySnapshots(ctx)
		}
	}
}

func runProxySnapshots(ctx context.Context) {
	start := time.Now()
	if err := database.SaveProxySnapshots(ctx); err != nil {
		log.Error("Failed to persist proxy metric snapshot", "error", err)
		return
	}
	log.Info("Proxy metric snapshots stored", "duration", time.Since(start))
}
