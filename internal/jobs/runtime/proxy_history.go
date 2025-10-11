package runtime

import (
	"context"
	"time"

	"magpie/internal/database"

	"github.com/charmbracelet/log"
)

const proxyHistoryInterval = time.Hour

func StartProxyHistoryRoutine(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}

	runProxyHistorySnapshot(ctx)

	ticker := time.NewTicker(proxyHistoryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			runProxyHistorySnapshot(ctx)
		}
	}
}

func runProxyHistorySnapshot(ctx context.Context) {
	start := time.Now()
	if err := database.SaveProxyHistorySnapshot(ctx); err != nil {
		log.Error("Failed to persist proxy history snapshot", "error", err)
		return
	}
	log.Info("Proxy history snapshot stored", "duration", time.Since(start))
}
