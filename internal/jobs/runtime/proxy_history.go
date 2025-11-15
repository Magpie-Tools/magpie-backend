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
	proxyHistoryInterval = time.Hour
	proxyHistoryLockKey  = "magpie:leader:proxy_history"
)

func StartProxyHistoryRoutine(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}

	err := support.RunWithLeader(ctx, proxyHistoryLockKey, support.DefaultLeadershipTTL, func(leaderCtx context.Context) {
		runProxyHistoryLoop(leaderCtx)
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Error("Proxy history routine stopped", "error", err)
	}
}

func runProxyHistoryLoop(ctx context.Context) {
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
