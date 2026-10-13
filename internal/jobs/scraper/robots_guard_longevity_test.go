package scraper

import (
	"fmt"
	"testing"
	"time"

	"magpie/internal/support"
)

func TestRobotsCachePrunesRetiredHostsAndBoundsChurn(t *testing.T) {
	old := robotsCache
	robotsCache = support.NewExpiryCache[string, robotsCacheEntry](2000)
	t.Cleanup(func() { robotsCache = old })
	now := time.Now()
	for i := 0; i < 10000; i++ {
		robotsCache.Set(fmt.Sprint(i), robotsCacheEntry{}, now.Add(-time.Hour))
	}
	robotsCache.Set("active", robotsCacheEntry{fetched: now}, now.Add(time.Hour))
	PruneRobotsCache(now)
	if robotsCache.Len() != 1 {
		t.Fatalf("retained entries = %d", robotsCache.Len())
	}
	if _, ok := getCachedRobotsEntry("active"); !ok {
		t.Fatal("live entry pruned")
	}
}
