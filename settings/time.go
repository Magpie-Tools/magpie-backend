package settings

import (
	"sync/atomic"
	"time"
)

var (
	timeBetweenChecks  atomic.Value
	timeBetweenScrapes atomic.Value
)

func SetBetweenTime() {
	cfg := GetConfig()
	timeBetweenChecks.Store(CalculateBetweenTime(cfg.Timer))
	timeBetweenScrapes.Store(CalculateBetweenTime(cfg.Scraper.ScraperTimer))
}

// CalculateBetweenTime Also works with e.g a judgeCount
func CalculateBetweenTime(timer Timer) time.Duration {
	intervalMs := CalculateMillisecondsOfCheckingPeriod(timer)

	// Enforce minimum interval (e.g., 1 second)
	minInterval := uint64(1000)
	if intervalMs < minInterval {
		intervalMs = minInterval
	}

	return time.Duration(intervalMs) * time.Millisecond
}

func CalculateMillisecondsOfCheckingPeriod(timer Timer) uint64 {
	// Calculate total duration in milliseconds
	return uint64(timer.Days)*24*60*60*1000 +
		uint64(timer.Hours)*60*60*1000 +
		uint64(timer.Minutes)*60*1000 +
		uint64(timer.Seconds)*1000
}

func GetTimeBetweenChecks() time.Duration {
	return timeBetweenChecks.Load().(time.Duration)
}

func GetTimeBetweenScrapes() time.Duration {
	return timeBetweenScrapes.Load().(time.Duration)
}
