package config

import (
	"sync"
	"sync/atomic"
	"time"
)

const defaultProxyGeoRefreshInterval = 24 * time.Hour

var (
	timeBetweenChecks        atomic.Value
	timeBetweenScrapes       atomic.Value
	proxyGeoRefreshInterval  atomic.Value
	proxyGeoRefreshListeners []chan time.Duration
	listenersMu              sync.Mutex
)

func init() {
	timeBetweenChecks.Store(time.Second)
	timeBetweenScrapes.Store(time.Second)
	proxyGeoRefreshInterval.Store(defaultProxyGeoRefreshInterval)
}

func SetBetweenTime() {
	cfg := GetConfig()
	timeBetweenChecks.Store(CalculateBetweenTime(cfg.Checker.CheckerTimer))
	timeBetweenScrapes.Store(CalculateBetweenTime(cfg.Scraper.ScraperTimer))
	setProxyGeoRefreshInterval(calculateProxyGeoRefreshInterval(cfg))
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

func GetProxyGeoRefreshInterval() time.Duration {
	return proxyGeoRefreshInterval.Load().(time.Duration)
}

func ProxyGeoRefreshIntervalUpdates() <-chan time.Duration {
	ch := make(chan time.Duration, 1)
	listenersMu.Lock()
	proxyGeoRefreshListeners = append(proxyGeoRefreshListeners, ch)
	listenersMu.Unlock()

	ch <- GetProxyGeoRefreshInterval()
	return ch
}

func setProxyGeoRefreshInterval(interval time.Duration) {
	if interval <= 0 {
		interval = defaultProxyGeoRefreshInterval
	}
	current := GetProxyGeoRefreshInterval()
	if current == interval {
		return
	}
	proxyGeoRefreshInterval.Store(interval)

	listenersMu.Lock()
	defer listenersMu.Unlock()
	for _, ch := range proxyGeoRefreshListeners {
		select {
		case ch <- interval:
		default:
		}
	}
}

func calculateProxyGeoRefreshInterval(cfg Config) time.Duration {
	timer := cfg.Runtime.ProxyGeoRefreshTimer
	if timer.Days == 0 && timer.Hours == 0 && timer.Minutes == 0 && timer.Seconds == 0 {
		return defaultProxyGeoRefreshInterval
	}
	return CalculateBetweenTime(timer)
}
