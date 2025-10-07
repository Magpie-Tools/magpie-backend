package config

import (
	"testing"
	"time"
)

func TestCalculateMillisecondsOfCheckingPeriod(t *testing.T) {
	timer := Timer{Days: 1, Hours: 2, Minutes: 3, Seconds: 4}
	want := uint64((24*60*60 + 2*60*60 + 3*60 + 4) * 1000)

	if got := CalculateMillisecondsOfCheckingPeriod(timer); got != want {
		t.Fatalf("CalculateMillisecondsOfCheckingPeriod returned %d, want %d", got, want)
	}
}

func TestCalculateBetweenTime(t *testing.T) {
	t.Run("enforces minimum interval", func(t *testing.T) {
		if got := CalculateBetweenTime(Timer{}); got != time.Second {
			t.Fatalf("CalculateBetweenTime returned %s, want 1s", got)
		}
	})

	t.Run("returns configured duration", func(t *testing.T) {
		if got := CalculateBetweenTime(Timer{Minutes: 1, Seconds: 30}); got != 90*time.Second {
			t.Fatalf("CalculateBetweenTime returned %s, want 1m30s", got)
		}
	})
}

func TestSetBetweenTime(t *testing.T) {
	origCfg := GetConfig()
	origChecks := GetTimeBetweenChecks()
	origScrapes := GetTimeBetweenScrapes()
	origRefresh := GetProxyGeoRefreshInterval()
	origListeners := proxyGeoRefreshListeners

	t.Cleanup(func() {
		configValue.Store(origCfg)
		timeBetweenChecks.Store(origChecks)
		timeBetweenScrapes.Store(origScrapes)
		proxyGeoRefreshInterval.Store(origRefresh)
		proxyGeoRefreshListeners = origListeners
	})

	testCfg := Config{}
	testCfg.Checker.CheckerTimer = Timer{Seconds: 10}
	testCfg.Scraper.ScraperTimer = Timer{Minutes: 2}
	testCfg.Runtime.ProxyGeoRefreshTimer = Timer{Hours: 6}

	configValue.Store(testCfg)
	proxyGeoRefreshListeners = nil

	SetBetweenTime()

	if got := GetTimeBetweenChecks(); got != 10*time.Second {
		t.Fatalf("GetTimeBetweenChecks returned %s, want 10s", got)
	}
	if got := GetTimeBetweenScrapes(); got != 2*time.Minute {
		t.Fatalf("GetTimeBetweenScrapes returned %s, want 2m", got)
	}
	if got := GetProxyGeoRefreshInterval(); got != 6*time.Hour {
		t.Fatalf("GetProxyGeoRefreshInterval returned %s, want 6h", got)
	}
}
