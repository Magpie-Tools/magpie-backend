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
	origBlacklist := GetBlacklistRefreshInterval()
	origRefresh := GetProxyGeoRefreshInterval()
	origCheckListeners := checkIntervalListeners
	origScrapeListeners := scrapeIntervalListeners
	origBlacklistListeners := blacklistIntervalListeners
	origListeners := proxyGeoRefreshListeners

	t.Cleanup(func() {
		configValue.Store(origCfg)
		timeBetweenChecks.Store(origChecks)
		timeBetweenScrapes.Store(origScrapes)
		blacklistRefreshInterval.Store(origBlacklist)
		proxyGeoRefreshInterval.Store(origRefresh)
		checkIntervalListeners = origCheckListeners
		scrapeIntervalListeners = origScrapeListeners
		blacklistIntervalListeners = origBlacklistListeners
		proxyGeoRefreshListeners = origListeners
	})

	testCfg := Config{}
	testCfg.Checker.CheckerTimer = Timer{Seconds: 10}
	testCfg.Scraper.ScraperTimer = Timer{Minutes: 2}
	testCfg.BlacklistTimer = Timer{Minutes: 30}
	testCfg.Runtime.ProxyGeoRefreshTimer = Timer{Hours: 6}

	configValue.Store(testCfg)
	scrapeIntervalListeners = nil
	blacklistIntervalListeners = nil
	proxyGeoRefreshListeners = nil

	SetBetweenTime()

	if got := GetTimeBetweenChecks(); got != 10*time.Second {
		t.Fatalf("GetTimeBetweenChecks returned %s, want 10s", got)
	}
	if got := GetTimeBetweenScrapes(); got != 2*time.Minute {
		t.Fatalf("GetTimeBetweenScrapes returned %s, want 2m", got)
	}
	if got := GetBlacklistRefreshInterval(); got != 30*time.Minute {
		t.Fatalf("GetBlacklistRefreshInterval returned %s, want 30m", got)
	}
	if got := GetProxyGeoRefreshInterval(); got != 6*time.Hour {
		t.Fatalf("GetProxyGeoRefreshInterval returned %s, want 6h", got)
	}
}

func TestCheckIntervalUpdates(t *testing.T) {
	origChecks := GetTimeBetweenChecks()
	origListeners := checkIntervalListeners

	t.Cleanup(func() {
		timeBetweenChecks.Store(origChecks)
		checkIntervalListeners = origListeners
	})

	timeBetweenChecks.Store(time.Second)
	checkIntervalListeners = nil

	ch := CheckIntervalUpdates()
	first := <-ch
	if first != time.Second {
		t.Fatalf("initial update = %s, want 1s", first)
	}

	setTimeBetweenChecks(5 * time.Second)

	select {
	case next := <-ch:
		if next != 5*time.Second {
			t.Fatalf("next update = %s, want 5s", next)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for interval update")
	}

	// Verify no duplicate notification when same interval is set.
	setTimeBetweenChecks(5 * time.Second)
	select {
	case <-ch:
		t.Fatal("unexpected update when interval unchanged")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestScrapeIntervalUpdates(t *testing.T) {
	origScrapes := GetTimeBetweenScrapes()
	origListeners := scrapeIntervalListeners

	t.Cleanup(func() {
		timeBetweenScrapes.Store(origScrapes)
		scrapeIntervalListeners = origListeners
	})

	timeBetweenScrapes.Store(time.Second)
	scrapeIntervalListeners = nil

	ch := ScrapeIntervalUpdates()
	first := <-ch
	if first != time.Second {
		t.Fatalf("initial update = %s, want 1s", first)
	}

	setTimeBetweenScrapes(3 * time.Second)

	select {
	case next := <-ch:
		if next != 3*time.Second {
			t.Fatalf("next update = %s, want 3s", next)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for interval update")
	}

	setTimeBetweenScrapes(3 * time.Second)
	select {
	case <-ch:
		t.Fatal("unexpected update when interval unchanged")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestBlacklistIntervalUpdates(t *testing.T) {
	origInterval := GetBlacklistRefreshInterval()
	origListeners := blacklistIntervalListeners

	t.Cleanup(func() {
		blacklistRefreshInterval.Store(origInterval)
		blacklistIntervalListeners = origListeners
	})

	blacklistRefreshInterval.Store(time.Second)
	blacklistIntervalListeners = nil

	ch := BlacklistIntervalUpdates()
	first := <-ch
	if first != time.Second {
		t.Fatalf("initial update = %s, want 1s", first)
	}

	setBlacklistRefreshInterval(5 * time.Second)

	select {
	case next := <-ch:
		if next != 5*time.Second {
			t.Fatalf("next update = %s, want 5s", next)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for interval update")
	}

	setBlacklistRefreshInterval(5 * time.Second)
	select {
	case <-ch:
		t.Fatal("unexpected update when interval unchanged")
	case <-time.After(50 * time.Millisecond):
	}
}
