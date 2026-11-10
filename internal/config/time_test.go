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
	origBlacklist := GetBlacklistRefreshInterval()
	origRefresh := GetProxyGeoRefreshInterval()
	origBlacklistListeners := blacklistIntervalListeners
	origListeners := proxyGeoRefreshListeners

	t.Cleanup(func() {
		configValue.Store(origCfg)
		blacklistRefreshInterval.Store(origBlacklist)
		proxyGeoRefreshInterval.Store(origRefresh)
		blacklistIntervalListeners = origBlacklistListeners
		proxyGeoRefreshListeners = origListeners
	})

	testCfg := Config{}
	testCfg.BlacklistTimer = Timer{Minutes: 30}
	testCfg.Runtime.ProxyGeoRefreshTimer = Timer{Hours: 6}

	configValue.Store(testCfg)
	blacklistIntervalListeners = nil
	proxyGeoRefreshListeners = nil

	SetBetweenTime()

	if got := GetBlacklistRefreshInterval(); got != 30*time.Minute {
		t.Fatalf("GetBlacklistRefreshInterval returned %s, want 30m", got)
	}
	if got := GetProxyGeoRefreshInterval(); got != 6*time.Hour {
		t.Fatalf("GetProxyGeoRefreshInterval returned %s, want 6h", got)
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
