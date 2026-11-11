package geolite

import (
	"fmt"
	"testing"
	"time"

	"magpie/internal/support"
	"runtime"
)

func TestBackendLongevityExpiredDNSCacheChurn(t *testing.T) {
	saved := dnsCache
	dnsCache = support.NewExpiryCache[string, []string](20_000)
	t.Cleanup(func() { dnsCache = saved })
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	const total = 200000
	for i := 0; i < total; i++ {
		ip := fmt.Sprintf("10.%d.%d.%d", i>>16, i>>8&255, i&255)
		dnsCache.Set(ip, []string{fmt.Sprintf("customer-%d.provider.example", i)}, time.Now().Add(-180*24*time.Hour))
	}
	dnsCache.Set("192.0.2.1", []string{"active.example"}, time.Now().Add(time.Hour))
	PruneDNSCache(time.Now())
	_ = getCachedDNS("192.0.2.1")
	runtime.GC()
	runtime.ReadMemStats(&after)
	count := dnsCache.Len()
	t.Logf("expired_ips=%d retained_keys=%d retained_heap_delta_bytes=%d", total, count, int64(after.HeapAlloc)-int64(before.HeapAlloc))
	if count > 1 {
		t.Fatalf("DNS cache retains %d expired, unused entries", count-1)
	}
}
