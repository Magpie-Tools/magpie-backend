package server

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestBackendLongevityUnknownRouteMetricCardinality(t *testing.T) {
	httpRequestsTotal.Reset()
	httpRequestDurationSeconds.Reset()
	t.Cleanup(func() { httpRequestsTotal.Reset(); httpRequestDurationSeconds.Reset() })
	mux := http.NewServeMux()
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	const requests = 20000
	for i := 0; i < requests; i++ {
		r := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/unknown-path-%d", i), nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		if w.Code != 404 || r.Pattern != "" {
			t.Fatalf("unexpected unmatched request: status=%d pattern=%q", w.Code, r.Pattern)
		}
		observeRequestMetrics(r, w.Code, time.Millisecond)
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	metrics := make(chan prometheus.Metric)
	go func() { httpRequestsTotal.Collect(metrics); close(metrics) }()
	retained := 0
	for range metrics {
		retained++
	}
	t.Logf("unauthenticated_unknown_paths=%d permanent_counter_label_sets=%d retained_heap_delta_bytes=%d", requests, retained, int64(after.HeapAlloc)-int64(before.HeapAlloc))
	if retained > 1 {
		t.Fatal("unmatched request paths create permanent Prometheus series without a cardinality bound")
	}
}

func TestUnknownHTTPMethodsHaveBoundedMetrics(t *testing.T) {
	httpRequestsTotal.Reset()
	httpRequestDurationSeconds.Reset()
	t.Cleanup(func() { httpRequestsTotal.Reset(); httpRequestDurationSeconds.Reset() })
	for i := 0; i < 10000; i++ {
		r := httptest.NewRequest(fmt.Sprintf("METHOD_%d", i), fmt.Sprintf("/rejected/%d", i), nil)
		observeRequestMetrics(r, 403, time.Millisecond)
	}
	metrics := make(chan prometheus.Metric)
	go func() { httpRequestsTotal.Collect(metrics); close(metrics) }()
	count := 0
	for range metrics {
		count++
	}
	if count != 1 {
		t.Fatalf("unknown methods retained %d series", count)
	}
}
