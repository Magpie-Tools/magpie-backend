package checker

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"magpie/internal/domain"
	proxyqueue "magpie/internal/jobs/queue/proxy"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestLongRouteRetriesRenewLeaseBetweenAttempts(t *testing.T) {
	t.Setenv("PROXY_QUEUE_ENCRYPT_CREDENTIALS", "false")
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	queue := proxyqueue.NewRedisProxyQueue(client)
	originalQueue := proxyqueue.PublicProxyQueue
	proxyqueue.PublicProxyQueue = *queue
	resetCheckerHTTPClientCacheForTests()
	originalFactory := checkerTransportFactory
	t.Cleanup(func() {
		checkerTransportFactory = originalFactory
		resetCheckerHTTPClientCacheForTests()
		proxyqueue.PublicProxyQueue = originalQueue
	})
	proxy := domain.Proxy{ID: 1, IP: "192.0.2.1", Port: 8080, Hash: []byte("long-route"), Workspaces: []domain.Workspace{{ID: 1}}}
	if err := queue.AddToQueue([]domain.Proxy{proxy}); err != nil {
		t.Fatal(err)
	}
	worker, _, err := queue.GetNextProxyContext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	expireSoon := func() {
		worker.QueueLease.ScoreMS = time.Now().Add(time.Minute).UnixMilli()
		if err := client.ZAdd(context.Background(), worker.QueueLease.QueueKey, redis.Z{Score: float64(worker.QueueLease.ScoreMS) + 0.5, Member: string(worker.Hash)}).Err(); err != nil {
			t.Fatal(err)
		}
	}
	expireSoon()
	attempts := 0
	checkerTransportFactory = func(domain.Proxy, *domain.Judge, string, string, ...uint16) (http.RoundTripper, func(), error) {
		return roundTripFunc(func(*http.Request) (*http.Response, error) {
			if time.Until(time.UnixMilli(worker.QueueLease.ScoreMS)) < 2*time.Minute {
				t.Error("attempt started without renewing an expiring lease")
			}
			attempts++
			if attempts < 6 {
				expireSoon()
			}
			return nil, errors.New("unreachable")
		}), nil, nil
	}
	_, err, _, attempt := CheckProxyWithRetries(worker, &domain.Judge{FullString: "http://127.0.0.1/"}, "http", "tcp", 60000, 5)
	if err == nil || attempts != 6 || attempt != 5 {
		t.Fatalf("retry budget changed: attempts=%d final=%d error=%v", attempts, attempt, err)
	}
}

func TestBulkRequeueKeepsRetryingWorkerExclusive(t *testing.T) {
	t.Setenv("PROXY_QUEUE_ENCRYPT_CREDENTIALS", "false")
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	queue := proxyqueue.NewRedisProxyQueue(client)
	originalQueue, originalFactory := proxyqueue.PublicProxyQueue, checkerTransportFactory
	proxyqueue.PublicProxyQueue = *queue
	resetCheckerHTTPClientCacheForTests()
	t.Cleanup(func() {
		checkerTransportFactory = originalFactory
		resetCheckerHTTPClientCacheForTests()
		proxyqueue.PublicProxyQueue = originalQueue
	})
	proxy := domain.Proxy{ID: 1, IP: "192.0.2.1", Port: 8080, Hash: []byte("retrying-route"), Workspaces: []domain.Workspace{{ID: 1}}}
	ctx := context.Background()
	if err := queue.AddToQueue([]domain.Proxy{proxy}); err != nil {
		t.Fatal(err)
	}
	worker, _, err := queue.GetNextProxyContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	attempts := 0
	checkerTransportFactory = func(domain.Proxy, *domain.Judge, string, string, ...uint16) (http.RoundTripper, func(), error) {
		return roundTripFunc(func(*http.Request) (*http.Response, error) {
			attempts++
			if attempts == 1 {
				if count, err := queue.RequeueAll(); err != nil || count != 0 {
					t.Errorf("bulk scheduling changed the running route: count=%d error=%v", count, err)
				}
				limited, cancel := context.WithTimeout(ctx, 75*time.Millisecond)
				defer cancel()
				if _, _, err := queue.GetNextProxyContext(limited); !errors.Is(err, context.DeadlineExceeded) {
					t.Errorf("second worker began checking during retries: %v", err)
				}
			}
			return nil, errors.New("unreachable")
		}), nil, nil
	}
	_, err, _, _ = CheckProxyWithRetries(worker, &domain.Judge{FullString: "http://127.0.0.1/"}, "http", "tcp", 1000, 2)
	if attempts != 3 || errors.Is(err, proxyqueue.ErrProxyLeaseLost) {
		t.Fatalf("original worker's retry ownership changed: attempts=%d error=%v", attempts, err)
	}
}
