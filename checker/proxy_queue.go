package checker

import (
	"magpie/models"
	"sync"
)

type ProxyQueue struct {
	queue []models.Proxy
	mu    sync.Mutex
	cond  *sync.Cond
}

var PublicProxyQueue *ProxyQueue

func init() {
	PublicProxyQueue = NewProxyQueue()
}

func NewProxyQueue() *ProxyQueue {
	q := &ProxyQueue{
		queue: []models.Proxy{},
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// AddToQueue adds proxies to the queue.
func (pq *ProxyQueue) AddToQueue(proxies []models.Proxy) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	pq.queue = append(pq.queue, proxies...)
	pq.cond.Broadcast() // Notify waiting goroutines that items are available
}

// GetNextProxy retrieves the next proxy from the queue.
func (pq *ProxyQueue) GetNextProxy() models.Proxy {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	for len(pq.queue) == 0 {
		pq.cond.Wait() // Wait until proxies are available
	}

	proxy := pq.queue[0]
	pq.queue = pq.queue[1:]
	return proxy
}
