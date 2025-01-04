package checker

import (
	"errors"
	"magpie/models"
	"sync"
)

type ProxyQueue struct {
	queue    []models.Proxy
	mu       sync.Mutex
	cond     *sync.Cond
	stopping bool // Indicates if the queue is stopping
}

var PublicProxyQueue *ProxyQueue

func init() {
	PublicProxyQueue = NewProxyQueue()
}

func NewProxyQueue() *ProxyQueue {
	q := &ProxyQueue{
		queue:    []models.Proxy{},
		stopping: false,
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// AddToQueue adds proxies to the queue.
func (pq *ProxyQueue) AddToQueue(proxies []models.Proxy) error {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if pq.stopping {
		return errors.New("cannot add to queue: stopping")
	}

	pq.queue = append(pq.queue, proxies...)
	pq.cond.Broadcast() // Notify waiting goroutines that items are available
	return nil
}

// GetNextProxy retrieves the next proxy from the queue.
func (pq *ProxyQueue) GetNextProxy() (models.Proxy, error) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	for len(pq.queue) == 0 && !pq.stopping {
		pq.cond.Wait() // Wait until proxies are available or stopping
		// Wait also unlocks the mutex
	}

	if pq.stopping && len(pq.queue) == 0 {
		return models.Proxy{}, errors.New("queue is stopping and empty")
	}

	proxy := pq.queue[0]
	pq.queue = pq.queue[1:]
	return proxy, nil
}

// Stop stops the queue and wakes up all waiting goroutines.
func (pq *ProxyQueue) Stop() {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	pq.stopping = true
	pq.cond.Broadcast() // Wake up all waiting goroutines to allow them to exit
}

// IsStopping returns whether the queue is stopping.
func (pq *ProxyQueue) IsStopping() bool {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	return pq.stopping
}
