package checker

import (
	"container/heap"
	"magpie/models"
	"magpie/settings"
	"sync"
	"time"
)

type proxyItem struct {
	proxy     models.Proxy
	nextCheck time.Time
	index     int
}

type ProxyQueue struct {
	heap    []*proxyItem
	mu      sync.Mutex
	cond    *sync.Cond
	hashMap map[string]*proxyItem // Track proxies by hash
}

var PublicProxyQueue *ProxyQueue

func init() {
	PublicProxyQueue = NewProxyQueue()
}

func NewProxyQueue() *ProxyQueue {
	q := &ProxyQueue{
		heap:    []*proxyItem{},
		hashMap: make(map[string]*proxyItem),
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// Implement heap.Interface methods
func (pq *ProxyQueue) Len() int { return len(pq.heap) }

func (pq *ProxyQueue) Less(i, j int) bool {
	return pq.heap[i].nextCheck.Before(pq.heap[j].nextCheck)
}

func (pq *ProxyQueue) Swap(i, j int) {
	pq.heap[i], pq.heap[j] = pq.heap[j], pq.heap[i]
	pq.heap[i].index = i
	pq.heap[j].index = j
}

func (pq *ProxyQueue) Push(x interface{}) {
	item := x.(*proxyItem)
	item.index = len(pq.heap)
	pq.heap = append(pq.heap, item)
}

func (pq *ProxyQueue) Pop() interface{} {
	old := pq.heap
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // Avoid memory leak
	item.index = -1 // For safety
	pq.heap = old[0 : n-1]
	return item
}

// removeFromQueue removes a proxy item from both heap and hashMap
func (pq *ProxyQueue) removeFromQueue(item *proxyItem) {
	// Remove from hashMap
	delete(pq.hashMap, string(item.proxy.Hash))

	// Remove from heap
	idx := item.index
	if idx >= 0 && idx < len(pq.heap) {
		heap.Remove(pq, idx)
	}
}

// AddToQueue adds proxies with staggered initial check times to spread load.
// Only unique proxies (by hash) are added.
func (pq *ProxyQueue) AddToQueue(proxies []models.Proxy) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	interval := settings.GetTimeBetweenChecks()
	now := time.Now()
	addedCount := 0

	for _, proxy := range proxies {
		hashKey := string(proxy.Hash)

		// If proxy already exists in queue, update its check time
		if existingItem, exists := pq.hashMap[hashKey]; exists {
			pq.removeFromQueue(existingItem)
		}

		// Add new proxy item
		offset := (interval * time.Duration(addedCount)) / time.Duration(len(proxies))
		item := &proxyItem{
			proxy:     proxy,
			nextCheck: now.Add(offset),
		}

		heap.Push(pq, item)
		pq.hashMap[hashKey] = item
		addedCount++
	}

	if addedCount > 0 {
		pq.cond.Broadcast()
	}
}

// GetNextProxy retrieves the next proxy, blocking until its check time arrives.
func (pq *ProxyQueue) GetNextProxy() (models.Proxy, time.Time) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	for {
		// Wait until there are proxies
		for len(pq.heap) == 0 {
			pq.cond.Wait()
		}

		now := time.Now()
		earliest := pq.heap[0]

		if earliest.nextCheck.After(now) {
			waitTime := earliest.nextCheck.Sub(now)
			pq.mu.Unlock()
			time.Sleep(waitTime)
			pq.mu.Lock()
			continue
		}

		item := heap.Pop(pq).(*proxyItem)
		delete(pq.hashMap, string(item.proxy.Hash)) // Remove from hashMap
		return item.proxy, item.nextCheck
	}
}

// RequeueProxy reinserts a proxy with the next check time set to now + interval.
// If the proxy already exists in the queue, it will be updated instead of duplicated.
func (pq *ProxyQueue) RequeueProxy(proxy models.Proxy, lastCheckTime time.Time) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	hashKey := string(proxy.Hash)

	// If proxy already exists in queue, remove it
	if existingItem, exists := pq.hashMap[hashKey]; exists {
		pq.removeFromQueue(existingItem)
	}

	// Add updated proxy item
	item := &proxyItem{
		proxy:     proxy,
		nextCheck: lastCheckTime.Add(settings.GetTimeBetweenChecks()),
	}

	heap.Push(pq, item)
	pq.hashMap[hashKey] = item

	if pq.heap[0] == item {
		pq.cond.Signal()
	}
}
