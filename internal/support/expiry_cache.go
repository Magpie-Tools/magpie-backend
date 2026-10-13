package support

import (
	"container/list"
	"sync"
	"time"
)

// ExpiryCache bounds retained entries even when every lookup uses a new key.
// Expiry is independent of LRU access, so reads never extend a stale value's life.
type ExpiryCache[K comparable, V any] struct {
	mu       sync.Mutex
	capacity int
	entries  map[K]*list.Element
	order    list.List
}

type expiryCacheEntry[K comparable, V any] struct {
	key     K
	value   V
	expires time.Time
}

func NewExpiryCache[K comparable, V any](capacity int) *ExpiryCache[K, V] {
	return &ExpiryCache[K, V]{capacity: max(1, capacity), entries: make(map[K]*list.Element)}
}

func (c *ExpiryCache[K, V]) Get(key K, now time.Time) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.entries[key]; ok {
		entry := elem.Value.(expiryCacheEntry[K, V])
		if now.Before(entry.expires) {
			c.order.MoveToFront(elem)
			return entry.value, true
		}
		c.remove(elem)
	}
	var zero V
	return zero, false
}

func (c *ExpiryCache[K, V]) Set(key K, value V, expires time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := expiryCacheEntry[K, V]{key: key, value: value, expires: expires}
	if elem, ok := c.entries[key]; ok {
		elem.Value = entry
		c.order.MoveToFront(elem)
		return
	}
	if len(c.entries) >= c.capacity {
		c.remove(c.order.Back())
	}
	c.entries[key] = c.order.PushFront(entry)
}

func (c *ExpiryCache[K, V]) Prune(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for elem := c.order.Back(); elem != nil; {
		previous := elem.Prev()
		if !now.Before(elem.Value.(expiryCacheEntry[K, V]).expires) {
			c.remove(elem)
		}
		elem = previous
	}
}

func (c *ExpiryCache[K, V]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

func (c *ExpiryCache[K, V]) remove(elem *list.Element) {
	delete(c.entries, elem.Value.(expiryCacheEntry[K, V]).key)
	c.order.Remove(elem)
}
