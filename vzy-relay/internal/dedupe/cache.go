package dedupe

import (
	"sync"
	"time"
)

type entry struct {
	expiresAt time.Time
}

type Cache struct {
	mu    sync.Mutex
	ttl   time.Duration
	items map[string]entry
}

func New(ttl time.Duration) *Cache {
	return &Cache{ttl: ttl, items: make(map[string]entry)}
}

func (c *Cache) SeenOrAdd(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	for k, v := range c.items {
		if now.After(v.expiresAt) {
			delete(c.items, k)
		}
	}
	if _, ok := c.items[key]; ok {
		return true
	}
	c.items[key] = entry{expiresAt: now.Add(c.ttl)}
	return false
}
