package httpapi

import (
	"sync"
	"time"

	"github.com/sniperman/ledger/internal/command"
)

type commandCacheKey struct {
	commandType    string
	idempotencyKey string
}

type commandCacheEntry struct {
	envelope  command.Envelope
	expiresAt time.Time
}

type idempotencyCache struct {
	ttl   time.Duration
	mu    sync.RWMutex
	items map[commandCacheKey]commandCacheEntry
	now   func() time.Time
}

func newIdempotencyCache(ttl time.Duration) *idempotencyCache {
	return &idempotencyCache{
		ttl:   ttl,
		items: make(map[commandCacheKey]commandCacheEntry),
		now: func() time.Time {
			return time.Now().UTC()
		},
	}
}

func (c *idempotencyCache) Enabled() bool {
	return c != nil && c.ttl > 0
}

func (c *idempotencyCache) Get(commandType command.Type, idempotencyKey string) (command.Envelope, bool) {
	if !c.Enabled() || idempotencyKey == "" {
		return command.Envelope{}, false
	}

	key := commandCacheKey{commandType: string(commandType), idempotencyKey: idempotencyKey}

	c.mu.RLock()
	entry, ok := c.items[key]
	c.mu.RUnlock()
	if !ok {
		return command.Envelope{}, false
	}

	if c.now().After(entry.expiresAt) {
		c.mu.Lock()
		delete(c.items, key)
		c.mu.Unlock()
		return command.Envelope{}, false
	}

	return entry.envelope, true
}

func (c *idempotencyCache) Put(commandType command.Type, idempotencyKey string, envelope command.Envelope) {
	if !c.Enabled() || idempotencyKey == "" {
		return
	}

	key := commandCacheKey{commandType: string(commandType), idempotencyKey: idempotencyKey}
	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.now()
	for existingKey, entry := range c.items {
		if now.After(entry.expiresAt) {
			delete(c.items, existingKey)
		}
	}

	c.items[key] = commandCacheEntry{
		envelope:  envelope,
		expiresAt: now.Add(c.ttl),
	}
}
