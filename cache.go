package dbconnector

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Cache interface allows pluggable cache strategies.
// Delete supports a trailing "*" glob to invalidate all keys sharing a prefix
// (e.g. "users:*" removes every key whose name starts with "users:").
type Cache interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
	// Delete removes a single key, or all keys matching a "prefix*" pattern.
	Delete(ctx context.Context, key string) error
}

// RedisCache implements Cache using Redis
type RedisCache struct {
	client *redis.Client
}

func NewRedisCache(addr string, password string, db int) *RedisCache {
	return &RedisCache{
		client: redis.NewClient(&redis.Options{
			Addr:     addr,
			Password: password,
			DB:       db,
		}),
	}
}

func (r *RedisCache) Get(ctx context.Context, key string) ([]byte, error) {
	return r.client.Get(ctx, key).Bytes()
}

func (r *RedisCache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return r.client.Set(ctx, key, value, ttl).Err()
}

// Delete removes a single key or, when key ends with "*", removes all keys
// matching that prefix using Redis SCAN + DEL (safe on large key spaces).
func (r *RedisCache) Delete(ctx context.Context, key string) error {
	if strings.HasSuffix(key, "*") {
		return r.deleteByPattern(ctx, key)
	}
	return r.client.Del(ctx, key).Err()
}

func (r *RedisCache) deleteByPattern(ctx context.Context, pattern string) error {
	var cursor uint64
	for {
		keys, next, err := r.client.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return err
		}
		if len(keys) > 0 {
			if err := r.client.Del(ctx, keys...).Err(); err != nil {
				return err
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return nil
}

// InMemoryCache implements Cache using in-memory storage with TTL.
// Call Close() when the cache is no longer needed to stop the background
// cleanup goroutine and avoid resource leaks.
type InMemoryCache struct {
	mu    sync.RWMutex
	store map[string]*cacheEntry
	done  chan struct{}
}

type cacheEntry struct {
	value     []byte
	expiresAt time.Time
}

func NewInMemoryCache() *InMemoryCache {
	cache := &InMemoryCache{
		store: make(map[string]*cacheEntry),
		done:  make(chan struct{}),
	}
	go cache.cleanup()
	return cache
}

// Close stops the background cleanup goroutine.
func (c *InMemoryCache) Close() {
	close(c.done)
}

func (c *InMemoryCache) Get(ctx context.Context, key string) ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, exists := c.store[key]
	if !exists || time.Now().After(entry.expiresAt) {
		return nil, redis.Nil
	}
	return entry.value, nil
}

func (c *InMemoryCache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.store[key] = &cacheEntry{
		value:     value,
		expiresAt: time.Now().Add(ttl),
	}
	return nil
}

// Delete removes a single key or, when key ends with "*", removes all keys
// whose names start with the given prefix.
func (c *InMemoryCache) Delete(ctx context.Context, key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if strings.HasSuffix(key, "*") {
		prefix := strings.TrimSuffix(key, "*")
		for k := range c.store {
			if strings.HasPrefix(k, prefix) {
				delete(c.store, k)
			}
		}
		return nil
	}

	delete(c.store, key)
	return nil
}

func (c *InMemoryCache) cleanup() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.mu.Lock()
			now := time.Now()
			for key, entry := range c.store {
				if now.After(entry.expiresAt) {
					delete(c.store, key)
				}
			}
			c.mu.Unlock()
		case <-c.done:
			return
		}
	}
}

// generateCacheKey creates a consistent, table-prefixed hash for query caching.
// Format: "tableName:sha256hex" — allows prefix-based invalidation per table.
func generateCacheKey(tablePrefix, query string, args ...interface{}) string {
	data, _ := json.Marshal(struct {
		Query string
		Args  []interface{}
	}{Query: query, Args: args})

	hash := sha256.Sum256(data)
	key := hex.EncodeToString(hash[:])
	if tablePrefix != "" {
		return tablePrefix + ":" + key
	}
	return key
}
