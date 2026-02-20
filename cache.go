package dbconnector

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Cache interface allows pluggable cache strategies
type Cache interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
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

func (r *RedisCache) Delete(ctx context.Context, key string) error {
	return r.client.Del(ctx, key).Err()
}

// InMemoryCache implements Cache using in-memory storage
type InMemoryCache struct {
	mu    sync.RWMutex
	store map[string]*cacheEntry
}

type cacheEntry struct {
	value     []byte
	expiresAt time.Time
}

func NewInMemoryCache() *InMemoryCache {
	cache := &InMemoryCache{
		store: make(map[string]*cacheEntry),
	}
	go cache.cleanup()
	return cache
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

func (c *InMemoryCache) Delete(ctx context.Context, key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.store, key)
	return nil
}

func (c *InMemoryCache) cleanup() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		c.mu.Lock()
		now := time.Now()
		for key, entry := range c.store {
			if now.After(entry.expiresAt) {
				delete(c.store, key)
			}
		}
		c.mu.Unlock()
	}
}

// generateCacheKey creates a consistent hash for query caching
func generateCacheKey(query string, args ...interface{}) string {
	data, _ := json.Marshal(struct {
		Query string
		Args  []interface{}
	}{Query: query, Args: args})
	
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}
