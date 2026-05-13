package dbconnector

import (
	"context"
	"testing"
	"time"
)

func TestQuery(t *testing.T) {
	q := newQuery(
		func(ctx context.Context) (string, error) {
			return "test result", nil
		},
		"SELECT * FROM test",
	)

	result, err := q.Exec(context.Background())
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if result != "test result" {
		t.Errorf("expected 'test result', got '%s'", result)
	}

	sql := q.SQL()
	if sql != "SELECT * FROM test" {
		t.Errorf("expected 'SELECT * FROM test', got '%s'", sql)
	}
}

func TestQueryWithCache(t *testing.T) {
	cache := NewInMemoryCache()
	ctx := context.Background()

	execCount := 0
	q := newQuery(
		func(ctx context.Context) (string, error) {
			execCount++
			return "cached result", nil
		},
		"SELECT * FROM test",
	)
	q.cache = cache
	q.cacheTTL = 5 * time.Minute

	// First call - hits database
	result1, err := q.Exec(ctx)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if result1 != "cached result" {
		t.Errorf("expected 'cached result', got '%s'", result1)
	}

	if execCount != 1 {
		t.Errorf("expected 1 execution, got %d", execCount)
	}

	// Second call - hits cache
	result2, err := q.Exec(ctx)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if result2 != "cached result" {
		t.Errorf("expected 'cached result', got '%s'", result2)
	}

	// Should still be 1 because second call used cache
	if execCount != 1 {
		t.Errorf("expected 1 execution (cached), got %d", execCount)
	}
}

func TestQueryWithCacheOverride(t *testing.T) {
	cache := NewInMemoryCache()
	ctx := context.Background()

	q := newQuery(
		func(ctx context.Context) (string, error) {
			return "result", nil
		},
		"SELECT * FROM test",
	)
	q.cache = cache
	q.cacheTTL = 5 * time.Minute

	// Override cache TTL
	q2 := q.WithCache(1 * time.Hour)

	result, err := q2.Exec(ctx)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if result != "result" {
		t.Errorf("expected 'result', got '%s'", result)
	}
}

func TestInMemoryCache(t *testing.T) {
	cache := NewInMemoryCache()
	ctx := context.Background()

	// Set value
	err := cache.Set(ctx, "key1", []byte("value1"), 5*time.Minute)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// Get value
	val, err := cache.Get(ctx, "key1")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if string(val) != "value1" {
		t.Errorf("expected 'value1', got '%s'", string(val))
	}

	// Delete value
	err = cache.Delete(ctx, "key1")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// Get deleted value
	val, err = cache.Get(ctx, "key1")
	if err == nil {
		t.Error("expected error for deleted key")
	}
}

func TestCacheKeyGeneration(t *testing.T) {
	// Without table prefix: key is bare sha256 hex (64 chars)
	key1 := generateCacheKey("", "SELECT * FROM users WHERE id = $1", "arg1", "arg2")
	key2 := generateCacheKey("", "SELECT * FROM users WHERE id = $1", "arg1", "arg2")

	if key1 != key2 {
		t.Error("same query and args should generate same key")
	}

	key3 := generateCacheKey("", "SELECT * FROM users WHERE id = $1", "arg1", "arg3")
	if key1 == key3 {
		t.Error("different args should generate different keys")
	}

	if len(key1) != 64 { // SHA-256 produces 64 hex characters when prefix is empty
		t.Errorf("expected SHA-256 hash length 64 (no prefix), got %d", len(key1))
	}

	// With table prefix: key is "tableName:sha256hex"
	keyPrefixed := generateCacheKey("users", "SELECT * FROM users WHERE id = $1", "arg1")
	if len(keyPrefixed) != 64+1+5 { // "users:" + 64 chars
		t.Errorf("expected prefixed key length %d, got %d", 64+1+5, len(keyPrefixed))
	}
	if keyPrefixed[:6] != "users:" {
		t.Errorf("expected key to start with 'users:', got %q", keyPrefixed[:6])
	}

	// Same query, different prefix → different keys
	keyNoPrefix := generateCacheKey("", "SELECT * FROM users WHERE id = $1", "arg1")
	if keyPrefixed == keyNoPrefix {
		t.Error("different prefixes should produce different keys")
	}
}
