package dbconnector

import (
	"context"
	"encoding/json"
	"time"
)

type QueryExecutor[T any] func(ctx context.Context) (T, error)

type Query[T any] interface {
	Exec(ctx context.Context) (T, error)
	WithCache(ttl time.Duration) Query[T]
	SQL() string
}

type query[T any] struct {
	executor QueryExecutor[T]
	sql      string
	args     []interface{}
	cache    Cache
	cacheTTL time.Duration
}

func newQuery[T any](executor QueryExecutor[T], sql string, args ...interface{}) *query[T] {
	return &query[T]{
		executor: executor,
		sql:      sql,
		args:     args,
	}
}

func (q *query[T]) Exec(ctx context.Context) (T, error) {
	// Try cache first if enabled
	if q.cache != nil && q.cacheTTL > 0 {
		cacheKey := generateCacheKey(q.sql, q.args...)
		
		if cached, err := q.cache.Get(ctx, cacheKey); err == nil {
			var result T
			if err := json.Unmarshal(cached, &result); err == nil {
				return result, nil
			}
		}

		// Execute query
		result, err := q.executor(ctx)
		if err != nil {
			var zero T
			return zero, err
		}

		// Cache result
		if data, err := json.Marshal(result); err == nil {
			_ = q.cache.Set(ctx, cacheKey, data, q.cacheTTL)
		}

		return result, nil
	}

	return q.executor(ctx)
}

func (q *query[T]) WithCache(ttl time.Duration) Query[T] {
	// Return new instance (immutable)
	return &query[T]{
		executor: q.executor,
		sql:      q.sql,
		args:     q.args,
		cache:    q.cache,
		cacheTTL: ttl,
	}
}

func (q *query[T]) SQL() string {
	return q.sql
}
