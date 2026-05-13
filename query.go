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
	// WithTimeout returns a new Query that cancels execution after d.
	WithTimeout(d time.Duration) Query[T]
	// SQL returns the raw SQL with $N placeholders.
	SQL() string
	// ToSQL returns the SQL with all $N placeholders replaced by their actual
	// values – intended for debugging/logging only, not for execution.
	ToSQL() string
}

type query[T any] struct {
	executor    QueryExecutor[T]
	sql         string
	args        []interface{}
	cache       Cache
	cacheTTL    time.Duration
	tablePrefix string            // used for table-scoped cache key prefix
	timeout     time.Duration     // when > 0, Exec wraps ctx with a deadline
	middlewares []QueryMiddleware // applied around the core executor
}

func newQuery[T any](executor QueryExecutor[T], sql string, args ...interface{}) *query[T] {
	return &query[T]{
		executor: executor,
		sql:      sql,
		args:     args,
	}
}

func (q *query[T]) Exec(ctx context.Context) (T, error) {
	// Apply timeout if configured
	if q.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, q.timeout)
		defer cancel()
	}

	// Core execution (possibly wrapped with cache)
	coreExec := func(ctx context.Context) (T, error) {
		if q.cache != nil && q.cacheTTL > 0 {
			cacheKey := generateCacheKey(q.tablePrefix, q.sql, q.args...)

			if cached, err := q.cache.Get(ctx, cacheKey); err == nil {
				var result T
				if err := json.Unmarshal(cached, &result); err == nil {
					return result, nil
				}
			}

			result, err := q.executor(ctx)
			if err != nil {
				var zero T
				return zero, err
			}

			if data, err := json.Marshal(result); err == nil {
				_ = q.cache.Set(ctx, cacheKey, data, q.cacheTTL)
			}
			return result, nil
		}
		return q.executor(ctx)
	}

	// If no middleware, run directly
	if len(q.middlewares) == 0 {
		return coreExec(ctx)
	}

	// Wrap coreExec with the middleware chain (last middleware = innermost)
	var result T
	var execErr error

	// Build a func(context.Context) error bridge for the middleware signature
	bridge := func(ctx context.Context) error {
		result, execErr = coreExec(ctx)
		return execErr
	}

	// Apply middlewares from last to first so the first one in the slice is outermost
	chain := bridge
	for i := len(q.middlewares) - 1; i >= 0; i-- {
		mw := q.middlewares[i]
		next := chain
		chain = func(ctx context.Context) error {
			return mw(ctx, q.sql, next)
		}
	}

	if err := chain(ctx); err != nil {
		var zero T
		return zero, err
	}
	return result, nil
}

// WithCache returns a new Query with the given TTL applied.
func (q *query[T]) WithCache(ttl time.Duration) Query[T] {
	return &query[T]{
		executor:    q.executor,
		sql:         q.sql,
		args:        q.args,
		cache:       q.cache,
		cacheTTL:    ttl,
		tablePrefix: q.tablePrefix,
		timeout:     q.timeout,
		middlewares: q.middlewares,
	}
}

// WithTimeout returns a new Query that cancels execution after d.
func (q *query[T]) WithTimeout(d time.Duration) Query[T] {
	return &query[T]{
		executor:    q.executor,
		sql:         q.sql,
		args:        q.args,
		cache:       q.cache,
		cacheTTL:    q.cacheTTL,
		tablePrefix: q.tablePrefix,
		timeout:     d,
		middlewares: q.middlewares,
	}
}

func (q *query[T]) SQL() string {
	return q.sql
}

// ToSQL returns the SQL with all $N placeholders replaced by their actual
// argument values.  For display/logging only – do NOT execute this string.
func (q *query[T]) ToSQL() string {
	return InterpolateSQL(q.sql, q.args...)
}
