package dbconnector

import (
	"context"
	"log"
	"time"
)

// QueryMiddleware wraps query execution with additional behavior
type QueryMiddleware func(ctx context.Context, query string, execute func(context.Context) error) error

// WithLogging logs query execution time
func WithLogging(logger func(query string, duration time.Duration)) QueryMiddleware {
	return func(ctx context.Context, query string, execute func(context.Context) error) error {
		start := time.Now()
		err := execute(ctx)
		logger(query, time.Since(start))
		return err
	}
}

// WithRetry retries failed queries
func WithRetry(maxRetries int) QueryMiddleware {
	return func(ctx context.Context, query string, execute func(context.Context) error) error {
		var err error
		for i := 0; i <= maxRetries; i++ {
			err = execute(ctx)
			if err == nil {
				return nil
			}
			if i < maxRetries {
				time.Sleep(time.Millisecond * 100 * time.Duration(i+1))
			}
		}
		return err
	}
}

// WithCircuitBreaker implements circuit breaker pattern
func WithCircuitBreaker(threshold int, timeout time.Duration) QueryMiddleware {
	failures := 0
	lastFailTime := time.Time{}

	return func(ctx context.Context, query string, execute func(context.Context) error) error {
		if failures >= threshold && time.Since(lastFailTime) < timeout {
			return ErrCircuitOpen
		}

		err := execute(ctx)
		if err != nil {
			failures++
			lastFailTime = time.Now()
			return err
		}

		failures = 0
		return nil
	}
}

// DefaultLogger provides basic query logging
func DefaultLogger(query string, duration time.Duration) {
	log.Printf("[DB] Query: %s | Duration: %v", query, duration)
}
