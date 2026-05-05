package dbconnector

import (
	"context"
	"log"
	"sync"
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

// WithRetry retries failed queries up to maxRetries times with exponential back-off.
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

// WithTransientRetry retries only transient database errors (deadlock,
// serialization failure) up to maxRetries times with exponential back-off.
func WithTransientRetry(maxRetries int) QueryMiddleware {
	return func(ctx context.Context, query string, execute func(context.Context) error) error {
		var err error
		for i := 0; i <= maxRetries; i++ {
			err = execute(ctx)
			if err == nil {
				return nil
			}
			if !IsTransient(err) {
				return err
			}
			if i < maxRetries {
				time.Sleep(time.Millisecond * 100 * time.Duration(i+1))
			}
		}
		return err
	}
}

// circuitBreakerState holds the mutable state of a circuit breaker, protected
// by a mutex so it is safe for concurrent use across goroutines.
type circuitBreakerState struct {
	mu           sync.Mutex
	failures     int
	lastFailTime time.Time
}

// WithCircuitBreaker implements the circuit-breaker pattern.
// After threshold consecutive failures the breaker opens and every subsequent
// call returns ErrCircuitOpen until the timeout elapses and the breaker
// half-opens again.
func WithCircuitBreaker(threshold int, timeout time.Duration) QueryMiddleware {
	state := &circuitBreakerState{}

	return func(ctx context.Context, query string, execute func(context.Context) error) error {
		state.mu.Lock()
		open := state.failures >= threshold && time.Since(state.lastFailTime) < timeout
		state.mu.Unlock()

		if open {
			return ErrCircuitOpen
		}

		err := execute(ctx)

		state.mu.Lock()
		if err != nil {
			state.failures++
			state.lastFailTime = time.Now()
		} else {
			state.failures = 0
		}
		state.mu.Unlock()

		return err
	}
}

// WithSlowQueryLog logs any query whose execution time exceeds threshold.
func WithSlowQueryLog(threshold time.Duration, logger func(query string, duration time.Duration)) QueryMiddleware {
	return func(ctx context.Context, query string, execute func(context.Context) error) error {
		start := time.Now()
		err := execute(ctx)
		if d := time.Since(start); d >= threshold {
			logger(query, d)
		}
		return err
	}
}

// DefaultLogger provides basic query logging
func DefaultLogger(query string, duration time.Duration) {
	log.Printf("[DB] Query: %s | Duration: %v", query, duration)
}
