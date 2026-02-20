package dbconnector

import "time"

// ConnectionOption configures connection behavior
type ConnectionOption func(*connectionOptions)

type connectionOptions struct {
	connMaxLifetime time.Duration
	connMaxIdleTime time.Duration
}

func WithConnMaxLifetime(d time.Duration) ConnectionOption {
	return func(o *connectionOptions) {
		o.connMaxLifetime = d
	}
}

func WithConnMaxIdleTime(d time.Duration) ConnectionOption {
	return func(o *connectionOptions) {
		o.connMaxIdleTime = d
	}
}

// QueryOption configures query behavior
type QueryOption func(*queryOptions)

type queryOptions struct {
	timeout time.Duration
}

func WithTimeout(d time.Duration) QueryOption {
	return func(o *queryOptions) {
		o.timeout = d
	}
}
