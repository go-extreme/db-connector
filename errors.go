package dbconnector

import "errors"

var (
	ErrNotFound      = errors.New("record not found")
	ErrCircuitOpen   = errors.New("circuit breaker is open")
	ErrTimeout       = errors.New("query timeout")
	ErrNoConnection  = errors.New("no database connection")
	ErrInvalidConfig = errors.New("invalid configuration")
)
