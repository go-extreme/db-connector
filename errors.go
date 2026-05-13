package dbconnector

import (
	"errors"

	"github.com/lib/pq"
)

// Sentinel errors
var (
	ErrNotFound             = errors.New("record not found")
	ErrCircuitOpen          = errors.New("circuit breaker is open")
	ErrTimeout              = errors.New("query timeout")
	ErrNoConnection         = errors.New("no database connection")
	ErrInvalidConfig        = errors.New("invalid configuration")
	ErrUniqueViolation      = errors.New("unique constraint violation")
	ErrForeignKeyViolation  = errors.New("foreign key constraint violation")
	ErrCheckViolation       = errors.New("check constraint violation")
	ErrDeadlock             = errors.New("deadlock detected")
	ErrSerializationFailure = errors.New("serialization failure")
)

// PostgreSQL error class codes (SQLSTATE)
const (
	pgUniqueViolation      = "23505"
	pgForeignKeyViolation  = "23503"
	pgCheckViolation       = "23514"
	pgDeadlock             = "40P01"
	pgSerializationFailure = "40001"
	pgInvalidPassword      = "28P01"
	pgInvalidCatalogName   = "3D000"
)

// IsNotFound reports whether err is (or wraps) ErrNotFound.
func IsNotFound(err error) bool {
	return errors.Is(err, ErrNotFound)
}

// IsUniqueViolation reports whether err is a PostgreSQL unique-constraint violation.
func IsUniqueViolation(err error) bool {
	if errors.Is(err, ErrUniqueViolation) {
		return true
	}
	var pqErr *pq.Error
	return errors.As(err, &pqErr) && string(pqErr.Code) == pgUniqueViolation
}

// IsForeignKeyViolation reports whether err is a PostgreSQL foreign-key violation.
func IsForeignKeyViolation(err error) bool {
	if errors.Is(err, ErrForeignKeyViolation) {
		return true
	}
	var pqErr *pq.Error
	return errors.As(err, &pqErr) && string(pqErr.Code) == pgForeignKeyViolation
}

// IsCheckViolation reports whether err is a PostgreSQL check-constraint violation.
func IsCheckViolation(err error) bool {
	if errors.Is(err, ErrCheckViolation) {
		return true
	}
	var pqErr *pq.Error
	return errors.As(err, &pqErr) && string(pqErr.Code) == pgCheckViolation
}

// IsDeadlock reports whether err is a PostgreSQL deadlock error.
func IsDeadlock(err error) bool {
	if errors.Is(err, ErrDeadlock) {
		return true
	}
	var pqErr *pq.Error
	return errors.As(err, &pqErr) && string(pqErr.Code) == pgDeadlock
}

// IsSerializationFailure reports whether err is a PostgreSQL serialization failure
// (occurs with REPEATABLE READ / SERIALIZABLE isolation under concurrent writes).
func IsSerializationFailure(err error) bool {
	if errors.Is(err, ErrSerializationFailure) {
		return true
	}
	var pqErr *pq.Error
	return errors.As(err, &pqErr) && string(pqErr.Code) == pgSerializationFailure
}

// IsConstraintViolation reports whether err is any PostgreSQL constraint violation
// (unique, foreign key, or check).
func IsConstraintViolation(err error) bool {
	return IsUniqueViolation(err) || IsForeignKeyViolation(err) || IsCheckViolation(err)
}

// IsTransient reports whether err is a transient database error that is safe to
// retry (deadlock or serialization failure).
func IsTransient(err error) bool {
	return IsDeadlock(err) || IsSerializationFailure(err)
}
