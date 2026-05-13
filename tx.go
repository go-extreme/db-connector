package dbconnector

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"
)

// Transaction wraps a Connection and provides a safe Execute helper with
// automatic rollback and optional isolation-level control.
type Transaction struct {
	conn           Connection
	isolationLevel sql.IsolationLevel
}

func NewTransaction(conn Connection) *Transaction {
	return &Transaction{
		conn:           conn,
		isolationLevel: sql.LevelDefault,
	}
}

// WithIsolationLevel returns a new Transaction that opens with the given
// PostgreSQL isolation level.  Common values:
//
//	sql.LevelReadCommitted    (default)
//	sql.LevelRepeatableRead
//	sql.LevelSerializable
func (t *Transaction) WithIsolationLevel(level sql.IsolationLevel) *Transaction {
	return &Transaction{conn: t.conn, isolationLevel: level}
}

// Execute runs fn inside a database transaction.
//
//   - If fn returns an error the transaction is rolled back and that error is
//     returned (with the rollback error appended if rollback itself fails).
//   - If fn returns nil the transaction is committed.
func (t *Transaction) Execute(ctx context.Context, fn func(ctx context.Context, tx *sqlx.Tx) error) error {
	var tx *sqlx.Tx
	var err error

	if t.isolationLevel == sql.LevelDefault {
		tx, err = t.conn.BeginTx(ctx)
	} else {
		tx, err = t.conn.DB().BeginTxx(ctx, &sql.TxOptions{Isolation: t.isolationLevel})
	}
	if err != nil {
		return err
	}

	if fnErr := fn(ctx, tx); fnErr != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			return fmt.Errorf("tx rollback failed (%w) after: %w", rbErr, fnErr)
		}
		return fnErr
	}

	return tx.Commit()
}

// ExecuteWithSavepoint runs fn inside a named savepoint within an existing
// transaction.  On error, only the savepoint is rolled back; the outer
// transaction remains open.
func ExecuteWithSavepoint(ctx context.Context, tx *sqlx.Tx, name string, fn func() error) error {
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("SAVEPOINT %s", name)); err != nil {
		return err
	}

	if fnErr := fn(); fnErr != nil {
		if _, rbErr := tx.ExecContext(ctx, fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", name)); rbErr != nil {
			return errors.Join(fnErr, fmt.Errorf("rollback to savepoint %q failed: %w", name, rbErr))
		}
		return fnErr
	}

	_, err := tx.ExecContext(ctx, fmt.Sprintf("RELEASE SAVEPOINT %s", name))
	return err
}
