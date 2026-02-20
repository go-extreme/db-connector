package dbconnector

import (
	"context"

	"github.com/jmoiron/sqlx"
)

type Transaction struct {
	conn Connection
}

func NewTransaction(conn Connection) *Transaction {
	return &Transaction{conn: conn}
}

func (t *Transaction) Execute(ctx context.Context, fn func(ctx context.Context, tx *sqlx.Tx) error) error {
	tx, err := t.conn.BeginTx(ctx)
	if err != nil {
		return err
	}

	if err := fn(ctx, tx); err != nil {
		_ = tx.Rollback()
		return err
	}

	return tx.Commit()
}
