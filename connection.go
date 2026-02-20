package dbconnector

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type Connection interface {
	DB() *sqlx.DB
	Connect(ctx context.Context) error
	Close() error
	Connected() bool
	BeginTx(ctx context.Context) (*sqlx.Tx, error)
}

type PostgresConnection struct {
	config *Config
	db     *sqlx.DB
}

func NewPostgresConnection(config *Config) *PostgresConnection {
	return &PostgresConnection{config: config}
}

func (c *PostgresConnection) DB() *sqlx.DB {
	if c.db == nil {
		panic("database connection is not established")
	}
	return c.db
}

func (c *PostgresConnection) Connect(ctx context.Context) error {
	if c.Connected() {
		return nil
	}

	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		c.config.Host, c.config.Port, c.config.User, c.config.Password, c.config.Database, c.config.SSLMode)

	db, err := sqlx.ConnectContext(ctx, "postgres", dsn)
	if err != nil {
		if c.config.AutoDatabaseCreation && isDatabaseNotExistError(err) {
			if err := c.createDatabase(ctx); err != nil {
				return err
			}
			return c.Connect(ctx)
		}
		return err
	}

	db.SetMaxOpenConns(c.config.MaxOpenConnection)
	db.SetMaxIdleConns(c.config.MaxIdleConnection)
	if c.config.ConnMaxLifetime > 0 {
		db.SetConnMaxLifetime(c.config.ConnMaxLifetime)
	}
	if c.config.ConnMaxIdleTime > 0 {
		db.SetConnMaxIdleTime(c.config.ConnMaxIdleTime)
	}

	c.db = db
	return nil
}

func (c *PostgresConnection) Close() error {
	if c.db != nil {
		err := c.db.Close()
		c.db = nil
		return err
	}
	return nil
}

func (c *PostgresConnection) Connected() bool {
	return c.db != nil
}

func (c *PostgresConnection) BeginTx(ctx context.Context) (*sqlx.Tx, error) {
	return c.DB().BeginTxx(ctx, nil)
}

func (c *PostgresConnection) createDatabase(ctx context.Context) error {
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=postgres sslmode=%s",
		c.config.Host, c.config.Port, c.config.User, c.config.Password, c.config.SSLMode)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	_, err = db.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE %s", c.config.Database))
	return err
}

func isDatabaseNotExistError(err error) bool {
	return err != nil && err.Error() == "pq: database \""+err.Error()+"\" does not exist"
}
