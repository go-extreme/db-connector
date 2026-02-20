package dbconnector

import (
	"context"
	"fmt"
	"sort"
)

type Migration struct {
	Version int
	Name    string
	Up      string
	Down    string
}

type Migrator struct {
	conn       Connection
	migrations []Migration
}

func NewMigrator(conn Connection) *Migrator {
	return &Migrator{conn: conn}
}

func (m *Migrator) Add(migration Migration) {
	m.migrations = append(m.migrations, migration)
	sort.Slice(m.migrations, func(i, j int) bool {
		return m.migrations[i].Version < m.migrations[j].Version
	})
}

func (m *Migrator) Up(ctx context.Context) error {
	if err := m.createMigrationTable(ctx); err != nil {
		return err
	}

	currentVersion, err := m.getCurrentVersion(ctx)
	if err != nil {
		return err
	}

	for _, migration := range m.migrations {
		if migration.Version > currentVersion {
			if err := m.runMigration(ctx, migration); err != nil {
				return fmt.Errorf("migration %d failed: %w", migration.Version, err)
			}
		}
	}

	return nil
}

func (m *Migrator) createMigrationTable(ctx context.Context) error {
	query := `CREATE TABLE IF NOT EXISTS schema_migrations (
		version INT PRIMARY KEY,
		name VARCHAR(255),
		applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	)`
	_, err := m.conn.DB().ExecContext(ctx, query)
	return err
}

func (m *Migrator) getCurrentVersion(ctx context.Context) (int, error) {
	var version int
	err := m.conn.DB().GetContext(ctx, &version,
		"SELECT COALESCE(MAX(version), 0) FROM schema_migrations")
	return version, err
}

func (m *Migrator) runMigration(ctx context.Context, migration Migration) error {
	tx, err := m.conn.BeginTx(ctx)
	if err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx, migration.Up); err != nil {
		tx.Rollback()
		return err
	}

	if _, err := tx.ExecContext(ctx,
		"INSERT INTO schema_migrations (version, name) VALUES ($1, $2)",
		migration.Version, migration.Name); err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}
