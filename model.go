package dbconnector

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
)

// Model provides unified CQRS-compliant interface
// Automatically routes reads to readConn and writes to writeConn
type Model[T any] struct {
	readConn  Connection
	writeConn Connection
	tableName string
	cache     Cache
	cacheTTL  time.Duration
}

func NewModel[T any](connector Connector, tableName string) *Model[T] {
	return &Model[T]{
		readConn:  connector.Read(),
		writeConn: connector.Write(),
		tableName: tableName,
	}
}

// WithCache enables automatic caching for read operations
func (m *Model[T]) WithCache(cache Cache, ttl time.Duration) *Model[T] {
	m.cache = cache
	m.cacheTTL = ttl
	return m
}

// READ OPERATIONS (use readConn)

func (m *Model[T]) Find(id string) Query[T] {
	sql := fmt.Sprintf("SELECT * FROM %s WHERE id = $1", m.tableName)
	executor := func(ctx context.Context) (T, error) {
		var result T
		err := m.readConn.DB().GetContext(ctx, &result, sql, id)
		return result, err
	}

	q := newQuery(executor, sql, id)
	if m.cache != nil {
		q.cache = m.cache
		q.cacheTTL = m.cacheTTL
	}
	return q
}

func (m *Model[T]) FindBy(field string, value interface{}) Query[T] {
	sql := fmt.Sprintf("SELECT * FROM %s WHERE %s = $1", m.tableName, field)
	executor := func(ctx context.Context) (T, error) {
		var result T
		err := m.readConn.DB().GetContext(ctx, &result, sql, value)
		return result, err
	}

	q := newQuery(executor, sql, value)
	if m.cache != nil {
		q.cache = m.cache
		q.cacheTTL = m.cacheTTL
	}
	return q
}

func (m *Model[T]) GetBy(conditions map[string]interface{}) Query[[]T] {
	sql, args := m.buildWhereQuery(fmt.Sprintf("SELECT * FROM %s", m.tableName), conditions)
	executor := func(ctx context.Context) ([]T, error) {
		var result []T
		err := m.readConn.DB().SelectContext(ctx, &result, sql, args...)
		return result, err
	}

	q := newQuery(executor, sql, args...)
	if m.cache != nil {
		q.cache = m.cache
		q.cacheTTL = m.cacheTTL
	}
	return q
}

func (m *Model[T]) All() Query[[]T] {
	sql := fmt.Sprintf("SELECT * FROM %s", m.tableName)
	executor := func(ctx context.Context) ([]T, error) {
		var result []T
		err := m.readConn.DB().SelectContext(ctx, &result, sql)
		return result, err
	}

	q := newQuery(executor, sql)
	if m.cache != nil {
		q.cache = m.cache
		q.cacheTTL = m.cacheTTL
	}
	return q
}

func (m *Model[T]) Count(ctx context.Context, conditions map[string]interface{}) (int, error) {
	sql, args := m.buildWhereQuery(fmt.Sprintf("SELECT COUNT(*) FROM %s", m.tableName), conditions)
	var count int
	err := m.readConn.DB().GetContext(ctx, &count, sql, args...)
	return count, err
}

func (m *Model[T]) Exists(ctx context.Context, id string) (bool, error) {
	sql := fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM %s WHERE id = $1)", m.tableName)
	var exists bool
	err := m.readConn.DB().GetContext(ctx, &exists, sql, id)
	return exists, err
}

func (m *Model[T]) Query() *QueryBuilder[T] {
	return NewQueryBuilder(m)
}

// WRITE OPERATIONS (use writeConn)

func (m *Model[T]) Create(ctx context.Context, data T) error {
	query := fmt.Sprintf("INSERT INTO %s (id, name, email, age, status) VALUES (:id, :name, :email, :age, :status)", m.tableName)
	_, err := m.writeConn.DB().NamedExecContext(ctx, query, data)

	if m.cache != nil {
		m.invalidateCache(ctx)
	}
	return err
}

func (m *Model[T]) CreateMany(ctx context.Context, data []T) error {
	if len(data) == 0 {
		return nil
	}

	query := fmt.Sprintf("INSERT INTO %s (id, name, email, age, status) VALUES (:id, :name, :email, :age, :status)", m.tableName)
	_, err := m.writeConn.DB().NamedExecContext(ctx, query, data)

	if m.cache != nil {
		m.invalidateCache(ctx)
	}
	return err
}

func (m *Model[T]) Update(ctx context.Context, id string, data map[string]interface{}) error {
	if len(data) == 0 {
		return fmt.Errorf("no data to update")
	}

	setClauses := make([]string, 0, len(data))
	args := make([]interface{}, 0, len(data)+1)
	i := 1

	for key, value := range data {
		setClauses = append(setClauses, fmt.Sprintf("%s = $%d", key, i))
		args = append(args, value)
		i++
	}

	args = append(args, id)
	sql := fmt.Sprintf("UPDATE %s SET %s WHERE id = $%d", m.tableName, strings.Join(setClauses, ", "), i)

	_, err := m.writeConn.DB().ExecContext(ctx, sql, args...)

	if m.cache != nil {
		m.invalidateCache(ctx)
	}
	return err
}

func (m *Model[T]) UpdateBy(ctx context.Context, data map[string]interface{}, conditions map[string]interface{}) error {
	if len(data) == 0 {
		return fmt.Errorf("no data to update")
	}

	setClauses := make([]string, 0, len(data))
	args := make([]interface{}, 0, len(data)+len(conditions))
	i := 1

	for key, value := range data {
		setClauses = append(setClauses, fmt.Sprintf("%s = $%d", key, i))
		args = append(args, value)
		i++
	}

	sql := fmt.Sprintf("UPDATE %s SET %s", m.tableName, strings.Join(setClauses, ", "))

	if len(conditions) > 0 {
		whereClause, whereArgs := m.buildWhereClause(conditions, i)
		sql += " WHERE " + whereClause
		args = append(args, whereArgs...)
	}

	_, err := m.writeConn.DB().ExecContext(ctx, sql, args...)

	if m.cache != nil {
		m.invalidateCache(ctx)
	}
	return err
}

func (m *Model[T]) Delete(ctx context.Context, id string) error {
	sql := fmt.Sprintf("DELETE FROM %s WHERE id = $1", m.tableName)
	_, err := m.writeConn.DB().ExecContext(ctx, sql, id)

	if m.cache != nil {
		m.invalidateCache(ctx)
	}
	return err
}

func (m *Model[T]) DeleteBy(ctx context.Context, conditions map[string]interface{}) error {
	sql, args := m.buildWhereQuery(fmt.Sprintf("DELETE FROM %s", m.tableName), conditions)
	_, err := m.writeConn.DB().ExecContext(ctx, sql, args...)

	if m.cache != nil {
		m.invalidateCache(ctx)
	}
	return err
}

// TRANSACTION SUPPORT

func (m *Model[T]) WriteTransaction() *Transaction {
	return NewTransaction(m.writeConn)
}
func (m *Model[T]) ReadTransaction() *Transaction {
	return NewTransaction(m.readConn)
}

// BATCH OPERATIONS

func (m *Model[T]) BatchCreate(ctx context.Context, data []T, batchSize int) error {
	for i := 0; i < len(data); i += batchSize {
		end := i + batchSize
		if end > len(data) {
			end = len(data)
		}
		if err := m.CreateMany(ctx, data[i:end]); err != nil {
			return err
		}
	}
	return nil
}

func (m *Model[T]) BatchDelete(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}

	placeholders := make([]string, len(ids))
	args := make([]interface{}, len(ids))
	for i, id := range ids {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = id
	}

	sql := fmt.Sprintf("DELETE FROM %s WHERE id IN (%s)", m.tableName, strings.Join(placeholders, ","))
	_, err := m.writeConn.DB().ExecContext(ctx, sql, args...)

	if m.cache != nil {
		m.invalidateCache(ctx)
	}
	return err
}

// PAGINATION

type Page[T any] struct {
	Items      []T
	Total      int
	Page       int
	PageSize   int
	TotalPages int
}

func (m *Model[T]) Paginate(ctx context.Context, page, pageSize int, conditions map[string]interface{}) (*Page[T], error) {
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 10
	}

	// Get total count
	total, err := m.Count(ctx, conditions)
	if err != nil {
		return nil, err
	}

	// Get items
	offset := (page - 1) * pageSize
	sql, args := m.buildWhereQuery(fmt.Sprintf("SELECT * FROM %s", m.tableName), conditions)
	sql += fmt.Sprintf(" LIMIT %d OFFSET %d", pageSize, offset)

	var items []T
	err = m.readConn.DB().SelectContext(ctx, &items, sql, args...)
	if err != nil {
		return nil, err
	}

	totalPages := (total + pageSize - 1) / pageSize

	return &Page[T]{
		Items:      items,
		Total:      total,
		Page:       page,
		PageSize:   pageSize,
		TotalPages: totalPages,
	}, nil
}

// Helper methods

func (m *Model[T]) buildWhereQuery(baseQuery string, conditions map[string]interface{}) (string, []interface{}) {
	if len(conditions) == 0 {
		return baseQuery, nil
	}

	whereClause, args := m.buildWhereClause(conditions, 1)
	return baseQuery + " WHERE " + whereClause, args
}

func (m *Model[T]) buildWhereClause(conditions map[string]interface{}, startIdx int) (string, []interface{}) {
	clauses := make([]string, 0, len(conditions))
	args := make([]interface{}, 0, len(conditions))
	i := startIdx

	for key, value := range conditions {
		clauses = append(clauses, fmt.Sprintf("%s = $%d", key, i))
		args = append(args, value)
		i++
	}

	return strings.Join(clauses, " AND "), args
}

func (m *Model[T]) invalidateCache(ctx context.Context) {
	// Simple cache invalidation - delete all keys for this table
	// In production, use more sophisticated cache invalidation
	_ = m.cache.Delete(ctx, m.tableName+"*")
}

// DB returns the underlying read database connection
func (m *Model[T]) DB() *sqlx.DB {
	return m.readConn.DB()
}
