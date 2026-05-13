package dbconnector

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
)

// Model provides unified CQRS-compliant interface
// Automatically routes reads to readConn and writes to writeConn
type Model[T any] struct {
	readConn      Connection
	writeConn     Connection
	tableName     string
	cache         Cache
	cacheTTL      time.Duration
	softDeleteCol string
	middlewares   []QueryMiddleware
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

// WithSoftDelete enables soft delete using the given column (e.g. "deleted_at")
func (m *Model[T]) WithSoftDelete(column string) *Model[T] {
	m.softDeleteCol = column
	return m
}

// WithMiddleware attaches one or more QueryMiddleware functions that wrap every
// query executed through this model.  Middlewares are applied in the order
// provided (first = outermost wrapper).
func (m *Model[T]) WithMiddleware(mw ...QueryMiddleware) *Model[T] {
	m.middlewares = append(m.middlewares, mw...)
	return m
}

// attachCache sets the cache, TTL, table-prefix, and middleware chain on a
// freshly created query.
func (m *Model[T]) attachCache(q *query[T]) {
	attachQueryMeta(q, m.tableName, m.middlewares, m.cache, m.cacheTTL)
}

// attachQueryMeta is the generic helper that sets metadata on any query[R].
// Using a package-level generic function lets it serve both *query[T] and
// *query[[]T] without duplicating code.
func attachQueryMeta[R any](q *query[R], tableName string, middlewares []QueryMiddleware, cache Cache, cacheTTL time.Duration) {
	q.tablePrefix = tableName
	q.middlewares = middlewares
	if cache != nil {
		q.cache = cache
		q.cacheTTL = cacheTTL
	}
}

func (m *Model[T]) softDeleteFilter() string {
	if m.softDeleteCol == "" {
		return ""
	}
	return fmt.Sprintf("%s IS NULL", m.softDeleteCol)
}

// applyBaseQuery injects the soft-delete IS NULL filter into a SQL string at
// the correct position: after all WHERE conditions but BEFORE any ORDER BY /
// GROUP BY / HAVING / LIMIT / OFFSET clauses.
func (m *Model[T]) applyBaseQuery(base string) string {
	return injectSoftDeleteFilter(base, m.softDeleteFilter())
}

// injectSoftDeleteFilter is the pure (model-free) helper that does the actual
// injection so it can be reused by QueryBuilder as well.
func injectSoftDeleteFilter(sql, filter string) string {
	if filter == "" {
		return sql
	}

	upper := strings.ToUpper(sql)

	// Find the earliest clause that must come AFTER the WHERE block.
	breakKeywords := []string{" ORDER BY ", " GROUP BY ", " HAVING ", " LIMIT ", " OFFSET "}
	insertPos := -1
	for _, kw := range breakKeywords {
		if idx := strings.Index(upper, kw); idx != -1 {
			if insertPos == -1 || idx < insertPos {
				insertPos = idx
			}
		}
	}

	if insertPos != -1 {
		// There is an ORDER BY / LIMIT / … clause – inject before it.
		before := sql[:insertPos]
		after := sql[insertPos:]
		if strings.Contains(strings.ToUpper(before), " WHERE ") {
			return before + " AND " + filter + after
		}
		return before + " WHERE " + filter + after
	}

	// No break keyword – append normally.
	if strings.Contains(upper, " WHERE ") {
		return sql + " AND " + filter
	}
	return sql + " WHERE " + filter
}

// READ OPERATIONS (use readConn)

func (m *Model[T]) Find(id string, columns ...string) Query[T] {
	cols := selectColumns(columns)
	base := fmt.Sprintf("SELECT %s FROM %s WHERE id = $1", cols, m.tableName)
	sql := m.applyBaseQuery(base)
	executor := func(ctx context.Context) (T, error) {
		return selectOne[T](ctx, m.readConn.DB(), sql, id)
	}

	q := newQuery(executor, sql, id)
	m.attachCache(q)
	return q
}

func (m *Model[T]) FindBy(field string, value interface{}, columns ...string) Query[T] {
	cols := selectColumns(columns)
	base := fmt.Sprintf("SELECT %s FROM %s WHERE %s = $1", cols, m.tableName, field)
	sql := m.applyBaseQuery(base)
	executor := func(ctx context.Context) (T, error) {
		return selectOne[T](ctx, m.readConn.DB(), sql, value)
	}

	q := newQuery(executor, sql, value)
	m.attachCache(q)
	return q
}

func (m *Model[T]) GetBy(conditions map[string]interface{}, columns ...string) Query[[]T] {
	cols := selectColumns(columns)
	base, args := m.buildWhereQuery(fmt.Sprintf("SELECT %s FROM %s", cols, m.tableName), conditions)
	sql := m.applyBaseQuery(base)
	executor := func(ctx context.Context) ([]T, error) {
		return selectMany[T](ctx, m.readConn.DB(), sql, args...)
	}

	q := newQuery(executor, sql, args...)
	attachQueryMeta(q, m.tableName, m.middlewares, m.cache, m.cacheTTL)
	return q
}

func (m *Model[T]) All(columns ...string) Query[[]T] {
	cols := selectColumns(columns)
	base := fmt.Sprintf("SELECT %s FROM %s", cols, m.tableName)
	sql := m.applyBaseQuery(base)
	executor := func(ctx context.Context) ([]T, error) {
		return selectMany[T](ctx, m.readConn.DB(), sql)
	}

	q := newQuery(executor, sql)
	attachQueryMeta(q, m.tableName, m.middlewares, m.cache, m.cacheTTL)
	return q
}

func (m *Model[T]) Count(ctx context.Context, conditions map[string]interface{}) (int, error) {
	base, args := m.buildWhereQuery(fmt.Sprintf("SELECT COUNT(*) FROM %s", m.tableName), conditions)
	sql := m.applyBaseQuery(base)
	var count int
	err := m.readConn.DB().GetContext(ctx, &count, sql, args...)
	return count, err
}

func (m *Model[T]) Exists(ctx context.Context, id string) (bool, error) {
	inner := fmt.Sprintf("SELECT 1 FROM %s WHERE id = $1", m.tableName)
	inner = m.applyBaseQuery(inner)
	sql := fmt.Sprintf("SELECT EXISTS(%s)", inner)
	var exists bool
	err := m.readConn.DB().GetContext(ctx, &exists, sql, id)
	return exists, err
}

// ExistsBy checks existence by arbitrary conditions
func (m *Model[T]) ExistsBy(ctx context.Context, conditions map[string]interface{}) (bool, error) {
	base, args := m.buildWhereQuery(fmt.Sprintf("SELECT 1 FROM %s", m.tableName), conditions)
	inner := m.applyBaseQuery(base)
	sql := fmt.Sprintf("SELECT EXISTS(%s)", inner)
	var exists bool
	err := m.readConn.DB().GetContext(ctx, &exists, sql, args...)
	return exists, err
}

func (m *Model[T]) Query() *QueryBuilder[T] {
	return NewQueryBuilder(m)
}

// QueryFromSQL creates a QueryBuilder pre-loaded with the given raw SQL and
// bound arguments.  Subsequent Where/OrderBy/Limit calls continue from where
// the raw SQL left off.
//
// Example:
//
//	qb := userModel.QueryFromSQL(
//	    "SELECT * FROM users WHERE tenant_id = $1", "tenant-abc")
//	qb.Where("status", "active").OrderBy("created_at", true)
func (m *Model[T]) QueryFromSQL(rawSQL string, args ...interface{}) *QueryBuilder[T] {
	return NewQueryBuilderFromSQL(m, rawSQL, args...)
}

// WRITE OPERATIONS (use writeConn)

// BeforeCreator is implemented by T to run logic before Create/Save inserts
type BeforeCreator interface{ BeforeCreate() error }

// AfterCreator is implemented by T to run logic after Create/Save inserts
type AfterCreator interface{ AfterCreate() error }

// BeforeUpdater is implemented by T to run logic before Update/UpdateFromStruct
type BeforeUpdater interface{ BeforeUpdate() error }

// AfterUpdater is implemented by T to run logic after Update/UpdateFromStruct
type AfterUpdater interface{ AfterUpdate() error }

// BeforeDeleter is implemented by T to run logic before Delete
type BeforeDeleter interface{ BeforeDelete() error }

// AfterDeleter is implemented by T to run logic after Delete
type AfterDeleter interface{ AfterDelete() error }

func (m *Model[T]) Create(ctx context.Context, data T) error {
	if h, ok := any(&data).(BeforeCreator); ok {
		if err := h.BeforeCreate(); err != nil {
			return err
		}
	}
	cols, placeholders := structInsertParts(data)
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", m.tableName, cols, placeholders)
	_, err := m.writeConn.DB().NamedExecContext(ctx, query, data)
	if err != nil {
		return err
	}
	if h, ok := any(&data).(AfterCreator); ok {
		_ = h.AfterCreate()
	}
	if m.cache != nil {
		m.invalidateCache(ctx)
	}
	return nil
}

func (m *Model[T]) CreateMany(ctx context.Context, data []T) error {
	if len(data) == 0 {
		return nil
	}
	cols, placeholders := structInsertParts(data[0])
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", m.tableName, cols, placeholders)
	_, err := m.writeConn.DB().NamedExecContext(ctx, query, data)
	if m.cache != nil {
		m.invalidateCache(ctx)
	}
	return err
}

// Save does INSERT ... ON CONFLICT (id) DO UPDATE SET ... for all db-tagged fields
func (m *Model[T]) Save(ctx context.Context, data T) error {
	if h, ok := any(&data).(BeforeCreator); ok {
		if err := h.BeforeCreate(); err != nil {
			return err
		}
	}
	cols, placeholders := structInsertParts(data)
	colList := strings.Split(cols, ", ")
	setClauses := make([]string, 0, len(colList))
	for _, c := range colList {
		if c != "id" {
			setClauses = append(setClauses, fmt.Sprintf("%s = EXCLUDED.%s", c, c))
		}
	}
	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (id) DO UPDATE SET %s",
		m.tableName, cols, placeholders, strings.Join(setClauses, ", "),
	)
	_, err := m.writeConn.DB().NamedExecContext(ctx, query, data)
	if err != nil {
		return err
	}
	if h, ok := any(&data).(AfterCreator); ok {
		_ = h.AfterCreate()
	}
	if m.cache != nil {
		m.invalidateCache(ctx)
	}
	return nil
}

// FindOrCreate returns existing record by field=value or creates it; returns (result, created, error)
func (m *Model[T]) FindOrCreate(ctx context.Context, field string, value interface{}, data T) (T, bool, error) {
	result, err := m.FindBy(field, value).Exec(ctx)
	if err == nil {
		return result, false, nil
	}
	if err := m.Create(ctx, data); err != nil {
		var zero T
		return zero, false, err
	}
	return data, true, nil
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

// UpdateFromStruct updates a record by id using db-tagged fields from a struct
func (m *Model[T]) UpdateFromStruct(ctx context.Context, id string, data T) error {
	if h, ok := any(&data).(BeforeUpdater); ok {
		if err := h.BeforeUpdate(); err != nil {
			return err
		}
	}
	t := reflect.TypeOf(data)
	v := reflect.ValueOf(data)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
		v = v.Elem()
	}
	setClauses := make([]string, 0, t.NumField())
	args := make([]interface{}, 0, t.NumField()+1)
	i := 1
	for idx := 0; idx < t.NumField(); idx++ {
		tag := t.Field(idx).Tag.Get("db")
		if tag == "" || tag == "-" || tag == "id" {
			continue
		}
		setClauses = append(setClauses, fmt.Sprintf("%s = $%d", tag, i))
		args = append(args, v.Field(idx).Interface())
		i++
	}
	if len(setClauses) == 0 {
		return fmt.Errorf("no fields to update")
	}
	args = append(args, id)
	sql := fmt.Sprintf("UPDATE %s SET %s WHERE id = $%d", m.tableName, strings.Join(setClauses, ", "), i)
	_, err := m.writeConn.DB().ExecContext(ctx, sql, args...)
	if err != nil {
		return err
	}
	if h, ok := any(&data).(AfterUpdater); ok {
		_ = h.AfterUpdate()
	}
	if m.cache != nil {
		m.invalidateCache(ctx)
	}
	return nil
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
	var err error
	if m.softDeleteCol != "" {
		sql := fmt.Sprintf("UPDATE %s SET %s = NOW() WHERE id = $1", m.tableName, m.softDeleteCol)
		_, err = m.writeConn.DB().ExecContext(ctx, sql, id)
	} else {
		sql := fmt.Sprintf("DELETE FROM %s WHERE id = $1", m.tableName)
		_, err = m.writeConn.DB().ExecContext(ctx, sql, id)
	}
	if m.cache != nil {
		m.invalidateCache(ctx)
	}
	return err
}

func (m *Model[T]) DeleteBy(ctx context.Context, conditions map[string]interface{}) error {
	var sql string
	var args []interface{}
	if m.softDeleteCol != "" {
		base, a := m.buildWhereQuery(fmt.Sprintf("UPDATE %s SET %s = NOW()", m.tableName, m.softDeleteCol), conditions)
		sql, args = base, a
	} else {
		sql, args = m.buildWhereQuery(fmt.Sprintf("DELETE FROM %s", m.tableName), conditions)
	}
	_, err := m.writeConn.DB().ExecContext(ctx, sql, args...)
	if m.cache != nil {
		m.invalidateCache(ctx)
	}
	return err
}

// Increment atomically increments a numeric column by delta
func (m *Model[T]) Increment(ctx context.Context, id string, column string, delta int) error {
	sql := fmt.Sprintf("UPDATE %s SET %s = %s + $1 WHERE id = $2", m.tableName, column, column)
	_, err := m.writeConn.DB().ExecContext(ctx, sql, delta, id)
	if m.cache != nil {
		m.invalidateCache(ctx)
	}
	return err
}

// Decrement atomically decrements a numeric column by delta
func (m *Model[T]) Decrement(ctx context.Context, id string, column string, delta int) error {
	return m.Increment(ctx, id, column, -delta)
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

// Pluck returns a slice of values for a single column
func (m *Model[T]) Pluck(ctx context.Context, column string, conditions map[string]interface{}) ([]interface{}, error) {
	base, args := m.buildWhereQuery(fmt.Sprintf("SELECT %s FROM %s", column, m.tableName), conditions)
	sql := m.applyBaseQuery(base)
	rows, err := m.readConn.DB().QueryContext(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []interface{}
	for rows.Next() {
		var val interface{}
		if err := rows.Scan(&val); err != nil {
			return nil, err
		}
		result = append(result, val)
	}
	return result, rows.Err()
}

// Chunk processes rows in batches of chunkSize, calling fn for each batch
func (m *Model[T]) Chunk(ctx context.Context, chunkSize int, conditions map[string]interface{}, fn func([]T) error) error {
	offset := 0
	var err error
	for {
		base, args := m.buildWhereQuery(fmt.Sprintf("SELECT * FROM %s", m.tableName), conditions)
		sql := m.applyBaseQuery(base)
		sql += fmt.Sprintf(" LIMIT %d OFFSET %d", chunkSize, offset)
		var batch []T
		batch, err = selectMany[T](ctx, m.readConn.DB(), sql, args...)
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			return nil
		}
		if err := fn(batch); err != nil {
			return err
		}
		if len(batch) < chunkSize {
			return nil
		}
		offset += chunkSize
	}
}

// Raw executes a raw SQL query and returns typed results
func (m *Model[T]) Raw(ctx context.Context, sql string, args ...interface{}) ([]T, error) {
	return selectMany[T](ctx, m.readConn.DB(), sql, args...)
}

// PAGINATION

// Page is a generic paginated result.
// T is the row type (can differ from the Model's T when using PaginateAs).
type Page[T any] struct {
	Items      []T
	Total      int
	Page       int
	PageSize   int
	TotalPages int
}

// HasNext reports whether there is a page after the current one.
func (p *Page[T]) HasNext() bool {
	return p.Page < p.TotalPages
}

// HasPrev reports whether there is a page before the current one.
func (p *Page[T]) HasPrev() bool {
	return p.Page > 1
}

// NextPage returns the next page number, or the current page if already on the last.
func (p *Page[T]) NextPage() int {
	if p.HasNext() {
		return p.Page + 1
	}
	return p.Page
}

// PrevPage returns the previous page number, or the current page if already on the first.
func (p *Page[T]) PrevPage() int {
	if p.HasPrev() {
		return p.Page - 1
	}
	return p.Page
}

// Paginatable[T] is satisfied by both *QueryBuilder[T] and *RawQuery[T].
// Any type that can provide a base SQL string, bound args, a soft-delete
// filter, and a table name can drive Paginate / PaginateAs.
type Paginatable[T any] interface {
	// paginatableSQL returns the fully-built SQL (without LIMIT/OFFSET).
	paginatableSQL() string
	// paginatableArgs returns the bound arguments for the SQL.
	paginatableArgs() []interface{}
	// paginatableTableName returns the table name (used for count alias).
	paginatableTableName() string
}

// ── *QueryBuilder[T] satisfies Paginatable[T] ─────────────────────────────

func (qb *QueryBuilder[T]) paginatableSQL() string {
	sql := qb.query.String()
	if !qb.withTrashed {
		sql = injectSoftDeleteFilter(sql, qb.model.softDeleteFilter())
	}
	return sql
}

func (qb *QueryBuilder[T]) paginatableArgs() []interface{} { return qb.args }

func (qb *QueryBuilder[T]) paginatableTableName() string { return qb.model.tableName }

// ── RawQuery[T] – raw SQL paginatable ──────────────────────────────────────

// RawQuery[T] wraps a hand-written SQL string so it can be passed to
// Paginate / PaginateAs.  Create one with NewRawQuery.
//
//	page, err := model.Paginate(ctx, 1, 20,
//	    dbconnector.NewRawQuery[User]("users",
//	        "SELECT * FROM users WHERE status = $1 ORDER BY created_at DESC",
//	        "active"))
type RawQuery[T any] struct {
	tableName string
	sql       string
	args      []interface{}
}

// NewRawQuery creates a Paginatable from a raw SQL string.
// tableName is only used for the COUNT subquery alias; it does not have to
// match any real table if the SQL already has its own FROM clause.
func NewRawQuery[T any](tableName, sql string, args ...interface{}) *RawQuery[T] {
	return &RawQuery[T]{tableName: tableName, sql: sql, args: args}
}

func (r *RawQuery[T]) paginatableSQL() string         { return r.sql }
func (r *RawQuery[T]) paginatableArgs() []interface{} { return r.args }
func (r *RawQuery[T]) paginatableTableName() string   { return r.tableName }

// ── Paginate & PaginateAs ──────────────────────────────────────────────────

// Paginate runs pagination over any Paginatable[T] source (a *QueryBuilder[T]
// or a *RawQuery[T]) and returns a typed Page[T].
//
// Example with QueryBuilder:
//
//	page, err := userModel.Paginate(ctx, 1, 20,
//	    userModel.Query().Where("status", "active").OrderBy("created_at", true))
//
// Example with raw SQL:
//
//	page, err := userModel.Paginate(ctx, 1, 20,
//	    dbconnector.NewRawQuery[User]("users",
//	        "SELECT * FROM users WHERE active = $1", true))
func (m *Model[T]) Paginate(ctx context.Context, page, pageSize int, src Paginatable[T]) (*Page[T], error) {
	return paginateCore[T, T](ctx, m.readConn, page, pageSize, src)
}

// PaginateAs is a free generic function that paginates any Paginatable[T]
// source but scans results into a different struct R.
// Useful for projections / DTOs.
//
// Example:
//
//	type UserDTO struct { ID string `db:"id"`; Name string `db:"name"` }
//	page, err := dbconnector.PaginateAs[User, UserDTO](ctx, conn, 1, 20,
//	    model.Query().Select("id", "name").Where("active", true))
func PaginateAs[T any, R any](ctx context.Context, conn Connection, page, pageSize int, src Paginatable[T]) (*Page[R], error) {
	return paginateCore[T, R](ctx, conn, page, pageSize, src)
}

// paginateCore is the shared implementation used by both Paginate and PaginateAs.
func paginateCore[T any, R any](ctx context.Context, conn Connection, page, pageSize int, src Paginatable[T]) (*Page[R], error) {
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 10
	}

	args := src.paginatableArgs()

	// Full SQL already has the soft-delete filter injected at the right position.
	fullSql := src.paginatableSQL()

	// Count query: rewrite SELECT … → SELECT COUNT(*) FROM …, strip ORDER BY/LIMIT/OFFSET.
	countSql := buildCountSQL(fullSql, src.paginatableTableName())
	var total int
	if err := conn.DB().GetContext(ctx, &total, countSql, args...); err != nil {
		return nil, err
	}

	// Data query: strip any pre-existing LIMIT/OFFSET, then append ours.
	dataSqlBase := stripLimitOffset(fullSql)
	offset := (page - 1) * pageSize
	dataSql := fmt.Sprintf("%s LIMIT %d OFFSET %d", dataSqlBase, pageSize, offset)

	items, itemsErr := selectMany[R](ctx, conn.DB(), dataSql, args...)
	if itemsErr != nil {
		return nil, itemsErr
	}

	totalPages := 0
	if total > 0 {
		totalPages = (total + pageSize - 1) / pageSize
	}

	return &Page[R]{
		Items:      items,
		Total:      total,
		Page:       page,
		PageSize:   pageSize,
		TotalPages: totalPages,
	}, nil
}

// buildCountSQL converts a SELECT … FROM … query into SELECT COUNT(*) FROM …
// by extracting only the FROM … WHERE … part and stripping ORDER BY / LIMIT / OFFSET.
// tableName is used as the alias for the inner subquery when no FROM clause can
// be found in the SQL (exotic shapes such as VALUES lists).
func buildCountSQL(sql, tableName string) string {
	upper := strings.ToUpper(sql)

	// Find the FROM position in the original SQL.
	fromIdx := strings.Index(upper, " FROM ")
	if fromIdx == -1 {
		// Fallback: wrap in a named subquery using the table name as alias.
		return fmt.Sprintf("SELECT COUNT(*) FROM (%s) AS %s_cnt", stripOrderByLimitOffset(sql), tableName)
	}

	// Take everything from FROM onwards, then strip ORDER BY/LIMIT/OFFSET.
	fromPart := sql[fromIdx:] // " FROM table WHERE ..."
	fromPart = stripOrderByLimitOffset(fromPart)
	return "SELECT COUNT(*)" + fromPart
}

// stripLimitOffset removes trailing LIMIT … and OFFSET … clauses from a SQL string.
// It leaves ORDER BY in place (needed for pagination result ordering).
func stripLimitOffset(sql string) string {
	upper := strings.ToUpper(sql)
	// Work from the back: remove OFFSET first, then LIMIT.
	if idx := strings.LastIndex(upper, " OFFSET "); idx != -1 {
		sql = sql[:idx]
		upper = upper[:idx]
	}
	if idx := strings.LastIndex(upper, " LIMIT "); idx != -1 {
		sql = sql[:idx]
	}
	return sql
}

// stripOrderByLimitOffset removes ORDER BY, LIMIT, and OFFSET clauses for count queries.
func stripOrderByLimitOffset(sql string) string {
	upper := strings.ToUpper(sql)
	// Strip in reverse order of typical appearance.
	for _, kw := range []string{" OFFSET ", " LIMIT ", " ORDER BY "} {
		if idx := strings.LastIndex(upper, kw); idx != -1 {
			sql = sql[:idx]
			upper = upper[:idx]
		}
	}
	return sql
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
	// Delete all cache keys for this table using the "prefix*" pattern supported
	// by both InMemoryCache and RedisCache implementations.
	_ = m.cache.Delete(ctx, m.tableName+":*")
}

// DB returns the underlying read database connection
func (m *Model[T]) DB() *sqlx.DB {
	return m.readConn.DB()
}

// selectColumns returns "*" or a comma-joined column list
func selectColumns(columns []string) string {
	if len(columns) == 0 {
		return "*"
	}
	return strings.Join(columns, ", ")
}

// structInsertParts extracts db-tagged field names and named placeholders from a struct
func structInsertParts(v any) (cols string, placeholders string) {
	t := reflect.TypeOf(v)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	var colList, phList []string
	for i := 0; i < t.NumField(); i++ {
		tag := t.Field(i).Tag.Get("db")
		if tag == "" || tag == "-" {
			continue
		}
		colList = append(colList, tag)
		phList = append(phList, ":"+tag)
	}
	return strings.Join(colList, ", "), strings.Join(phList, ", ")
}
