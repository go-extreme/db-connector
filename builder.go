package dbconnector

import (
	"context"
	"fmt"
	"strings"
	"time"
)

type QueryBuilder[T any] struct {
	model       *Model[T]
	query       strings.Builder
	args        []interface{}
	whereAdded  bool
	withTrashed bool
	// whereEnd tracks where the WHERE block ends so we can inject
	// the soft-delete filter in the right position inside Build().
	// It is updated every time a WHERE condition is appended.
	whereEnd int
}

func NewQueryBuilder[T any](model *Model[T]) *QueryBuilder[T] {
	qb := &QueryBuilder[T]{model: model}
	qb.query.WriteString("SELECT * FROM ")
	qb.query.WriteString(model.tableName)
	return qb
}

// NewQueryBuilderFromSQL creates a QueryBuilder pre-loaded with an existing
// SQL string and its bound arguments.  This is useful when you already have a
// hand-written or generated SQL statement and want to keep adding clauses
// (extra WHERE conditions, ORDER BY, LIMIT, etc.) using the fluent API.
//
// The function inspects the SQL to set internal state correctly:
//   - whereAdded is true when the SQL already contains a WHERE clause.
//   - args are stored so that subsequent Where* calls continue numbering
//     from len(args)+1 (i.e. $len(args)+1, $len(args)+2, …).
//
// Example:
//
//	qb := dbconnector.NewQueryBuilderFromSQL(model,
//	    "SELECT * FROM users WHERE tenant_id = $1", "tenant-abc")
//	qb.Where("status", "active").OrderBy("created_at", true)
//	// → SELECT * FROM users WHERE tenant_id = $1 AND status = $2 ORDER BY created_at DESC
func NewQueryBuilderFromSQL[T any](model *Model[T], rawSQL string, args ...interface{}) *QueryBuilder[T] {
	qb := &QueryBuilder[T]{
		model: model,
		args:  make([]interface{}, len(args)),
	}
	copy(qb.args, args)
	qb.query.WriteString(rawSQL)

	// Detect whether a WHERE clause is already present so the next Where*
	// call emits "AND …" instead of "WHERE …".
	upper := strings.ToUpper(rawSQL)
	qb.whereAdded = strings.Contains(upper, " WHERE ")

	// whereEnd tracks the byte position after the last WHERE condition so
	// Build() can inject the soft-delete filter in the right place.
	// For a freshly-parsed SQL we conservatively set it to the full length;
	// injectSoftDeleteFilter will still find the right insertion point by
	// scanning for ORDER BY / LIMIT / OFFSET keywords.
	qb.whereEnd = qb.query.Len()

	return qb
}

// WithTrashed includes soft-deleted rows in the query
func (qb *QueryBuilder[T]) WithTrashed() *QueryBuilder[T] {
	qb.withTrashed = true
	return qb
}

// Select overrides the selected columns (must be called before any Where/Order/etc.)
func (qb *QueryBuilder[T]) Select(columns ...string) *QueryBuilder[T] {
	qb.query.Reset()
	qb.query.WriteString("SELECT ")
	if len(columns) == 0 {
		qb.query.WriteString("*")
	} else {
		qb.query.WriteString(strings.Join(columns, ", "))
	}
	qb.query.WriteString(" FROM ")
	qb.query.WriteString(qb.model.tableName)
	return qb
}

// SelectRaw sets a completely custom SELECT expression.
// Example: qb.SelectRaw("id, COUNT(*) as cnt")
func (qb *QueryBuilder[T]) SelectRaw(expr string) *QueryBuilder[T] {
	qb.query.Reset()
	qb.query.WriteString("SELECT ")
	qb.query.WriteString(expr)
	qb.query.WriteString(" FROM ")
	qb.query.WriteString(qb.model.tableName)
	return qb
}

// Join adds an INNER JOIN clause.
func (qb *QueryBuilder[T]) Join(table, condition string) *QueryBuilder[T] {
	qb.query.WriteString(fmt.Sprintf(" JOIN %s ON %s", table, condition))
	return qb
}

// LeftJoin adds a LEFT JOIN clause.
func (qb *QueryBuilder[T]) LeftJoin(table, condition string) *QueryBuilder[T] {
	qb.query.WriteString(fmt.Sprintf(" LEFT JOIN %s ON %s", table, condition))
	return qb
}

// RightJoin adds a RIGHT JOIN clause.
func (qb *QueryBuilder[T]) RightJoin(table, condition string) *QueryBuilder[T] {
	qb.query.WriteString(fmt.Sprintf(" RIGHT JOIN %s ON %s", table, condition))
	return qb
}

// addWhereConjunction writes "WHERE" or "AND" as needed and marks the position
// right after the conjunction so we know where the WHERE block currently ends.
func (qb *QueryBuilder[T]) addWhereConjunction() {
	if !qb.whereAdded {
		qb.query.WriteString(" WHERE ")
		qb.whereAdded = true
	} else {
		qb.query.WriteString(" AND ")
	}
}

// Where adds a WHERE condition with = operator
func (qb *QueryBuilder[T]) Where(field string, value interface{}) *QueryBuilder[T] {
	qb.addWhereConjunction()
	qb.args = append(qb.args, value)
	qb.query.WriteString(fmt.Sprintf("%s = $%d", field, len(qb.args)))
	qb.whereEnd = qb.query.Len()
	return qb
}

// WhereNot adds a WHERE condition with != operator
func (qb *QueryBuilder[T]) WhereNot(field string, value interface{}) *QueryBuilder[T] {
	qb.addWhereConjunction()
	qb.args = append(qb.args, value)
	qb.query.WriteString(fmt.Sprintf("%s != $%d", field, len(qb.args)))
	qb.whereEnd = qb.query.Len()
	return qb
}

// WhereRaw adds a raw WHERE snippet with optional bound arguments.
// Example: qb.WhereRaw("created_at > $? AND status = $?", time.Now(), "active")
// The "?" placeholders are replaced with the correct $N indices automatically.
func (qb *QueryBuilder[T]) WhereRaw(rawExpr string, values ...interface{}) *QueryBuilder[T] {
	qb.addWhereConjunction()
	// Replace each "?" with "$N" in order
	expr := rawExpr
	for _, v := range values {
		qb.args = append(qb.args, v)
		expr = strings.Replace(expr, "?", fmt.Sprintf("$%d", len(qb.args)), 1)
	}
	qb.query.WriteString(expr)
	qb.whereEnd = qb.query.Len()
	return qb
}

// OrWhere starts an OR branch (wraps the given fn in parentheses joined by OR).
func (qb *QueryBuilder[T]) OrWhere(fn func(*QueryBuilder[T])) *QueryBuilder[T] {
	if qb.whereAdded {
		qb.query.WriteString(" OR (")
	} else {
		qb.query.WriteString(" WHERE (")
		qb.whereAdded = true
	}

	sub := &QueryBuilder[T]{model: qb.model, args: qb.args, whereAdded: false}
	fn(sub)

	subQuery := sub.query.String()
	subQuery = strings.TrimPrefix(subQuery, " WHERE ")
	qb.query.WriteString(subQuery)
	qb.query.WriteString(")")
	qb.args = sub.args
	qb.whereEnd = qb.query.Len()
	return qb
}

// WhereIn adds a WHERE IN condition
func (qb *QueryBuilder[T]) WhereIn(field string, values []interface{}) *QueryBuilder[T] {
	if len(values) == 0 {
		return qb
	}
	qb.addWhereConjunction()
	placeholders := make([]string, len(values))
	for i, v := range values {
		qb.args = append(qb.args, v)
		placeholders[i] = fmt.Sprintf("$%d", len(qb.args))
	}
	qb.query.WriteString(fmt.Sprintf("%s IN (%s)", field, strings.Join(placeholders, ",")))
	qb.whereEnd = qb.query.Len()
	return qb
}

// WhereNotIn adds a WHERE NOT IN condition
func (qb *QueryBuilder[T]) WhereNotIn(field string, values []interface{}) *QueryBuilder[T] {
	if len(values) == 0 {
		return qb
	}
	qb.addWhereConjunction()
	placeholders := make([]string, len(values))
	for i, v := range values {
		qb.args = append(qb.args, v)
		placeholders[i] = fmt.Sprintf("$%d", len(qb.args))
	}
	qb.query.WriteString(fmt.Sprintf("%s NOT IN (%s)", field, strings.Join(placeholders, ",")))
	qb.whereEnd = qb.query.Len()
	return qb
}

// WhereLike adds a WHERE LIKE condition
func (qb *QueryBuilder[T]) WhereLike(field string, pattern string) *QueryBuilder[T] {
	qb.addWhereConjunction()
	qb.args = append(qb.args, pattern)
	qb.query.WriteString(fmt.Sprintf("%s LIKE $%d", field, len(qb.args)))
	qb.whereEnd = qb.query.Len()
	return qb
}

// WhereILike adds a case-insensitive WHERE ILIKE condition (PostgreSQL).
func (qb *QueryBuilder[T]) WhereILike(field string, pattern string) *QueryBuilder[T] {
	qb.addWhereConjunction()
	qb.args = append(qb.args, pattern)
	qb.query.WriteString(fmt.Sprintf("%s ILIKE $%d", field, len(qb.args)))
	qb.whereEnd = qb.query.Len()
	return qb
}

// WhereNull adds a WHERE IS NULL condition
func (qb *QueryBuilder[T]) WhereNull(field string) *QueryBuilder[T] {
	qb.addWhereConjunction()
	qb.query.WriteString(fmt.Sprintf("%s IS NULL", field))
	qb.whereEnd = qb.query.Len()
	return qb
}

// WhereNotNull adds a WHERE IS NOT NULL condition
func (qb *QueryBuilder[T]) WhereNotNull(field string) *QueryBuilder[T] {
	qb.addWhereConjunction()
	qb.query.WriteString(fmt.Sprintf("%s IS NOT NULL", field))
	qb.whereEnd = qb.query.Len()
	return qb
}

// WhereBetween adds a WHERE BETWEEN condition
func (qb *QueryBuilder[T]) WhereBetween(field string, start, end interface{}) *QueryBuilder[T] {
	qb.addWhereConjunction()
	qb.args = append(qb.args, start, end)
	qb.query.WriteString(fmt.Sprintf("%s BETWEEN $%d AND $%d", field, len(qb.args)-1, len(qb.args)))
	qb.whereEnd = qb.query.Len()
	return qb
}

// WhereDate filters rows where the DATE() part of a timestamp column equals the given time.
// The comparison is done using date arithmetic: column >= date AND column < date+1day.
func (qb *QueryBuilder[T]) WhereDate(field string, t time.Time) *QueryBuilder[T] {
	day := t.Truncate(24 * time.Hour)
	nextDay := day.Add(24 * time.Hour)
	qb.addWhereConjunction()
	qb.args = append(qb.args, day, nextDay)
	qb.query.WriteString(fmt.Sprintf("%s >= $%d AND %s < $%d", field, len(qb.args)-1, field, len(qb.args)))
	qb.whereEnd = qb.query.Len()
	return qb
}

// WhereDateBetween filters rows where the timestamp column falls within [from, to] (inclusive day range).
func (qb *QueryBuilder[T]) WhereDateBetween(field string, from, to time.Time) *QueryBuilder[T] {
	start := from.Truncate(24 * time.Hour)
	end := to.Truncate(24 * time.Hour).Add(24 * time.Hour)
	qb.addWhereConjunction()
	qb.args = append(qb.args, start, end)
	qb.query.WriteString(fmt.Sprintf("%s >= $%d AND %s < $%d", field, len(qb.args)-1, field, len(qb.args)))
	qb.whereEnd = qb.query.Len()
	return qb
}

// WhereTimestampNull filters rows where a nullable timestamp column IS NULL.
// This is an alias for WhereNull but makes intent explicit for timestamp columns.
func (qb *QueryBuilder[T]) WhereTimestampNull(field string) *QueryBuilder[T] {
	return qb.WhereNull(field)
}

// WhereTimestampNotNull filters rows where a nullable timestamp column IS NOT NULL.
func (qb *QueryBuilder[T]) WhereTimestampNotNull(field string) *QueryBuilder[T] {
	return qb.WhereNotNull(field)
}

// WhereGreaterThan adds a WHERE > condition
func (qb *QueryBuilder[T]) WhereGreaterThan(field string, value interface{}) *QueryBuilder[T] {
	qb.addWhereConjunction()
	qb.args = append(qb.args, value)
	qb.query.WriteString(fmt.Sprintf("%s > $%d", field, len(qb.args)))
	qb.whereEnd = qb.query.Len()
	return qb
}

// WhereGreaterThanOrEqual adds a WHERE >= condition
func (qb *QueryBuilder[T]) WhereGreaterThanOrEqual(field string, value interface{}) *QueryBuilder[T] {
	qb.addWhereConjunction()
	qb.args = append(qb.args, value)
	qb.query.WriteString(fmt.Sprintf("%s >= $%d", field, len(qb.args)))
	qb.whereEnd = qb.query.Len()
	return qb
}

// WhereLessThan adds a WHERE < condition
func (qb *QueryBuilder[T]) WhereLessThan(field string, value interface{}) *QueryBuilder[T] {
	qb.addWhereConjunction()
	qb.args = append(qb.args, value)
	qb.query.WriteString(fmt.Sprintf("%s < $%d", field, len(qb.args)))
	qb.whereEnd = qb.query.Len()
	return qb
}

// WhereLessThanOrEqual adds a WHERE <= condition
func (qb *QueryBuilder[T]) WhereLessThanOrEqual(field string, value interface{}) *QueryBuilder[T] {
	qb.addWhereConjunction()
	qb.args = append(qb.args, value)
	qb.query.WriteString(fmt.Sprintf("%s <= $%d", field, len(qb.args)))
	qb.whereEnd = qb.query.Len()
	return qb
}

// WhereExists adds a WHERE EXISTS (subquery) condition.
// The subquery string may contain "?" placeholders; values are bound in order.
func (qb *QueryBuilder[T]) WhereExists(subquery string, values ...interface{}) *QueryBuilder[T] {
	qb.addWhereConjunction()
	expr := subquery
	for _, v := range values {
		qb.args = append(qb.args, v)
		expr = strings.Replace(expr, "?", fmt.Sprintf("$%d", len(qb.args)), 1)
	}
	qb.query.WriteString(fmt.Sprintf("EXISTS (%s)", expr))
	qb.whereEnd = qb.query.Len()
	return qb
}

// WhereNotExists adds a WHERE NOT EXISTS (subquery) condition.
func (qb *QueryBuilder[T]) WhereNotExists(subquery string, values ...interface{}) *QueryBuilder[T] {
	qb.addWhereConjunction()
	expr := subquery
	for _, v := range values {
		qb.args = append(qb.args, v)
		expr = strings.Replace(expr, "?", fmt.Sprintf("$%d", len(qb.args)), 1)
	}
	qb.query.WriteString(fmt.Sprintf("NOT EXISTS (%s)", expr))
	qb.whereEnd = qb.query.Len()
	return qb
}

// Or starts an OR condition group
func (qb *QueryBuilder[T]) Or(fn func(*QueryBuilder[T])) *QueryBuilder[T] {
	if qb.whereAdded {
		qb.query.WriteString(" OR (")
	} else {
		qb.query.WriteString(" WHERE (")
		qb.whereAdded = true
	}

	// Create a sub-builder that shares args but has its own query state
	subBuilder := &QueryBuilder[T]{
		model:      qb.model,
		args:       qb.args,
		whereAdded: false,
	}

	// Execute the callback
	fn(subBuilder)

	// Extract only the WHERE conditions (strip " WHERE " prefix if present)
	subQuery := subBuilder.query.String()
	subQuery = strings.TrimPrefix(subQuery, " WHERE ")

	// Append the sub-query conditions
	qb.query.WriteString(subQuery)
	qb.query.WriteString(")")

	// Update args from sub-builder
	qb.args = subBuilder.args
	qb.whereEnd = qb.query.Len()

	return qb
}

// OrderBy adds an ORDER BY clause
func (qb *QueryBuilder[T]) OrderBy(field string, desc bool) *QueryBuilder[T] {
	qb.query.WriteString(fmt.Sprintf(" ORDER BY %s", field))
	if desc {
		qb.query.WriteString(" DESC")
	}
	return qb
}

// OrderByMultiple adds an ORDER BY clause with multiple fields.
// fields should be like []string{"name ASC", "created_at DESC"}.
func (qb *QueryBuilder[T]) OrderByMultiple(fields ...string) *QueryBuilder[T] {
	qb.query.WriteString(" ORDER BY ")
	qb.query.WriteString(strings.Join(fields, ", "))
	return qb
}

// GroupBy adds a GROUP BY clause
func (qb *QueryBuilder[T]) GroupBy(fields ...string) *QueryBuilder[T] {
	qb.query.WriteString(" GROUP BY ")
	qb.query.WriteString(strings.Join(fields, ", "))
	return qb
}

// Having adds a HAVING clause
func (qb *QueryBuilder[T]) Having(condition string, value interface{}) *QueryBuilder[T] {
	qb.args = append(qb.args, value)
	qb.query.WriteString(fmt.Sprintf(" HAVING %s $%d", condition, len(qb.args)))
	return qb
}

// Limit adds a LIMIT clause using a literal integer (not a bound parameter)
// so it is safe for use inside paginate count subqueries.
func (qb *QueryBuilder[T]) Limit(limit int) *QueryBuilder[T] {
	qb.query.WriteString(fmt.Sprintf(" LIMIT %d", limit))
	return qb
}

// Offset adds an OFFSET clause using a literal integer (not a bound parameter).
func (qb *QueryBuilder[T]) Offset(offset int) *QueryBuilder[T] {
	qb.query.WriteString(fmt.Sprintf(" OFFSET %d", offset))
	return qb
}

// Clone returns a deep copy of this QueryBuilder so you can branch queries.
func (qb *QueryBuilder[T]) Clone() *QueryBuilder[T] {
	clone := &QueryBuilder[T]{
		model:       qb.model,
		args:        make([]interface{}, len(qb.args)),
		whereAdded:  qb.whereAdded,
		withTrashed: qb.withTrashed,
		whereEnd:    qb.whereEnd,
	}
	copy(clone.args, qb.args)
	clone.query.WriteString(qb.query.String())
	return clone
}

// SQL returns the generated SQL query string (with $N placeholders).
func (qb *QueryBuilder[T]) SQL() string {
	return qb.query.String()
}

// ToSQL returns the SQL string with all $N placeholders replaced by their
// actual bound values — useful for debugging and logging.
// WARNING: do NOT use the result as an actual query; it is for display only.
func (qb *QueryBuilder[T]) ToSQL() string {
	return InterpolateSQL(qb.query.String(), qb.args...)
}

// Args returns the query arguments
func (qb *QueryBuilder[T]) Args() []interface{} {
	return qb.args
}

// InterpolateSQL replaces $1, $2, … placeholders in a SQL string with their
// corresponding argument values.  The result is for display/logging only.
func InterpolateSQL(sql string, args ...interface{}) string {
	result := sql
	// Replace from highest index down so "$10" isn't partially replaced by "$1".
	for i := len(args); i >= 1; i-- {
		placeholder := fmt.Sprintf("$%d", i)
		result = strings.ReplaceAll(result, placeholder, formatArg(args[i-1]))
	}
	return result
}

// formatArg converts a single argument to a SQL-literal-ish string for display.
func formatArg(v interface{}) string {
	if v == nil {
		return "NULL"
	}
	switch val := v.(type) {
	case string:
		escaped := strings.ReplaceAll(val, "'", "''")
		return fmt.Sprintf("'%s'", escaped)
	case []byte:
		escaped := strings.ReplaceAll(string(val), "'", "''")
		return fmt.Sprintf("'%s'", escaped)
	case bool:
		if val {
			return "true"
		}
		return "false"
	case time.Time:
		return fmt.Sprintf("'%s'", val.Format(time.RFC3339Nano))
	case *time.Time:
		if val == nil {
			return "NULL"
		}
		return fmt.Sprintf("'%s'", val.Format(time.RFC3339Nano))
	default:
		return fmt.Sprintf("%v", v)
	}
}

// Build finalizes the query, injects the soft-delete filter at the correct
// position (before any ORDER BY / LIMIT / OFFSET), and returns an executable Query.
func (qb *QueryBuilder[T]) Build() Query[[]T] {
	sql := qb.query.String()
	if !qb.withTrashed {
		sql = injectSoftDeleteFilter(sql, qb.model.softDeleteFilter())
	}
	args := qb.args

	executor := func(ctx context.Context) ([]T, error) {
		return selectMany[T](ctx, qb.model.readConn.DB(), sql, args...)
	}

	q := newQuery(executor, sql, args...)
	attachQueryMeta(q, qb.model.tableName, qb.model.middlewares, qb.model.cache, qb.model.cacheTTL)
	return q
}
