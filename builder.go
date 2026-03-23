package dbconnector

import (
	"context"
	"fmt"
	"strings"
)

type QueryBuilder[T any] struct {
	model       *Model[T]
	query       strings.Builder
	args        []interface{}
	whereAdded  bool
	withTrashed bool
}

func NewQueryBuilder[T any](model *Model[T]) *QueryBuilder[T] {
	qb := &QueryBuilder[T]{model: model}
	qb.query.WriteString("SELECT * FROM ")
	qb.query.WriteString(model.tableName)
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

// Where adds a WHERE condition with = operator
func (qb *QueryBuilder[T]) Where(field string, value interface{}) *QueryBuilder[T] {
	if !qb.whereAdded {
		qb.query.WriteString(" WHERE ")
		qb.whereAdded = true
	} else {
		qb.query.WriteString(" AND ")
	}
	qb.args = append(qb.args, value)
	qb.query.WriteString(fmt.Sprintf("%s = $%d", field, len(qb.args)))
	return qb
}

// WhereNot adds a WHERE condition with != operator
func (qb *QueryBuilder[T]) WhereNot(field string, value interface{}) *QueryBuilder[T] {
	if !qb.whereAdded {
		qb.query.WriteString(" WHERE ")
		qb.whereAdded = true
	} else {
		qb.query.WriteString(" AND ")
	}
	qb.args = append(qb.args, value)
	qb.query.WriteString(fmt.Sprintf("%s != $%d", field, len(qb.args)))
	return qb
}

// WhereIn adds a WHERE IN condition
func (qb *QueryBuilder[T]) WhereIn(field string, values []interface{}) *QueryBuilder[T] {
	if len(values) == 0 {
		return qb
	}

	if !qb.whereAdded {
		qb.query.WriteString(" WHERE ")
		qb.whereAdded = true
	} else {
		qb.query.WriteString(" AND ")
	}

	placeholders := make([]string, len(values))
	for i, v := range values {
		qb.args = append(qb.args, v)
		placeholders[i] = fmt.Sprintf("$%d", len(qb.args))
	}

	qb.query.WriteString(fmt.Sprintf("%s IN (%s)", field, strings.Join(placeholders, ",")))
	return qb
}

// WhereNotIn adds a WHERE NOT IN condition
func (qb *QueryBuilder[T]) WhereNotIn(field string, values []interface{}) *QueryBuilder[T] {
	if len(values) == 0 {
		return qb
	}

	if !qb.whereAdded {
		qb.query.WriteString(" WHERE ")
		qb.whereAdded = true
	} else {
		qb.query.WriteString(" AND ")
	}

	placeholders := make([]string, len(values))
	for i, v := range values {
		qb.args = append(qb.args, v)
		placeholders[i] = fmt.Sprintf("$%d", len(qb.args))
	}

	qb.query.WriteString(fmt.Sprintf("%s NOT IN (%s)", field, strings.Join(placeholders, ",")))
	return qb
}

// WhereLike adds a WHERE LIKE condition
func (qb *QueryBuilder[T]) WhereLike(field string, pattern string) *QueryBuilder[T] {
	if !qb.whereAdded {
		qb.query.WriteString(" WHERE ")
		qb.whereAdded = true
	} else {
		qb.query.WriteString(" AND ")
	}
	qb.args = append(qb.args, pattern)
	qb.query.WriteString(fmt.Sprintf("%s LIKE $%d", field, len(qb.args)))
	return qb
}

// WhereNull adds a WHERE IS NULL condition
func (qb *QueryBuilder[T]) WhereNull(field string) *QueryBuilder[T] {
	if !qb.whereAdded {
		qb.query.WriteString(" WHERE ")
		qb.whereAdded = true
	} else {
		qb.query.WriteString(" AND ")
	}
	qb.query.WriteString(fmt.Sprintf("%s IS NULL", field))
	return qb
}

// WhereNotNull adds a WHERE IS NOT NULL condition
func (qb *QueryBuilder[T]) WhereNotNull(field string) *QueryBuilder[T] {
	if !qb.whereAdded {
		qb.query.WriteString(" WHERE ")
		qb.whereAdded = true
	} else {
		qb.query.WriteString(" AND ")
	}
	qb.query.WriteString(fmt.Sprintf("%s IS NOT NULL", field))
	return qb
}

// WhereBetween adds a WHERE BETWEEN condition
func (qb *QueryBuilder[T]) WhereBetween(field string, start, end interface{}) *QueryBuilder[T] {
	if !qb.whereAdded {
		qb.query.WriteString(" WHERE ")
		qb.whereAdded = true
	} else {
		qb.query.WriteString(" AND ")
	}
	qb.args = append(qb.args, start, end)
	qb.query.WriteString(fmt.Sprintf("%s BETWEEN $%d AND $%d", field, len(qb.args)-1, len(qb.args)))
	return qb
}

// WhereGreaterThan adds a WHERE > condition
func (qb *QueryBuilder[T]) WhereGreaterThan(field string, value interface{}) *QueryBuilder[T] {
	if !qb.whereAdded {
		qb.query.WriteString(" WHERE ")
		qb.whereAdded = true
	} else {
		qb.query.WriteString(" AND ")
	}
	qb.args = append(qb.args, value)
	qb.query.WriteString(fmt.Sprintf("%s > $%d", field, len(qb.args)))
	return qb
}

// WhereGreaterThanOrEqual adds a WHERE >= condition
func (qb *QueryBuilder[T]) WhereGreaterThanOrEqual(field string, value interface{}) *QueryBuilder[T] {
	if !qb.whereAdded {
		qb.query.WriteString(" WHERE ")
		qb.whereAdded = true
	} else {
		qb.query.WriteString(" AND ")
	}
	qb.args = append(qb.args, value)
	qb.query.WriteString(fmt.Sprintf("%s >= $%d", field, len(qb.args)))
	return qb
}

// WhereLessThan adds a WHERE < condition
func (qb *QueryBuilder[T]) WhereLessThan(field string, value interface{}) *QueryBuilder[T] {
	if !qb.whereAdded {
		qb.query.WriteString(" WHERE ")
		qb.whereAdded = true
	} else {
		qb.query.WriteString(" AND ")
	}
	qb.args = append(qb.args, value)
	qb.query.WriteString(fmt.Sprintf("%s < $%d", field, len(qb.args)))
	return qb
}

// WhereLessThanOrEqual adds a WHERE <= condition
func (qb *QueryBuilder[T]) WhereLessThanOrEqual(field string, value interface{}) *QueryBuilder[T] {
	if !qb.whereAdded {
		qb.query.WriteString(" WHERE ")
		qb.whereAdded = true
	} else {
		qb.query.WriteString(" AND ")
	}
	qb.args = append(qb.args, value)
	qb.query.WriteString(fmt.Sprintf("%s <= $%d", field, len(qb.args)))
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

	// Extract only the WHERE conditions (strip "WHERE " prefix if present)
	subQuery := subBuilder.query.String()
	if len(subQuery) > 7 && subQuery[:7] == " WHERE " {
		subQuery = subQuery[7:]
	}

	// Append the sub-query conditions
	qb.query.WriteString(subQuery)
	qb.query.WriteString(")")

	// Update args from sub-builder
	qb.args = subBuilder.args

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

// Limit adds a LIMIT clause
func (qb *QueryBuilder[T]) Limit(limit int) *QueryBuilder[T] {
	qb.args = append(qb.args, limit)
	qb.query.WriteString(fmt.Sprintf(" LIMIT $%d", len(qb.args)))
	return qb
}

// Offset adds an OFFSET clause
func (qb *QueryBuilder[T]) Offset(offset int) *QueryBuilder[T] {
	qb.args = append(qb.args, offset)
	qb.query.WriteString(fmt.Sprintf(" OFFSET $%d", len(qb.args)))
	return qb
}

// SQL returns the generated SQL query string
func (qb *QueryBuilder[T]) SQL() string {
	return qb.query.String()
}

// Args returns the query arguments
func (qb *QueryBuilder[T]) Args() []interface{} {
	return qb.args
}

// Build finalizes the query and returns an executable Query
func (qb *QueryBuilder[T]) Build() Query[[]T] {
	sql := qb.query.String()
	if !qb.withTrashed {
		sql = qb.model.applyBaseQuery(sql)
	}
	args := qb.args

	executor := func(ctx context.Context) ([]T, error) {
		var result []T
		err := qb.model.readConn.DB().SelectContext(ctx, &result, sql, args...)
		return result, err
	}

	q := newQuery(executor, sql, args...)
	if qb.model.cache != nil {
		q.cache = qb.model.cache
		q.cacheTTL = qb.model.cacheTTL
	}
	return q
}
