package dbconnector

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"unsafe"

	"github.com/jmoiron/sqlx"
)

// RowScanner is an optional interface that a domain struct can implement to take
// full control over how a single database row is scanned into itself.
//
// If T implements RowScanner, every Model read operation will call ScanRow
// instead of the default reflect/unsafe path.
//
// Example:
//
//	func (a *Account) ScanRow(rows *sql.Rows) error {
//	    return rows.Scan(&a.id, &a.name, ...)
//	}
type RowScanner interface {
	ScanRow(rows *sql.Rows) error
}

// hasUnexportedFields returns true when the struct type contains at least one
// field that has a "db" tag but is unexported.
func hasUnexportedFields(t reflect.Type) bool {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return false
	}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.Tag.Get("db") != "" && !f.IsExported() {
			return true
		}
	}
	return false
}

// dbTagIndex builds a map of db-tag → field-index for a struct type.
func dbTagIndex(t reflect.Type) map[string]int {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	m := make(map[string]int, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		tag := t.Field(i).Tag.Get("db")
		if tag != "" && tag != "-" {
			m[tag] = i
		}
	}
	return m
}

// unsafeSetField writes val into the struct field at fieldIndex using unsafe,
// bypassing Go's export restriction.
func unsafeSetField(structPtr unsafe.Pointer, field reflect.StructField, val interface{}) error {
	if val == nil {
		return nil
	}

	fieldPtr := unsafe.Pointer(uintptr(structPtr) + field.Offset)
	fv := reflect.NewAt(field.Type, fieldPtr).Elem()

	// Handle *sql.NullXxx → concrete type conversions automatically.
	src := reflect.ValueOf(val)

	// database/sql returns []byte for some drivers; handle common cases.
	if src.IsValid() && src.Type().AssignableTo(field.Type) {
		fv.Set(src)
		return nil
	}
	if src.IsValid() && src.Type().ConvertibleTo(field.Type) {
		fv.Set(src.Convert(field.Type))
		return nil
	}

	// Pointer target: allocate and recurse.
	if field.Type.Kind() == reflect.Ptr {
		elem := field.Type.Elem()
		newVal := reflect.New(elem)
		inner := reflect.ValueOf(val)
		if inner.IsValid() && inner.Type().ConvertibleTo(elem) {
			newVal.Elem().Set(inner.Convert(elem))
			fv.Set(newVal)
			return nil
		}
	}

	return fmt.Errorf("dbconnector: cannot assign %T to field %s (%s)", val, field.Name, field.Type)
}

// unsafeScanRow scans a single *sql.Rows cursor into a *T using unsafe field
// access, enabling unexported fields to be populated via their "db" tags.
func unsafeScanRow[T any](rows *sql.Rows) (T, error) {
	var zero T
	t := reflect.TypeOf(zero)
	isPtr := t.Kind() == reflect.Ptr
	if isPtr {
		t = t.Elem()
	}

	cols, err := rows.Columns()
	if err != nil {
		return zero, err
	}

	index := dbTagIndex(t)

	// Build a slice of scan targets – one *interface{} per column.
	dest := make([]interface{}, len(cols))
	ptrs := make([]interface{}, len(cols))
	for i := range dest {
		ptrs[i] = &dest[i]
	}

	if err := rows.Scan(ptrs...); err != nil {
		return zero, err
	}

	// Allocate the struct and write each column into the matching field.
	structVal := reflect.New(t).Elem()
	structPtr := unsafe.Pointer(structVal.UnsafeAddr())

	for i, col := range cols {
		fieldIdx, ok := index[col]
		if !ok {
			continue
		}
		if err := unsafeSetField(structPtr, t.Field(fieldIdx), dest[i]); err != nil {
			return zero, err
		}
	}

	if isPtr {
		result := structVal.Addr().Interface().(T)
		return result, nil
	}
	return structVal.Interface().(T), nil
}

// unsafeScanRows scans all remaining rows into []T using unsafe field access.
func unsafeScanRows[T any](rows *sql.Rows) ([]T, error) {
	var result []T
	for rows.Next() {
		item, err := unsafeScanRow[T](rows)
		if err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

// selectOne executes rawSQL + args against db and returns a single T.
// Strategy (in order):
//  1. *T implements RowScanner → ScanRow
//  2. T has unexported db-tagged fields → unsafe scanner
//  3. Fallback to sqlx.GetContext (standard exported-field path)
func selectOne[T any](ctx context.Context, db *sqlx.DB, rawSQL string, args ...interface{}) (T, error) {
	var zero T

	// Check RowScanner
	if _, ok := any(new(T)).(RowScanner); ok {
		rows, err := db.QueryContext(ctx, rawSQL, args...)
		if err != nil {
			return zero, err
		}
		defer rows.Close()
		if !rows.Next() {
			if err := rows.Err(); err != nil {
				return zero, err
			}
			return zero, sql.ErrNoRows
		}
		result := new(T)
		if err := any(result).(RowScanner).ScanRow(rows); err != nil {
			return zero, err
		}
		return *result, nil
	}

	// Check unexported fields
	var t T
	if hasUnexportedFields(reflect.TypeOf(t)) {
		rows, err := db.QueryContext(ctx, rawSQL, args...)
		if err != nil {
			return zero, err
		}
		defer rows.Close()
		if !rows.Next() {
			if err := rows.Err(); err != nil {
				return zero, err
			}
			return zero, sql.ErrNoRows
		}
		return unsafeScanRow[T](rows)
	}

	// Standard sqlx path
	var result T
	err := db.GetContext(ctx, &result, rawSQL, args...)
	return result, err
}

// selectMany executes rawSQL + args against db and returns []T.
// Same strategy selection as selectOne.
func selectMany[T any](ctx context.Context, db *sqlx.DB, rawSQL string, args ...interface{}) ([]T, error) {
	// Check RowScanner
	if _, ok := any(new(T)).(RowScanner); ok {
		rows, err := db.QueryContext(ctx, rawSQL, args...)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var result []T
		for rows.Next() {
			item := new(T)
			if err := any(item).(RowScanner).ScanRow(rows); err != nil {
				return nil, err
			}
			result = append(result, *item)
		}
		return result, rows.Err()
	}

	// Check unexported fields
	var t T
	if hasUnexportedFields(reflect.TypeOf(t)) {
		rows, err := db.QueryContext(ctx, rawSQL, args...)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		return unsafeScanRows[T](rows)
	}

	// Standard sqlx path
	var result []T
	err := db.SelectContext(ctx, &result, rawSQL, args...)
	return result, err
}

