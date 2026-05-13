package dbconnector

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"unsafe"

	"github.com/jmoiron/sqlx"
)

// sqlScannerType is the reflect.Type of the sql.Scanner interface.
var sqlScannerType = reflect.TypeOf((*sql.Scanner)(nil)).Elem()

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
//
// Coercion priority (first match wins):
//  1. val == nil                               → skip (leave zero value)
//  2. []byte dest is string field              → string(val.([]byte))
//  3. []byte/string dest is map/slice          → JSON-unmarshal
//  4. dest type directly assignable / convertible
//  5. *dest implements sql.Scanner             → allocate + Scan
//  6. dest directly convertible
//  7. dest is ptr → allocate elem:
//     a. *elem implements sql.Scanner
//     b. val directly convertible to elem
//     c. single-field struct introspection (e.g. *utils.UniqueEntityID)
//  8. dest is non-ptr struct with sql.Scanner on *dest
//  9. single-field struct introspection (e.g. utils.UniqueEntityID value field)
// 10. error
func unsafeSetField(structPtr unsafe.Pointer, field reflect.StructField, val interface{}) error {
	if val == nil {
		return nil
	}

	fieldPtr := unsafe.Pointer(uintptr(structPtr) + field.Offset)
	fv := reflect.NewAt(field.Type, fieldPtr).Elem()
	src := reflect.ValueOf(val)

	// ── 1. []byte coercions ──────────────────────────────────────────────────

	if b, ok := val.([]byte); ok {
		// []byte → string
		if field.Type.Kind() == reflect.String {
			fv.SetString(string(b))
			return nil
		}
		// []byte → map or slice (JSON column)
		if field.Type.Kind() == reflect.Map || field.Type.Kind() == reflect.Slice {
			target := reflect.New(field.Type)
			if err := json.Unmarshal(b, target.Interface()); err != nil {
				return fmt.Errorf("dbconnector: json.Unmarshal for field %s: %w", field.Name, err)
			}
			fv.Set(target.Elem())
			return nil
		}
	}

	// ── 2. string → map / slice (JSON stored as TEXT) ───────────────────────
	if s, ok := val.(string); ok {
		if field.Type.Kind() == reflect.Map || field.Type.Kind() == reflect.Slice {
			target := reflect.New(field.Type)
			if err := json.Unmarshal([]byte(s), target.Interface()); err != nil {
				return fmt.Errorf("dbconnector: json.Unmarshal(string) for field %s: %w", field.Name, err)
			}
			fv.Set(target.Elem())
			return nil
		}
	}

	// ── 3. direct assign / convert ──────────────────────────────────────────
	if src.IsValid() && src.Type().AssignableTo(field.Type) {
		fv.Set(src)
		return nil
	}
	if src.IsValid() && src.Type().ConvertibleTo(field.Type) {
		fv.Set(src.Convert(field.Type))
		return nil
	}

	// ── 4. pointer field ─────────────────────────────────────────────────────
	if field.Type.Kind() == reflect.Ptr {
		elem := field.Type.Elem()
		newVal := reflect.New(elem) // *elem

		// (a) sql.Scanner on *elem
		if newVal.Type().Implements(sqlScannerType) {
			if err := newVal.Interface().(sql.Scanner).Scan(val); err != nil {
				return fmt.Errorf("dbconnector: Scanner.Scan for ptr field %s: %w", field.Name, err)
			}
			fv.Set(newVal)
			return nil
		}

		// (b) direct conversion
		if src.IsValid() && src.Type().ConvertibleTo(elem) {
			newVal.Elem().Set(src.Convert(elem))
			fv.Set(newVal)
			return nil
		}

		// (c) single-field struct introspection (e.g. *utils.UniqueEntityID{value string})
		if elem.Kind() == reflect.Struct && src.IsValid() {
			for i := 0; i < elem.NumField(); i++ {
				sf := elem.Field(i)
				if src.Type().ConvertibleTo(sf.Type) {
					// newVal.Pointer() gives the address of the allocated elem
					innerPtr := unsafe.Pointer(uintptr(newVal.Pointer()) + sf.Offset)
					reflect.NewAt(sf.Type, innerPtr).Elem().Set(src.Convert(sf.Type))
					fv.Set(newVal)
					return nil
				}
			}
		}
	}

	// ── 5. non-pointer struct with sql.Scanner (*FieldType implements it) ────
	ptrToField := reflect.New(field.Type)
	if ptrToField.Type().Implements(sqlScannerType) {
		if err := ptrToField.Interface().(sql.Scanner).Scan(val); err != nil {
			return fmt.Errorf("dbconnector: Scanner.Scan for field %s: %w", field.Name, err)
		}
		fv.Set(ptrToField.Elem())
		return nil
	}

	// ── 6. single-field struct introspection (value, non-pointer) ────────────
	if field.Type.Kind() == reflect.Struct && src.IsValid() {
		for i := 0; i < field.Type.NumField(); i++ {
			sf := field.Type.Field(i)
			if src.Type().ConvertibleTo(sf.Type) {
				innerPtr := unsafe.Pointer(uintptr(structPtr) + field.Offset + sf.Offset)
				reflect.NewAt(sf.Type, innerPtr).Elem().Set(src.Convert(sf.Type))
				return nil
			}
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

	// Standard sqlx path – use Unsafe so columns not present in T are ignored.
	var result T
	err := db.Unsafe().GetContext(ctx, &result, rawSQL, args...)
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

	// Standard sqlx path – use Unsafe so columns not present in T are ignored.
	var result []T
	err := db.Unsafe().SelectContext(ctx, &result, rawSQL, args...)
	return result, err
}

