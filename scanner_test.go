package dbconnector

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// Test structs
// ─────────────────────────────────────────────────────────────────────────────

// privateAccount mirrors the user's Account struct – every domain field is
// unexported but carries a `db` tag so the unsafe scanner must populate them.
type privateAccount struct {
	id    string `db:"id"`
	name  string `db:"name"`
	email string `db:"email"`
	age   int    `db:"age"`
}

// Exported getters so tests can read values without reflection.
func (a privateAccount) ID() string    { return a.id }
func (a privateAccount) Name() string  { return a.name }
func (a privateAccount) Email() string { return a.email }
func (a privateAccount) Age() int      { return a.age }

// rowScannerAccount implements RowScanner explicitly.
type rowScannerAccount struct {
	id    string
	name  string
	email string
	age   int
}

func (a *rowScannerAccount) ScanRow(rows *sql.Rows) error {
	return rows.Scan(&a.id, &a.name, &a.email, &a.age)
}

func (a rowScannerAccount) ID() string    { return a.id }
func (a rowScannerAccount) Name() string  { return a.name }
func (a rowScannerAccount) Email() string { return a.email }
func (a rowScannerAccount) Age() int      { return a.age }

// ─────────────────────────────────────────────────────────────────────────────
// Setup helper for the shared table schema (id TEXT, name TEXT, email TEXT, age INT)
// ─────────────────────────────────────────────────────────────────────────────

func setupScannerTable[T any](t *testing.T) (*Model[T], func()) {
	t.Helper()
	conn := NewPostgresConnection(__TestDBconfig)
	if err := conn.Connect(context.Background()); err != nil {
		t.Skipf("no DB available: %v", err)
	}
	db := conn.DB()
	table := "test_scanner_" + strings.ReplaceAll(t.Name(), "/", "_")
	_, err := db.Exec(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id    TEXT PRIMARY KEY,
		name  TEXT,
		email TEXT,
		age   INT
	)`, table))
	if err != nil {
		conn.Close()
		t.Fatalf("create table: %v", err)
	}
	m := NewModel[T](NewConnector(conn, conn), table)
	cleanup := func() {
		db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
		conn.Close()
	}
	return m, cleanup
}

// ─────────────────────────────────────────────────────────────────────────────
// Unit tests – hasUnexportedFields / dbTagIndex (no DB needed)
// ─────────────────────────────────────────────────────────────────────────────

func TestHasUnexportedFields_True(t *testing.T) {
	if !hasUnexportedFields(reflect.TypeOf(privateAccount{})) {
		t.Error("expected hasUnexportedFields = true for privateAccount")
	}
}

func TestHasUnexportedFields_False(t *testing.T) {
	if hasUnexportedFields(reflect.TypeOf(User{})) {
		t.Error("expected hasUnexportedFields = false for User (all exported)")
	}
}

func TestHasUnexportedFields_NonStruct(t *testing.T) {
	if hasUnexportedFields(reflect.TypeOf("string")) {
		t.Error("expected hasUnexportedFields = false for non-struct")
	}
}

func TestDBTagIndex(t *testing.T) {
	idx := dbTagIndex(reflect.TypeOf(privateAccount{}))
	for _, tag := range []string{"id", "name", "email", "age"} {
		if _, ok := idx[tag]; !ok {
			t.Errorf("dbTagIndex missing tag %q", tag)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Integration – unsafe scanner (unexported fields, no RowScanner)
// ─────────────────────────────────────────────────────────────────────────────

func TestUnsafeScanner_Find(t *testing.T) {
	m, cleanup := setupScannerTable[privateAccount](t)
	defer cleanup()
	ctx := context.Background()

	// Insert directly via raw SQL because Create uses NamedExec (needs exported fields).
	_, err := m.writeConn.DB().ExecContext(ctx,
		fmt.Sprintf("INSERT INTO %s (id, name, email, age) VALUES ($1,$2,$3,$4)", m.tableName),
		"acc1", "Alice", "alice@example.com", 30)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	got, err := m.Find("acc1").Exec(ctx)
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if got.ID() != "acc1" {
		t.Errorf("ID: want acc1, got %q", got.ID())
	}
	if got.Name() != "Alice" {
		t.Errorf("Name: want Alice, got %q", got.Name())
	}
	if got.Email() != "alice@example.com" {
		t.Errorf("Email: want alice@example.com, got %q", got.Email())
	}
	if got.Age() != 30 {
		t.Errorf("Age: want 30, got %d", got.Age())
	}
}

func TestUnsafeScanner_FindBy(t *testing.T) {
	m, cleanup := setupScannerTable[privateAccount](t)
	defer cleanup()
	ctx := context.Background()

	_, err := m.writeConn.DB().ExecContext(ctx,
		fmt.Sprintf("INSERT INTO %s (id, name, email, age) VALUES ($1,$2,$3,$4)", m.tableName),
		"acc2", "Bob", "bob@example.com", 25)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	got, err := m.FindBy("name", "Bob").Exec(ctx)
	if err != nil {
		t.Fatalf("FindBy: %v", err)
	}
	if got.Name() != "Bob" {
		t.Errorf("Name: want Bob, got %q", got.Name())
	}
}

func TestUnsafeScanner_All(t *testing.T) {
	m, cleanup := setupScannerTable[privateAccount](t)
	defer cleanup()
	ctx := context.Background()

	for i, name := range []string{"Alice", "Bob", "Carol"} {
		_, err := m.writeConn.DB().ExecContext(ctx,
			fmt.Sprintf("INSERT INTO %s (id, name, email, age) VALUES ($1,$2,$3,$4)", m.tableName),
			fmt.Sprintf("acc%d", i+1), name, fmt.Sprintf("%s@x.com", strings.ToLower(name)), 20+i)
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	all, err := m.All().Exec(ctx)
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	if len(all) != 3 {
		t.Errorf("expected 3 rows, got %d", len(all))
	}
	// Verify fields are non-zero for every row
	for _, a := range all {
		if a.ID() == "" {
			t.Error("got empty ID from unsafe scan")
		}
		if a.Name() == "" {
			t.Error("got empty Name from unsafe scan")
		}
	}
}

func TestUnsafeScanner_GetBy(t *testing.T) {
	m, cleanup := setupScannerTable[privateAccount](t)
	defer cleanup()
	ctx := context.Background()

	for i, age := range []int{30, 30, 25} {
		_, err := m.writeConn.DB().ExecContext(ctx,
			fmt.Sprintf("INSERT INTO %s (id, name, email, age) VALUES ($1,$2,$3,$4)", m.tableName),
			fmt.Sprintf("x%d", i), fmt.Sprintf("User%d", i), fmt.Sprintf("u%d@x.com", i), age)
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	results, err := m.GetBy(map[string]interface{}{"age": 30}).Exec(ctx)
	if err != nil {
		t.Fatalf("GetBy: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 rows with age=30, got %d", len(results))
	}
	for _, r := range results {
		if r.Age() != 30 {
			t.Errorf("expected age 30, got %d", r.Age())
		}
	}
}

func TestUnsafeScanner_Raw(t *testing.T) {
	m, cleanup := setupScannerTable[privateAccount](t)
	defer cleanup()
	ctx := context.Background()

	_, err := m.writeConn.DB().ExecContext(ctx,
		fmt.Sprintf("INSERT INTO %s (id, name, email, age) VALUES ($1,$2,$3,$4)", m.tableName),
		"raw1", "RawUser", "raw@x.com", 99)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	results, err := m.Raw(ctx, fmt.Sprintf("SELECT * FROM %s WHERE id = $1", m.tableName), "raw1")
	if err != nil {
		t.Fatalf("Raw: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Age() != 99 {
		t.Errorf("Age: want 99, got %d", results[0].Age())
	}
}

func TestUnsafeScanner_Paginate(t *testing.T) {
	m, cleanup := setupScannerTable[privateAccount](t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		_, err := m.writeConn.DB().ExecContext(ctx,
			fmt.Sprintf("INSERT INTO %s (id, name, email, age) VALUES ($1,$2,$3,$4)", m.tableName),
			fmt.Sprintf("p%d", i), fmt.Sprintf("User%d", i), fmt.Sprintf("u%d@x.com", i), i)
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	page, err := m.Paginate(ctx, 1, 3, m.Query())
	if err != nil {
		t.Fatalf("Paginate: %v", err)
	}
	if page.Total != 5 {
		t.Errorf("Total: want 5, got %d", page.Total)
	}
	if len(page.Items) != 3 {
		t.Errorf("Items: want 3, got %d", len(page.Items))
	}
	if page.TotalPages != 2 {
		t.Errorf("TotalPages: want 2, got %d", page.TotalPages)
	}
	for _, item := range page.Items {
		if item.ID() == "" {
			t.Error("got empty ID in paginated unsafe-scanned result")
		}
	}
}

func TestUnsafeScanner_Chunk(t *testing.T) {
	m, cleanup := setupScannerTable[privateAccount](t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 7; i++ {
		_, err := m.writeConn.DB().ExecContext(ctx,
			fmt.Sprintf("INSERT INTO %s (id, name, email, age) VALUES ($1,$2,$3,$4)", m.tableName),
			fmt.Sprintf("c%d", i), fmt.Sprintf("User%d", i), fmt.Sprintf("u%d@x.com", i), i)
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	var collected []privateAccount
	err := m.Chunk(ctx, 3, nil, func(batch []privateAccount) error {
		collected = append(collected, batch...)
		return nil
	})
	if err != nil {
		t.Fatalf("Chunk: %v", err)
	}
	if len(collected) != 7 {
		t.Errorf("expected 7 rows from Chunk, got %d", len(collected))
	}
	for _, a := range collected {
		if a.ID() == "" {
			t.Error("got empty ID in chunked unsafe-scanned result")
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Integration – RowScanner strategy
// ─────────────────────────────────────────────────────────────────────────────

func TestRowScannerStrategy_FindAndAll(t *testing.T) {
	m, cleanup := setupScannerTable[rowScannerAccount](t)
	defer cleanup()
	ctx := context.Background()

	for i, row := range []struct {
		id, name, email string
		age             int
	}{
		{"rs1", "Diana", "diana@x.com", 28},
		{"rs2", "Eve", "eve@x.com", 32},
	} {
		_, err := m.writeConn.DB().ExecContext(ctx,
			fmt.Sprintf("INSERT INTO %s (id, name, email, age) VALUES ($1,$2,$3,$4)", m.tableName),
			row.id, row.name, row.email, row.age)
		if err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	// Find single row
	got, err := m.Find("rs1").Exec(ctx)
	if err != nil {
		t.Fatalf("Find via RowScanner: %v", err)
	}
	if got.Name() != "Diana" {
		t.Errorf("Name: want Diana, got %q", got.Name())
	}

	// All rows
	all, err := m.All().Exec(ctx)
	if err != nil {
		t.Fatalf("All via RowScanner: %v", err)
	}
	if len(all) != 2 {
		t.Errorf("expected 2 rows, got %d", len(all))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Unit – unsafeScanRow with nil value (nullable column)
// ─────────────────────────────────────────────────────────────────────────────

type nullableRow struct {
	id   string  `db:"id"`
	note *string `db:"note"` // nullable pointer field
}

func TestUnsafeSetField_NilValue(t *testing.T) {
	conn := NewPostgresConnection(__TestDBconfig)
	if err := conn.Connect(context.Background()); err != nil {
		t.Skipf("no DB available: %v", err)
	}
	defer conn.Close()

	db := conn.DB()
	table := "test_nullable_scanner"
	db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	_, err := db.Exec(fmt.Sprintf(`CREATE TABLE %s (id TEXT PRIMARY KEY, note TEXT)`, table))
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	defer db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", table))

	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s VALUES ($1, NULL)", table), "n1")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	m := NewModel[nullableRow](NewConnector(conn, conn), table)
	got, err := m.Find("n1").Exec(context.Background())
	if err != nil {
		t.Fatalf("Find nullable: %v", err)
	}
	if got.id != "n1" {
		t.Errorf("id: want n1, got %q", got.id)
	}
	// note is NULL → pointer should be nil
	if got.note != nil {
		t.Errorf("note: want nil, got %v", *got.note)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Unit – selectOne returns sql.ErrNoRows when no row found
// ─────────────────────────────────────────────────────────────────────────────

func TestUnsafeScanner_NoRows(t *testing.T) {
	m, cleanup := setupScannerTable[privateAccount](t)
	defer cleanup()

	_, err := m.Find("nonexistent").Exec(context.Background())
	if err == nil {
		t.Error("expected sql.ErrNoRows, got nil")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Unit – hasUnexportedFields handles pointer-to-struct
// ─────────────────────────────────────────────────────────────────────────────

func TestHasUnexportedFields_PointerStruct(t *testing.T) {
	if !hasUnexportedFields(reflect.TypeOf(&privateAccount{})) {
		t.Error("expected hasUnexportedFields = true for *privateAccount")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Time field – ensure time.Time unexported field scans correctly
// ─────────────────────────────────────────────────────────────────────────────

type timestampRow struct {
	id        string    `db:"id"`
	createdAt time.Time `db:"created_at"`
}

func TestUnsafeScanner_TimeField(t *testing.T) {
	conn := NewPostgresConnection(__TestDBconfig)
	if err := conn.Connect(context.Background()); err != nil {
		t.Skipf("no DB available: %v", err)
	}
	defer conn.Close()

	db := conn.DB()
	table := "test_ts_scanner"
	db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	_, err := db.Exec(fmt.Sprintf(`CREATE TABLE %s (id TEXT PRIMARY KEY, created_at TIMESTAMPTZ)`, table))
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	defer db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", table))

	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s VALUES ($1, NOW())", table), "ts1")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	m := NewModel[timestampRow](NewConnector(conn, conn), table)
	got, err := m.Find("ts1").Exec(context.Background())
	if err != nil {
		t.Fatalf("Find time field: %v", err)
	}
	if got.id != "ts1" {
		t.Errorf("id: want ts1, got %q", got.id)
	}
	if got.createdAt.IsZero() {
		t.Error("createdAt should not be zero")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// unsafeSetField – pointer-field path
// Tests that a *string unexported field is populated correctly.
// ─────────────────────────────────────────────────────────────────────────────

type ptrFieldRow struct {
	id   string  `db:"id"`
	note *string `db:"note"`
}

func TestUnsafeSetField_PtrField_NonNil(t *testing.T) {
	conn := NewPostgresConnection(__TestDBconfig)
	if err := conn.Connect(context.Background()); err != nil {
		t.Skipf("no DB: %v", err)
	}
	defer conn.Close()

	db := conn.DB()
	table := "test_ptrf_scanner"
	db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	if _, err := db.Exec(fmt.Sprintf(`CREATE TABLE %s (id TEXT PRIMARY KEY, note TEXT)`, table)); err != nil {
		t.Fatalf("create: %v", err)
	}
	defer db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", table))

	if _, err := db.Exec(fmt.Sprintf("INSERT INTO %s VALUES ($1, $2)", table), "pf1", "hello"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	m := NewModel[ptrFieldRow](NewConnector(conn, conn), table)
	got, err := m.Find("pf1").Exec(context.Background())
	if err != nil {
		t.Fatalf("Find ptr field: %v", err)
	}
	if got.note == nil {
		t.Fatal("note should not be nil")
	}
	if *got.note != "hello" {
		t.Errorf("note: want 'hello', got %q", *got.note)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// unsafeScanRow – pointer receiver T = *privateAccount
// ─────────────────────────────────────────────────────────────────────────────

func TestUnsafeScanner_PtrResult(t *testing.T) {
	// privateAccount has unexported fields – scan as *privateAccount
	conn := NewPostgresConnection(__TestDBconfig)
	if err := conn.Connect(context.Background()); err != nil {
		t.Skipf("no DB: %v", err)
	}
	defer conn.Close()

	db := conn.DB()
	table := "test_ptr_result_scanner"
	db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	if _, err := db.Exec(fmt.Sprintf(`CREATE TABLE %s (id TEXT PRIMARY KEY, name TEXT, email TEXT, age INT)`, table)); err != nil {
		t.Fatalf("create: %v", err)
	}
	defer db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", table))

	if _, err := db.Exec(fmt.Sprintf("INSERT INTO %s VALUES ($1,$2,$3,$4)", table), "ptr1", "PtrUser", "p@p.com", 42); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Use *privateAccount so the isPtr branch in unsafeScanRow is exercised.
	m := NewModel[*privateAccount](NewConnector(conn, conn), table)
	got, err := m.Find("ptr1").Exec(context.Background())
	if err != nil {
		t.Fatalf("Find *T: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil *privateAccount")
	}
	if got.ID() != "ptr1" {
		t.Errorf("ID: want ptr1, got %q", got.ID())
	}
	if got.Name() != "PtrUser" {
		t.Errorf("Name: want PtrUser, got %q", got.Name())
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// formatArg – nil *time.Time branch
// ─────────────────────────────────────────────────────────────────────────────

func TestFormatArg_NilPtrTimestamp(t *testing.T) {
	var nilTs *time.Time
	result := InterpolateSQL("col=$1", nilTs)
	if result != "col=NULL" {
		t.Errorf("nil *time.Time should render as NULL, got %q", result)
	}
}
