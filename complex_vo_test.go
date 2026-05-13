package dbconnector

// complex_vo_test.go
// Tests for complex domain structs that use DDD value-objects as unexported
// fields.  The scanner must populate value-objects via:
//   1. sql.Scanner interface (primary path)
//   2. single-primitive-field struct introspection (fallback)
//
// Covered scenarios:
//   - UniqueEntityID  (sql.Scanner wrapping a string)
//   - Name / Email    (sql.Scanner wrapping a string with validation)
//   - Age             (sql.Scanner wrapping an int)
//   - Status          (sql.Scanner wrapping a string enum)
//   - Nullable        (*time.Time unexported field via pointer)
//   - Find / FindBy / All / GetBy / Raw (single & multi-row)
//   - QueryBuilder fluent API on domain structs
//   - Paginate with *QueryBuilder[AccountDomain]
//   - Paginate with *RawQuery[AccountDomain]
//   - PaginateAs projecting AccountDomain → AccountSummary
//   - NewQueryBuilderFromSQL + chained conditions
//   - Page navigation helpers (HasNext/HasPrev/NextPage/PrevPage)
//   - Soft-delete with domain struct
//   - OrderBy, Limit, WhereGreaterThan, WhereLike, WhereIn

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// Value-Object primitives
// ─────────────────────────────────────────────────────────────────────────────

// UniqueEntityID wraps a string identifier (UUID / ULID / etc.).
type UniqueEntityID struct {
	value string
}

func NewUniqueEntityID(id string) UniqueEntityID { return UniqueEntityID{value: id} }
func (u UniqueEntityID) Value() string           { return u.value }
func (u UniqueEntityID) String() string          { return u.value }
func (u UniqueEntityID) IsZero() bool            { return u.value == "" }

// Scan implements sql.Scanner so the unsafe scanner can populate unexported fields.
func (u *UniqueEntityID) Scan(src interface{}) error {
	switch v := src.(type) {
	case string:
		u.value = v
	case []byte:
		u.value = string(v)
	case nil:
		u.value = ""
	default:
		return fmt.Errorf("UniqueEntityID: cannot scan %T", src)
	}
	return nil
}

// Value implements driver.Valuer so NamedExec / insert works.
func (u UniqueEntityID) DBValue() (driver.Value, error) { return u.value, nil }

// ─────────────────────────────────────────────────────────────────────────────

// VoName is a validated name value-object.
type VoName struct {
	value string
}

func NewVoName(v string) (VoName, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return VoName{}, fmt.Errorf("name cannot be blank")
	}
	return VoName{value: v}, nil
}
func MustVoName(v string) VoName {
	n, err := NewVoName(v)
	if err != nil {
		panic(err)
	}
	return n
}
func (n VoName) String() string { return n.value }

func (n *VoName) Scan(src interface{}) error {
	switch v := src.(type) {
	case string:
		n.value = v
	case []byte:
		n.value = string(v)
	case nil:
		n.value = ""
	default:
		return fmt.Errorf("VoName: cannot scan %T", src)
	}
	return nil
}
func (n VoName) Value() (driver.Value, error) { return n.value, nil }

// ─────────────────────────────────────────────────────────────────────────────

// VoEmail wraps an e-mail string.
type VoEmail struct{ value string }

func NewVoEmail(e string) VoEmail  { return VoEmail{value: strings.ToLower(strings.TrimSpace(e))} }
func (e VoEmail) String() string   { return e.value }
func (e VoEmail) Domain() string {
	parts := strings.SplitN(e.value, "@", 2)
	if len(parts) == 2 {
		return parts[1]
	}
	return ""
}

func (e *VoEmail) Scan(src interface{}) error {
	switch v := src.(type) {
	case string:
		e.value = v
	case []byte:
		e.value = string(v)
	case nil:
		e.value = ""
	default:
		return fmt.Errorf("VoEmail: cannot scan %T", src)
	}
	return nil
}
func (e VoEmail) Value() (driver.Value, error) { return e.value, nil }

// ─────────────────────────────────────────────────────────────────────────────

// VoAge wraps a non-negative integer age.
type VoAge struct{ value int }

func NewVoAge(v int) (VoAge, error) {
	if v < 0 {
		return VoAge{}, fmt.Errorf("age must be >= 0")
	}
	return VoAge{value: v}, nil
}
func MustVoAge(v int) VoAge {
	a, err := NewVoAge(v)
	if err != nil {
		panic(err)
	}
	return a
}
func (a VoAge) Int() int { return a.value }

func (a *VoAge) Scan(src interface{}) error {
	switch v := src.(type) {
	case int64:
		a.value = int(v)
	case int:
		a.value = v
	case nil:
		a.value = 0
	default:
		return fmt.Errorf("VoAge: cannot scan %T", src)
	}
	return nil
}
func (a VoAge) Value() (driver.Value, error) { return int64(a.value), nil }

// ─────────────────────────────────────────────────────────────────────────────

// VoStatus is a string-enum value-object.
type VoStatus struct{ value string }

const (
	StatusActive   = "active"
	StatusInactive = "inactive"
	StatusBanned   = "banned"
)

func NewVoStatus(v string) (VoStatus, error) {
	switch v {
	case StatusActive, StatusInactive, StatusBanned:
		return VoStatus{value: v}, nil
	default:
		return VoStatus{}, fmt.Errorf("invalid status %q", v)
	}
}
func MustVoStatus(v string) VoStatus {
	s, err := NewVoStatus(v)
	if err != nil {
		panic(err)
	}
	return s
}
func (s VoStatus) String() string  { return s.value }
func (s VoStatus) IsActive() bool  { return s.value == StatusActive }

func (s *VoStatus) Scan(src interface{}) error {
	switch v := src.(type) {
	case string:
		s.value = v
	case []byte:
		s.value = string(v)
	case nil:
		s.value = ""
	default:
		return fmt.Errorf("VoStatus: cannot scan %T", src)
	}
	return nil
}
func (s VoStatus) Value() (driver.Value, error) { return s.value, nil }

// ─────────────────────────────────────────────────────────────────────────────
// Domain entity with unexported fields that are value-objects
// ─────────────────────────────────────────────────────────────────────────────

// AccountDomain is a DDD-style entity with all unexported fields.
// It stores data in the `accounts_vo` table.
type AccountDomain struct {
	id        UniqueEntityID `db:"id"`
	name      VoName         `db:"name"`
	email     VoEmail        `db:"email"`
	age       VoAge          `db:"age"`
	status    VoStatus       `db:"status"`
	createdAt time.Time      `db:"created_at"`
	deletedAt *time.Time     `db:"deleted_at"`
}

// Exported accessors
func (a AccountDomain) ID() UniqueEntityID  { return a.id }
func (a AccountDomain) Name() VoName        { return a.name }
func (a AccountDomain) Email() VoEmail      { return a.email }
func (a AccountDomain) Age() VoAge          { return a.age }
func (a AccountDomain) Status() VoStatus    { return a.status }
func (a AccountDomain) CreatedAt() time.Time { return a.createdAt }
func (a AccountDomain) DeletedAt() *time.Time { return a.deletedAt }

// AccountSummary is a DTO for projection tests.
type AccountSummary struct {
	ID     string `db:"id"`
	Name   string `db:"name"`
	Status string `db:"status"`
}

// ─────────────────────────────────────────────────────────────────────────────
// Test table setup
// ─────────────────────────────────────────────────────────────────────────────

func setupVOTable(t *testing.T) (*Model[AccountDomain], func()) {
	t.Helper()
	conn := NewPostgresConnection(__TestDBconfig)
	if err := conn.Connect(context.Background()); err != nil {
		t.Skipf("no DB available: %v", err)
	}
	db := conn.DB()
	table := "test_vo_" + strings.ReplaceAll(t.Name(), "/", "_")
	_, err := db.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id         TEXT PRIMARY KEY,
			name       TEXT NOT NULL,
			email      TEXT NOT NULL,
			age        INT  NOT NULL DEFAULT 0,
			status     TEXT NOT NULL DEFAULT 'active',
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			deleted_at TIMESTAMPTZ
		)`, table))
	if err != nil {
		conn.Close()
		t.Fatalf("create table: %v", err)
	}
	m := NewModel[AccountDomain](NewConnector(conn, conn), table)
	cleanup := func() {
		db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
		conn.Close()
	}
	return m, cleanup
}

func setupVOTableWithSoftDelete(t *testing.T) (*Model[AccountDomain], func()) {
	t.Helper()
	conn := NewPostgresConnection(__TestDBconfig)
	if err := conn.Connect(context.Background()); err != nil {
		t.Skipf("no DB available: %v", err)
	}
	db := conn.DB()
	table := "test_vosd_" + strings.ReplaceAll(t.Name(), "/", "_")
	_, err := db.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id         TEXT PRIMARY KEY,
			name       TEXT NOT NULL,
			email      TEXT NOT NULL,
			age        INT  NOT NULL DEFAULT 0,
			status     TEXT NOT NULL DEFAULT 'active',
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			deleted_at TIMESTAMPTZ
		)`, table))
	if err != nil {
		conn.Close()
		t.Fatalf("create table: %v", err)
	}
	m := NewModel[AccountDomain](NewConnector(conn, conn), table).WithSoftDelete("deleted_at")
	cleanup := func() {
		db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
		conn.Close()
	}
	return m, cleanup
}

// insertAccount is a raw INSERT helper that bypasses NamedExec
// (which doesn't understand value-object types automatically).
func insertAccount(t *testing.T, m *Model[AccountDomain], a AccountDomain) {
	t.Helper()
	_, err := m.writeConn.DB().ExecContext(context.Background(),
		fmt.Sprintf(
			"INSERT INTO %s (id, name, email, age, status) VALUES ($1,$2,$3,$4,$5)",
			m.tableName,
		),
		a.id.Value(), a.name.String(), a.email.String(), a.age.Int(), a.status.String(),
	)
	if err != nil {
		t.Fatalf("insertAccount: %v", err)
	}
}

func newAccount(id, name, email string, age int, status string) AccountDomain {
	return AccountDomain{
		id:     NewUniqueEntityID(id),
		name:   MustVoName(name),
		email:  NewVoEmail(email),
		age:    MustVoAge(age),
		status: MustVoStatus(status),
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Value-object unit tests (no DB)
// ─────────────────────────────────────────────────────────────────────────────

func TestVoUniqueEntityID_Scan(t *testing.T) {
	var id UniqueEntityID
	if err := id.Scan("hello-uuid"); err != nil {
		t.Fatalf("Scan string: %v", err)
	}
	if id.Value() != "hello-uuid" {
		t.Errorf("want hello-uuid, got %q", id.Value())
	}

	if err := id.Scan([]byte("bytes-uuid")); err != nil {
		t.Fatalf("Scan []byte: %v", err)
	}
	if id.Value() != "bytes-uuid" {
		t.Errorf("want bytes-uuid, got %q", id.Value())
	}

	if err := id.Scan(nil); err != nil {
		t.Fatalf("Scan nil: %v", err)
	}
	if !id.IsZero() {
		t.Error("nil scan should leave IsZero=true")
	}

	if err := id.Scan(42); err == nil {
		t.Error("expected error scanning int into UniqueEntityID")
	}
}

func TestVoAge_Scan(t *testing.T) {
	var a VoAge
	if err := a.Scan(int64(25)); err != nil {
		t.Fatalf("Scan int64: %v", err)
	}
	if a.Int() != 25 {
		t.Errorf("want 25, got %d", a.Int())
	}
	if err := a.Scan(nil); err != nil {
		t.Fatalf("Scan nil: %v", err)
	}
	if a.Int() != 0 {
		t.Errorf("want 0 after nil, got %d", a.Int())
	}
}

func TestVoStatus_Valid(t *testing.T) {
	for _, v := range []string{StatusActive, StatusInactive, StatusBanned} {
		s, err := NewVoStatus(v)
		if err != nil {
			t.Errorf("expected valid status %q, got error: %v", v, err)
		}
		if s.String() != v {
			t.Errorf("String() mismatch: %q", s.String())
		}
	}
}

func TestVoStatus_Invalid(t *testing.T) {
	if _, err := NewVoStatus("unknown"); err == nil {
		t.Error("expected error for unknown status")
	}
}

func TestVoName_Blank(t *testing.T) {
	if _, err := NewVoName("   "); err == nil {
		t.Error("expected error for blank name")
	}
}

func TestVoEmail_Domain(t *testing.T) {
	e := NewVoEmail("USER@Example.COM")
	if e.String() != "user@example.com" {
		t.Errorf("want lowercase, got %q", e.String())
	}
	if e.Domain() != "example.com" {
		t.Errorf("domain: want example.com, got %q", e.Domain())
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Integration – sql.Scanner path in unsafe scanner
// ─────────────────────────────────────────────────────────────────────────────

func TestVO_Find(t *testing.T) {
	m, cleanup := setupVOTable(t)
	defer cleanup()
	ctx := context.Background()

	acc := newAccount("vo-find-1", "Alice", "alice@test.com", 30, StatusActive)
	insertAccount(t, m, acc)

	got, err := m.Find("vo-find-1").Exec(ctx)
	if err != nil {
		t.Fatalf("Find: %v", err)
	}

	if got.ID().Value() != "vo-find-1" {
		t.Errorf("ID: want vo-find-1, got %q", got.ID().Value())
	}
	if got.Name().String() != "Alice" {
		t.Errorf("Name: want Alice, got %q", got.Name().String())
	}
	if got.Email().String() != "alice@test.com" {
		t.Errorf("Email: want alice@test.com, got %q", got.Email().String())
	}
	if got.Age().Int() != 30 {
		t.Errorf("Age: want 30, got %d", got.Age().Int())
	}
	if got.Status().String() != StatusActive {
		t.Errorf("Status: want active, got %q", got.Status().String())
	}
}

func TestVO_FindBy(t *testing.T) {
	m, cleanup := setupVOTable(t)
	defer cleanup()
	ctx := context.Background()

	insertAccount(t, m, newAccount("vo-fb-1", "Bob", "bob@test.com", 25, StatusActive))

	got, err := m.FindBy("name", "Bob").Exec(ctx)
	if err != nil {
		t.Fatalf("FindBy: %v", err)
	}
	if got.Name().String() != "Bob" {
		t.Errorf("Name: want Bob, got %q", got.Name().String())
	}
	if got.Age().Int() != 25 {
		t.Errorf("Age: want 25, got %d", got.Age().Int())
	}
}

func TestVO_All(t *testing.T) {
	m, cleanup := setupVOTable(t)
	defer cleanup()
	ctx := context.Background()

	accounts := []AccountDomain{
		newAccount("vo-all-1", "Alice", "alice@test.com", 30, StatusActive),
		newAccount("vo-all-2", "Bob", "bob@test.com", 25, StatusInactive),
		newAccount("vo-all-3", "Carol", "carol@test.com", 35, StatusActive),
	}
	for _, a := range accounts {
		insertAccount(t, m, a)
	}

	all, err := m.All().Exec(ctx)
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(all))
	}
	for _, row := range all {
		if row.ID().IsZero() {
			t.Error("empty ID after All scan")
		}
		if row.Name().String() == "" {
			t.Error("empty Name after All scan")
		}
		if row.Age().Int() < 0 {
			t.Error("negative Age after All scan")
		}
	}
}

func TestVO_GetBy(t *testing.T) {
	m, cleanup := setupVOTable(t)
	defer cleanup()
	ctx := context.Background()

	insertAccount(t, m, newAccount("vo-gb-1", "Alice", "a@t.com", 30, StatusActive))
	insertAccount(t, m, newAccount("vo-gb-2", "Bob", "b@t.com", 40, StatusInactive))
	insertAccount(t, m, newAccount("vo-gb-3", "Carol", "c@t.com", 30, StatusBanned))

	rows, err := m.GetBy(map[string]interface{}{"age": 30}).Exec(ctx)
	if err != nil {
		t.Fatalf("GetBy: %v", err)
	}
	if len(rows) != 2 {
		t.Errorf("expected 2 rows with age=30, got %d", len(rows))
	}
	for _, r := range rows {
		if r.Age().Int() != 30 {
			t.Errorf("expected age 30, got %d", r.Age().Int())
		}
	}
}

func TestVO_Raw(t *testing.T) {
	m, cleanup := setupVOTable(t)
	defer cleanup()
	ctx := context.Background()

	insertAccount(t, m, newAccount("vo-raw-1", "Dave", "dave@t.com", 45, StatusActive))

	rows, err := m.Raw(ctx,
		fmt.Sprintf("SELECT * FROM %s WHERE id = $1", m.tableName), "vo-raw-1")
	if err != nil {
		t.Fatalf("Raw: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if rows[0].Email().String() != "dave@t.com" {
		t.Errorf("Email: want dave@t.com, got %q", rows[0].Email().String())
	}
}

func TestVO_Exists(t *testing.T) {
	m, cleanup := setupVOTable(t)
	defer cleanup()
	ctx := context.Background()

	insertAccount(t, m, newAccount("vo-ex-1", "Eve", "eve@t.com", 28, StatusActive))

	if ok, err := m.Exists(ctx, "vo-ex-1"); err != nil || !ok {
		t.Errorf("Exists: want true, got %v (err: %v)", ok, err)
	}
	if ok, err := m.Exists(ctx, "nonexistent"); err != nil || ok {
		t.Errorf("Exists: want false for nonexistent, got %v (err: %v)", ok, err)
	}
}

func TestVO_Count(t *testing.T) {
	m, cleanup := setupVOTable(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 4; i++ {
		insertAccount(t, m, newAccount(
			fmt.Sprintf("vo-cnt-%d", i),
			fmt.Sprintf("User%d", i),
			fmt.Sprintf("u%d@t.com", i),
			20+i,
			StatusActive,
		))
	}

	n, err := m.Count(ctx, nil)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n != 4 {
		t.Errorf("expected 4, got %d", n)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// QueryBuilder fluent API on AccountDomain
// ─────────────────────────────────────────────────────────────────────────────

func TestVO_QueryBuilder_Where(t *testing.T) {
	m, cleanup := setupVOTable(t)
	defer cleanup()
	ctx := context.Background()

	insertAccount(t, m, newAccount("vo-qb-1", "Alice", "a@t.com", 30, StatusActive))
	insertAccount(t, m, newAccount("vo-qb-2", "Bob", "b@t.com", 25, StatusInactive))
	insertAccount(t, m, newAccount("vo-qb-3", "Carol", "c@t.com", 30, StatusActive))

	results, err := m.Query().Where("age", 30).Build().Exec(ctx)
	if err != nil {
		t.Fatalf("QB Where: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 rows, got %d", len(results))
	}
	for _, r := range results {
		if r.Age().Int() != 30 {
			t.Errorf("unexpected age %d", r.Age().Int())
		}
	}
}

func TestVO_QueryBuilder_WhereAndStatus(t *testing.T) {
	m, cleanup := setupVOTable(t)
	defer cleanup()
	ctx := context.Background()

	insertAccount(t, m, newAccount("vo-ws-1", "A", "a@t.com", 30, StatusActive))
	insertAccount(t, m, newAccount("vo-ws-2", "B", "b@t.com", 30, StatusInactive))
	insertAccount(t, m, newAccount("vo-ws-3", "C", "c@t.com", 25, StatusActive))

	results, err := m.Query().
		Where("age", 30).
		Where("status", StatusActive).
		Build().Exec(ctx)
	if err != nil {
		t.Fatalf("QB Where+Status: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 row, got %d", len(results))
	}
	if results[0].Name().String() != "A" {
		t.Errorf("want A, got %q", results[0].Name().String())
	}
}

func TestVO_QueryBuilder_OrderByAndLimit(t *testing.T) {
	m, cleanup := setupVOTable(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		insertAccount(t, m, newAccount(
			fmt.Sprintf("vo-ol-%d", i),
			fmt.Sprintf("User%d", i),
			fmt.Sprintf("u%d@t.com", i),
			10+i, StatusActive,
		))
	}

	results, err := m.Query().OrderBy("age", true).Limit(3).Build().Exec(ctx)
	if err != nil {
		t.Fatalf("QB OrderBy+Limit: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("expected 3, got %d", len(results))
	}
	// Descending: ages 14, 13, 12
	if results[0].Age().Int() < results[1].Age().Int() {
		t.Error("expected descending age order")
	}
}

func TestVO_QueryBuilder_WhereGreaterThan(t *testing.T) {
	m, cleanup := setupVOTable(t)
	defer cleanup()
	ctx := context.Background()

	for i, age := range []int{10, 20, 30, 40} {
		insertAccount(t, m, newAccount(
			fmt.Sprintf("vo-gt-%d", i),
			fmt.Sprintf("U%d", i),
			fmt.Sprintf("u%d@t.com", i),
			age, StatusActive,
		))
	}

	results, err := m.Query().WhereGreaterThan("age", 20).Build().Exec(ctx)
	if err != nil {
		t.Fatalf("WhereGreaterThan: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 rows with age > 20, got %d", len(results))
	}
	for _, r := range results {
		if r.Age().Int() <= 20 {
			t.Errorf("unexpected age %d (should be > 20)", r.Age().Int())
		}
	}
}

func TestVO_QueryBuilder_WhereLike(t *testing.T) {
	m, cleanup := setupVOTable(t)
	defer cleanup()
	ctx := context.Background()

	insertAccount(t, m, newAccount("vo-lk-1", "Alice Smith", "a@t.com", 30, StatusActive))
	insertAccount(t, m, newAccount("vo-lk-2", "Bob Jones", "b@t.com", 25, StatusActive))
	insertAccount(t, m, newAccount("vo-lk-3", "Alice Brown", "c@t.com", 35, StatusActive))

	results, err := m.Query().WhereLike("name", "Alice%").Build().Exec(ctx)
	if err != nil {
		t.Fatalf("WhereLike: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 rows matching 'Alice%%', got %d", len(results))
	}
}

func TestVO_QueryBuilder_WhereIn(t *testing.T) {
	m, cleanup := setupVOTable(t)
	defer cleanup()
	ctx := context.Background()

	insertAccount(t, m, newAccount("vo-wi-1", "Alice", "a@t.com", 30, StatusActive))
	insertAccount(t, m, newAccount("vo-wi-2", "Bob", "b@t.com", 25, StatusInactive))
	insertAccount(t, m, newAccount("vo-wi-3", "Carol", "c@t.com", 35, StatusBanned))

	results, err := m.Query().
		WhereIn("status", []interface{}{StatusActive, StatusBanned}).
		Build().Exec(ctx)
	if err != nil {
		t.Fatalf("WhereIn: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 rows (active+banned), got %d", len(results))
	}
}

func TestVO_QueryBuilder_WhereNot(t *testing.T) {
	m, cleanup := setupVOTable(t)
	defer cleanup()
	ctx := context.Background()

	insertAccount(t, m, newAccount("vo-wn-1", "Alice", "a@t.com", 30, StatusActive))
	insertAccount(t, m, newAccount("vo-wn-2", "Bob", "b@t.com", 25, StatusInactive))

	results, err := m.Query().WhereNot("status", StatusInactive).Build().Exec(ctx)
	if err != nil {
		t.Fatalf("WhereNot: %v", err)
	}
	if len(results) != 1 || results[0].Name().String() != "Alice" {
		t.Errorf("expected only Alice, got %d rows", len(results))
	}
}

func TestVO_QueryBuilder_ToSQL(t *testing.T) {
	m, cleanup := setupVOTable(t)
	defer cleanup()

	sql := m.Query().
		Where("status", StatusActive).
		WhereGreaterThan("age", 18).
		OrderBy("name", false).
		Limit(10).
		ToSQL()

	if !strings.Contains(sql, "'active'") {
		t.Errorf("ToSQL should interpolate 'active': %q", sql)
	}
	if !strings.Contains(sql, "18") {
		t.Errorf("ToSQL should interpolate 18: %q", sql)
	}
	if strings.Contains(sql, "$1") || strings.Contains(sql, "$2") {
		t.Errorf("ToSQL should not contain placeholders: %q", sql)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Paginate with *QueryBuilder[AccountDomain]
// ─────────────────────────────────────────────────────────────────────────────

func TestVO_Paginate_QueryBuilder(t *testing.T) {
	m, cleanup := setupVOTable(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		insertAccount(t, m, newAccount(
			fmt.Sprintf("vo-pqb-%d", i),
			fmt.Sprintf("User%d", i),
			fmt.Sprintf("u%d@t.com", i),
			20+i, StatusActive,
		))
	}

	page, err := m.Paginate(ctx, 1, 4, m.Query().OrderBy("age", false))
	if err != nil {
		t.Fatalf("Paginate QB: %v", err)
	}
	if page.Total != 10 {
		t.Errorf("Total: want 10, got %d", page.Total)
	}
	if len(page.Items) != 4 {
		t.Errorf("Items: want 4, got %d", len(page.Items))
	}
	if page.TotalPages != 3 {
		t.Errorf("TotalPages: want 3, got %d", page.TotalPages)
	}
	for _, item := range page.Items {
		if item.ID().IsZero() {
			t.Error("empty ID in paginated value-object result")
		}
	}
}

func TestVO_Paginate_Page2(t *testing.T) {
	m, cleanup := setupVOTable(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 7; i++ {
		insertAccount(t, m, newAccount(
			fmt.Sprintf("vo-p2-%d", i),
			fmt.Sprintf("U%d", i),
			fmt.Sprintf("u%d@t.com", i),
			i, StatusActive,
		))
	}

	page, err := m.Paginate(ctx, 2, 3, m.Query().OrderBy("age", false))
	if err != nil {
		t.Fatalf("Paginate page2: %v", err)
	}
	if page.Page != 2 {
		t.Errorf("Page: want 2, got %d", page.Page)
	}
	if len(page.Items) != 3 {
		t.Errorf("Items: want 3 on page 2, got %d", len(page.Items))
	}
}

func TestVO_Paginate_EmptyTable(t *testing.T) {
	m, cleanup := setupVOTable(t)
	defer cleanup()
	ctx := context.Background()

	page, err := m.Paginate(ctx, 1, 10, m.Query())
	if err != nil {
		t.Fatalf("Paginate empty: %v", err)
	}
	if page.Total != 0 {
		t.Errorf("Total: want 0, got %d", page.Total)
	}
	if page.TotalPages != 0 {
		t.Errorf("TotalPages: want 0, got %d", page.TotalPages)
	}
}

func TestVO_Paginate_WithStatusFilter(t *testing.T) {
	m, cleanup := setupVOTable(t)
	defer cleanup()
	ctx := context.Background()

	insertAccount(t, m, newAccount("vo-sf-1", "A", "a@t.com", 20, StatusActive))
	insertAccount(t, m, newAccount("vo-sf-2", "B", "b@t.com", 21, StatusActive))
	insertAccount(t, m, newAccount("vo-sf-3", "C", "c@t.com", 22, StatusInactive))
	insertAccount(t, m, newAccount("vo-sf-4", "D", "d@t.com", 23, StatusActive))
	insertAccount(t, m, newAccount("vo-sf-5", "E", "e@t.com", 24, StatusInactive))

	page, err := m.Paginate(ctx, 1, 2,
		m.Query().Where("status", StatusActive).OrderBy("age", false))
	if err != nil {
		t.Fatalf("Paginate filtered: %v", err)
	}
	if page.Total != 3 {
		t.Errorf("Total: want 3 active, got %d", page.Total)
	}
	if len(page.Items) != 2 {
		t.Errorf("Items page 1: want 2, got %d", len(page.Items))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Paginate with *RawQuery[AccountDomain]
// ─────────────────────────────────────────────────────────────────────────────

func TestVO_Paginate_RawQuery(t *testing.T) {
	m, cleanup := setupVOTable(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 8; i++ {
		insertAccount(t, m, newAccount(
			fmt.Sprintf("vo-rq-%d", i),
			fmt.Sprintf("User%d", i),
			fmt.Sprintf("u%d@t.com", i),
			10+i, StatusActive,
		))
	}

	rq := NewRawQuery[AccountDomain](m.tableName,
		fmt.Sprintf("SELECT * FROM %s WHERE age >= $1 ORDER BY age ASC", m.tableName),
		13,
	)
	page, err := m.Paginate(ctx, 1, 3, rq)
	if err != nil {
		t.Fatalf("Paginate RawQuery: %v", err)
	}
	// age >= 13 → 5 rows (13,14,15,16,17)
	if page.Total != 5 {
		t.Errorf("Total: want 5, got %d", page.Total)
	}
	if len(page.Items) != 3 {
		t.Errorf("Items: want 3, got %d", len(page.Items))
	}
	for _, item := range page.Items {
		if item.Age().Int() < 13 {
			t.Errorf("unexpected age %d (should be >= 13)", item.Age().Int())
		}
		if item.ID().IsZero() {
			t.Error("empty ID after RawQuery paginate")
		}
	}
}

func TestVO_Paginate_RawQuery_NoArgs(t *testing.T) {
	m, cleanup := setupVOTable(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		insertAccount(t, m, newAccount(
			fmt.Sprintf("vo-rqna-%d", i),
			fmt.Sprintf("U%d", i),
			fmt.Sprintf("u%d@t.com", i),
			i, StatusActive,
		))
	}

	rq := NewRawQuery[AccountDomain](m.tableName,
		fmt.Sprintf("SELECT * FROM %s", m.tableName),
	)
	page, err := m.Paginate(ctx, 1, 10, rq)
	if err != nil {
		t.Fatalf("Paginate RawQuery no-args: %v", err)
	}
	if page.Total != 3 {
		t.Errorf("Total: want 3, got %d", page.Total)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// PaginateAs – project AccountDomain → AccountSummary
// ─────────────────────────────────────────────────────────────────────────────

func TestVO_PaginateAs_Projection(t *testing.T) {
	m, cleanup := setupVOTable(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 6; i++ {
		insertAccount(t, m, newAccount(
			fmt.Sprintf("vo-pa-%d", i),
			fmt.Sprintf("User%d", i),
			fmt.Sprintf("u%d@t.com", i),
			i, StatusActive,
		))
	}

	rq := NewRawQuery[AccountDomain](m.tableName,
		fmt.Sprintf("SELECT id, name, status FROM %s ORDER BY name", m.tableName),
	)
	page, err := PaginateAs[AccountDomain, AccountSummary](ctx, m.readConn, 1, 3, rq)
	if err != nil {
		t.Fatalf("PaginateAs: %v", err)
	}
	if page.Total != 6 {
		t.Errorf("Total: want 6, got %d", page.Total)
	}
	if len(page.Items) != 3 {
		t.Errorf("Items: want 3, got %d", len(page.Items))
	}
	for _, s := range page.Items {
		if s.Name == "" {
			t.Error("projected Name should not be empty")
		}
		if s.Status == "" {
			t.Error("projected Status should not be empty")
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// NewQueryBuilderFromSQL with value-object struct
// ─────────────────────────────────────────────────────────────────────────────

func TestVO_QueryFromSQL_Chained(t *testing.T) {
	m, cleanup := setupVOTable(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 6; i++ {
		insertAccount(t, m, newAccount(
			fmt.Sprintf("vo-qfs-%d", i),
			fmt.Sprintf("User%d", i),
			fmt.Sprintf("u%d@t.com", i),
			10+i, StatusActive,
		))
	}

	// Base SQL with one condition, add another via fluent API.
	qb := m.QueryFromSQL(
		fmt.Sprintf("SELECT * FROM %s WHERE age > $1", m.tableName),
		10,
	).WhereGreaterThanOrEqual("age", 13).OrderBy("age", false)

	results, err := qb.Build().Exec(ctx)
	if err != nil {
		t.Fatalf("QueryFromSQL exec: %v", err)
	}
	// age > 10 AND age >= 13 → 3 rows (13,14,15)
	if len(results) != 3 {
		t.Errorf("expected 3 rows, got %d", len(results))
	}
	for _, r := range results {
		if r.Age().Int() < 13 {
			t.Errorf("unexpected age %d", r.Age().Int())
		}
	}
}

func TestVO_QueryFromSQL_Paginate(t *testing.T) {
	m, cleanup := setupVOTable(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 9; i++ {
		insertAccount(t, m, newAccount(
			fmt.Sprintf("vo-qfsp-%d", i),
			fmt.Sprintf("User%d", i),
			fmt.Sprintf("u%d@t.com", i),
			i, StatusActive,
		))
	}

	qb := m.QueryFromSQL(
		fmt.Sprintf("SELECT * FROM %s WHERE age >= $1", m.tableName),
		3,
	).OrderBy("age", false)

	page, err := m.Paginate(ctx, 1, 3, qb)
	if err != nil {
		t.Fatalf("QueryFromSQL+Paginate: %v", err)
	}
	// age >= 3 → 6 rows
	if page.Total != 6 {
		t.Errorf("Total: want 6, got %d", page.Total)
	}
	if len(page.Items) != 3 {
		t.Errorf("Items: want 3, got %d", len(page.Items))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Soft-delete with value-object domain struct
// ─────────────────────────────────────────────────────────────────────────────

func TestVO_SoftDelete(t *testing.T) {
	m, cleanup := setupVOTableWithSoftDelete(t)
	defer cleanup()
	ctx := context.Background()

	insertAccount(t, m, newAccount("vo-sd-1", "Alice", "a@t.com", 30, StatusActive))
	insertAccount(t, m, newAccount("vo-sd-2", "Bob", "b@t.com", 25, StatusActive))

	// Soft-delete Bob
	if err := m.Delete(ctx, "vo-sd-2"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	all, err := m.All().Exec(ctx)
	if err != nil {
		t.Fatalf("All after soft-delete: %v", err)
	}
	if len(all) != 1 {
		t.Errorf("expected 1 active row, got %d", len(all))
	}
	if all[0].Name().String() != "Alice" {
		t.Errorf("want Alice, got %q", all[0].Name().String())
	}
}

func TestVO_SoftDelete_Paginate(t *testing.T) {
	m, cleanup := setupVOTableWithSoftDelete(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		insertAccount(t, m, newAccount(
			fmt.Sprintf("vo-sdp-%d", i),
			fmt.Sprintf("U%d", i),
			fmt.Sprintf("u%d@t.com", i),
			i, StatusActive,
		))
	}
	// Soft-delete two rows
	m.writeConn.DB().ExecContext(ctx,
		fmt.Sprintf("UPDATE %s SET deleted_at=NOW() WHERE id IN ($1,$2)", m.tableName),
		"vo-sdp-0", "vo-sdp-1",
	)

	page, err := m.Paginate(ctx, 1, 10, m.Query())
	if err != nil {
		t.Fatalf("SoftDelete Paginate: %v", err)
	}
	if page.Total != 3 {
		t.Errorf("Total: want 3 active rows, got %d", page.Total)
	}
}

func TestVO_SoftDelete_WithTrashed(t *testing.T) {
	m, cleanup := setupVOTableWithSoftDelete(t)
	defer cleanup()
	ctx := context.Background()

	insertAccount(t, m, newAccount("vo-wt-1", "A", "a@t.com", 1, StatusActive))
	insertAccount(t, m, newAccount("vo-wt-2", "B", "b@t.com", 2, StatusActive))
	m.writeConn.DB().ExecContext(ctx,
		fmt.Sprintf("UPDATE %s SET deleted_at=NOW() WHERE id=$1", m.tableName),
		"vo-wt-2",
	)

	all, err := m.Query().WithTrashed().Build().Exec(ctx)
	if err != nil {
		t.Fatalf("WithTrashed: %v", err)
	}
	if len(all) != 2 {
		t.Errorf("WithTrashed should include deleted rows, want 2, got %d", len(all))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Page navigation helpers with value-object paginated results
// ─────────────────────────────────────────────────────────────────────────────

func TestVO_Page_Navigation(t *testing.T) {
	m, cleanup := setupVOTable(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 9; i++ {
		insertAccount(t, m, newAccount(
			fmt.Sprintf("vo-nav-%d", i),
			fmt.Sprintf("U%d", i),
			fmt.Sprintf("u%d@t.com", i),
			i, StatusActive,
		))
	}

	p1, _ := m.Paginate(ctx, 1, 3, m.Query().OrderBy("age", false))
	if !p1.HasNext() {
		t.Error("p1 HasNext should be true")
	}
	if p1.HasPrev() {
		t.Error("p1 HasPrev should be false")
	}
	if p1.NextPage() != 2 {
		t.Errorf("p1.NextPage want 2, got %d", p1.NextPage())
	}
	if p1.PrevPage() != 1 {
		t.Errorf("p1.PrevPage want 1 (clamped), got %d", p1.PrevPage())
	}

	p3, _ := m.Paginate(ctx, 3, 3, m.Query().OrderBy("age", false))
	if p3.HasNext() {
		t.Error("p3 HasNext should be false")
	}
	if !p3.HasPrev() {
		t.Error("p3 HasPrev should be true")
	}
	if p3.NextPage() != 3 {
		t.Errorf("p3.NextPage want 3 (clamped), got %d", p3.NextPage())
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Chunk with value-object domain struct
// ─────────────────────────────────────────────────────────────────────────────

func TestVO_Chunk(t *testing.T) {
	m, cleanup := setupVOTable(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 7; i++ {
		insertAccount(t, m, newAccount(
			fmt.Sprintf("vo-ch-%d", i),
			fmt.Sprintf("U%d", i),
			fmt.Sprintf("u%d@t.com", i),
			i, StatusActive,
		))
	}

	var seen []string
	err := m.Chunk(ctx, 3, nil, func(batch []AccountDomain) error {
		for _, a := range batch {
			seen = append(seen, a.ID().Value())
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Chunk: %v", err)
	}
	if len(seen) != 7 {
		t.Errorf("expected 7 IDs from Chunk, got %d", len(seen))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Complex query: multi-condition + ORDER BY + Limit + WhereBetween
// ─────────────────────────────────────────────────────────────────────────────

func TestVO_ComplexQuery(t *testing.T) {
	m, cleanup := setupVOTable(t)
	defer cleanup()
	ctx := context.Background()

	data := []struct {
		id, name, email, status string
		age                     int
	}{
		{"cq-1", "Alice", "alice@t.com", StatusActive, 25},
		{"cq-2", "Bob", "bob@t.com", StatusActive, 32},
		{"cq-3", "Carol", "carol@t.com", StatusInactive, 28},
		{"cq-4", "Dave", "dave@t.com", StatusActive, 45},
		{"cq-5", "Eve", "eve@t.com", StatusActive, 18},
		{"cq-6", "Frank", "frank@t.com", StatusBanned, 30},
	}
	for _, d := range data {
		insertAccount(t, m, newAccount(d.id, d.name, d.email, d.age, d.status))
	}

	// active users with age between 20 and 35 ordered by age desc, max 3
	results, err := m.Query().
		Where("status", StatusActive).
		WhereGreaterThanOrEqual("age", 20).
		WhereLessThanOrEqual("age", 35).
		OrderBy("age", true).
		Limit(3).
		Build().Exec(ctx)
	if err != nil {
		t.Fatalf("ComplexQuery: %v", err)
	}

	// Matching: Alice(25), Bob(32), Carol excluded(inactive), Dave excluded(45), Eve excluded(18)
	// Active AND 20<=age<=35: Alice(25), Bob(32) → 2 rows
	if len(results) != 2 {
		t.Errorf("expected 2 rows, got %d", len(results))
	}
	// DESC order: Bob(32) first
	if results[0].Age().Int() != 32 {
		t.Errorf("first row age want 32, got %d", results[0].Age().Int())
	}
}

func TestVO_QueryFromSQL_Complex_WithSoftDelete(t *testing.T) {
	m, cleanup := setupVOTableWithSoftDelete(t)
	defer cleanup()
	ctx := context.Background()

	data := []struct {
		id, name, email, status string
		age                     int
	}{
		{"cs-1", "Alice", "a@t.com", StatusActive, 22},
		{"cs-2", "Bob", "b@t.com", StatusActive, 35},
		{"cs-3", "Carol", "c@t.com", StatusActive, 27},
	}
	for _, d := range data {
		insertAccount(t, m, newAccount(d.id, d.name, d.email, d.age, d.status))
	}
	// Soft-delete Carol
	m.writeConn.DB().ExecContext(ctx,
		fmt.Sprintf("UPDATE %s SET deleted_at=NOW() WHERE id=$1", m.tableName), "cs-3")

	qb := m.QueryFromSQL(
		fmt.Sprintf("SELECT * FROM %s WHERE status = $1", m.tableName), StatusActive,
	).OrderBy("age", false)

	page, err := m.Paginate(ctx, 1, 10, qb)
	if err != nil {
		t.Fatalf("QueryFromSQL SoftDelete paginate: %v", err)
	}
	// Carol is soft-deleted → only Alice and Bob remain
	if page.Total != 2 {
		t.Errorf("Total: want 2, got %d", page.Total)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// sql.Scanner verify – value-objects implement interface at compile time
// ─────────────────────────────────────────────────────────────────────────────

func TestVO_ScannerInterfaceSatisfaction(t *testing.T) {
	var _ sql.Scanner = (*UniqueEntityID)(nil)
	var _ sql.Scanner = (*VoName)(nil)
	var _ sql.Scanner = (*VoEmail)(nil)
	var _ sql.Scanner = (*VoAge)(nil)
	var _ sql.Scanner = (*VoStatus)(nil)
	t.Log("all value-objects implement sql.Scanner")
}

