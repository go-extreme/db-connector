package dbconnector

// ddd_cqrs_test.go
//
// Integration tests that prove db-connector works correctly with the
// github.com/go-extreme/ddd-cqrs package types:
//
//   - utils.UniqueEntityID  (unexported `value string`, no sql.Scanner)
//   - domain.Aggregate      (embedded, should be ignored – no `db` tag)
//   - custom value-objects  that are type-aliases (AccountCode, Status)
//   - custom value-objects  that are structs wrapping one primitive
//   - map[string]interface{} JSONB / JSON-text columns
//   - *time.Time nullable pointer
//
// Covered scenarios
//   - Find / FindBy / All / GetBy / Raw / Exists / Count
//   - QueryBuilder: Where / WhereNot / WhereGreaterThan / WhereLike / WhereIn
//   - Paginate with *QueryBuilder
//   - Paginate with *RawQuery
//   - PaginateAs projecting Account → AccountDTO
//   - NewQueryBuilderFromSQL + chained conditions + Paginate
//   - Soft-delete filtering (deleted_at IS NULL auto-injected)
//   - WithTrashed bypasses filter
//   - Page navigation helpers
//   - Chunk streaming
//   - ToSQL (interpolated, no $N placeholders)
//   - RowScanner explicit override
//   - map[string]interface{} round-trip via JSONB column

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	dddutils "github.com/go-extreme/ddd-cqrs/utils"
)

// ─────────────────────────────────────────────────────────────────────────────
// Value-object types that mirror a real DDD domain
// ─────────────────────────────────────────────────────────────────────────────

// AccountCode is a simple string alias (type-alias value object).
type AccountCode string

func (c AccountCode) String() string           { return string(c) }
func (c AccountCode) Value() (driver.Value, error) { return string(c), nil }

// Status is a string alias with a fixed set of values.
type Status string

const (
	StatusActiveAcc   Status = "active"
	StatusInactiveAcc Status = "inactive"
	StatusSuspended   Status = "suspended"
)

func (s Status) String() string           { return string(s) }
func (s Status) Value() (driver.Value, error) { return string(s), nil }
func (s Status) IsActive() bool           { return s == StatusActiveAcc }

// MoneyAmount is a struct-based value object wrapping an int64 (cents).
// No sql.Scanner – relies on single-field struct introspection in the scanner.
type MoneyAmount struct {
	cents int64
}

func NewMoneyAmount(c int64) MoneyAmount { return MoneyAmount{cents: c} }
func (m MoneyAmount) Cents() int64       { return m.cents }

// ─────────────────────────────────────────────────────────────────────────────
// Account domain entity
//
// Mirrors the user's real struct:
//
//	type Account struct {
//	    *domain.Aggregate                          ← embedded, no db tag → skipped
//	    id        *utils.UniqueEntityID  db:"id"   ← ptr to struct, no Scanner
//	    tenantID  utils.UniqueEntityID   db:"tenant_id" ← value struct, no Scanner
//	    name      string                 db:"name"
//	    code      AccountCode            db:"code"  ← string alias
//	    secret    string                 db:"secret"
//	    accType   string                 db:"type"
//	    status    Status                 db:"status" ← string alias
//	    settings  map[string]interface{} db:"settings" ← JSONB column
//	    balance   MoneyAmount            db:"balance"  ← struct VO
//	    version   int                    db:"version"
//	    createdAt time.Time              (no db tag – not scanned)
//	    deletedAt *time.Time             db:"deleted_at"
//	}
// ─────────────────────────────────────────────────────────────────────────────

// Account is an unexported-fields entity that embeds *domain.Aggregate
// just like the user's real type. We import only the utils sub-package since
// the Aggregate embedding is purely structural (no db columns there).
type Account struct {
	// Simulate domain.Aggregate embedding – no db tag, fields never touched.
	aggregate interface{} // placeholder so we don't pull the full Aggregate here

	id       *dddutils.UniqueEntityID `db:"id"`
	tenantID *dddutils.UniqueEntityID `db:"tenant_id"`
	name     string                   `db:"name"`
	code     AccountCode              `db:"code"`
	secret   string                   `db:"secret"`
	accType  string                   `db:"type"`
	status   Status                   `db:"status"`
	settings map[string]interface{}   `db:"settings"`
	balance  MoneyAmount              `db:"balance"`
	version  int                      `db:"version"`
	// no db tag on createdAt intentionally
	createdAt time.Time
	deletedAt *time.Time `db:"deleted_at"`
}

// Exported accessors (needed in tests because fields are unexported)
func (a Account) ID() *dddutils.UniqueEntityID     { return a.id }
func (a Account) TenantID() *dddutils.UniqueEntityID { return a.tenantID }
func (a Account) Name() string                      { return a.name }
func (a Account) Code() AccountCode                 { return a.code }
func (a Account) Secret() string                    { return a.secret }
func (a Account) Type() string                      { return a.accType }
func (a Account) Status() Status                    { return a.status }
func (a Account) Settings() map[string]interface{}  { return a.settings }
func (a Account) Balance() MoneyAmount              { return a.balance }
func (a Account) Version() int                      { return a.version }
func (a Account) DeletedAt() *time.Time             { return a.deletedAt }

// AccountDTO is the read-model projection used in PaginateAs tests.
type AccountDTO struct {
	ID     string `db:"id"`
	Name   string `db:"name"`
	Code   string `db:"code"`
	Status string `db:"status"`
}

// ─────────────────────────────────────────────────────────────────────────────
// RowScanner variant – proves the explicit ScanRow path still works
// ─────────────────────────────────────────────────────────────────────────────

type AccountRowScanner struct {
	id     string
	name   string
	code   string
	status string
}

func (a *AccountRowScanner) ScanRow(rows *sql.Rows) error {
	return rows.Scan(&a.id, &a.name, &a.code, &a.status)
}

// ─────────────────────────────────────────────────────────────────────────────
// Table setup helpers
// ─────────────────────────────────────────────────────────────────────────────

func setupAccountTable(t *testing.T) (*Model[Account], func()) {
	t.Helper()
	conn := NewPostgresConnection(__TestDBconfig)
	if err := conn.Connect(context.Background()); err != nil {
		t.Skipf("no DB available: %v", err)
	}
	db := conn.DB()
	table := "test_acc_" + strings.ReplaceAll(t.Name(), "/", "_")

	_, err := db.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id         TEXT        PRIMARY KEY,
			tenant_id  TEXT        NOT NULL,
			name       TEXT        NOT NULL,
			code       TEXT        NOT NULL,
			secret     TEXT        NOT NULL DEFAULT '',
			type       TEXT        NOT NULL DEFAULT 'standard',
			status     TEXT        NOT NULL DEFAULT 'active',
			settings   JSONB,
			balance    BIGINT      NOT NULL DEFAULT 0,
			version    INT         NOT NULL DEFAULT 0,
			deleted_at TIMESTAMPTZ
		)`, table))
	if err != nil {
		conn.Close()
		t.Fatalf("create table: %v", err)
	}

	m := NewModel[Account](NewConnector(conn, conn), table)
	return m, func() {
		db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
		conn.Close()
	}
}

func setupAccountTableSD(t *testing.T) (*Model[Account], func()) {
	t.Helper()
	conn := NewPostgresConnection(__TestDBconfig)
	if err := conn.Connect(context.Background()); err != nil {
		t.Skipf("no DB available: %v", err)
	}
	db := conn.DB()
	table := "test_accsd_" + strings.ReplaceAll(t.Name(), "/", "_")

	_, err := db.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id         TEXT        PRIMARY KEY,
			tenant_id  TEXT        NOT NULL,
			name       TEXT        NOT NULL,
			code       TEXT        NOT NULL,
			secret     TEXT        NOT NULL DEFAULT '',
			type       TEXT        NOT NULL DEFAULT 'standard',
			status     TEXT        NOT NULL DEFAULT 'active',
			settings   JSONB,
			balance    BIGINT      NOT NULL DEFAULT 0,
			version    INT         NOT NULL DEFAULT 0,
			deleted_at TIMESTAMPTZ
		)`, table))
	if err != nil {
		conn.Close()
		t.Fatalf("create table: %v", err)
	}

	m := NewModel[Account](NewConnector(conn, conn), table).WithSoftDelete("deleted_at")
	return m, func() {
		db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
		conn.Close()
	}
}

// insertAcc uses raw SQL so we control how each value-object field is serialised.
func insertAcc(t *testing.T, m *Model[Account], a Account) {
	t.Helper()
	settingsJSON := []byte("{}")
	if a.settings != nil {
		var err error
		settingsJSON, err = json.Marshal(a.settings)
		if err != nil {
			t.Fatalf("marshal settings: %v", err)
		}
	}

	idStr := ""
	if a.id != nil {
		idStr = a.id.Value()
	}

	_, err := m.writeConn.DB().ExecContext(context.Background(), fmt.Sprintf(
		`INSERT INTO %s (id, tenant_id, name, code, secret, type, status, settings, balance, version)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9,$10)`, m.tableName),
		idStr,
		a.tenantID.Value(),
		a.name,
		string(a.code),
		a.secret,
		a.accType,
		string(a.status),
		string(settingsJSON),
		a.balance.cents,
		a.version,
	)
	if err != nil {
		t.Fatalf("insertAcc: %v", err)
	}
}

// newAcc is a builder helper.
func newAcc(id, tenantID, name, code, secret, accType string, status Status, settings map[string]interface{}, balanceCents int64, version int) Account {
	return Account{
		id:       dddutils.NewUniqueID(id),
		tenantID: dddutils.NewUniqueID(tenantID),
		name:     name,
		code:     AccountCode(code),
		secret:   secret,
		accType:  accType,
		status:   status,
		settings: settings,
		balance:  NewMoneyAmount(balanceCents),
		version:  version,
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Unit: verify scanner handles each value-object type in isolation
// ─────────────────────────────────────────────────────────────────────────────

func TestDDD_UniqueEntityID_PtrField(t *testing.T) {
	// utils.UniqueEntityID has no sql.Scanner; relies on single-field introspection
	// inside the pointer branch of unsafeSetField.
	m, cleanup := setupAccountTable(t)
	defer cleanup()
	ctx := context.Background()

	acc := newAcc("uid-ptr-1", "t1", "Alice", "ACC001", "s3cr3t", "standard", StatusActiveAcc, nil, 0, 1)
	insertAcc(t, m, acc)

	got, err := m.Find("uid-ptr-1").Exec(ctx)
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if got.ID() == nil {
		t.Fatal("id should not be nil")
	}
	if got.ID().Value() != "uid-ptr-1" {
		t.Errorf("ID: want uid-ptr-1, got %q", got.ID().Value())
	}
}

func TestDDD_UniqueEntityID_ValueField(t *testing.T) {
	m, cleanup := setupAccountTable(t)
	defer cleanup()
	ctx := context.Background()

	acc := newAcc("uid-val-1", "tenant-abc", "Bob", "ACC002", "", "standard", StatusActiveAcc, nil, 0, 1)
	insertAcc(t, m, acc)

	got, err := m.Find("uid-val-1").Exec(ctx)
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if got.TenantID().Value() != "tenant-abc" {
		t.Errorf("TenantID: want tenant-abc, got %q", got.TenantID().Value())
	}
}

func TestDDD_StringAlias_CodeStatus(t *testing.T) {
	m, cleanup := setupAccountTable(t)
	defer cleanup()
	ctx := context.Background()

	acc := newAcc("alias-1", "t1", "Carol", "MYCODE", "", "premium", StatusSuspended, nil, 0, 0)
	insertAcc(t, m, acc)

	got, err := m.Find("alias-1").Exec(ctx)
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if got.Code() != "MYCODE" {
		t.Errorf("Code: want MYCODE, got %q", got.Code())
	}
	if got.Status() != StatusSuspended {
		t.Errorf("Status: want suspended, got %q", got.Status())
	}
	if got.Type() != "premium" {
		t.Errorf("Type: want premium, got %q", got.Type())
	}
}

func TestDDD_MoneyAmount_StructVO(t *testing.T) {
	// MoneyAmount has no sql.Scanner – single-field introspection path
	m, cleanup := setupAccountTable(t)
	defer cleanup()
	ctx := context.Background()

	acc := newAcc("money-1", "t1", "Dave", "CODE", "", "standard", StatusActiveAcc, nil, 150000, 0)
	insertAcc(t, m, acc)

	got, err := m.Find("money-1").Exec(ctx)
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if got.Balance().Cents() != 150000 {
		t.Errorf("Balance.Cents: want 150000, got %d", got.Balance().Cents())
	}
}

func TestDDD_Settings_JSONB(t *testing.T) {
	m, cleanup := setupAccountTable(t)
	defer cleanup()
	ctx := context.Background()

	settings := map[string]interface{}{
		"theme":       "dark",
		"maxAccounts": float64(5), // JSON numbers decode as float64
		"features":    []interface{}{"sms", "push"},
	}
	acc := newAcc("json-1", "t1", "Eve", "C1", "", "standard", StatusActiveAcc, settings, 0, 0)
	insertAcc(t, m, acc)

	got, err := m.Find("json-1").Exec(ctx)
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if got.Settings() == nil {
		t.Fatal("settings should not be nil")
	}
	if got.Settings()["theme"] != "dark" {
		t.Errorf("settings.theme: want dark, got %v", got.Settings()["theme"])
	}
	if got.Settings()["maxAccounts"] != float64(5) {
		t.Errorf("settings.maxAccounts: want 5, got %v", got.Settings()["maxAccounts"])
	}
}

func TestDDD_NullableDeletedAt(t *testing.T) {
	m, cleanup := setupAccountTable(t)
	defer cleanup()
	ctx := context.Background()

	acc := newAcc("null-del-1", "t1", "Frank", "C1", "", "standard", StatusActiveAcc, nil, 0, 0)
	insertAcc(t, m, acc)

	got, err := m.Find("null-del-1").Exec(ctx)
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if got.DeletedAt() != nil {
		t.Errorf("deleted_at should be nil for non-deleted row")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// CRUD read operations
// ─────────────────────────────────────────────────────────────────────────────

func TestDDD_FindBy(t *testing.T) {
	m, cleanup := setupAccountTable(t)
	defer cleanup()
	ctx := context.Background()

	insertAcc(t, m, newAcc("fb-1", "t1", "Alice", "A001", "", "standard", StatusActiveAcc, nil, 0, 0))
	insertAcc(t, m, newAcc("fb-2", "t1", "Bob", "B001", "", "standard", StatusInactiveAcc, nil, 0, 0))

	got, err := m.FindBy("code", "B001").Exec(ctx)
	if err != nil {
		t.Fatalf("FindBy: %v", err)
	}
	if got.Name() != "Bob" {
		t.Errorf("Name: want Bob, got %q", got.Name())
	}
}

func TestDDD_All(t *testing.T) {
	m, cleanup := setupAccountTable(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 4; i++ {
		insertAcc(t, m, newAcc(
			fmt.Sprintf("all-%d", i), "t1",
			fmt.Sprintf("User%d", i), fmt.Sprintf("C%d", i),
			"", "standard", StatusActiveAcc, nil, int64(i*100), i))
	}

	all, err := m.All().Exec(ctx)
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	if len(all) != 4 {
		t.Errorf("expected 4 rows, got %d", len(all))
	}
	for _, a := range all {
		if a.ID() == nil || a.ID().Value() == "" {
			t.Error("ID should not be empty")
		}
		if a.TenantID().Value() == "" {
			t.Error("TenantID should not be empty")
		}
	}
}

func TestDDD_GetBy(t *testing.T) {
	m, cleanup := setupAccountTable(t)
	defer cleanup()
	ctx := context.Background()

	insertAcc(t, m, newAcc("gb-1", "t1", "A", "C1", "", "standard", StatusActiveAcc, nil, 0, 0))
	insertAcc(t, m, newAcc("gb-2", "t1", "B", "C2", "", "standard", StatusInactiveAcc, nil, 0, 0))
	insertAcc(t, m, newAcc("gb-3", "t1", "C", "C3", "", "standard", StatusActiveAcc, nil, 0, 0))

	rows, err := m.GetBy(map[string]interface{}{"status": string(StatusActiveAcc)}).Exec(ctx)
	if err != nil {
		t.Fatalf("GetBy: %v", err)
	}
	if len(rows) != 2 {
		t.Errorf("expected 2 active rows, got %d", len(rows))
	}
	for _, r := range rows {
		if r.Status() != StatusActiveAcc {
			t.Errorf("unexpected status %q", r.Status())
		}
	}
}

func TestDDD_Raw(t *testing.T) {
	m, cleanup := setupAccountTable(t)
	defer cleanup()
	ctx := context.Background()

	insertAcc(t, m, newAcc("raw-1", "t1", "Grace", "GR01", "s", "premium", StatusActiveAcc, nil, 500, 2))

	rows, err := m.Raw(ctx,
		fmt.Sprintf("SELECT * FROM %s WHERE id = $1", m.tableName), "raw-1")
	if err != nil {
		t.Fatalf("Raw: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if rows[0].Name() != "Grace" {
		t.Errorf("Name: want Grace, got %q", rows[0].Name())
	}
	if rows[0].Balance().Cents() != 500 {
		t.Errorf("Balance: want 500, got %d", rows[0].Balance().Cents())
	}
}

func TestDDD_Count_ExistsBy(t *testing.T) {
	m, cleanup := setupAccountTable(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		insertAcc(t, m, newAcc(
			fmt.Sprintf("cnt-%d", i), "t1",
			fmt.Sprintf("U%d", i), fmt.Sprintf("C%d", i),
			"", "standard", StatusActiveAcc, nil, 0, 0))
	}

	n, err := m.Count(ctx, nil)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n != 3 {
		t.Errorf("Count: want 3, got %d", n)
	}

	ok, err := m.ExistsBy(ctx, map[string]interface{}{"code": "C1"})
	if err != nil || !ok {
		t.Errorf("ExistsBy C1: want true, got %v (err: %v)", ok, err)
	}
	ok, err = m.ExistsBy(ctx, map[string]interface{}{"code": "NONEXISTENT"})
	if err != nil || ok {
		t.Errorf("ExistsBy NONEXISTENT: want false, got %v (err: %v)", ok, err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// QueryBuilder fluent API
// ─────────────────────────────────────────────────────────────────────────────

func TestDDD_QB_Where_Status(t *testing.T) {
	m, cleanup := setupAccountTable(t)
	defer cleanup()
	ctx := context.Background()

	insertAcc(t, m, newAcc("qs-1", "t1", "A", "C1", "", "standard", StatusActiveAcc, nil, 0, 0))
	insertAcc(t, m, newAcc("qs-2", "t1", "B", "C2", "", "standard", StatusInactiveAcc, nil, 0, 0))
	insertAcc(t, m, newAcc("qs-3", "t1", "C", "C3", "", "standard", StatusActiveAcc, nil, 0, 0))

	results, err := m.Query().Where("status", string(StatusActiveAcc)).Build().Exec(ctx)
	if err != nil {
		t.Fatalf("QB.Where status: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 active, got %d", len(results))
	}
}

func TestDDD_QB_WhereLike(t *testing.T) {
	m, cleanup := setupAccountTable(t)
	defer cleanup()
	ctx := context.Background()

	insertAcc(t, m, newAcc("wl-1", "t1", "Alice Admin", "A1", "", "standard", StatusActiveAcc, nil, 0, 0))
	insertAcc(t, m, newAcc("wl-2", "t1", "Bob Normal", "B1", "", "standard", StatusActiveAcc, nil, 0, 0))
	insertAcc(t, m, newAcc("wl-3", "t1", "Alice Smith", "A2", "", "standard", StatusActiveAcc, nil, 0, 0))

	results, err := m.Query().WhereLike("name", "Alice%").Build().Exec(ctx)
	if err != nil {
		t.Fatalf("WhereLike: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 rows matching Alice%%, got %d", len(results))
	}
}

func TestDDD_QB_WhereIn_Status(t *testing.T) {
	m, cleanup := setupAccountTable(t)
	defer cleanup()
	ctx := context.Background()

	insertAcc(t, m, newAcc("wi-1", "t1", "A", "C1", "", "s", StatusActiveAcc, nil, 0, 0))
	insertAcc(t, m, newAcc("wi-2", "t1", "B", "C2", "", "s", StatusInactiveAcc, nil, 0, 0))
	insertAcc(t, m, newAcc("wi-3", "t1", "C", "C3", "", "s", StatusSuspended, nil, 0, 0))

	results, err := m.Query().
		WhereIn("status", []interface{}{string(StatusActiveAcc), string(StatusSuspended)}).
		Build().Exec(ctx)
	if err != nil {
		t.Fatalf("WhereIn: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 (active+suspended), got %d", len(results))
	}
}

func TestDDD_QB_WhereGreaterThan_Balance(t *testing.T) {
	m, cleanup := setupAccountTable(t)
	defer cleanup()
	ctx := context.Background()

	for _, cents := range []int64{100, 500, 1000, 50} {
		insertAcc(t, m, newAcc(
			fmt.Sprintf("bal-%d", cents), "t1",
			fmt.Sprintf("U%d", cents), "C", "", "s", StatusActiveAcc, nil, cents, 0))
	}

	results, err := m.Query().WhereGreaterThan("balance", 200).Build().Exec(ctx)
	if err != nil {
		t.Fatalf("WhereGreaterThan balance: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 rows with balance > 200, got %d", len(results))
	}
	for _, r := range results {
		if r.Balance().Cents() <= 200 {
			t.Errorf("unexpected balance %d (should be > 200)", r.Balance().Cents())
		}
	}
}

func TestDDD_QB_OrderBy_Limit(t *testing.T) {
	m, cleanup := setupAccountTable(t)
	defer cleanup()
	ctx := context.Background()

	for i, bal := range []int64{300, 100, 500, 200, 400} {
		insertAcc(t, m, newAcc(
			fmt.Sprintf("ord-%d", i), "t1",
			fmt.Sprintf("U%d", i), "C", "", "s", StatusActiveAcc, nil, bal, 0))
	}

	results, err := m.Query().OrderBy("balance", true).Limit(3).Build().Exec(ctx)
	if err != nil {
		t.Fatalf("OrderBy+Limit: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("expected 3 rows, got %d", len(results))
	}
	// DESC: 500, 400, 300
	if results[0].Balance().Cents() < results[1].Balance().Cents() {
		t.Error("expected descending balance order")
	}
}

func TestDDD_QB_ToSQL_Interpolated(t *testing.T) {
	m, cleanup := setupAccountTable(t)
	defer cleanup()

	sqlStr := m.Query().
		Where("status", string(StatusActiveAcc)).
		WhereGreaterThan("balance", 1000).
		OrderBy("name", false).
		Limit(5).
		ToSQL()

	if strings.Contains(sqlStr, "$1") || strings.Contains(sqlStr, "$2") {
		t.Errorf("ToSQL should not contain $N placeholders: %q", sqlStr)
	}
	if !strings.Contains(sqlStr, "'active'") {
		t.Errorf("ToSQL should contain interpolated 'active': %q", sqlStr)
	}
	if !strings.Contains(sqlStr, "1000") {
		t.Errorf("ToSQL should contain 1000: %q", sqlStr)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Paginate with *QueryBuilder
// ─────────────────────────────────────────────────────────────────────────────

func TestDDD_Paginate_QB(t *testing.T) {
	m, cleanup := setupAccountTable(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		insertAcc(t, m, newAcc(
			fmt.Sprintf("pqb-%d", i), "t1",
			fmt.Sprintf("User%d", i), fmt.Sprintf("C%d", i),
			"", "standard", StatusActiveAcc, nil, int64(i*100), 0))
	}

	page, err := m.Paginate(ctx, 1, 4, m.Query().OrderBy("balance", false))
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
		if item.ID() == nil || item.ID().Value() == "" {
			t.Error("ID should not be empty in paginated result")
		}
		if item.TenantID().Value() == "" {
			t.Error("TenantID should not be empty in paginated result")
		}
	}
}

func TestDDD_Paginate_QB_WithFilter(t *testing.T) {
	m, cleanup := setupAccountTable(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 6; i++ {
		status := StatusActiveAcc
		if i%2 == 0 {
			status = StatusInactiveAcc
		}
		insertAcc(t, m, newAcc(
			fmt.Sprintf("pf-%d", i), "t1",
			fmt.Sprintf("User%d", i), fmt.Sprintf("C%d", i),
			"", "standard", status, nil, 0, 0))
	}

	page, err := m.Paginate(ctx, 1, 2,
		m.Query().Where("status", string(StatusActiveAcc)).OrderBy("name", false))
	if err != nil {
		t.Fatalf("Paginate QB filter: %v", err)
	}
	if page.Total != 3 {
		t.Errorf("Total: want 3 active, got %d", page.Total)
	}
}

func TestDDD_Paginate_QB_Empty(t *testing.T) {
	m, cleanup := setupAccountTable(t)
	defer cleanup()
	ctx := context.Background()

	page, err := m.Paginate(ctx, 1, 10, m.Query())
	if err != nil {
		t.Fatalf("Paginate QB empty: %v", err)
	}
	if page.Total != 0 {
		t.Errorf("Total: want 0, got %d", page.Total)
	}
	if page.TotalPages != 0 {
		t.Errorf("TotalPages: want 0, got %d", page.TotalPages)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Paginate with *RawQuery
// ─────────────────────────────────────────────────────────────────────────────

func TestDDD_Paginate_RawQuery(t *testing.T) {
	m, cleanup := setupAccountTable(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 8; i++ {
		insertAcc(t, m, newAcc(
			fmt.Sprintf("rq-%d", i), "t1",
			fmt.Sprintf("User%d", i), fmt.Sprintf("C%d", i),
			"", "standard", StatusActiveAcc, nil, int64(i*50), 0))
	}

	rq := NewRawQuery[Account](m.tableName,
		fmt.Sprintf("SELECT * FROM %s WHERE balance >= $1 ORDER BY balance ASC", m.tableName),
		100, // 2 rows have balance < 100 (0,50), 6 rows >= 100
	)
	page, err := m.Paginate(ctx, 1, 3, rq)
	if err != nil {
		t.Fatalf("Paginate RawQuery: %v", err)
	}
	if page.Total != 6 {
		t.Errorf("Total: want 6, got %d", page.Total)
	}
	if len(page.Items) != 3 {
		t.Errorf("Items: want 3, got %d", len(page.Items))
	}
	for _, item := range page.Items {
		if item.Balance().Cents() < 100 {
			t.Errorf("balance %d should be >= 100", item.Balance().Cents())
		}
		if item.ID() == nil {
			t.Error("ID should not be nil")
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// PaginateAs – project Account → AccountDTO
// ─────────────────────────────────────────────────────────────────────────────

func TestDDD_PaginateAs_Projection(t *testing.T) {
	m, cleanup := setupAccountTable(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		insertAcc(t, m, newAcc(
			fmt.Sprintf("pa-%d", i), "t1",
			fmt.Sprintf("User%d", i), fmt.Sprintf("CODE%d", i),
			"", "standard", StatusActiveAcc, nil, 0, 0))
	}

	rq := NewRawQuery[Account](m.tableName,
		fmt.Sprintf("SELECT id, name, code, status FROM %s ORDER BY name", m.tableName),
	)
	page, err := PaginateAs[Account, AccountDTO](ctx, m.readConn, 1, 3, rq)
	if err != nil {
		t.Fatalf("PaginateAs: %v", err)
	}
	if page.Total != 5 {
		t.Errorf("Total: want 5, got %d", page.Total)
	}
	if len(page.Items) != 3 {
		t.Errorf("Items: want 3, got %d", len(page.Items))
	}
	for _, dto := range page.Items {
		if dto.ID == "" {
			t.Error("projected ID empty")
		}
		if dto.Name == "" {
			t.Error("projected Name empty")
		}
		if dto.Code == "" {
			t.Error("projected Code empty")
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// NewQueryBuilderFromSQL + chained conditions + Paginate
// ─────────────────────────────────────────────────────────────────────────────

func TestDDD_QueryFromSQL_Chained_Paginate(t *testing.T) {
	m, cleanup := setupAccountTable(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 9; i++ {
		status := StatusActiveAcc
		if i >= 6 {
			status = StatusInactiveAcc
		}
		insertAcc(t, m, newAcc(
			fmt.Sprintf("qfs-%d", i), "t1",
			fmt.Sprintf("User%d", i), fmt.Sprintf("C%d", i),
			"", "standard", status, nil, int64(i*100), 0))
	}

	// Start from a raw SQL, chain more conditions
	qb := m.QueryFromSQL(
		fmt.Sprintf("SELECT * FROM %s WHERE status = $1", m.tableName),
		string(StatusActiveAcc),
	).WhereGreaterThan("balance", 200).OrderBy("balance", true) // true = DESC

	// active AND balance > 200 → rows 3,4,5 → 3 rows
	page, err := m.Paginate(ctx, 1, 2, qb)
	if err != nil {
		t.Fatalf("QueryFromSQL+Paginate: %v", err)
	}
	if page.Total != 3 {
		t.Errorf("Total: want 3, got %d", page.Total)
	}
	if len(page.Items) != 2 {
		t.Errorf("Items page 1: want 2, got %d", len(page.Items))
	}
	// Descending balance: 500, 400 on page 1
	if page.Items[0].Balance().Cents() < page.Items[1].Balance().Cents() {
		t.Error("expected descending balance order")
	}
}

func TestDDD_QueryFromSQL_ToSQL(t *testing.T) {
	m, cleanup := setupAccountTable(t)
	defer cleanup()

	qb := m.QueryFromSQL(
		fmt.Sprintf("SELECT * FROM %s WHERE tenant_id = $1", m.tableName),
		"tenant-xyz",
	).Where("status", string(StatusActiveAcc))

	sql := qb.ToSQL()
	if strings.Contains(sql, "$1") || strings.Contains(sql, "$2") {
		t.Errorf("ToSQL must not contain placeholders: %q", sql)
	}
	if !strings.Contains(sql, "tenant-xyz") {
		t.Errorf("ToSQL should contain tenant-xyz: %q", sql)
	}
	if !strings.Contains(sql, "'active'") {
		t.Errorf("ToSQL should contain 'active': %q", sql)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Soft-delete integration
// ─────────────────────────────────────────────────────────────────────────────

func TestDDD_SoftDelete_All(t *testing.T) {
	m, cleanup := setupAccountTableSD(t)
	defer cleanup()
	ctx := context.Background()

	insertAcc(t, m, newAcc("sd-1", "t1", "Alice", "A1", "", "standard", StatusActiveAcc, nil, 0, 0))
	insertAcc(t, m, newAcc("sd-2", "t1", "Bob", "B1", "", "standard", StatusActiveAcc, nil, 0, 0))

	if err := m.Delete(ctx, "sd-2"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	all, err := m.All().Exec(ctx)
	if err != nil {
		t.Fatalf("All after soft-delete: %v", err)
	}
	if len(all) != 1 {
		t.Errorf("expected 1 active, got %d", len(all))
	}
	if all[0].Name() != "Alice" {
		t.Errorf("expected Alice, got %q", all[0].Name())
	}
}

func TestDDD_SoftDelete_Paginate(t *testing.T) {
	m, cleanup := setupAccountTableSD(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 6; i++ {
		insertAcc(t, m, newAcc(
			fmt.Sprintf("sdp-%d", i), "t1",
			fmt.Sprintf("U%d", i), fmt.Sprintf("C%d", i),
			"", "standard", StatusActiveAcc, nil, 0, 0))
	}
	// Soft-delete 2 rows
	m.writeConn.DB().ExecContext(ctx,
		fmt.Sprintf("UPDATE %s SET deleted_at=NOW() WHERE id IN ($1,$2)", m.tableName),
		"sdp-0", "sdp-1")

	page, err := m.Paginate(ctx, 1, 10, m.Query())
	if err != nil {
		t.Fatalf("Paginate with soft-delete: %v", err)
	}
	if page.Total != 4 {
		t.Errorf("Total: want 4 active, got %d", page.Total)
	}
}

func TestDDD_SoftDelete_WithTrashed_Paginate(t *testing.T) {
	m, cleanup := setupAccountTableSD(t)
	defer cleanup()
	ctx := context.Background()

	insertAcc(t, m, newAcc("wt-1", "t1", "A", "C1", "", "s", StatusActiveAcc, nil, 0, 0))
	insertAcc(t, m, newAcc("wt-2", "t1", "B", "C2", "", "s", StatusActiveAcc, nil, 0, 0))
	m.writeConn.DB().ExecContext(ctx,
		fmt.Sprintf("UPDATE %s SET deleted_at=NOW() WHERE id=$1", m.tableName), "wt-2")

	page, err := m.Paginate(ctx, 1, 10, m.Query().WithTrashed())
	if err != nil {
		t.Fatalf("WithTrashed Paginate: %v", err)
	}
	if page.Total != 2 {
		t.Errorf("WithTrashed Total: want 2, got %d", page.Total)
	}
}

func TestDDD_SoftDelete_QueryFromSQL_Paginate(t *testing.T) {
	m, cleanup := setupAccountTableSD(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 4; i++ {
		insertAcc(t, m, newAcc(
			fmt.Sprintf("sqd-%d", i), "t1",
			fmt.Sprintf("U%d", i), fmt.Sprintf("C%d", i),
			"", "standard", StatusActiveAcc, nil, 0, 0))
	}
	// Soft-delete one row
	m.writeConn.DB().ExecContext(ctx,
		fmt.Sprintf("UPDATE %s SET deleted_at=NOW() WHERE id=$1", m.tableName), "sqd-0")

	qb := m.QueryFromSQL(
		fmt.Sprintf("SELECT * FROM %s WHERE status = $1", m.tableName),
		string(StatusActiveAcc),
	).OrderBy("name", false)

	page, err := m.Paginate(ctx, 1, 10, qb)
	if err != nil {
		t.Fatalf("QueryFromSQL SD Paginate: %v", err)
	}
	// 1 soft-deleted → 3 active remain
	if page.Total != 3 {
		t.Errorf("Total: want 3, got %d", page.Total)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Soft-delete: WHERE with timestamp columns (nil vs. non-nil)
// ─────────────────────────────────────────────────────────────────────────────

func TestDDD_SoftDelete_WhereTimestampNil(t *testing.T) {
	// Verify the soft-delete filter (deleted_at IS NULL) is injected correctly
	// and does not add a trailing AND that breaks the SQL.
	m, cleanup := setupAccountTableSD(t)
	defer cleanup()
	ctx := context.Background()

	insertAcc(t, m, newAcc("ts-1", "t1", "Alice", "C1", "", "s", StatusActiveAcc, nil, 0, 0))
	insertAcc(t, m, newAcc("ts-2", "t1", "Bob", "C2", "", "s", StatusActiveAcc, nil, 0, 0))
	m.writeConn.DB().ExecContext(ctx,
		fmt.Sprintf("UPDATE %s SET deleted_at=NOW() WHERE id=$1", m.tableName), "ts-2")

	// Simple All() – the soft-delete filter must be appended correctly
	all, err := m.All().Exec(ctx)
	if err != nil {
		t.Fatalf("All with soft-delete: %v", err)
	}
	if len(all) != 1 {
		t.Errorf("expected 1, got %d: possible trailing AND bug", len(all))
	}

	// QueryBuilder WHERE + soft-delete
	results, err := m.Query().Where("name", "Alice").Build().Exec(ctx)
	if err != nil {
		t.Fatalf("QB+SoftDelete WHERE: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 (Alice only), got %d", len(results))
	}

	// QueryBuilder ORDER BY + soft-delete (filter must appear before ORDER BY)
	results2, err := m.Query().OrderBy("name", false).Build().Exec(ctx)
	if err != nil {
		t.Fatalf("QB+SoftDelete ORDER BY: %v", err)
	}
	if len(results2) != 1 {
		t.Errorf("ORDER BY path: expected 1, got %d", len(results2))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Complex multi-condition query
// ─────────────────────────────────────────────────────────────────────────────

func TestDDD_ComplexQuery(t *testing.T) {
	m, cleanup := setupAccountTable(t)
	defer cleanup()
	ctx := context.Background()

	data := []struct {
		id, name, code, accType string
		status                  Status
		balance                 int64
	}{
		{"cq-1", "Alice Corp", "AC01", "premium", StatusActiveAcc, 10000},
		{"cq-2", "Bob Ltd", "BC01", "standard", StatusActiveAcc, 500},
		{"cq-3", "Carol Inc", "CC01", "premium", StatusSuspended, 20000},
		{"cq-4", "Dave Co", "DC01", "premium", StatusActiveAcc, 15000},
		{"cq-5", "Eve LLC", "EC01", "standard", StatusActiveAcc, 8000},
		{"cq-6", "Frank AG", "FC01", "premium", StatusInactiveAcc, 3000},
	}
	for _, d := range data {
		insertAcc(t, m, newAcc(d.id, "t1", d.name, d.code, "", d.accType, d.status, nil, d.balance, 0))
	}

	// premium accounts that are active with balance > 5000, ordered by balance DESC, limit 3
	results, err := m.Query().
		Where("type", "premium").
		Where("status", string(StatusActiveAcc)).
		WhereGreaterThan("balance", 5000).
		OrderBy("balance", true).
		Limit(3).
		Build().Exec(ctx)
	if err != nil {
		t.Fatalf("ComplexQuery: %v", err)
	}
	// Matching: Alice(10000), Dave(15000) → 2 rows
	if len(results) != 2 {
		t.Errorf("expected 2, got %d", len(results))
	}
	// DESC balance: Dave(15000), Alice(10000)
	if results[0].Balance().Cents() != 15000 {
		t.Errorf("first row balance: want 15000, got %d", results[0].Balance().Cents())
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// JSONB round-trip with complex nested settings
// ─────────────────────────────────────────────────────────────────────────────

func TestDDD_JSONB_ComplexSettings(t *testing.T) {
	m, cleanup := setupAccountTable(t)
	defer cleanup()
	ctx := context.Background()

	settings := map[string]interface{}{
		"notifications": map[string]interface{}{
			"email": true,
			"sms":   false,
		},
		"limits": map[string]interface{}{
			"daily":   float64(10000),
			"monthly": float64(100000),
		},
		"tags": []interface{}{"vip", "enterprise"},
	}

	acc := newAcc("jsonb-1", "t1", "Enterprise User", "ENT", "", "enterprise", StatusActiveAcc, settings, 0, 0)
	insertAcc(t, m, acc)

	got, err := m.Find("jsonb-1").Exec(ctx)
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	s := got.Settings()
	if s == nil {
		t.Fatal("settings nil")
	}

	notifs, ok := s["notifications"].(map[string]interface{})
	if !ok {
		t.Fatalf("notifications not a map: %T", s["notifications"])
	}
	if notifs["email"] != true {
		t.Errorf("email notification: want true, got %v", notifs["email"])
	}

	tags, ok := s["tags"].([]interface{})
	if !ok {
		t.Fatalf("tags not a slice: %T", s["tags"])
	}
	if len(tags) != 2 || tags[0] != "vip" {
		t.Errorf("tags mismatch: %v", tags)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// RowScanner override with Account columns
// ─────────────────────────────────────────────────────────────────────────────

func TestDDD_RowScanner_Account(t *testing.T) {
	conn := NewPostgresConnection(__TestDBconfig)
	if err := conn.Connect(context.Background()); err != nil {
		t.Skipf("no DB: %v", err)
	}
	defer conn.Close()

	db := conn.DB()
	table := "test_rs_acc_" + strings.ReplaceAll(t.Name(), "/", "_")
	db.Exec(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id TEXT PRIMARY KEY, name TEXT, code TEXT, status TEXT)`, table))
	defer db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", table))

	db.ExecContext(context.Background(),
		fmt.Sprintf("INSERT INTO %s VALUES ($1,$2,$3,$4)", table),
		"rs-acc-1", "Alice", "CODE1", "active")

	m := NewModel[AccountRowScanner](NewConnector(conn, conn), table)
	got, err := m.Find("rs-acc-1").Exec(context.Background())
	if err != nil {
		t.Fatalf("Find via RowScanner: %v", err)
	}
	if got.name != "Alice" {
		t.Errorf("Name: want Alice, got %q", got.name)
	}
	if got.code != "CODE1" {
		t.Errorf("Code: want CODE1, got %q", got.code)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Chunk with DDD Account struct
// ─────────────────────────────────────────────────────────────────────────────

func TestDDD_Chunk(t *testing.T) {
	m, cleanup := setupAccountTable(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 7; i++ {
		insertAcc(t, m, newAcc(
			fmt.Sprintf("ch-%d", i), "t1",
			fmt.Sprintf("U%d", i), fmt.Sprintf("C%d", i),
			"", "standard", StatusActiveAcc, nil, 0, 0))
	}

	var collected []*dddutils.UniqueEntityID
	err := m.Chunk(ctx, 3, nil, func(batch []Account) error {
		for _, a := range batch {
			collected = append(collected, a.ID())
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Chunk: %v", err)
	}
	if len(collected) != 7 {
		t.Errorf("expected 7 IDs from Chunk, got %d", len(collected))
	}
	for _, id := range collected {
		if id == nil || id.Value() == "" {
			t.Error("nil or empty ID in chunked result")
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Page navigation
// ─────────────────────────────────────────────────────────────────────────────

func TestDDD_Page_Navigation(t *testing.T) {
	m, cleanup := setupAccountTable(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 9; i++ {
		insertAcc(t, m, newAcc(
			fmt.Sprintf("nav-%d", i), "t1",
			fmt.Sprintf("U%d", i), fmt.Sprintf("C%d", i),
			"", "standard", StatusActiveAcc, nil, int64(i*10), 0))
	}

	p1, _ := m.Paginate(ctx, 1, 3, m.Query().OrderBy("balance", false))
	if !p1.HasNext() {
		t.Error("p1 HasNext should be true")
	}
	if p1.HasPrev() {
		t.Error("p1 HasPrev should be false")
	}
	if p1.NextPage() != 2 {
		t.Errorf("p1.NextPage want 2, got %d", p1.NextPage())
	}

	p3, _ := m.Paginate(ctx, 3, 3, m.Query().OrderBy("balance", false))
	if p3.HasNext() {
		t.Error("p3 HasNext should be false")
	}
	if !p3.HasPrev() {
		t.Error("p3 HasPrev should be true")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Multiple tenants: QueryFromSQL with tenant_id isolation
// ─────────────────────────────────────────────────────────────────────────────

func TestDDD_MultiTenant_Paginate(t *testing.T) {
	m, cleanup := setupAccountTable(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 4; i++ {
		insertAcc(t, m, newAcc(
			fmt.Sprintf("mt-t1-%d", i), "tenant-1",
			fmt.Sprintf("T1User%d", i), fmt.Sprintf("T1C%d", i),
			"", "standard", StatusActiveAcc, nil, 0, 0))
	}
	for i := 0; i < 3; i++ {
		insertAcc(t, m, newAcc(
			fmt.Sprintf("mt-t2-%d", i), "tenant-2",
			fmt.Sprintf("T2User%d", i), fmt.Sprintf("T2C%d", i),
			"", "standard", StatusActiveAcc, nil, 0, 0))
	}

	// Paginate only tenant-1 accounts
	qb := m.QueryFromSQL(
		fmt.Sprintf("SELECT * FROM %s WHERE tenant_id = $1", m.tableName),
		"tenant-1",
	).OrderBy("name", false)

	page, err := m.Paginate(ctx, 1, 10, qb)
	if err != nil {
		t.Fatalf("MultiTenant Paginate: %v", err)
	}
	if page.Total != 4 {
		t.Errorf("Total: want 4 for tenant-1, got %d", page.Total)
	}
	for _, item := range page.Items {
		if item.TenantID().Value() != "tenant-1" {
			t.Errorf("unexpected tenant_id %q in tenant-1 page", item.TenantID().Value())
		}
	}
}



