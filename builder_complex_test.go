package dbconnector

// builder_complex_test.go
// End-to-end integration tests for QueryBuilder with complex queries against
// a real PostgreSQL database.  Each test seeds its own isolated table and
// cleans it up on exit so tests can run in parallel without interference.

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

// OrderItem is the richer domain type used in complex-query tests.
type OrderItem struct {
	ID         string    `db:"id"`
	CustomerID string    `db:"customer_id"`
	Product    string    `db:"product"`
	Category   string    `db:"category"`
	Amount     float64   `db:"amount"`
	Qty        int       `db:"qty"`
	Status     string    `db:"status"`
	CreatedAt  time.Time `db:"created_at"`
}

// setupOrdersTable creates an isolated table with OrderItem schema and returns
// the model and a cleanup func.
func setupOrdersTable(t *testing.T) (*Model[OrderItem], func()) {
	t.Helper()
	conn := NewPostgresConnection(__TestDBconfig)
	if err := conn.Connect(context.Background()); err != nil {
		t.Skipf("no DB available: %v", err)
	}

	db := conn.DB()
	table := "test_orders_" + strings.ReplaceAll(t.Name(), "/", "_")
	_, err := db.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id          TEXT        PRIMARY KEY,
			customer_id TEXT        NOT NULL,
			product     TEXT        NOT NULL,
			category    TEXT        NOT NULL,
			amount      NUMERIC     NOT NULL,
			qty         INT         NOT NULL DEFAULT 1,
			status      TEXT        NOT NULL DEFAULT 'pending',
			created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`, table))
	if err != nil {
		conn.Close()
		t.Fatalf("create table: %v", err)
	}

	m := NewModel[OrderItem](NewConnector(conn, conn), table)
	cleanup := func() {
		db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
		conn.Close()
	}
	return m, cleanup
}

// seedOrders inserts a fixed, deterministic dataset into the model's table.
func seedOrders(t *testing.T, m *Model[OrderItem]) {
	t.Helper()
	ctx := context.Background()
	now := time.Now()

	rows := []OrderItem{
		{ID: "o01", CustomerID: "c1", Product: "Laptop", Category: "electronics", Amount: 1200.00, Qty: 1, Status: "completed", CreatedAt: now.Add(-10 * 24 * time.Hour)},
		{ID: "o02", CustomerID: "c1", Product: "Mouse", Category: "electronics", Amount: 25.00, Qty: 2, Status: "completed", CreatedAt: now.Add(-9 * 24 * time.Hour)},
		{ID: "o03", CustomerID: "c2", Product: "Desk", Category: "furniture", Amount: 450.00, Qty: 1, Status: "completed", CreatedAt: now.Add(-8 * 24 * time.Hour)},
		{ID: "o04", CustomerID: "c2", Product: "Chair", Category: "furniture", Amount: 300.00, Qty: 2, Status: "pending", CreatedAt: now.Add(-7 * 24 * time.Hour)},
		{ID: "o05", CustomerID: "c3", Product: "Monitor", Category: "electronics", Amount: 600.00, Qty: 1, Status: "completed", CreatedAt: now.Add(-6 * 24 * time.Hour)},
		{ID: "o06", CustomerID: "c3", Product: "Keyboard", Category: "electronics", Amount: 120.00, Qty: 1, Status: "pending", CreatedAt: now.Add(-5 * 24 * time.Hour)},
		{ID: "o07", CustomerID: "c4", Product: "Lamp", Category: "furniture", Amount: 60.00, Qty: 3, Status: "cancelled", CreatedAt: now.Add(-4 * 24 * time.Hour)},
		{ID: "o08", CustomerID: "c4", Product: "Notebook", Category: "stationery", Amount: 8.00, Qty: 10, Status: "completed", CreatedAt: now.Add(-3 * 24 * time.Hour)},
		{ID: "o09", CustomerID: "c5", Product: "Headphones", Category: "electronics", Amount: 250.00, Qty: 1, Status: "pending", CreatedAt: now.Add(-2 * 24 * time.Hour)},
		{ID: "o10", CustomerID: "c5", Product: "USB Hub", Category: "electronics", Amount: 40.00, Qty: 2, Status: "completed", CreatedAt: now.Add(-1 * 24 * time.Hour)},
	}

	if err := m.BatchCreate(ctx, rows, 50); err != nil {
		t.Fatalf("seed: %v", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 1. Multi-condition AND chain
// ─────────────────────────────────────────────────────────────────────────────

func TestBuilderComplex_MultiConditionAND(t *testing.T) {
	m, cleanup := setupOrdersTable(t)
	defer cleanup()
	seedOrders(t, m)
	ctx := context.Background()

	// electronics + completed + amount > 100
	results, err := m.Query().
		Where("category", "electronics").
		Where("status", "completed").
		WhereGreaterThan("amount", 100.0).
		OrderBy("amount", true). // DESC: Laptop(1200) first
		Build().Exec(ctx)

	if err != nil {
		t.Fatalf("exec: %v", err)
	}
	// Laptop(1200), Monitor(600)
	if len(results) != 2 {
		t.Errorf("expected 2 rows, got %d: %+v", len(results), results)
	}
	if results[0].Product != "Laptop" {
		t.Errorf("expected Laptop first, got %s", results[0].Product)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. WhereIn + WhereNotIn
// ─────────────────────────────────────────────────────────────────────────────

func TestBuilderComplex_WhereIn_WhereNotIn(t *testing.T) {
	m, cleanup := setupOrdersTable(t)
	defer cleanup()
	seedOrders(t, m)
	ctx := context.Background()

	// customers c1 or c2, but exclude cancelled
	results, err := m.Query().
		WhereIn("customer_id", []interface{}{"c1", "c2"}).
		WhereNotIn("status", []interface{}{"cancelled"}).
		OrderBy("id", false).
		Build().Exec(ctx)

	if err != nil {
		t.Fatalf("exec: %v", err)
	}
	// o01,o02 (c1 completed), o03 (c2 completed), o04 (c2 pending) = 4 rows
	if len(results) != 4 {
		t.Errorf("expected 4 rows, got %d", len(results))
	}
	for _, r := range results {
		if r.CustomerID != "c1" && r.CustomerID != "c2" {
			t.Errorf("unexpected customer_id: %s", r.CustomerID)
		}
		if r.Status == "cancelled" {
			t.Errorf("cancelled order should have been excluded: %+v", r)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. WhereBetween on numeric amount
// ─────────────────────────────────────────────────────────────────────────────

func TestBuilderComplex_WhereBetween(t *testing.T) {
	m, cleanup := setupOrdersTable(t)
	defer cleanup()
	seedOrders(t, m)
	ctx := context.Background()

	// amount between 100 and 500 inclusive
	results, err := m.Query().
		WhereBetween("amount", 100.0, 500.0).
		OrderBy("amount", false).
		Build().Exec(ctx)

	if err != nil {
		t.Fatalf("exec: %v", err)
	}
	// Chair(300), Monitor is 600 (out), Desk(450), Keyboard(120), Headphones(250) = 4
	for _, r := range results {
		if r.Amount < 100 || r.Amount > 500 {
			t.Errorf("amount %v out of [100,500]: %s", r.Amount, r.Product)
		}
	}
	if len(results) != 4 {
		t.Errorf("expected 4 rows, got %d", len(results))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 4. OR groups
// ─────────────────────────────────────────────────────────────────────────────

func TestBuilderComplex_OrGroups(t *testing.T) {
	m, cleanup := setupOrdersTable(t)
	defer cleanup()
	seedOrders(t, m)
	ctx := context.Background()

	// (category = 'stationery') OR (category = 'furniture' AND status = 'completed')
	results, err := m.Query().
		Or(func(qb *QueryBuilder[OrderItem]) {
			qb.Where("category", "stationery")
		}).
		OrWhere(func(qb *QueryBuilder[OrderItem]) {
			qb.Where("category", "furniture").Where("status", "completed")
		}).
		OrderBy("id", false).
		Build().Exec(ctx)

	if err != nil {
		t.Fatalf("exec: %v", err)
	}
	// o08 (stationery), o03 (furniture+completed) = 2
	if len(results) != 2 {
		t.Errorf("expected 2 rows, got %d: %+v", len(results), results)
	}
	ids := map[string]bool{}
	for _, r := range results {
		ids[r.ID] = true
	}
	if !ids["o08"] || !ids["o03"] {
		t.Errorf("expected o03 and o08, got %v", ids)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 5. WhereLike / WhereILike
// ─────────────────────────────────────────────────────────────────────────────

func TestBuilderComplex_WhereLike_WhereILike(t *testing.T) {
	m, cleanup := setupOrdersTable(t)
	defer cleanup()
	seedOrders(t, m)
	ctx := context.Background()

	// LIKE: products ending with 'k' (Desk, Notebook) — "Keyboard" ends with 'd'
	like, err := m.Query().
		WhereLike("product", "%k").
		OrderBy("product", false).
		Build().Exec(ctx)
	if err != nil {
		t.Fatalf("LIKE exec: %v", err)
	}
	if len(like) != 2 {
		t.Errorf("LIKE: expected 2, got %d", len(like))
	}

	// ILIKE: case-insensitive match for 'LAPTOP'
	ilike, err := m.Query().
		WhereILike("product", "LAPTOP").
		Build().Exec(ctx)
	if err != nil {
		t.Fatalf("ILIKE exec: %v", err)
	}
	if len(ilike) != 1 || ilike[0].Product != "Laptop" {
		t.Errorf("ILIKE: expected [Laptop], got %+v", ilike)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 6. WhereGreaterThanOrEqual / WhereLessThanOrEqual
// ─────────────────────────────────────────────────────────────────────────────

func TestBuilderComplex_GTE_LTE(t *testing.T) {
	m, cleanup := setupOrdersTable(t)
	defer cleanup()
	seedOrders(t, m)
	ctx := context.Background()

	// qty >= 2 AND qty <= 3
	results, err := m.Query().
		WhereGreaterThanOrEqual("qty", 2).
		WhereLessThanOrEqual("qty", 3).
		OrderBy("qty", false).
		Build().Exec(ctx)

	if err != nil {
		t.Fatalf("exec: %v", err)
	}
	// Mouse(qty=2), Chair(qty=2), Lamp(qty=3), USB Hub(qty=2) = 4
	if len(results) != 4 {
		t.Errorf("expected 4 rows, got %d", len(results))
	}
	for _, r := range results {
		if r.Qty < 2 || r.Qty > 3 {
			t.Errorf("qty %d out of [2,3]: %s", r.Qty, r.Product)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 7. WhereRaw with multiple placeholders
// ─────────────────────────────────────────────────────────────────────────────

func TestBuilderComplex_WhereRaw(t *testing.T) {
	m, cleanup := setupOrdersTable(t)
	defer cleanup()
	seedOrders(t, m)
	ctx := context.Background()

	// raw: amount * qty > threshold using WhereRaw
	results, err := m.Query().
		WhereRaw("amount * qty > ?", 200.0).
		OrderBy("amount", true).
		Build().Exec(ctx)

	if err != nil {
		t.Fatalf("exec: %v", err)
	}
	// Laptop:1200*1=1200, Desk:450*1=450, Chair:300*2=600, Monitor:600*1=600, Notebook:8*10=80(no), Headphones:250 > 200
	for _, r := range results {
		if r.Amount*float64(r.Qty) <= 200 {
			t.Errorf("amount*qty=%v <= 200 for %s", r.Amount*float64(r.Qty), r.Product)
		}
	}
	if len(results) == 0 {
		t.Error("expected at least one row")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 8. WhereExists subquery
// ─────────────────────────────────────────────────────────────────────────────

func TestBuilderComplex_WhereExists(t *testing.T) {
	m, cleanup := setupOrdersTable(t)
	defer cleanup()
	seedOrders(t, m)
	ctx := context.Background()

	table := m.tableName

	// EXISTS: customers that have at least one completed order
	// Query: distinct customer_ids that exist in completed rows
	// We self-join using EXISTS subquery
	results, err := m.Query().
		WhereExists(
			fmt.Sprintf("SELECT 1 FROM %s sub WHERE sub.customer_id = %s.customer_id AND sub.status = ?", table, table),
			"completed",
		).
		WhereNot("status", "completed"). // pick non-completed rows of those customers
		OrderBy("id", false).
		Build().Exec(ctx)

	if err != nil {
		t.Fatalf("exec: %v", err)
	}
	// c1..c5 all have at least one completed order;
	// non-completed rows: o04(c2 pending), o06(c3 pending), o07(c4 cancelled), o09(c5 pending)
	if len(results) != 4 {
		t.Errorf("expected 4, got %d: %+v", len(results), results)
	}
	for _, r := range results {
		if r.Status == "completed" {
			t.Errorf("expected non-completed, got: %+v", r)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 9. WhereNotExists subquery
// ─────────────────────────────────────────────────────────────────────────────

func TestBuilderComplex_WhereNotExists(t *testing.T) {
	m, cleanup := setupOrdersTable(t)
	defer cleanup()
	seedOrders(t, m)
	ctx := context.Background()

	table := m.tableName

	// Customers that have NO cancelled orders
	results, err := m.Query().
		WhereNotExists(
			fmt.Sprintf("SELECT 1 FROM %s sub WHERE sub.customer_id = %s.customer_id AND sub.status = ?", table, table),
			"cancelled",
		).
		OrderBy("id", false).
		Build().Exec(ctx)

	if err != nil {
		t.Fatalf("exec: %v", err)
	}
	// c4 has a cancelled order (o07); all c4 rows should be excluded
	for _, r := range results {
		if r.CustomerID == "c4" {
			t.Errorf("c4 should be excluded (has cancelled), got: %+v", r)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 10. OrderByMultiple + Limit + Offset
// ─────────────────────────────────────────────────────────────────────────────

func TestBuilderComplex_OrderByMultiple_LimitOffset(t *testing.T) {
	m, cleanup := setupOrdersTable(t)
	defer cleanup()
	seedOrders(t, m)
	ctx := context.Background()

	// All completed, ordered by category ASC then amount DESC, take rows 3-5 (offset=2 limit=3)
	results, err := m.Query().
		Where("status", "completed").
		OrderByMultiple("category ASC", "amount DESC").
		Limit(3).
		Offset(2).
		Build().Exec(ctx)

	if err != nil {
		t.Fatalf("exec: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("expected 3 rows, got %d", len(results))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 11. Select specific columns (projection)
// ─────────────────────────────────────────────────────────────────────────────

func TestBuilderComplex_SelectColumns(t *testing.T) {
	m, cleanup := setupOrdersTable(t)
	defer cleanup()
	seedOrders(t, m)
	ctx := context.Background()

	results, err := m.Query().
		Select("id", "product", "amount").
		Where("category", "electronics").
		OrderBy("amount", true).
		Build().Exec(ctx)

	if err != nil {
		t.Fatalf("exec: %v", err)
	}
	// 6 electronics rows
	if len(results) != 6 {
		t.Errorf("expected 6 electronics, got %d", len(results))
	}
	// Non-selected fields should be zero values
	for _, r := range results {
		if r.Category != "" {
			t.Errorf("category should be empty (not selected), got %q", r.Category)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 12. Clone independence — two branches from the same base query
// ─────────────────────────────────────────────────────────────────────────────

func TestBuilderComplex_Clone_Independence(t *testing.T) {
	m, cleanup := setupOrdersTable(t)
	defer cleanup()
	seedOrders(t, m)
	ctx := context.Background()

	base := m.Query().Where("category", "electronics")

	completedQ := base.Clone().Where("status", "completed").OrderBy("amount", true).Build()
	pendingQ := base.Clone().Where("status", "pending").OrderBy("amount", true).Build()

	completed, err := completedQ.Exec(ctx)
	if err != nil {
		t.Fatalf("completed exec: %v", err)
	}
	pending, err := pendingQ.Exec(ctx)
	if err != nil {
		t.Fatalf("pending exec: %v", err)
	}

	// Verify no cross-contamination
	for _, r := range completed {
		if r.Status != "completed" {
			t.Errorf("completed branch has non-completed: %+v", r)
		}
	}
	for _, r := range pending {
		if r.Status != "pending" {
			t.Errorf("pending branch has non-pending: %+v", r)
		}
	}
	// Together they should cover all electronics (6 total, no cancelled in electronics)
	if len(completed)+len(pending) != 6 {
		t.Errorf("completed(%d)+pending(%d) should equal 6", len(completed), len(pending))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 13. WhereDateBetween on created_at
// ─────────────────────────────────────────────────────────────────────────────

func TestBuilderComplex_WhereDateBetween(t *testing.T) {
	m, cleanup := setupOrdersTable(t)
	defer cleanup()
	seedOrders(t, m)
	ctx := context.Background()

	now := time.Now()
	from := now.Add(-5 * 24 * time.Hour) // 5 days ago
	to := now.Add(-3 * 24 * time.Hour)   // 3 days ago

	results, err := m.Query().
		WhereDateBetween("created_at", from, to).
		OrderBy("created_at", false).
		Build().Exec(ctx)

	if err != nil {
		t.Fatalf("exec: %v", err)
	}
	// o06 (-5d), o07 (-4d), o08 (-3d) = 3 rows
	if len(results) != 3 {
		t.Errorf("expected 3 rows in date range, got %d", len(results))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 14. WhereNull / WhereNotNull
// ─────────────────────────────────────────────────────────────────────────────

func TestBuilderComplex_WhereNull_WhereNotNull_Integration(t *testing.T) {
	// Use the standard User model which has a nullable email column
	m, cleanup := setupTable(t)
	defer cleanup()
	ctx := context.Background()

	m.Create(ctx, User{ID: "n1", Name: "NoEmail", Email: "", Age: 20})
	m.Create(ctx, User{ID: "n2", Name: "HasEmail", Email: "has@email.com", Age: 25})
	m.Create(ctx, User{ID: "n3", Name: "AlsoNoEmail", Email: "", Age: 30})

	// WhereNotNull: rows with a non-empty email — note: empty string is NOT NULL
	// To test properly use WhereNot for empty string
	nonEmpty, err := m.Query().
		WhereNot("email", "").
		Build().Exec(ctx)
	if err != nil {
		t.Fatalf("exec: %v", err)
	}
	if len(nonEmpty) != 1 || nonEmpty[0].ID != "n2" {
		t.Errorf("expected [n2], got %+v", nonEmpty)
	}

	empty, err := m.Query().
		Where("email", "").
		Build().Exec(ctx)
	if err != nil {
		t.Fatalf("exec: %v", err)
	}
	if len(empty) != 2 {
		t.Errorf("expected 2 empty-email rows, got %d", len(empty))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 15. Full "kitchen sink" complex query
// ─────────────────────────────────────────────────────────────────────────────

func TestBuilderComplex_KitchenSink(t *testing.T) {
	m, cleanup := setupOrdersTable(t)
	defer cleanup()
	seedOrders(t, m)
	ctx := context.Background()

	// Query intent:
	//   electronics OR furniture
	//   AND NOT cancelled
	//   AND amount between 50 and 700
	//   AND qty >= 1
	//   ORDER BY amount DESC, id ASC
	//   LIMIT 5

	results, err := m.Query().
		WhereIn("category", []interface{}{"electronics", "furniture"}).
		WhereNotIn("status", []interface{}{"cancelled"}).
		WhereBetween("amount", 50.0, 700.0).
		WhereGreaterThanOrEqual("qty", 1).
		OrderByMultiple("amount DESC", "id ASC").
		Limit(5).
		Build().Exec(ctx)

	if err != nil {
		t.Fatalf("exec: %v", err)
	}
	if len(results) > 5 {
		t.Errorf("LIMIT 5 violated: got %d rows", len(results))
	}
	for _, r := range results {
		if r.Category != "electronics" && r.Category != "furniture" {
			t.Errorf("unexpected category %q", r.Category)
		}
		if r.Status == "cancelled" {
			t.Errorf("cancelled row should be excluded: %+v", r)
		}
		if r.Amount < 50 || r.Amount > 700 {
			t.Errorf("amount %v out of [50,700]", r.Amount)
		}
	}
	// Verify descending amount order
	for i := 1; i < len(results); i++ {
		if results[i].Amount > results[i-1].Amount {
			t.Errorf("not sorted DESC: %v > %v", results[i].Amount, results[i-1].Amount)
		}
	}
}

// ═════════════════════════════════════════════════════════════════════════════
// SQL STATEMENT CONSTRUCTION TESTS  (no DB – pure string assertions)
//
// Each test calls qb.SQL() / qb.Args() / qb.ToSQL() and verifies the produced
// statement is exactly what PostgreSQL will receive.
// ═════════════════════════════════════════════════════════════════════════════

// sqlAssert is a tiny helper that fails with a diff when SQL doesn't match.
func sqlAssert(t *testing.T, got, want string) {
	t.Helper()
	if got != want {
		t.Errorf("\nSQL mismatch\n  got : %s\n  want: %s", got, want)
	}
}

func argsAssert(t *testing.T, got []interface{}, want ...interface{}) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("args len: got %d want %d\n  got : %v\n  want: %v", len(got), len(want), got, want)
		return
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("args[%d]: got %v (%T) want %v (%T)", i, got[i], got[i], want[i], want[i])
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Basic SELECT *
// ─────────────────────────────────────────────────────────────────────────────

func TestBuilderSQL_SelectStar(t *testing.T) {
	m := &Model[OrderItem]{tableName: "orders"}
	qb := NewQueryBuilder(m)
	sqlAssert(t, qb.SQL(), "SELECT * FROM orders")
	argsAssert(t, qb.Args())
}

// ─────────────────────────────────────────────────────────────────────────────
// Column projection
// ─────────────────────────────────────────────────────────────────────────────

func TestBuilderSQL_Select_Columns(t *testing.T) {
	m := &Model[OrderItem]{tableName: "orders"}
	qb := NewQueryBuilder(m).Select("id", "product", "amount")
	sqlAssert(t, qb.SQL(), "SELECT id, product, amount FROM orders")
	argsAssert(t, qb.Args())
}

func TestBuilderSQL_SelectRaw(t *testing.T) {
	m := &Model[OrderItem]{tableName: "orders"}
	qb := NewQueryBuilder(m).SelectRaw("id, SUM(amount) AS total")
	sqlAssert(t, qb.SQL(), "SELECT id, SUM(amount) AS total FROM orders")
}

// ─────────────────────────────────────────────────────────────────────────────
// Single WHERE conditions
// ─────────────────────────────────────────────────────────────────────────────

func TestBuilderSQL_Where(t *testing.T) {
	m := &Model[OrderItem]{tableName: "orders"}
	qb := NewQueryBuilder(m).Where("status", "completed")
	sqlAssert(t, qb.SQL(), "SELECT * FROM orders WHERE status = $1")
	argsAssert(t, qb.Args(), "completed")
}

func TestBuilderSQL_WhereNot(t *testing.T) {
	m := &Model[OrderItem]{tableName: "orders"}
	qb := NewQueryBuilder(m).WhereNot("status", "cancelled")
	sqlAssert(t, qb.SQL(), "SELECT * FROM orders WHERE status != $1")
	argsAssert(t, qb.Args(), "cancelled")
}

func TestBuilderSQL_WhereIn(t *testing.T) {
	m := &Model[OrderItem]{tableName: "orders"}
	qb := NewQueryBuilder(m).WhereIn("category", []interface{}{"electronics", "furniture"})
	sqlAssert(t, qb.SQL(), "SELECT * FROM orders WHERE category IN ($1,$2)")
	argsAssert(t, qb.Args(), "electronics", "furniture")
}

func TestBuilderSQL_WhereNotIn(t *testing.T) {
	m := &Model[OrderItem]{tableName: "orders"}
	qb := NewQueryBuilder(m).WhereNotIn("status", []interface{}{"pending", "cancelled"})
	sqlAssert(t, qb.SQL(), "SELECT * FROM orders WHERE status NOT IN ($1,$2)")
	argsAssert(t, qb.Args(), "pending", "cancelled")
}

func TestBuilderSQL_WhereLike(t *testing.T) {
	m := &Model[OrderItem]{tableName: "orders"}
	qb := NewQueryBuilder(m).WhereLike("product", "Lap%")
	sqlAssert(t, qb.SQL(), "SELECT * FROM orders WHERE product LIKE $1")
	argsAssert(t, qb.Args(), "Lap%")
}

func TestBuilderSQL_WhereILike(t *testing.T) {
	m := &Model[OrderItem]{tableName: "orders"}
	qb := NewQueryBuilder(m).WhereILike("product", "lap%")
	sqlAssert(t, qb.SQL(), "SELECT * FROM orders WHERE product ILIKE $1")
	argsAssert(t, qb.Args(), "lap%")
}

func TestBuilderSQL_WhereNull(t *testing.T) {
	m := &Model[OrderItem]{tableName: "orders"}
	qb := NewQueryBuilder(m).WhereNull("deleted_at")
	sqlAssert(t, qb.SQL(), "SELECT * FROM orders WHERE deleted_at IS NULL")
	argsAssert(t, qb.Args())
}

func TestBuilderSQL_WhereNotNull(t *testing.T) {
	m := &Model[OrderItem]{tableName: "orders"}
	qb := NewQueryBuilder(m).WhereNotNull("email")
	sqlAssert(t, qb.SQL(), "SELECT * FROM orders WHERE email IS NOT NULL")
	argsAssert(t, qb.Args())
}

func TestBuilderSQL_WhereBetween(t *testing.T) {
	m := &Model[OrderItem]{tableName: "orders"}
	qb := NewQueryBuilder(m).WhereBetween("amount", 100.0, 500.0)
	sqlAssert(t, qb.SQL(), "SELECT * FROM orders WHERE amount BETWEEN $1 AND $2")
	argsAssert(t, qb.Args(), 100.0, 500.0)
}

func TestBuilderSQL_WhereGreaterThan(t *testing.T) {
	m := &Model[OrderItem]{tableName: "orders"}
	qb := NewQueryBuilder(m).WhereGreaterThan("amount", 50.0)
	sqlAssert(t, qb.SQL(), "SELECT * FROM orders WHERE amount > $1")
	argsAssert(t, qb.Args(), 50.0)
}

func TestBuilderSQL_WhereGreaterThanOrEqual(t *testing.T) {
	m := &Model[OrderItem]{tableName: "orders"}
	qb := NewQueryBuilder(m).WhereGreaterThanOrEqual("qty", 2)
	sqlAssert(t, qb.SQL(), "SELECT * FROM orders WHERE qty >= $1")
	argsAssert(t, qb.Args(), 2)
}

func TestBuilderSQL_WhereLessThan(t *testing.T) {
	m := &Model[OrderItem]{tableName: "orders"}
	qb := NewQueryBuilder(m).WhereLessThan("amount", 1000.0)
	sqlAssert(t, qb.SQL(), "SELECT * FROM orders WHERE amount < $1")
	argsAssert(t, qb.Args(), 1000.0)
}

func TestBuilderSQL_WhereLessThanOrEqual(t *testing.T) {
	m := &Model[OrderItem]{tableName: "orders"}
	qb := NewQueryBuilder(m).WhereLessThanOrEqual("qty", 5)
	sqlAssert(t, qb.SQL(), "SELECT * FROM orders WHERE qty <= $1")
	argsAssert(t, qb.Args(), 5)
}

// ─────────────────────────────────────────────────────────────────────────────
// WhereRaw – placeholder renumbering
// ─────────────────────────────────────────────────────────────────────────────

func TestBuilderSQL_WhereRaw_SingleArg(t *testing.T) {
	m := &Model[OrderItem]{tableName: "orders"}
	qb := NewQueryBuilder(m).WhereRaw("amount * qty > ?", 200.0)
	sqlAssert(t, qb.SQL(), "SELECT * FROM orders WHERE amount * qty > $1")
	argsAssert(t, qb.Args(), 200.0)
}

func TestBuilderSQL_WhereRaw_MultipleArgs_Renumbered(t *testing.T) {
	m := &Model[OrderItem]{tableName: "orders"}
	// Pre-existing $1, then WhereRaw adds two more → should be $2, $3
	qb := NewQueryBuilder(m).
		Where("status", "active").
		WhereRaw("amount > ? AND qty < ?", 50.0, 10)
	sqlAssert(t, qb.SQL(), "SELECT * FROM orders WHERE status = $1 AND amount > $2 AND qty < $3")
	argsAssert(t, qb.Args(), "active", 50.0, 10)
}

// ─────────────────────────────────────────────────────────────────────────────
// WhereExists / WhereNotExists
// ─────────────────────────────────────────────────────────────────────────────

func TestBuilderSQL_WhereExists(t *testing.T) {
	m := &Model[OrderItem]{tableName: "orders"}
	qb := NewQueryBuilder(m).
		WhereExists("SELECT 1 FROM customers c WHERE c.id = orders.customer_id AND c.active = ?", true)
	want := "SELECT * FROM orders WHERE EXISTS (SELECT 1 FROM customers c WHERE c.id = orders.customer_id AND c.active = $1)"
	sqlAssert(t, qb.SQL(), want)
	argsAssert(t, qb.Args(), true)
}

func TestBuilderSQL_WhereNotExists(t *testing.T) {
	m := &Model[OrderItem]{tableName: "orders"}
	qb := NewQueryBuilder(m).
		WhereNotExists("SELECT 1 FROM refunds r WHERE r.order_id = orders.id AND r.status = ?", "approved")
	want := "SELECT * FROM orders WHERE NOT EXISTS (SELECT 1 FROM refunds r WHERE r.order_id = orders.id AND r.status = $1)"
	sqlAssert(t, qb.SQL(), want)
	argsAssert(t, qb.Args(), "approved")
}

// ─────────────────────────────────────────────────────────────────────────────
// OR groups
// ─────────────────────────────────────────────────────────────────────────────

func TestBuilderSQL_Or_FirstCondition(t *testing.T) {
	m := &Model[OrderItem]{tableName: "orders"}
	qb := NewQueryBuilder(m).
		Or(func(q *QueryBuilder[OrderItem]) {
			q.Where("category", "electronics")
		}).
		OrWhere(func(q *QueryBuilder[OrderItem]) {
			q.Where("category", "furniture").Where("status", "completed")
		})
	want := "SELECT * FROM orders WHERE (category = $1) OR (category = $2 AND status = $3)"
	sqlAssert(t, qb.SQL(), want)
	argsAssert(t, qb.Args(), "electronics", "furniture", "completed")
}

func TestBuilderSQL_Where_Then_Or(t *testing.T) {
	m := &Model[OrderItem]{tableName: "orders"}
	qb := NewQueryBuilder(m).
		Where("status", "active").
		OrWhere(func(q *QueryBuilder[OrderItem]) {
			q.Where("amount", 0.0).Where("qty", 0)
		})
	want := "SELECT * FROM orders WHERE status = $1 OR (amount = $2 AND qty = $3)"
	sqlAssert(t, qb.SQL(), want)
	argsAssert(t, qb.Args(), "active", 0.0, 0)
}

// ─────────────────────────────────────────────────────────────────────────────
// JOIN variants
// ─────────────────────────────────────────────────────────────────────────────

func TestBuilderSQL_InnerJoin(t *testing.T) {
	m := &Model[OrderItem]{tableName: "orders"}
	qb := NewQueryBuilder(m).
		Join("customers c", "c.id = orders.customer_id").
		Where("c.active", true)
	want := "SELECT * FROM orders JOIN customers c ON c.id = orders.customer_id WHERE c.active = $1"
	sqlAssert(t, qb.SQL(), want)
	argsAssert(t, qb.Args(), true)
}

func TestBuilderSQL_LeftJoin(t *testing.T) {
	m := &Model[OrderItem]{tableName: "orders"}
	qb := NewQueryBuilder(m).
		LeftJoin("refunds r", "r.order_id = orders.id").
		WhereNull("r.id")
	want := "SELECT * FROM orders LEFT JOIN refunds r ON r.order_id = orders.id WHERE r.id IS NULL"
	sqlAssert(t, qb.SQL(), want)
}

func TestBuilderSQL_RightJoin(t *testing.T) {
	m := &Model[OrderItem]{tableName: "orders"}
	qb := NewQueryBuilder(m).
		RightJoin("customers c", "c.id = orders.customer_id").
		WhereNotNull("orders.id")
	want := "SELECT * FROM orders RIGHT JOIN customers c ON c.id = orders.customer_id WHERE orders.id IS NOT NULL"
	sqlAssert(t, qb.SQL(), want)
}

// ─────────────────────────────────────────────────────────────────────────────
// ORDER BY, GROUP BY, HAVING
// ─────────────────────────────────────────────────────────────────────────────

func TestBuilderSQL_OrderByAsc(t *testing.T) {
	m := &Model[OrderItem]{tableName: "orders"}
	qb := NewQueryBuilder(m).OrderBy("amount", false)
	sqlAssert(t, qb.SQL(), "SELECT * FROM orders ORDER BY amount")
}

func TestBuilderSQL_OrderByDesc(t *testing.T) {
	m := &Model[OrderItem]{tableName: "orders"}
	qb := NewQueryBuilder(m).OrderBy("amount", true)
	sqlAssert(t, qb.SQL(), "SELECT * FROM orders ORDER BY amount DESC")
}

func TestBuilderSQL_OrderByMultiple(t *testing.T) {
	m := &Model[OrderItem]{tableName: "orders"}
	qb := NewQueryBuilder(m).OrderByMultiple("category ASC", "amount DESC")
	sqlAssert(t, qb.SQL(), "SELECT * FROM orders ORDER BY category ASC, amount DESC")
}

func TestBuilderSQL_GroupBy_Having(t *testing.T) {
	m := &Model[OrderItem]{tableName: "orders"}
	qb := NewQueryBuilder(m).
		SelectRaw("customer_id, SUM(amount) AS total").
		Where("status", "completed").
		GroupBy("customer_id").
		Having("SUM(amount) >", 500.0)
	want := "SELECT customer_id, SUM(amount) AS total FROM orders WHERE status = $1 GROUP BY customer_id HAVING SUM(amount) > $2"
	sqlAssert(t, qb.SQL(), want)
	argsAssert(t, qb.Args(), "completed", 500.0)
}

// ─────────────────────────────────────────────────────────────────────────────
// LIMIT / OFFSET – must be literals, not bound params
// ─────────────────────────────────────────────────────────────────────────────

func TestBuilderSQL_Limit_IsLiteralNotParam(t *testing.T) {
	m := &Model[OrderItem]{tableName: "orders"}
	qb := NewQueryBuilder(m).Where("status", "pending").Limit(10).Offset(20)
	sqlAssert(t, qb.SQL(), "SELECT * FROM orders WHERE status = $1 LIMIT 10 OFFSET 20")
	// Only the WHERE arg is bound; LIMIT/OFFSET are literals
	argsAssert(t, qb.Args(), "pending")
}

// ─────────────────────────────────────────────────────────────────────────────
// Soft-delete injection – filter appears BEFORE ORDER BY / LIMIT / OFFSET
// ─────────────────────────────────────────────────────────────────────────────

func TestBuilderSQL_SoftDelete_InjectedBeforeOrderBy(t *testing.T) {
	m := &Model[OrderItem]{tableName: "orders", softDeleteCol: "deleted_at"}
	qb := NewQueryBuilder(m).
		Where("status", "completed").
		OrderBy("amount", true)
	q := qb.Build()
	sql := q.SQL()
	idxFilter := strings.Index(sql, "deleted_at IS NULL")
	idxOrder := strings.Index(sql, "ORDER BY")
	if idxFilter == -1 {
		t.Fatalf("soft-delete filter missing: %q", sql)
	}
	if idxOrder != -1 && idxFilter > idxOrder {
		t.Errorf("soft-delete must appear before ORDER BY:\n  %q", sql)
	}
	sqlAssert(t, sql,
		"SELECT * FROM orders WHERE status = $1 AND deleted_at IS NULL ORDER BY amount DESC")
	argsAssert(t, qb.Args(), "completed")
}

func TestBuilderSQL_SoftDelete_InjectedBeforeLimit(t *testing.T) {
	m := &Model[OrderItem]{tableName: "orders", softDeleteCol: "deleted_at"}
	q := NewQueryBuilder(m).
		Where("status", "completed").
		Limit(5).Offset(0).
		Build()
	sqlAssert(t, q.SQL(),
		"SELECT * FROM orders WHERE status = $1 AND deleted_at IS NULL LIMIT 5 OFFSET 0")
}

func TestBuilderSQL_SoftDelete_NoWhere_AddsWhereClause(t *testing.T) {
	m := &Model[OrderItem]{tableName: "orders", softDeleteCol: "deleted_at"}
	q := NewQueryBuilder(m).OrderBy("id", false).Build()
	sqlAssert(t, q.SQL(),
		"SELECT * FROM orders WHERE deleted_at IS NULL ORDER BY id")
}

func TestBuilderSQL_WithTrashed_NoFilter(t *testing.T) {
	m := &Model[OrderItem]{tableName: "orders", softDeleteCol: "deleted_at"}
	q := NewQueryBuilder(m).WithTrashed().Where("status", "completed").Build()
	sql := q.SQL()
	if strings.Contains(sql, "deleted_at IS NULL") {
		t.Errorf("WithTrashed should suppress soft-delete filter: %q", sql)
	}
	sqlAssert(t, sql, "SELECT * FROM orders WHERE status = $1")
}

// ─────────────────────────────────────────────────────────────────────────────
// Clone – SQL and args are independent after cloning
// ─────────────────────────────────────────────────────────────────────────────

func TestBuilderSQL_Clone_Independent(t *testing.T) {
	m := &Model[OrderItem]{tableName: "orders"}
	base := NewQueryBuilder(m).Where("category", "electronics")

	a := base.Clone().Where("status", "completed").OrderBy("amount", true)
	b := base.Clone().Where("status", "pending").Limit(5)

	sqlAssert(t, a.SQL(),
		"SELECT * FROM orders WHERE category = $1 AND status = $2 ORDER BY amount DESC")
	sqlAssert(t, b.SQL(),
		"SELECT * FROM orders WHERE category = $1 AND status = $2 LIMIT 5")

	// base must be unchanged
	sqlAssert(t, base.SQL(), "SELECT * FROM orders WHERE category = $1")
	argsAssert(t, base.Args(), "electronics")
}

// ─────────────────────────────────────────────────────────────────────────────
// ToSQL – interpolated display string
// ─────────────────────────────────────────────────────────────────────────────

func TestBuilderSQL_ToSQL_Interpolated(t *testing.T) {
	m := &Model[OrderItem]{tableName: "orders"}
	qb := NewQueryBuilder(m).
		Where("status", "completed").
		WhereGreaterThan("amount", 100.0).
		Limit(10)

	interpolated := qb.ToSQL()

	if strings.Contains(interpolated, "$1") || strings.Contains(interpolated, "$2") {
		t.Errorf("ToSQL should not contain placeholders: %q", interpolated)
	}
	if !strings.Contains(interpolated, "'completed'") {
		t.Errorf("ToSQL should contain 'completed': %q", interpolated)
	}
	if !strings.Contains(interpolated, "100") {
		t.Errorf("ToSQL should contain 100: %q", interpolated)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Full complex statement – matches exactly
// ─────────────────────────────────────────────────────────────────────────────

func TestBuilderSQL_FullComplexStatement(t *testing.T) {
	m := &Model[OrderItem]{tableName: "orders"}
	qb := NewQueryBuilder(m).
		Select("id", "customer_id", "product", "amount").
		LeftJoin("customers c", "c.id = orders.customer_id").
		Where("c.active", true).
		WhereIn("category", []interface{}{"electronics", "furniture"}).
		WhereNotIn("status", []interface{}{"cancelled"}).
		WhereBetween("amount", 50.0, 1500.0).
		WhereGreaterThanOrEqual("qty", 1).
		WhereLike("product", "L%").
		OrderByMultiple("amount DESC", "id ASC").
		Limit(20).
		Offset(0)

	want := "SELECT id, customer_id, product, amount FROM orders" +
		" LEFT JOIN customers c ON c.id = orders.customer_id" +
		" WHERE c.active = $1" +
		" AND category IN ($2,$3)" +
		" AND status NOT IN ($4)" +
		" AND amount BETWEEN $5 AND $6" +
		" AND qty >= $7" +
		" AND product LIKE $8" +
		" ORDER BY amount DESC, id ASC" +
		" LIMIT 20 OFFSET 0"

	sqlAssert(t, qb.SQL(), want)
	argsAssert(t, qb.Args(), true, "electronics", "furniture", "cancelled", 50.0, 1500.0, 1, "L%")
}

func TestBuilderSQL_FullComplexStatement_WithSoftDelete(t *testing.T) {
	m := &Model[OrderItem]{tableName: "orders", softDeleteCol: "deleted_at"}
	qb := NewQueryBuilder(m).
		Select("id", "customer_id", "amount").
		Where("status", "completed").
		WhereGreaterThan("amount", 100.0).
		OrderBy("amount", true).
		Limit(10)

	want := "SELECT id, customer_id, amount FROM orders" +
		" WHERE status = $1" +
		" AND amount > $2" +
		" AND deleted_at IS NULL" +
		" ORDER BY amount DESC" +
		" LIMIT 10"

	sqlAssert(t, qb.Build().SQL(), want)
	argsAssert(t, qb.Args(), "completed", 100.0)
}
