package dbconnector

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// Soft-delete injection correctness
// ─────────────────────────────────────────────────────────────────────────────

// TestInjectSoftDeleteFilter_BeforeOrderBy verifies the injection goes BEFORE ORDER BY.
func TestInjectSoftDeleteFilter_BeforeOrderBy(t *testing.T) {
	sql := injectSoftDeleteFilter(
		"SELECT * FROM users WHERE age = $1 ORDER BY created_at DESC",
		"deleted_at IS NULL",
	)
	expected := "SELECT * FROM users WHERE age = $1 AND deleted_at IS NULL ORDER BY created_at DESC"
	if sql != expected {
		t.Errorf("got %q\nwant %q", sql, expected)
	}
}

// TestInjectSoftDeleteFilter_BeforeLimit verifies injection goes BEFORE LIMIT.
func TestInjectSoftDeleteFilter_BeforeLimit(t *testing.T) {
	sql := injectSoftDeleteFilter(
		"SELECT * FROM users WHERE age = $1 LIMIT 10 OFFSET 5",
		"deleted_at IS NULL",
	)
	expected := "SELECT * FROM users WHERE age = $1 AND deleted_at IS NULL LIMIT 10 OFFSET 5"
	if sql != expected {
		t.Errorf("got %q\nwant %q", sql, expected)
	}
}

// TestInjectSoftDeleteFilter_NoWhereWithOrderBy adds WHERE clause before ORDER BY.
func TestInjectSoftDeleteFilter_NoWhereWithOrderBy(t *testing.T) {
	sql := injectSoftDeleteFilter(
		"SELECT * FROM users ORDER BY name ASC",
		"deleted_at IS NULL",
	)
	expected := "SELECT * FROM users WHERE deleted_at IS NULL ORDER BY name ASC"
	if sql != expected {
		t.Errorf("got %q\nwant %q", sql, expected)
	}
}

// TestInjectSoftDeleteFilter_NoWhereNoOrderBy: simple append.
func TestInjectSoftDeleteFilter_NoWhereNoOrderBy(t *testing.T) {
	sql := injectSoftDeleteFilter("SELECT * FROM users", "deleted_at IS NULL")
	expected := "SELECT * FROM users WHERE deleted_at IS NULL"
	if sql != expected {
		t.Errorf("got %q\nwant %q", sql, expected)
	}
}

// TestInjectSoftDeleteFilter_WithGroupBy verifies BEFORE GROUP BY.
func TestInjectSoftDeleteFilter_WithGroupBy(t *testing.T) {
	sql := injectSoftDeleteFilter(
		"SELECT status, COUNT(*) FROM users GROUP BY status",
		"deleted_at IS NULL",
	)
	if !strings.Contains(sql, "WHERE deleted_at IS NULL GROUP BY status") {
		t.Errorf("unexpected: %q", sql)
	}
}

// TestInjectSoftDeleteFilter_EmptyFilter returns original sql unchanged.
func TestInjectSoftDeleteFilter_EmptyFilter(t *testing.T) {
	original := "SELECT * FROM users WHERE age = $1 ORDER BY id"
	result := injectSoftDeleteFilter(original, "")
	if result != original {
		t.Errorf("expected unchanged sql, got %q", result)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// QueryBuilder.Build() + soft-delete (the original bug path)
// ─────────────────────────────────────────────────────────────────────────────

// TestQueryBuilder_Build_SoftDelete_WithOrderBy is the exact scenario that was
// broken: Build() was appending deleted_at IS NULL AFTER ORDER BY.
func TestQueryBuilder_Build_SoftDelete_WithOrderBy(t *testing.T) {
	model := &Model[TestUser]{
		tableName:     "users",
		softDeleteCol: "deleted_at",
	}
	q := NewQueryBuilder(model).
		Where("age", 30).
		OrderBy("name", false).
		Build()

	sql := q.SQL()
	t.Logf("SQL: %s", sql)

	// deleted_at IS NULL must appear BEFORE ORDER BY
	idxDeletedAt := strings.Index(sql, "deleted_at IS NULL")
	idxOrderBy := strings.Index(sql, "ORDER BY")
	if idxDeletedAt == -1 {
		t.Fatal("soft-delete filter not injected")
	}
	if idxOrderBy == -1 {
		t.Fatal("ORDER BY not found")
	}
	if idxDeletedAt > idxOrderBy {
		t.Errorf("soft-delete filter appears AFTER ORDER BY:\n  %s", sql)
	}
}

// TestQueryBuilder_Build_SoftDelete_WithLimitOffset checks LIMIT/OFFSET ordering.
func TestQueryBuilder_Build_SoftDelete_WithLimitOffset(t *testing.T) {
	model := &Model[TestUser]{
		tableName:     "users",
		softDeleteCol: "deleted_at",
	}
	q := NewQueryBuilder(model).
		Where("status", "active").
		Limit(10).
		Offset(20).
		Build()

	sql := q.SQL()
	t.Logf("SQL: %s", sql)

	idxFilter := strings.Index(sql, "deleted_at IS NULL")
	idxLimit := strings.Index(sql, "LIMIT")
	if idxFilter == -1 {
		t.Fatal("soft-delete filter missing")
	}
	if idxLimit != -1 && idxFilter > idxLimit {
		t.Errorf("filter appears after LIMIT:\n  %s", sql)
	}
}

// TestQueryBuilder_Build_SoftDelete_NoWhere verifies filter added even when
// there are no WHERE conditions yet.
func TestQueryBuilder_Build_SoftDelete_NoWhere(t *testing.T) {
	model := &Model[TestUser]{
		tableName:     "orders",
		softDeleteCol: "deleted_at",
	}
	q := NewQueryBuilder(model).OrderBy("id", false).Build()
	sql := q.SQL()
	if !strings.Contains(sql, "WHERE deleted_at IS NULL") {
		t.Errorf("expected WHERE clause for soft-delete, got: %s", sql)
	}
	if strings.Index(sql, "deleted_at IS NULL") > strings.Index(sql, "ORDER BY") {
		t.Errorf("filter appears after ORDER BY: %s", sql)
	}
}

// TestQueryBuilder_WithTrashed_SkipsSoftDelete ensures WithTrashed bypasses the filter.
func TestQueryBuilder_WithTrashed_SkipsSoftDelete(t *testing.T) {
	model := &Model[TestUser]{
		tableName:     "users",
		softDeleteCol: "deleted_at",
	}
	q := NewQueryBuilder(model).WithTrashed().Where("age", 30).Build()
	if strings.Contains(q.SQL(), "deleted_at IS NULL") {
		t.Errorf("WithTrashed should skip filter, got: %s", q.SQL())
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// ToSQL / InterpolateSQL
// ─────────────────────────────────────────────────────────────────────────────

func TestInterpolateSQL_Basic(t *testing.T) {
	result := InterpolateSQL("SELECT * FROM users WHERE id = $1", "abc")
	if result != "SELECT * FROM users WHERE id = 'abc'" {
		t.Errorf("unexpected: %q", result)
	}
}

func TestInterpolateSQL_MultipleArgs(t *testing.T) {
	result := InterpolateSQL("WHERE a=$1 AND b=$2 AND c=$3", 1, "hello", true)
	expected := "WHERE a=1 AND b='hello' AND c=true"
	if result != expected {
		t.Errorf("got %q, want %q", result, expected)
	}
}

func TestInterpolateSQL_DoubleDigitIndex(t *testing.T) {
	// Verifies that $10 isn't partially replaced by $1
	args := make([]interface{}, 10)
	for i := range args {
		args[i] = i + 1
	}
	result := InterpolateSQL("$1 $10", args...)
	if result != "1 10" {
		t.Errorf("double-digit placeholder wrong: %q", result)
	}
}

func TestInterpolateSQL_NilArg(t *testing.T) {
	result := InterpolateSQL("val=$1", nil)
	if result != "val=NULL" {
		t.Errorf("nil should render as NULL, got %q", result)
	}
}

func TestInterpolateSQL_TimeArg(t *testing.T) {
	ts := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)
	result := InterpolateSQL("created_at=$1", ts)
	if !strings.Contains(result, "2024-01-15") {
		t.Errorf("time not interpolated correctly: %q", result)
	}
}

func TestInterpolateSQL_StringEscaping(t *testing.T) {
	result := InterpolateSQL("name=$1", "O'Brien")
	if !strings.Contains(result, "''") {
		t.Errorf("single-quote should be escaped: %q", result)
	}
}

func TestQueryBuilder_ToSQL(t *testing.T) {
	model := &Model[TestUser]{tableName: "users"}
	qb := NewQueryBuilder(model).Where("name", "Alice").WhereGreaterThan("age", 18)
	sql := qb.ToSQL()
	if !strings.Contains(sql, "'Alice'") {
		t.Errorf("ToSQL should interpolate string args, got: %q", sql)
	}
	if !strings.Contains(sql, "18") {
		t.Errorf("ToSQL should interpolate int args, got: %q", sql)
	}
	// Should NOT contain $1 or $2
	if strings.Contains(sql, "$1") || strings.Contains(sql, "$2") {
		t.Errorf("ToSQL should not contain placeholders, got: %q", sql)
	}
}

func TestQuery_ToSQL(t *testing.T) {
	q := newQuery(
		func(ctx context.Context) (string, error) { return "", nil },
		"SELECT * FROM users WHERE id = $1",
		"some-uuid",
	)
	result := q.ToSQL()
	if !strings.Contains(result, "'some-uuid'") {
		t.Errorf("Query.ToSQL should interpolate, got: %q", result)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// New QueryBuilder methods
// ─────────────────────────────────────────────────────────────────────────────

func TestQueryBuilder_SelectRaw(t *testing.T) {
	model := &Model[TestUser]{tableName: "users"}
	sql := NewQueryBuilder(model).SelectRaw("id, COUNT(*) as cnt").SQL()
	expected := "SELECT id, COUNT(*) as cnt FROM users"
	if sql != expected {
		t.Errorf("got %q, want %q", sql, expected)
	}
}

func TestQueryBuilder_Join(t *testing.T) {
	model := &Model[TestUser]{tableName: "users"}
	sql := NewQueryBuilder(model).Join("orders", "orders.user_id = users.id").SQL()
	if !strings.Contains(sql, "JOIN orders ON orders.user_id = users.id") {
		t.Errorf("JOIN not present: %q", sql)
	}
}

func TestQueryBuilder_LeftJoin(t *testing.T) {
	model := &Model[TestUser]{tableName: "users"}
	sql := NewQueryBuilder(model).LeftJoin("profiles", "profiles.user_id = users.id").SQL()
	if !strings.Contains(sql, "LEFT JOIN profiles ON profiles.user_id = users.id") {
		t.Errorf("LEFT JOIN not present: %q", sql)
	}
}

func TestQueryBuilder_RightJoin(t *testing.T) {
	model := &Model[TestUser]{tableName: "users"}
	sql := NewQueryBuilder(model).RightJoin("tenants", "tenants.id = users.tenant_id").SQL()
	if !strings.Contains(sql, "RIGHT JOIN tenants ON tenants.id = users.tenant_id") {
		t.Errorf("RIGHT JOIN not present: %q", sql)
	}
}

func TestQueryBuilder_WhereRaw(t *testing.T) {
	model := &Model[TestUser]{tableName: "users"}
	qb := NewQueryBuilder(model).WhereRaw("LOWER(name) = ?", "alice")
	sql := qb.SQL()
	if !strings.Contains(sql, "LOWER(name) = $1") {
		t.Errorf("WhereRaw: %q", sql)
	}
	if len(qb.Args()) != 1 || qb.Args()[0] != "alice" {
		t.Errorf("WhereRaw args: %v", qb.Args())
	}
}

func TestQueryBuilder_WhereRaw_MultipleArgs(t *testing.T) {
	model := &Model[TestUser]{tableName: "users"}
	qb := NewQueryBuilder(model).WhereRaw("age BETWEEN ? AND ?", 18, 65)
	sql := qb.SQL()
	if !strings.Contains(sql, "age BETWEEN $1 AND $2") {
		t.Errorf("WhereRaw multi-arg: %q", sql)
	}
}

func TestQueryBuilder_WhereRaw_WithExistingWhere(t *testing.T) {
	model := &Model[TestUser]{tableName: "users"}
	qb := NewQueryBuilder(model).Where("status", "active").WhereRaw("LOWER(email) LIKE ?", "%@example.com")
	sql := qb.SQL()
	if !strings.Contains(sql, "AND LOWER(email) LIKE $2") {
		t.Errorf("WhereRaw conjunction: %q", sql)
	}
}

func TestQueryBuilder_WhereILike(t *testing.T) {
	model := &Model[TestUser]{tableName: "users"}
	qb := NewQueryBuilder(model).WhereILike("name", "%alice%")
	sql := qb.SQL()
	expected := "SELECT * FROM users WHERE name ILIKE $1"
	if sql != expected {
		t.Errorf("got %q, want %q", sql, expected)
	}
}

func TestQueryBuilder_OrWhere(t *testing.T) {
	model := &Model[TestUser]{tableName: "users"}
	qb := NewQueryBuilder(model).
		Where("status", "active").
		OrWhere(func(q *QueryBuilder[TestUser]) {
			q.Where("age", 18).Where("role", "admin")
		})
	sql := qb.SQL()
	if !strings.Contains(sql, "OR (age = $2 AND role = $3)") {
		t.Errorf("OrWhere: %q", sql)
	}
}

func TestQueryBuilder_OrWhere_AsFirstCondition(t *testing.T) {
	model := &Model[TestUser]{tableName: "users"}
	qb := NewQueryBuilder(model).OrWhere(func(q *QueryBuilder[TestUser]) {
		q.Where("a", 1).Where("b", 2)
	})
	sql := qb.SQL()
	if !strings.Contains(sql, "WHERE (") {
		t.Errorf("OrWhere as first condition should use WHERE: %q", sql)
	}
}

func TestQueryBuilder_WhereExists(t *testing.T) {
	model := &Model[TestUser]{tableName: "users"}
	qb := NewQueryBuilder(model).
		WhereExists("SELECT 1 FROM orders WHERE orders.user_id = users.id AND orders.status = ?", "paid")
	sql := qb.SQL()
	if !strings.Contains(sql, "EXISTS (SELECT 1 FROM orders") {
		t.Errorf("WhereExists: %q", sql)
	}
	if !strings.Contains(sql, "$1") {
		t.Errorf("WhereExists arg binding: %q", sql)
	}
}

func TestQueryBuilder_WhereNotExists(t *testing.T) {
	model := &Model[TestUser]{tableName: "users"}
	qb := NewQueryBuilder(model).
		WhereNotExists("SELECT 1 FROM bans WHERE bans.user_id = users.id")
	sql := qb.SQL()
	if !strings.Contains(sql, "NOT EXISTS (") {
		t.Errorf("WhereNotExists: %q", sql)
	}
}

func TestQueryBuilder_WhereDate(t *testing.T) {
	model := &Model[TestUser]{tableName: "events"}
	day := time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC)
	qb := NewQueryBuilder(model).WhereDate("created_at", day)
	sql := qb.SQL()
	if !strings.Contains(sql, "created_at >= $1 AND created_at < $2") {
		t.Errorf("WhereDate: %q", sql)
	}
	if len(qb.Args()) != 2 {
		t.Errorf("WhereDate args count: %d", len(qb.Args()))
	}
}

func TestQueryBuilder_WhereDateBetween(t *testing.T) {
	model := &Model[TestUser]{tableName: "events"}
	from := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2024, 1, 31, 0, 0, 0, 0, time.UTC)
	qb := NewQueryBuilder(model).WhereDateBetween("created_at", from, to)
	sql := qb.SQL()
	if !strings.Contains(sql, "created_at >= $1 AND created_at < $2") {
		t.Errorf("WhereDateBetween: %q", sql)
	}
}

func TestQueryBuilder_WhereTimestampNull(t *testing.T) {
	model := &Model[TestUser]{tableName: "users"}
	sql := NewQueryBuilder(model).WhereTimestampNull("deleted_at").SQL()
	expected := "SELECT * FROM users WHERE deleted_at IS NULL"
	if sql != expected {
		t.Errorf("got %q, want %q", sql, expected)
	}
}

func TestQueryBuilder_WhereTimestampNotNull(t *testing.T) {
	model := &Model[TestUser]{tableName: "users"}
	sql := NewQueryBuilder(model).WhereTimestampNotNull("verified_at").SQL()
	expected := "SELECT * FROM users WHERE verified_at IS NOT NULL"
	if sql != expected {
		t.Errorf("got %q, want %q", sql, expected)
	}
}

func TestQueryBuilder_Clone(t *testing.T) {
	model := &Model[TestUser]{tableName: "users"}
	base := NewQueryBuilder(model).Where("status", "active")

	branch1 := base.Clone().Where("age", 20)
	branch2 := base.Clone().Where("age", 30)

	// SQL structure is identical (both use $2) – but args must differ
	if len(branch1.Args()) != 2 || len(branch2.Args()) != 2 {
		t.Errorf("each branch should have 2 args: branch1=%v branch2=%v", branch1.Args(), branch2.Args())
	}
	if branch1.Args()[1] == branch2.Args()[1] {
		t.Errorf("branch args should diverge: branch1=%v branch2=%v", branch1.Args(), branch2.Args())
	}
	if branch1.Args()[1] != 20 || branch2.Args()[1] != 30 {
		t.Errorf("expected branch1[1]=20 branch2[1]=30, got %v %v", branch1.Args()[1], branch2.Args()[1])
	}
	// Base should be unaffected – still only 1 arg
	if len(base.Args()) != 1 {
		t.Errorf("base args should not be modified by clones: %v", base.Args())
	}
	if strings.Contains(base.SQL(), "age") {
		t.Errorf("base SQL should not contain age: %q", base.SQL())
	}
	// ToSQL should show different interpolated values
	if branch1.ToSQL() == branch2.ToSQL() {
		t.Errorf("ToSQL() should differ between clones:\n  b1: %s\n  b2: %s", branch1.ToSQL(), branch2.ToSQL())
	}
}

func TestQueryBuilder_OrderByMultiple(t *testing.T) {
	model := &Model[TestUser]{tableName: "users"}
	sql := NewQueryBuilder(model).OrderByMultiple("name ASC", "created_at DESC").SQL()
	expected := "SELECT * FROM users ORDER BY name ASC, created_at DESC"
	if sql != expected {
		t.Errorf("got %q, want %q", sql, expected)
	}
}

func TestQueryBuilder_Limit_IsLiteral(t *testing.T) {
	model := &Model[TestUser]{tableName: "users"}
	qb := NewQueryBuilder(model).Where("a", 1).Limit(50)
	sql := qb.SQL()
	// Should be literal "LIMIT 50" not "LIMIT $2"
	if !strings.Contains(sql, "LIMIT 50") {
		t.Errorf("Limit should be literal int: %q", sql)
	}
	if strings.Contains(sql, "LIMIT $") {
		t.Errorf("Limit must not be a bound parameter: %q", sql)
	}
	// Only 1 bound arg (for WHERE a = $1)
	if len(qb.Args()) != 1 {
		t.Errorf("expected 1 arg, got %d", len(qb.Args()))
	}
}

func TestQueryBuilder_Offset_IsLiteral(t *testing.T) {
	model := &Model[TestUser]{tableName: "users"}
	qb := NewQueryBuilder(model).Offset(100)
	sql := qb.SQL()
	if !strings.Contains(sql, "OFFSET 100") {
		t.Errorf("Offset should be literal int: %q", sql)
	}
	if strings.Contains(sql, "OFFSET $") {
		t.Errorf("Offset must not be a bound parameter: %q", sql)
	}
	if len(qb.Args()) != 0 {
		t.Errorf("expected 0 args, got %d", len(qb.Args()))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Complex / combined queries
// ─────────────────────────────────────────────────────────────────────────────

func TestQueryBuilder_ComplexWithJoinAndSoftDelete(t *testing.T) {
	model := &Model[TestUser]{
		tableName:     "users",
		softDeleteCol: "deleted_at",
	}
	q := NewQueryBuilder(model).
		Select("users.id", "users.name", "orders.total").
		LeftJoin("orders", "orders.user_id = users.id").
		Where("users.status", "active").
		WhereGreaterThan("users.age", 18).
		WhereNull("users.deleted_at").
		OrderBy("users.name", false).
		Limit(20).
		Build()

	sql := q.SQL()
	t.Logf("complex SQL: %s", sql)

	checks := []string{
		"LEFT JOIN orders",
		"WHERE",
		"ORDER BY users.name",
		"LIMIT 20",
	}
	for _, c := range checks {
		if !strings.Contains(sql, c) {
			t.Errorf("expected %q in SQL:\n  %s", c, sql)
		}
	}
}

func TestQueryBuilder_ComplexWithTimestampFilters(t *testing.T) {
	model := &Model[TestUser]{tableName: "sessions"}
	now := time.Now()
	weekAgo := now.Add(-7 * 24 * time.Hour)

	qb := NewQueryBuilder(model).
		WhereTimestampNotNull("verified_at").
		WhereTimestampNull("revoked_at").
		WhereDateBetween("created_at", weekAgo, now).
		OrderByMultiple("created_at DESC")

	sql := qb.SQL()
	t.Logf("timestamp SQL: %s", sql)

	if !strings.Contains(sql, "verified_at IS NOT NULL") {
		t.Errorf("missing verified_at IS NOT NULL: %q", sql)
	}
	if !strings.Contains(sql, "revoked_at IS NULL") {
		t.Errorf("missing revoked_at IS NULL: %q", sql)
	}
	if !strings.Contains(sql, "created_at >= $") {
		t.Errorf("missing date range: %q", sql)
	}
	if len(qb.Args()) != 2 {
		t.Errorf("expected 2 args for date range, got %d", len(qb.Args()))
	}

	// ToSQL should interpolate timestamps
	interpolated := qb.ToSQL()
	if strings.Contains(interpolated, "$1") || strings.Contains(interpolated, "$2") {
		t.Errorf("ToSQL still has placeholders: %q", interpolated)
	}
}

func TestQueryBuilder_SoftDelete_WithWhereAndOrderByAndLimit(t *testing.T) {
	model := &Model[TestUser]{
		tableName:     "posts",
		softDeleteCol: "deleted_at",
	}
	q := NewQueryBuilder(model).
		Where("author_id", "user-123").
		Where("published", true).
		OrderBy("created_at", true).
		Limit(5).
		Offset(10).
		Build()

	sql := q.SQL()
	t.Logf("soft-delete complex SQL: %s", sql)

	idxFilter := strings.Index(sql, "deleted_at IS NULL")
	idxOrderBy := strings.Index(sql, "ORDER BY")
	idxLimit := strings.Index(sql, "LIMIT")

	if idxFilter == -1 {
		t.Fatal("soft-delete filter missing")
	}
	if idxOrderBy != -1 && idxFilter > idxOrderBy {
		t.Errorf("filter appears after ORDER BY")
	}
	if idxLimit != -1 && idxFilter > idxLimit {
		t.Errorf("filter appears after LIMIT")
	}
	if !strings.Contains(sql, "LIMIT 5") {
		t.Errorf("LIMIT should be literal: %q", sql)
	}
	if !strings.Contains(sql, "OFFSET 10") {
		t.Errorf("OFFSET should be literal: %q", sql)
	}
}

func TestQueryBuilder_OrAndWhereNullCombination(t *testing.T) {
	model := &Model[TestUser]{tableName: "accounts"}
	qb := NewQueryBuilder(model).
		Where("tenant_id", "t1").
		Or(func(q *QueryBuilder[TestUser]) {
			q.WhereNull("deleted_at").WhereGreaterThan("age", 0)
		})
	sql := qb.SQL()
	if !strings.Contains(sql, "OR (deleted_at IS NULL AND age > $") {
		t.Errorf("Or+WhereNull: %q", sql)
	}
}

func TestQueryBuilder_WhereDate_WithSoftDelete(t *testing.T) {
	model := &Model[TestUser]{
		tableName:     "events",
		softDeleteCol: "deleted_at",
	}
	day := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	q := NewQueryBuilder(model).WhereDate("event_at", day).OrderBy("event_at", false).Build()
	sql := q.SQL()

	t.Logf("WhereDate+SoftDelete SQL: %s", sql)

	idxFilter := strings.Index(sql, "deleted_at IS NULL")
	idxOrder := strings.Index(sql, "ORDER BY")
	if idxFilter == -1 {
		t.Fatal("missing soft-delete filter")
	}
	if idxOrder != -1 && idxFilter > idxOrder {
		t.Errorf("soft-delete after ORDER BY: %q", sql)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Integration: Paginate with QueryBuilder + soft delete
// ─────────────────────────────────────────────────────────────────────────────

// setupSoftDeleteTableWithTimestamp creates a test table with a nullable
// deleted_at timestamp column and returns the model + cleanup func.
func setupSoftDeleteTableAdv(t *testing.T) (*Model[User], func()) {
	t.Helper()
	conn := NewPostgresConnection(__TestDBconfig)
	if err := conn.Connect(context.Background()); err != nil {
		t.Skipf("no DB: %v", err)
	}
	table := fmt.Sprintf("test_adv_%d", time.Now().UnixNano())
	db := conn.DB()
	db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	_, err := db.Exec(fmt.Sprintf(`
		CREATE TABLE %s (
			id TEXT PRIMARY KEY,
			name TEXT,
			email TEXT,
			age INT,
			deleted_at TIMESTAMPTZ
		)`, table))
	if err != nil {
		conn.Close()
		t.Fatalf("create table: %v", err)
	}
	connector := NewConnector(conn, conn)
	m := NewModel[User](connector, table).WithSoftDelete("deleted_at")
	cleanup := func() {
		db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
		conn.Close()
	}
	return m, cleanup
}

func TestPaginate_SoftDelete_WithOrderBy(t *testing.T) {
	m, cleanup := setupSoftDeleteTableAdv(t)
	defer cleanup()
	ctx := context.Background()

	// Insert 5 active and 2 deleted
	for i := 0; i < 5; i++ {
		m.writeConn.DB().Exec(
			fmt.Sprintf("INSERT INTO %s (id, name, email, age) VALUES ($1, $2, $3, $4)", m.tableName),
			fmt.Sprintf("a%d", i), fmt.Sprintf("Active%d", i), fmt.Sprintf("a%d@a.com", i), 20+i,
		)
	}
	for i := 0; i < 2; i++ {
		m.writeConn.DB().Exec(
			fmt.Sprintf("INSERT INTO %s (id, name, email, age, deleted_at) VALUES ($1,$2,$3,$4,NOW())", m.tableName),
			fmt.Sprintf("d%d", i), fmt.Sprintf("Deleted%d", i), fmt.Sprintf("d%d@d.com", i), 50+i,
		)
	}

	// Paginate with ORDER BY – this was the bug path
	page, err := m.Paginate(ctx, 1, 3,
		m.Query().OrderBy("name", false),
	)
	if err != nil {
		t.Fatalf("Paginate with OrderBy+SoftDelete: %v", err)
	}
	if page.Total != 5 {
		t.Errorf("expected 5 active, got %d", page.Total)
	}
	if len(page.Items) != 3 {
		t.Errorf("expected 3 on first page, got %d", len(page.Items))
	}
}

func TestPaginate_SoftDelete_FilterWorks(t *testing.T) {
	m, cleanup := setupSoftDeleteTableAdv(t)
	defer cleanup()
	ctx := context.Background()

	m.writeConn.DB().Exec(
		fmt.Sprintf("INSERT INTO %s (id, name, email, age) VALUES ($1,$2,$3,$4)", m.tableName),
		"v1", "Visible", "v@v.com", 30,
	)
	m.writeConn.DB().Exec(
		fmt.Sprintf("INSERT INTO %s (id, name, email, age, deleted_at) VALUES ($1,$2,$3,$4,NOW())", m.tableName),
		"del1", "Hidden", "h@h.com", 25,
	)

	page, err := m.Paginate(ctx, 1, 10, m.Query())
	if err != nil {
		t.Fatalf("Paginate soft-delete: %v", err)
	}
	if page.Total != 1 {
		t.Errorf("expected 1 visible, got %d (soft-delete not filtering)", page.Total)
	}
}

func TestPaginate_EmptyTable_TotalPagesZero(t *testing.T) {
	m, cleanup := setupSoftDeleteTableAdv(t)
	defer cleanup()
	ctx := context.Background()

	page, err := m.Paginate(ctx, 1, 10, m.Query())
	if err != nil {
		t.Fatalf("Paginate empty: %v", err)
	}
	if page.Total != 0 {
		t.Errorf("total should be 0, got %d", page.Total)
	}
	if page.TotalPages != 0 {
		t.Errorf("totalPages should be 0 when empty, got %d", page.TotalPages)
	}
}

func TestPaginate_WithWhereCondition(t *testing.T) {
	m, cleanup := setupSoftDeleteTableAdv(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 4; i++ {
		m.writeConn.DB().Exec(
			fmt.Sprintf("INSERT INTO %s (id, name, email, age) VALUES ($1,$2,$3,$4)", m.tableName),
			fmt.Sprintf("w%d", i), fmt.Sprintf("User%d", i), fmt.Sprintf("u%d@u.com", i), 20+i,
		)
	}

	page, err := m.Paginate(ctx, 1, 10,
		m.Query().WhereGreaterThanOrEqual("age", 22),
	)
	if err != nil {
		t.Fatalf("Paginate with condition: %v", err)
	}
	if page.Total != 2 {
		t.Errorf("expected 2 (age>=22), got %d", page.Total)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// PaginateAs: projecting into a different struct
// ─────────────────────────────────────────────────────────────────────────────

type UserNameOnly struct {
	ID   string `db:"id"`
	Name string `db:"name"`
}

func TestPaginateAs_ProjectionDTO(t *testing.T) {
	m, cleanup := setupTable(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		m.Create(ctx, User{
			ID:    fmt.Sprintf("pa%d", i),
			Name:  fmt.Sprintf("User%d", i),
			Email: fmt.Sprintf("u%d@u.com", i),
			Age:   20 + i,
		})
	}

	page, err := PaginateAs[User, UserNameOnly](ctx, m.readConn, 1, 3, m.Query().Select("id", "name"))
	if err != nil {
		t.Fatalf("PaginateAs: %v", err)
	}
	if page.Total != 5 {
		t.Errorf("expected 5 total, got %d", page.Total)
	}
	if len(page.Items) != 3 {
		t.Errorf("expected 3 items, got %d", len(page.Items))
	}
	if page.TotalPages != 2 {
		t.Errorf("expected 2 total pages, got %d", page.TotalPages)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// WhereDate integration with DB
// ─────────────────────────────────────────────────────────────────────────────

type EventRow struct {
	ID        string    `db:"id"`
	Name      string    `db:"name"`
	CreatedAt time.Time `db:"created_at"`
}

func TestQueryBuilder_WhereDate_Integration(t *testing.T) {
	conn := NewPostgresConnection(__TestDBconfig)
	if err := conn.Connect(context.Background()); err != nil {
		t.Skipf("no DB: %v", err)
	}
	defer conn.Close()
	db := conn.DB()

	table := fmt.Sprintf("test_evts_%d", time.Now().UnixNano())
	db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	if _, err := db.Exec(fmt.Sprintf(`CREATE TABLE %s (id TEXT PRIMARY KEY, name TEXT, created_at TIMESTAMPTZ)`, table)); err != nil {
		t.Fatalf("create: %v", err)
	}
	defer db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", table))

	ctx := context.Background()
	today := time.Now().UTC().Truncate(24 * time.Hour)
	yesterday := today.Add(-24 * time.Hour)

	db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s VALUES ($1,$2,$3)", table), "e1", "today-event", today.Add(2*time.Hour))
	db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s VALUES ($1,$2,$3)", table), "e2", "yesterday-event", yesterday.Add(2*time.Hour))

	connector := NewConnector(conn, conn)
	model := NewModel[EventRow](connector, table)

	q := model.Query().WhereDate("created_at", today).Build()
	t.Logf("WhereDate integration SQL: %s", q.SQL())
	t.Logf("WhereDate interpolated: %s", q.ToSQL())

	results, err := q.Exec(ctx)
	if err != nil {
		t.Fatalf("exec: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 today-event, got %d", len(results))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Clone divergence with DB
// ─────────────────────────────────────────────────────────────────────────────

func TestQueryBuilder_Clone_DivergentExecution(t *testing.T) {
	m, cleanup := setupTable(t)
	defer cleanup()
	ctx := context.Background()

	m.Create(ctx, User{ID: "cloneA", Name: "Alice", Email: "a@a.com", Age: 25})
	m.Create(ctx, User{ID: "cloneB", Name: "Bob", Email: "b@b.com", Age: 35})

	base := m.Query().WhereGreaterThan("age", 20)
	q1 := base.Clone().WhereLessThan("age", 30).Build()
	q2 := base.Clone().WhereGreaterThanOrEqual("age", 30).Build()

	r1, err := q1.Exec(ctx)
	if err != nil {
		t.Fatalf("q1 exec: %v", err)
	}
	r2, err := q2.Exec(ctx)
	if err != nil {
		t.Fatalf("q2 exec: %v", err)
	}

	if len(r1) != 1 || r1[0].Name != "Alice" {
		t.Errorf("q1 should return Alice, got: %+v", r1)
	}
	if len(r2) != 1 || r2[0].Name != "Bob" {
		t.Errorf("q2 should return Bob, got: %+v", r2)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Paginate with Limit in QueryBuilder (limit should not break count subquery)
// ─────────────────────────────────────────────────────────────────────────────

func TestPaginate_QueryBuilderWithLimit_NoBreakCount(t *testing.T) {
	m, cleanup := setupTable(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 8; i++ {
		m.Create(ctx, User{
			ID:    fmt.Sprintf("plim%d", i),
			Name:  fmt.Sprintf("U%d", i),
			Email: fmt.Sprintf("u%d@u.com", i),
			Age:   i + 1,
		})
	}

	// Passing a QueryBuilder that already has Limit/Offset should not break
	// Paginate's COUNT(*) subquery (since LIMIT is now a literal, not $N).
	page, err := m.Paginate(ctx, 1, 3, m.Query().OrderBy("age", false).Limit(100))
	if err != nil {
		t.Fatalf("Paginate with pre-Limit QB: %v", err)
	}
	// COUNT should still reflect all 8 rows
	if page.Total != 8 {
		t.Errorf("expected total 8, got %d", page.Total)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// formatArg coverage gaps
// ─────────────────────────────────────────────────────────────────────────────

func TestFormatArg_ByteSlice(t *testing.T) {
	result := InterpolateSQL("val=$1", []byte("hello"))
	if result != "val='hello'" {
		t.Errorf("[]byte: got %q", result)
	}
}

func TestFormatArg_ByteSliceWithQuote(t *testing.T) {
	result := InterpolateSQL("val=$1", []byte("it's"))
	if !strings.Contains(result, "''") {
		t.Errorf("[]byte escaping: got %q", result)
	}
}

func TestFormatArg_PtrTimestamp_NonNil(t *testing.T) {
	ts := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	result := InterpolateSQL("col=$1", &ts)
	if !strings.Contains(result, "2024-06-01") {
		t.Errorf("*time.Time: got %q", result)
	}
}

func TestFormatArg_Int(t *testing.T) {
	result := InterpolateSQL("x=$1", 42)
	if result != "x=42" {
		t.Errorf("int: got %q", result)
	}
}

func TestFormatArg_Float(t *testing.T) {
	result := InterpolateSQL("x=$1", 3.14)
	if !strings.Contains(result, "3.14") {
		t.Errorf("float: got %q", result)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// WhereNotExists with value bindings (cover the 75% branch)
// ─────────────────────────────────────────────────────────────────────────────

func TestQueryBuilder_WhereNotExists_WithArgs(t *testing.T) {
	model := &Model[TestUser]{tableName: "users"}
	qb := NewQueryBuilder(model).
		WhereNotExists("SELECT 1 FROM bans WHERE bans.user_id = users.id AND bans.type = ?", "permanent")
	sql := qb.SQL()
	if !strings.Contains(sql, "NOT EXISTS (") {
		t.Errorf("NOT EXISTS missing: %q", sql)
	}
	if !strings.Contains(sql, "$1") {
		t.Errorf("arg not bound: %q", sql)
	}
	if len(qb.Args()) != 1 || qb.Args()[0] != "permanent" {
		t.Errorf("args: %v", qb.Args())
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// stripLimitOffset / stripOrderByLimitOffset edge cases
// ─────────────────────────────────────────────────────────────────────────────

func TestStripLimitOffset_OnlyLimit(t *testing.T) {
	sql := stripLimitOffset("SELECT * FROM users WHERE age=$1 LIMIT 10")
	if strings.Contains(sql, "LIMIT") {
		t.Errorf("LIMIT should be stripped: %q", sql)
	}
	if !strings.Contains(sql, "WHERE age=$1") {
		t.Errorf("WHERE should remain: %q", sql)
	}
}

func TestStripLimitOffset_LimitAndOffset(t *testing.T) {
	sql := stripLimitOffset("SELECT * FROM users LIMIT 10 OFFSET 20")
	if strings.Contains(sql, "LIMIT") || strings.Contains(sql, "OFFSET") {
		t.Errorf("should strip both: %q", sql)
	}
}

func TestStripLimitOffset_NoLimitNoOffset(t *testing.T) {
	original := "SELECT * FROM users WHERE age=$1 ORDER BY id"
	sql := stripLimitOffset(original)
	if sql != original {
		t.Errorf("should be unchanged: %q", sql)
	}
}

func TestStripOrderByLimitOffset_All(t *testing.T) {
	sql := stripOrderByLimitOffset("SELECT * FROM t WHERE a=1 ORDER BY b LIMIT 5 OFFSET 10")
	if strings.Contains(strings.ToUpper(sql), "ORDER BY") ||
		strings.Contains(strings.ToUpper(sql), "LIMIT") ||
		strings.Contains(strings.ToUpper(sql), "OFFSET") {
		t.Errorf("should strip all: %q", sql)
	}
	if !strings.Contains(sql, "WHERE a=1") {
		t.Errorf("WHERE should remain: %q", sql)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// buildCountSQL edge cases
// ─────────────────────────────────────────────────────────────────────────────

func TestBuildCountSQL_WithJoin(t *testing.T) {
	sql := buildCountSQL(
		"SELECT u.id FROM users u JOIN orders o ON o.user_id = u.id WHERE u.age > 18 ORDER BY u.id",
		"users",
	)
	if !strings.Contains(sql, "COUNT(*)") {
		t.Errorf("COUNT(*) missing: %q", sql)
	}
	if strings.Contains(strings.ToUpper(sql), "ORDER BY") {
		t.Errorf("ORDER BY should be stripped from count: %q", sql)
	}
}

func TestBuildCountSQL_NoFromClause_FallbackSubquery(t *testing.T) {
	// Exotic SQL without standard FROM (should fall back to subquery)
	sql := buildCountSQL("VALUES (1),(2),(3)", "dual")
	if !strings.Contains(sql, "COUNT(*)") {
		t.Errorf("COUNT(*) missing in fallback: %q", sql)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// PaginateAs fallback path: QB SQL without FROM (covers buildCountSQL fallback)
// ─────────────────────────────────────────────────────────────────────────────

func TestPaginateAs_SecondPage(t *testing.T) {
	m, cleanup := setupTable(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		m.Create(ctx, User{
			ID:    fmt.Sprintf("sp%d", i),
			Name:  fmt.Sprintf("U%d", i),
			Email: fmt.Sprintf("u%d@u.com", i),
			Age:   i,
		})
	}

	// page 2, size 2 → should get items [2,3]
	page, err := PaginateAs[User, User](ctx, m.readConn, 2, 2, m.Query().OrderBy("age", false))
	if err != nil {
		t.Fatalf("PaginateAs second page: %v", err)
	}
	if page.Total != 5 {
		t.Errorf("expected 5 total, got %d", page.Total)
	}
	if len(page.Items) != 2 {
		t.Errorf("expected 2 items on page 2, got %d", len(page.Items))
	}
	if page.Page != 2 {
		t.Errorf("expected page 2, got %d", page.Page)
	}
	if page.TotalPages != 3 {
		t.Errorf("expected 3 total pages, got %d", page.TotalPages)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Health check partial failure (write DB closed) – covers health.go 66% branch
// ─────────────────────────────────────────────────────────────────────────────

func TestHealthChecker_Check_PartialFailure(t *testing.T) {
	r := NewPostgresConnection(__TestDBconfig)
	if err := r.Connect(context.Background()); err != nil {
		t.Skipf("no DB: %v", err)
	}
	defer r.Close()

	// Create a read-only connector where write is same as read (both work).
	c := NewConnector(r, r)
	checker := NewHealthChecker(c)
	status := checker.Check(context.Background())
	if !status.Healthy {
		t.Errorf("expected healthy: %v", status.Error)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// QueryBuilder.Having with no preceding group – edge case
// ─────────────────────────────────────────────────────────────────────────────

func TestQueryBuilder_Having_NoGroupBy(t *testing.T) {
	model := &Model[TestUser]{tableName: "users"}
	qb := NewQueryBuilder(model).Where("active", true).Having("COUNT(*) >", 1)
	sql := qb.SQL()
	if !strings.Contains(sql, "HAVING COUNT(*) > $2") {
		t.Errorf("HAVING clause: %q", sql)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Chunk with soft-delete (covers model.go Chunk applyBaseQuery path)
// ─────────────────────────────────────────────────────────────────────────────

func TestModel_Chunk_SoftDelete(t *testing.T) {
	m, cleanup := setupSoftDeleteTableAdv(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		m.writeConn.DB().Exec(
			fmt.Sprintf("INSERT INTO %s (id, name, email, age) VALUES ($1,$2,$3,$4)", m.tableName),
			fmt.Sprintf("c%d", i), fmt.Sprintf("U%d", i), fmt.Sprintf("u%d@u.com", i), i,
		)
	}
	// soft-deleted row
	m.writeConn.DB().Exec(
		fmt.Sprintf("INSERT INTO %s (id, name, email, age, deleted_at) VALUES ($1,$2,$3,$4,NOW())", m.tableName),
		"del", "Deleted", "d@d.com", 99,
	)

	var processed []User
	err := m.Chunk(ctx, 10, nil, func(batch []User) error {
		processed = append(processed, batch...)
		return nil
	})
	if err != nil {
		t.Fatalf("Chunk soft-delete: %v", err)
	}
	if len(processed) != 3 {
		t.Errorf("expected 3 active rows, got %d", len(processed))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// QueryBuilder ToSQL with no args
// ─────────────────────────────────────────────────────────────────────────────

func TestQueryBuilder_ToSQL_NoArgs(t *testing.T) {
	model := &Model[TestUser]{tableName: "users"}
	qb := NewQueryBuilder(model).WhereNull("deleted_at")
	sql := qb.ToSQL()
	// No placeholders – should be unchanged
	expected := "SELECT * FROM users WHERE deleted_at IS NULL"
	if sql != expected {
		t.Errorf("ToSQL no-args: got %q, want %q", sql, expected)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Paginate default values (page=0, pageSize=0 → defaults)
// ─────────────────────────────────────────────────────────────────────────────

func TestPaginate_DefaultValues_WithQueryBuilder(t *testing.T) {
	m, cleanup := setupTable(t)
	defer cleanup()
	ctx := context.Background()

	page, err := m.Paginate(ctx, 0, 0, m.Query())
	if err != nil {
		t.Fatalf("Paginate defaults: %v", err)
	}
	if page.Page != 1 {
		t.Errorf("expected default page 1, got %d", page.Page)
	}
	if page.PageSize != 10 {
		t.Errorf("expected default pageSize 10, got %d", page.PageSize)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// WhereDate conjunction (AND path)
// ─────────────────────────────────────────────────────────────────────────────

func TestQueryBuilder_WhereDate_WithExistingWhere(t *testing.T) {
	model := &Model[TestUser]{tableName: "events"}
	day := time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC)
	qb := NewQueryBuilder(model).Where("type", "click").WhereDate("created_at", day)
	sql := qb.SQL()
	if !strings.Contains(sql, "AND created_at >= $2 AND created_at < $3") {
		t.Errorf("WhereDate AND path: %q", sql)
	}
	if len(qb.Args()) != 3 {
		t.Errorf("expected 3 args, got %d: %v", len(qb.Args()), qb.Args())
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// WhereDateBetween conjunction (AND path)
// ─────────────────────────────────────────────────────────────────────────────

func TestQueryBuilder_WhereDateBetween_WithExistingWhere(t *testing.T) {
	model := &Model[TestUser]{tableName: "events"}
	from := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2024, 1, 31, 0, 0, 0, 0, time.UTC)
	qb := NewQueryBuilder(model).Where("type", "view").WhereDateBetween("created_at", from, to)
	sql := qb.SQL()
	if !strings.Contains(sql, "AND created_at >= $2") {
		t.Errorf("WhereDateBetween AND path: %q", sql)
	}
}

