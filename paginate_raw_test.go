package dbconnector

// paginate_raw_test.go
// Tests for:
//   1. buildCountSQL – tableName used in fallback alias
//   2. Paginate / PaginateAs accepting Paginatable[T] (both *QueryBuilder and *RawQuery)
//   3. NewQueryBuilderFromSQL – parse raw SQL into a builder

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// buildCountSQL – tableName is now used in the fallback alias
// ─────────────────────────────────────────────────────────────────────────────

func TestBuildCountSQL_UsesTableNameInFallback(t *testing.T) {
	// SQL without a FROM clause → fallback subquery
	sql := buildCountSQL("VALUES (1),(2),(3)", "orders")
	if !strings.Contains(sql, "orders_cnt") {
		t.Errorf("expected tableName used as alias suffix, got: %q", sql)
	}
	if !strings.Contains(sql, "COUNT(*)") {
		t.Errorf("expected COUNT(*), got: %q", sql)
	}
}

func TestBuildCountSQL_NormalPath_TableNameUnaffected(t *testing.T) {
	// Normal SELECT … FROM … path – tableName is not used but must not break anything
	sql := buildCountSQL(
		"SELECT id, name FROM users WHERE status = $1 ORDER BY id",
		"users",
	)
	expected := "SELECT COUNT(*) FROM users WHERE status = $1"
	if sql != expected {
		t.Errorf("got %q\nwant %q", sql, expected)
	}
}

func TestBuildCountSQL_FallbackAlias_DifferentTableName(t *testing.T) {
	sql := buildCountSQL("VALUES ($1)", "accounts")
	if !strings.Contains(sql, "accounts_cnt") {
		t.Errorf("expected accounts_cnt alias, got: %q", sql)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// NewQueryBuilderFromSQL – unit tests (no DB needed)
// ─────────────────────────────────────────────────────────────────────────────

func TestNewQueryBuilderFromSQL_NoWhere(t *testing.T) {
	model := &Model[TestUser]{tableName: "users"}
	qb := NewQueryBuilderFromSQL(model, "SELECT * FROM users")

	if qb.whereAdded {
		t.Error("whereAdded should be false when SQL has no WHERE")
	}
	if len(qb.Args()) != 0 {
		t.Errorf("expected 0 args, got %d", len(qb.Args()))
	}
	if qb.SQL() != "SELECT * FROM users" {
		t.Errorf("SQL mismatch: %q", qb.SQL())
	}
}

func TestNewQueryBuilderFromSQL_WithWhere(t *testing.T) {
	model := &Model[TestUser]{tableName: "users"}
	qb := NewQueryBuilderFromSQL(model, "SELECT * FROM users WHERE tenant_id = $1", "tenant-abc")

	if !qb.whereAdded {
		t.Error("whereAdded should be true when SQL contains WHERE")
	}
	if len(qb.Args()) != 1 || qb.Args()[0] != "tenant-abc" {
		t.Errorf("args mismatch: %v", qb.Args())
	}
}

func TestNewQueryBuilderFromSQL_AppendWhere(t *testing.T) {
	model := &Model[TestUser]{tableName: "users"}
	qb := NewQueryBuilderFromSQL(model, "SELECT * FROM users WHERE tenant_id = $1", "tenant-abc").
		Where("status", "active")

	sql := qb.SQL()
	// Next condition must use AND and $2
	if !strings.Contains(sql, "AND status = $2") {
		t.Errorf("expected AND status = $2, got: %q", sql)
	}
	if len(qb.Args()) != 2 {
		t.Errorf("expected 2 args, got %d: %v", len(qb.Args()), qb.Args())
	}
}

func TestNewQueryBuilderFromSQL_AppendWhere_NoExistingWhere(t *testing.T) {
	model := &Model[TestUser]{tableName: "users"}
	qb := NewQueryBuilderFromSQL(model, "SELECT * FROM users").
		Where("age", 30)

	sql := qb.SQL()
	// Should emit WHERE (not AND) for the first condition
	if !strings.Contains(sql, "WHERE age = $1") {
		t.Errorf("expected WHERE age = $1, got: %q", sql)
	}
}

func TestNewQueryBuilderFromSQL_MultipleArgs(t *testing.T) {
	model := &Model[TestUser]{tableName: "users"}
	qb := NewQueryBuilderFromSQL(model,
		"SELECT * FROM users WHERE a = $1 AND b = $2",
		"x", 42,
	).WhereNot("status", "banned")

	if len(qb.Args()) != 3 {
		t.Errorf("expected 3 args, got %d: %v", len(qb.Args()), qb.Args())
	}
	if !strings.Contains(qb.SQL(), "status != $3") {
		t.Errorf("expected status != $3, got: %q", qb.SQL())
	}
}

func TestNewQueryBuilderFromSQL_WithOrderBy_ThenAppendWhere(t *testing.T) {
	// Edge case: raw SQL already has ORDER BY; adding Where must still use AND
	model := &Model[TestUser]{tableName: "users"}
	qb := NewQueryBuilderFromSQL(model,
		"SELECT * FROM users WHERE active = $1 ORDER BY name",
		true,
	).Where("age", 18)

	sql := qb.SQL()
	if !strings.Contains(sql, "AND age = $2") {
		t.Errorf("expected AND age = $2, got: %q", sql)
	}
}

func TestNewQueryBuilderFromSQL_ToSQL(t *testing.T) {
	model := &Model[TestUser]{tableName: "users"}
	qb := NewQueryBuilderFromSQL(model,
		"SELECT * FROM users WHERE id = $1", "abc-123",
	)
	interpolated := qb.ToSQL()
	if !strings.Contains(interpolated, "'abc-123'") {
		t.Errorf("ToSQL should interpolate args, got: %q", interpolated)
	}
	if strings.Contains(interpolated, "$1") {
		t.Errorf("ToSQL should not contain placeholder, got: %q", interpolated)
	}
}

func TestNewQueryBuilderFromSQL_SoftDeleteInjected(t *testing.T) {
	model := &Model[TestUser]{
		tableName:     "posts",
		softDeleteCol: "deleted_at",
	}
	q := NewQueryBuilderFromSQL(model,
		"SELECT * FROM posts WHERE author_id = $1 ORDER BY created_at DESC",
		"user-1",
	).Build()

	sql := q.SQL()
	idxFilter := strings.Index(sql, "deleted_at IS NULL")
	idxOrder := strings.Index(sql, "ORDER BY")

	if idxFilter == -1 {
		t.Fatalf("soft-delete filter missing from: %q", sql)
	}
	if idxOrder != -1 && idxFilter > idxOrder {
		t.Errorf("soft-delete filter must appear before ORDER BY:\n  %q", sql)
	}
}

func TestNewQueryBuilderFromSQL_Clone(t *testing.T) {
	model := &Model[TestUser]{tableName: "users"}
	base := NewQueryBuilderFromSQL(model, "SELECT * FROM users WHERE a = $1", 1)

	b1 := base.Clone().Where("b", 10)
	b2 := base.Clone().Where("b", 20)

	if b1.Args()[1] == b2.Args()[1] {
		t.Error("cloned builders must diverge")
	}
	// base untouched
	if len(base.Args()) != 1 {
		t.Errorf("base args modified: %v", base.Args())
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// RawQuery – unit tests (no DB)
// ─────────────────────────────────────────────────────────────────────────────

func TestNewRawQuery_Interface(t *testing.T) {
	rq := NewRawQuery[TestUser]("users", "SELECT * FROM users WHERE status = $1", "active")

	if rq.paginatableSQL() != "SELECT * FROM users WHERE status = $1" {
		t.Errorf("paginatableSQL: %q", rq.paginatableSQL())
	}
	if len(rq.paginatableArgs()) != 1 || rq.paginatableArgs()[0] != "active" {
		t.Errorf("paginatableArgs: %v", rq.paginatableArgs())
	}
	if rq.paginatableTableName() != "users" {
		t.Errorf("paginatableTableName: %q", rq.paginatableTableName())
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Integration: Paginate with *QueryBuilder[T] (existing behaviour preserved)
// ─────────────────────────────────────────────────────────────────────────────

func TestPaginate_WithQueryBuilder_Integration(t *testing.T) {
	m, cleanup := setupTable(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 6; i++ {
		m.Create(ctx, User{
			ID:    fmt.Sprintf("pqb%d", i),
			Name:  fmt.Sprintf("User%d", i),
			Email: fmt.Sprintf("u%d@u.com", i),
			Age:   20 + i,
		})
	}

	page, err := m.Paginate(ctx, 1, 4, m.Query().OrderBy("age", false))
	if err != nil {
		t.Fatalf("Paginate with QB: %v", err)
	}
	if page.Total != 6 {
		t.Errorf("expected total 6, got %d", page.Total)
	}
	if len(page.Items) != 4 {
		t.Errorf("expected 4 items on page 1, got %d", len(page.Items))
	}
	if page.TotalPages != 2 {
		t.Errorf("expected 2 total pages, got %d", page.TotalPages)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Integration: Paginate with *RawQuery[T]
// ─────────────────────────────────────────────────────────────────────────────

func TestPaginate_WithRawQuery_Integration(t *testing.T) {
	m, cleanup := setupTable(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 7; i++ {
		m.Create(ctx, User{
			ID:    fmt.Sprintf("prq%d", i),
			Name:  fmt.Sprintf("User%d", i),
			Email: fmt.Sprintf("u%d@u.com", i),
			Age:   10 + i,
		})
	}

	rq := NewRawQuery[User](m.tableName,
		fmt.Sprintf("SELECT * FROM %s WHERE age >= $1 ORDER BY age ASC", m.tableName),
		12, // age >= 12 → 5 rows (12,13,14,15,16)
	)
	page, err := m.Paginate(ctx, 1, 3, rq)
	if err != nil {
		t.Fatalf("Paginate with RawQuery: %v", err)
	}
	if page.Total != 5 {
		t.Errorf("expected total 5, got %d", page.Total)
	}
	if len(page.Items) != 3 {
		t.Errorf("expected 3 items on page 1, got %d", len(page.Items))
	}
	if page.TotalPages != 2 {
		t.Errorf("expected 2 total pages, got %d", page.TotalPages)
	}
}

func TestPaginate_WithRawQuery_Page2(t *testing.T) {
	m, cleanup := setupTable(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		m.Create(ctx, User{
			ID:    fmt.Sprintf("rqp2%d", i),
			Name:  fmt.Sprintf("U%d", i),
			Email: fmt.Sprintf("u%d@u.com", i),
			Age:   i,
		})
	}

	rq := NewRawQuery[User](m.tableName,
		fmt.Sprintf("SELECT * FROM %s ORDER BY age ASC", m.tableName),
	)
	page, err := m.Paginate(ctx, 2, 2, rq)
	if err != nil {
		t.Fatalf("Paginate RawQuery page 2: %v", err)
	}
	if page.Total != 5 {
		t.Errorf("expected total 5, got %d", page.Total)
	}
	if len(page.Items) != 2 {
		t.Errorf("expected 2 items on page 2, got %d", len(page.Items))
	}
	if page.Page != 2 {
		t.Errorf("expected page 2, got %d", page.Page)
	}
}

func TestPaginate_WithRawQuery_EmptyResult(t *testing.T) {
	m, cleanup := setupTable(t)
	defer cleanup()
	ctx := context.Background()

	rq := NewRawQuery[User](m.tableName,
		fmt.Sprintf("SELECT * FROM %s WHERE age > $1", m.tableName),
		9999,
	)
	page, err := m.Paginate(ctx, 1, 10, rq)
	if err != nil {
		t.Fatalf("Paginate RawQuery empty: %v", err)
	}
	if page.Total != 0 {
		t.Errorf("expected 0 total, got %d", page.Total)
	}
	if page.TotalPages != 0 {
		t.Errorf("expected 0 total pages, got %d", page.TotalPages)
	}
}

func TestPaginate_WithRawQuery_NoArgs(t *testing.T) {
	m, cleanup := setupTable(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		m.Create(ctx, User{
			ID:    fmt.Sprintf("rqna%d", i),
			Name:  fmt.Sprintf("U%d", i),
			Email: fmt.Sprintf("u%d@u.com", i),
			Age:   i,
		})
	}

	rq := NewRawQuery[User](m.tableName,
		fmt.Sprintf("SELECT * FROM %s", m.tableName),
	)
	page, err := m.Paginate(ctx, 1, 10, rq)
	if err != nil {
		t.Fatalf("Paginate RawQuery no-args: %v", err)
	}
	if page.Total != 3 {
		t.Errorf("expected 3 total, got %d", page.Total)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Integration: PaginateAs with *RawQuery[T] projecting into different struct
// ─────────────────────────────────────────────────────────────────────────────

func TestPaginateAs_WithRawQuery_Projection(t *testing.T) {
	m, cleanup := setupTable(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 4; i++ {
		m.Create(ctx, User{
			ID:    fmt.Sprintf("rqpr%d", i),
			Name:  fmt.Sprintf("User%d", i),
			Email: fmt.Sprintf("u%d@u.com", i),
			Age:   i,
		})
	}

	type NameOnly struct {
		ID   string `db:"id"`
		Name string `db:"name"`
	}

	rq := NewRawQuery[User](m.tableName,
		fmt.Sprintf("SELECT id, name FROM %s ORDER BY name", m.tableName),
	)
	page, err := PaginateAs[User, NameOnly](ctx, m.readConn, 1, 2, rq)
	if err != nil {
		t.Fatalf("PaginateAs with RawQuery: %v", err)
	}
	if page.Total != 4 {
		t.Errorf("expected 4 total, got %d", page.Total)
	}
	if len(page.Items) != 2 {
		t.Errorf("expected 2 items, got %d", len(page.Items))
	}
	for _, item := range page.Items {
		if item.Name == "" {
			t.Error("projected Name should not be empty")
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Integration: NewQueryBuilderFromSQL + Paginate
// ─────────────────────────────────────────────────────────────────────────────

func TestNewQueryBuilderFromSQL_Paginate(t *testing.T) {
	m, cleanup := setupTable(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 8; i++ {
		m.Create(ctx, User{
			ID:    fmt.Sprintf("fsp%d", i),
			Name:  fmt.Sprintf("User%d", i),
			Email: fmt.Sprintf("u%d@u.com", i),
			Age:   i + 1,
		})
	}

	// Build QB from a raw SQL that already has a WHERE clause,
	// then add more conditions and paginate.
	qb := NewQueryBuilderFromSQL(m,
		fmt.Sprintf("SELECT * FROM %s WHERE age > $1", m.tableName),
		0,
	).WhereGreaterThanOrEqual("age", 3).OrderBy("age", false)

	page, err := m.Paginate(ctx, 1, 3, qb)
	if err != nil {
		t.Fatalf("Paginate with NewQueryBuilderFromSQL: %v", err)
	}
	// age > 0 AND age >= 3 → users with age 3..8 = 6 rows
	if page.Total != 6 {
		t.Errorf("expected 6 total, got %d", page.Total)
	}
	if len(page.Items) != 3 {
		t.Errorf("expected 3 on page 1, got %d", len(page.Items))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Integration: NewQueryBuilderFromSQL + soft-delete + paginate
// ─────────────────────────────────────────────────────────────────────────────

func TestNewQueryBuilderFromSQL_SoftDelete_Paginate(t *testing.T) {
	m, cleanup := setupSoftDeleteTableAdv(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 4; i++ {
		m.writeConn.DB().Exec(
			fmt.Sprintf("INSERT INTO %s (id, name, email, age) VALUES ($1,$2,$3,$4)", m.tableName),
			fmt.Sprintf("fs%d", i), fmt.Sprintf("U%d", i), fmt.Sprintf("u%d@u.com", i), i,
		)
	}
	// 1 soft-deleted row
	m.writeConn.DB().Exec(
		fmt.Sprintf("INSERT INTO %s (id, name, email, age, deleted_at) VALUES ($1,$2,$3,$4,NOW())", m.tableName),
		"fsdel", "Deleted", "d@d.com", 99,
	)

	qb := NewQueryBuilderFromSQL(m,
		fmt.Sprintf("SELECT * FROM %s", m.tableName),
	).OrderBy("age", false)

	page, err := m.Paginate(ctx, 1, 10, qb)
	if err != nil {
		t.Fatalf("Paginate FromSQL+SoftDelete: %v", err)
	}
	// soft-deleted row must be excluded
	if page.Total != 4 {
		t.Errorf("expected 4 active rows, got %d", page.Total)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Paginate default values via RawQuery
// ─────────────────────────────────────────────────────────────────────────────

func TestPaginate_RawQuery_DefaultPageValues(t *testing.T) {
	m, cleanup := setupTable(t)
	defer cleanup()
	ctx := context.Background()

	rq := NewRawQuery[User](m.tableName,
		fmt.Sprintf("SELECT * FROM %s", m.tableName),
	)
	page, err := m.Paginate(ctx, 0, 0, rq) // page=0, pageSize=0 → defaults
	if err != nil {
		t.Fatalf("Paginate defaults with RawQuery: %v", err)
	}
	if page.Page != 1 {
		t.Errorf("expected default page 1, got %d", page.Page)
	}
	if page.PageSize != 10 {
		t.Errorf("expected default pageSize 10, got %d", page.PageSize)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// NewQueryBuilderFromSQL – helper method on Model
// ─────────────────────────────────────────────────────────────────────────────

func TestModel_QueryFromSQL_HelperMethod(t *testing.T) {
	m, cleanup := setupTable(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		m.Create(ctx, User{
			ID:    fmt.Sprintf("mfs%d", i),
			Name:  fmt.Sprintf("U%d", i),
			Email: fmt.Sprintf("u%d@u.com", i),
			Age:   i + 5,
		})
	}

	// Use QueryFromSQL on the model directly
	qb := m.QueryFromSQL(
		fmt.Sprintf("SELECT * FROM %s WHERE age >= $1", m.tableName), 6,
	)
	results, err := qb.Build().Exec(ctx)
	if err != nil {
		t.Fatalf("QueryFromSQL exec: %v", err)
	}
	// age >= 6 → 2 rows (age 6, 7)
	if len(results) != 2 {
		t.Errorf("expected 2 results, got %d", len(results))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Verify that RawQuery respects Paginatable interface in type system
// ─────────────────────────────────────────────────────────────────────────────

func TestPaginatable_Interface_Satisfaction(t *testing.T) {
	model := &Model[TestUser]{tableName: "users"}

	// Both must satisfy Paginatable[T] – compile-time check
	var _ Paginatable[TestUser] = NewQueryBuilder(model)
	var _ Paginatable[TestUser] = NewRawQuery[TestUser]("users", "SELECT * FROM users")
	var _ Paginatable[TestUser] = NewQueryBuilderFromSQL(model, "SELECT * FROM users")

	t.Log("all three types satisfy Paginatable[T]")
}

// ─────────────────────────────────────────────────────────────────────────────
// RawQuery error path: table doesn't exist
// ─────────────────────────────────────────────────────────────────────────────

func TestPaginate_RawQuery_BadTable(t *testing.T) {
	conn := NewPostgresConnection(__TestDBconfig)
	if err := conn.Connect(context.Background()); err != nil {
		t.Skipf("no DB: %v", err)
	}
	defer conn.Close()

	m := NewModel[User](NewConnector(conn, conn), "nonexistent_raw_xyz")
	rq := NewRawQuery[User]("nonexistent_raw_xyz", "SELECT * FROM nonexistent_raw_xyz")
	_, err := m.Paginate(context.Background(), 1, 10, rq)
	if err == nil {
		t.Error("expected error for nonexistent table in RawQuery Paginate")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// NewQueryBuilderFromSQL – preserve WithTrashed flag
// ─────────────────────────────────────────────────────────────────────────────

func TestNewQueryBuilderFromSQL_WithTrashed(t *testing.T) {
	model := &Model[TestUser]{
		tableName:     "users",
		softDeleteCol: "deleted_at",
	}
	qb := NewQueryBuilderFromSQL(model, "SELECT * FROM users").WithTrashed()
	q := qb.Build()
	if strings.Contains(q.SQL(), "deleted_at IS NULL") {
		t.Errorf("WithTrashed should suppress filter, got: %q", q.SQL())
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// buildCountSQL with JOIN (the table name in alias must not corrupt the SQL)
// ─────────────────────────────────────────────────────────────────────────────

func TestBuildCountSQL_WithJoinAndAlias(t *testing.T) {
	sql := buildCountSQL(
		"SELECT u.id, o.total FROM users u JOIN orders o ON o.user_id = u.id WHERE u.active = $1 ORDER BY u.id",
		"users",
	)
	if !strings.Contains(sql, "COUNT(*)") {
		t.Errorf("COUNT(*) missing: %q", sql)
	}
	if strings.Contains(strings.ToUpper(sql), "ORDER BY") {
		t.Errorf("ORDER BY should be stripped: %q", sql)
	}
	// Should contain the WHERE condition
	if !strings.Contains(sql, "WHERE u.active = $1") {
		t.Errorf("WHERE clause should be preserved: %q", sql)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// NewQueryBuilderFromSQL integration: Build + Execute
// ─────────────────────────────────────────────────────────────────────────────

func TestNewQueryBuilderFromSQL_BuildAndExec(t *testing.T) {
	m, cleanup := setupTable(t)
	defer cleanup()
	ctx := context.Background()

	m.Create(ctx, User{ID: "qfs1", Name: "Alice", Email: "a@a.com", Age: 30})
	m.Create(ctx, User{ID: "qfs2", Name: "Bob", Email: "b@b.com", Age: 25})

	qb := NewQueryBuilderFromSQL(m,
		fmt.Sprintf("SELECT * FROM %s WHERE age > $1", m.tableName),
		20,
	).Where("name", "Alice")

	results, err := qb.Build().Exec(ctx)
	if err != nil {
		t.Fatalf("Build+Exec: %v", err)
	}
	if len(results) != 1 || results[0].Name != "Alice" {
		t.Errorf("expected Alice, got: %+v", results)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Timestamp: NewQueryBuilderFromSQL handles time args
// ─────────────────────────────────────────────────────────────────────────────

func TestNewQueryBuilderFromSQL_TimestampArg_ToSQL(t *testing.T) {
	model := &Model[TestUser]{tableName: "events"}
	ts := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	qb := NewQueryBuilderFromSQL(model,
		"SELECT * FROM events WHERE created_at > $1", ts,
	)
	interpolated := qb.ToSQL()
	if !strings.Contains(interpolated, "2024-06-01") {
		t.Errorf("timestamp not interpolated: %q", interpolated)
	}
	if strings.Contains(interpolated, "$1") {
		t.Errorf("placeholder not replaced: %q", interpolated)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Page[T] navigation helpers – HasNext / HasPrev / NextPage / PrevPage
// ─────────────────────────────────────────────────────────────────────────────

func TestPage_HasNext_HasPrev(t *testing.T) {
	cases := []struct {
		page, totalPages int
		wantNext         bool
		wantPrev         bool
	}{
		{page: 1, totalPages: 1, wantNext: false, wantPrev: false},
		{page: 1, totalPages: 3, wantNext: true, wantPrev: false},
		{page: 2, totalPages: 3, wantNext: true, wantPrev: true},
		{page: 3, totalPages: 3, wantNext: false, wantPrev: true},
		{page: 1, totalPages: 0, wantNext: false, wantPrev: false},
	}

	for _, c := range cases {
		p := &Page[TestUser]{Page: c.page, TotalPages: c.totalPages}
		if p.HasNext() != c.wantNext {
			t.Errorf("page=%d totalPages=%d: HasNext()=%v, want %v",
				c.page, c.totalPages, p.HasNext(), c.wantNext)
		}
		if p.HasPrev() != c.wantPrev {
			t.Errorf("page=%d totalPages=%d: HasPrev()=%v, want %v",
				c.page, c.totalPages, p.HasPrev(), c.wantPrev)
		}
	}
}

func TestPage_NextPage_PrevPage(t *testing.T) {
	// Middle page
	p := &Page[TestUser]{Page: 2, TotalPages: 5}
	if got := p.NextPage(); got != 3 {
		t.Errorf("NextPage: got %d, want 3", got)
	}
	if got := p.PrevPage(); got != 1 {
		t.Errorf("PrevPage: got %d, want 1", got)
	}

	// Last page – NextPage clamps
	last := &Page[TestUser]{Page: 5, TotalPages: 5}
	if got := last.NextPage(); got != 5 {
		t.Errorf("NextPage at last: got %d, want 5 (clamped)", got)
	}

	// First page – PrevPage clamps
	first := &Page[TestUser]{Page: 1, TotalPages: 5}
	if got := first.PrevPage(); got != 1 {
		t.Errorf("PrevPage at first: got %d, want 1 (clamped)", got)
	}
}

func TestPage_Navigation_Integration(t *testing.T) {
	m, cleanup := setupTable(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 9; i++ {
		m.Create(ctx, User{
			ID:    fmt.Sprintf("nav%d", i),
			Name:  fmt.Sprintf("User%d", i),
			Email: fmt.Sprintf("u%d@nav.com", i),
			Age:   i,
		})
	}

	// page 1 of 3 (pageSize=3, total=9)
	p1, err := m.Paginate(ctx, 1, 3, m.Query().OrderBy("age", false))
	if err != nil {
		t.Fatalf("Paginate p1: %v", err)
	}
	if !p1.HasNext() {
		t.Error("page 1: HasNext should be true")
	}
	if p1.HasPrev() {
		t.Error("page 1: HasPrev should be false")
	}
	if p1.NextPage() != 2 {
		t.Errorf("page 1: NextPage=%d, want 2", p1.NextPage())
	}

	// page 2 of 3
	p2, err := m.Paginate(ctx, 2, 3, m.Query().OrderBy("age", false))
	if err != nil {
		t.Fatalf("Paginate p2: %v", err)
	}
	if !p2.HasNext() {
		t.Error("page 2: HasNext should be true")
	}
	if !p2.HasPrev() {
		t.Error("page 2: HasPrev should be true")
	}

	// page 3 of 3
	p3, err := m.Paginate(ctx, 3, 3, m.Query().OrderBy("age", false))
	if err != nil {
		t.Fatalf("Paginate p3: %v", err)
	}
	if p3.HasNext() {
		t.Error("page 3: HasNext should be false")
	}
	if !p3.HasPrev() {
		t.Error("page 3: HasPrev should be true")
	}
	if p3.NextPage() != 3 {
		t.Errorf("page 3: NextPage=%d, want 3 (clamped)", p3.NextPage())
	}
}

