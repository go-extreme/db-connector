package dbconnector

// soft_delete_test.go
//
// Exhaustive test suite for Model.WithSoftDelete / injectSoftDeleteFilter.
//
// ── Why these tests exist ────────────────────────────────────────────────────
//
// Known bugs that have been/could be present:
//
//  Bug-A  Filter appended at the very END of the SQL, even AFTER ORDER BY /
//         LIMIT / OFFSET (should be inserted BEFORE those clauses).
//
//  Bug-B  Filter not added at all because the table or column name contains
//         the substring "where" (e.g. "test_SoftDelete_WhereFilter_…") and a
//         check like strings.Contains(upper,"WHERE") fires a false positive,
//         producing " AND <filter>" instead of " WHERE <filter>" or vice-versa.
//
//  Bug-C  " AND <filter>" written without a preceding WHERE clause (bare AND).
//
//  Bug-D  QueryFromSQL: chained Where() after a raw SQL that already has
//         ORDER BY appends conditions AFTER ORDER BY.
//
//  Bug-E  Double soft-delete injection when paginatableSQL() and Build() are
//         both called on the same builder.
//
//  Bug-F  WithTrashed() still injects the filter (should be a no-op).
//
// Structure
// ─────────
//  Section 1 – Pure SQL-generation unit tests (no DB required).
//              Call injectSoftDeleteFilter / Model.applyBaseQuery directly.
//  Section 2 – QueryBuilder SQL-generation unit tests (no DB required).
//              Build a QB and inspect the generated SQL string.
//  Section 3 – Integration tests (DB required).
//              Verify that the DB actually returns the right rows.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
)

// ─────────────────────────────────────────────────────────────────────────────
// helpers
// ─────────────────────────────────────────────────────────────────────────────

// sdModel creates a Model[User] with softDeleteCol = "deleted_at".
// No DB connection is needed; we only inspect the generated SQL.
func sdModel(t *testing.T) *Model[User] {
	t.Helper()
	conn := NewPostgresConnection(__TestDBconfig)
	m := NewModel[User](NewConnector(conn, conn), "users").WithSoftDelete("deleted_at")
	return m
}

// mustContain fails if substr is not in s.
func mustContain(t *testing.T, s, substr, label string) {
	t.Helper()
	if !strings.Contains(s, substr) {
		t.Errorf("%s: expected %q to contain %q\n  full SQL: %s", label, s, substr, s)
	}
}

// mustNotContain fails if substr IS in s.
func mustNotContain(t *testing.T, s, substr, label string) {
	t.Helper()
	if strings.Contains(s, substr) {
		t.Errorf("%s: expected %q NOT to contain %q\n  full SQL: %s", label, s, substr, s)
	}
}

// filterBeforeKeyword fails unless <filter> appears before <keyword> in s.
func filterBeforeKeyword(t *testing.T, s, filter, keyword, label string) {
	t.Helper()
	fi := strings.Index(strings.ToUpper(s), strings.ToUpper(filter))
	ki := strings.Index(strings.ToUpper(s), strings.ToUpper(keyword))
	if fi == -1 {
		t.Errorf("%s: filter %q not found in SQL: %s", label, filter, s)
		return
	}
	if ki == -1 {
		t.Errorf("%s: keyword %q not found in SQL: %s", label, keyword, s)
		return
	}
	if fi > ki {
		t.Errorf("%s: filter %q appears AFTER %q (Bug-A)\n  full SQL: %s", label, filter, keyword, s)
	}
}

// noLeadingAnd fails if the SQL begins a condition block with AND (Bug-C).
func noLeadingAnd(t *testing.T, s, label string) {
	t.Helper()
	// "WHERE AND" is the tell-tale sign of a bare AND without a real condition.
	if strings.Contains(strings.ToUpper(s), "WHERE AND") {
		t.Errorf("%s: SQL has 'WHERE AND' (Bug-C)\n  full SQL: %s", label, s)
	}
	if strings.Contains(strings.ToUpper(s), "WHERE  AND") {
		t.Errorf("%s: SQL has 'WHERE  AND' (Bug-C)\n  full SQL: %s", label, s)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Section 1 – injectSoftDeleteFilter / applyBaseQuery unit tests
// ─────────────────────────────────────────────────────────────────────────────

const sdFilter = "deleted_at IS NULL"

// 1-A Plain SELECT – filter must be appended as WHERE clause
func TestSD_Inject_PlainSelect(t *testing.T) {
	sql := injectSoftDeleteFilter("SELECT * FROM t", sdFilter)
	mustContain(t, sql, "WHERE deleted_at IS NULL", "plain SELECT")
	noLeadingAnd(t, sql, "plain SELECT")
}

// 1-B SELECT with existing WHERE – filter must be appended with AND
func TestSD_Inject_ExistingWhere(t *testing.T) {
	sql := injectSoftDeleteFilter("SELECT * FROM t WHERE id = $1", sdFilter)
	mustContain(t, sql, "AND deleted_at IS NULL", "existing WHERE")
	mustNotContain(t, sql, "WHERE deleted_at IS NULL WHERE", "existing WHERE double-WHERE")
	noLeadingAnd(t, sql, "existing WHERE")
}

// 1-C SELECT with ORDER BY only – filter must appear BEFORE ORDER BY (Bug-A)
func TestSD_Inject_OrderByOnly(t *testing.T) {
	sql := injectSoftDeleteFilter("SELECT * FROM t ORDER BY name", sdFilter)
	mustContain(t, sql, "WHERE deleted_at IS NULL", "ORDER BY only")
	filterBeforeKeyword(t, sql, "deleted_at IS NULL", "ORDER BY", "ORDER BY only")
	noLeadingAnd(t, sql, "ORDER BY only")
}

// 1-D SELECT with WHERE + ORDER BY – filter between WHERE and ORDER BY
func TestSD_Inject_WhereAndOrderBy(t *testing.T) {
	sql := injectSoftDeleteFilter("SELECT * FROM t WHERE status = $1 ORDER BY name", sdFilter)
	mustContain(t, sql, "AND deleted_at IS NULL", "WHERE+ORDER BY")
	filterBeforeKeyword(t, sql, "deleted_at IS NULL", "ORDER BY", "WHERE+ORDER BY")
	noLeadingAnd(t, sql, "WHERE+ORDER BY")
}

// 1-E SELECT with LIMIT only
func TestSD_Inject_LimitOnly(t *testing.T) {
	sql := injectSoftDeleteFilter("SELECT * FROM t LIMIT 10", sdFilter)
	mustContain(t, sql, "WHERE deleted_at IS NULL", "LIMIT only")
	filterBeforeKeyword(t, sql, "deleted_at IS NULL", "LIMIT", "LIMIT only")
}

// 1-F SELECT with ORDER BY + LIMIT + OFFSET – filter must precede all three
func TestSD_Inject_OrderByLimitOffset(t *testing.T) {
	sql := injectSoftDeleteFilter(
		"SELECT * FROM t WHERE x = $1 ORDER BY name LIMIT 5 OFFSET 10", sdFilter)
	mustContain(t, sql, "AND deleted_at IS NULL", "ORDER BY+LIMIT+OFFSET")
	filterBeforeKeyword(t, sql, "deleted_at IS NULL", "ORDER BY", "ORDER BY+LIMIT+OFFSET")
	filterBeforeKeyword(t, sql, "deleted_at IS NULL", "LIMIT", "ORDER BY+LIMIT+OFFSET")
	filterBeforeKeyword(t, sql, "deleted_at IS NULL", "OFFSET", "ORDER BY+LIMIT+OFFSET")
}

// 1-G GROUP BY with no WHERE
func TestSD_Inject_GroupByOnly(t *testing.T) {
	sql := injectSoftDeleteFilter("SELECT status, COUNT(*) FROM t GROUP BY status", sdFilter)
	mustContain(t, sql, "WHERE deleted_at IS NULL", "GROUP BY only")
	filterBeforeKeyword(t, sql, "deleted_at IS NULL", "GROUP BY", "GROUP BY only")
	noLeadingAnd(t, sql, "GROUP BY only")
}

// 1-H WHERE + GROUP BY + HAVING
func TestSD_Inject_WhereGroupByHaving(t *testing.T) {
	sql := injectSoftDeleteFilter(
		"SELECT status, COUNT(*) FROM t WHERE x=$1 GROUP BY status HAVING COUNT(*) > 5",
		sdFilter)
	mustContain(t, sql, "AND deleted_at IS NULL", "WHERE+GROUP+HAVING")
	filterBeforeKeyword(t, sql, "deleted_at IS NULL", "GROUP BY", "WHERE+GROUP+HAVING")
	filterBeforeKeyword(t, sql, "deleted_at IS NULL", "HAVING", "WHERE+GROUP+HAVING")
}

// 1-I Bug-B: table name contains the word "where" – must not produce bare AND
// This is the regression test for the original "WHERE" substring bug.
func TestSD_Inject_TableNameContainsWhere(t *testing.T) {
	sql := injectSoftDeleteFilter(
		"SELECT * FROM test_SoftDelete_WhereFilter_Table", sdFilter)
	// Must add WHERE (not AND) since there is no real WHERE clause
	mustContain(t, sql, "WHERE deleted_at IS NULL", "table name contains WHERE")
	noLeadingAnd(t, sql, "table name contains WHERE")
	// Must not have a bare " AND " before the WHERE keyword
	if idx := strings.Index(strings.ToUpper(sql), " AND "); idx != -1 {
		whereIdx := strings.Index(strings.ToUpper(sql), " WHERE ")
		if whereIdx == -1 || idx < whereIdx {
			t.Errorf("Bug-B: spurious AND before WHERE\n  SQL: %s", sql)
		}
	}
}

// 1-J Bug-B variant: column name contains "where"
func TestSD_Inject_ColumnNameContainsWhere(t *testing.T) {
	sql := injectSoftDeleteFilter(
		"SELECT id, somewhere_col FROM t ORDER BY somewhere_col", sdFilter)
	mustContain(t, sql, "WHERE deleted_at IS NULL", "column name contains WHERE")
	filterBeforeKeyword(t, sql, "deleted_at IS NULL", "ORDER BY", "column name contains WHERE")
	noLeadingAnd(t, sql, "column name contains WHERE")
}

// 1-K Filter is empty – SQL must be returned unchanged
func TestSD_Inject_EmptyFilter(t *testing.T) {
	orig := "SELECT * FROM t WHERE id = $1 ORDER BY name"
	sql := injectSoftDeleteFilter(orig, "")
	if sql != orig {
		t.Errorf("empty filter: SQL was modified unexpectedly\n  got: %s", sql)
	}
}

// 1-L Custom soft-delete column name (removed_at)
func TestSD_Inject_CustomColumnName(t *testing.T) {
	sql := injectSoftDeleteFilter("SELECT * FROM t ORDER BY name", "removed_at IS NULL")
	mustContain(t, sql, "WHERE removed_at IS NULL", "custom column")
	filterBeforeKeyword(t, sql, "removed_at IS NULL", "ORDER BY", "custom column")
}

// 1-M Subquery: filter injected before ORDER BY when WHERE is in subquery
func TestSD_Inject_SubqueryInSelect(t *testing.T) {
	// The outer query has no WHERE; the ORDER BY is at the outer level.
	sql := injectSoftDeleteFilter(
		"SELECT * FROM (SELECT id FROM t WHERE inner_col = $1) sub ORDER BY id",
		sdFilter)
	// The filter should appear as WHERE on the outer query, before ORDER BY.
	filterBeforeKeyword(t, sql, "deleted_at IS NULL", "ORDER BY", "subquery outer ORDER BY")
}

// 1-N Model.applyBaseQuery via softDeleteFilter helper
func TestSD_ApplyBaseQuery_AllPaths(t *testing.T) {
	m := sdModel(t)

	cases := []struct {
		label   string
		base    string
		wantIn  string
		before  string // filter must appear before this keyword (empty = don't check)
	}{
		{"plain", "SELECT * FROM t", "WHERE deleted_at IS NULL", ""},
		{"has-where", "SELECT * FROM t WHERE id = $1", "AND deleted_at IS NULL", ""},
		{"order-by", "SELECT * FROM t ORDER BY x", "WHERE deleted_at IS NULL", "ORDER BY"},
		{"where+order", "SELECT * FROM t WHERE x=$1 ORDER BY y", "AND deleted_at IS NULL", "ORDER BY"},
		{"limit", "SELECT * FROM t LIMIT 5", "WHERE deleted_at IS NULL", "LIMIT"},
		{"order+limit", "SELECT * FROM t ORDER BY x LIMIT 5", "WHERE deleted_at IS NULL", "ORDER BY"},
	}

	for _, c := range cases {
		t.Run(c.label, func(t *testing.T) {
			got := m.applyBaseQuery(c.base)
			mustContain(t, got, c.wantIn, c.label)
			noLeadingAnd(t, got, c.label)
			if c.before != "" {
				filterBeforeKeyword(t, got, "deleted_at IS NULL", c.before, c.label)
			}
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Section 2 – QueryBuilder SQL-generation unit tests (no DB)
// ─────────────────────────────────────────────────────────────────────────────

// 2-A Empty builder – Build() must add WHERE clause
func TestSD_QB_Empty(t *testing.T) {
	m := sdModel(t)
	sql := m.Query().Build().SQL()
	mustContain(t, sql, "WHERE deleted_at IS NULL", "empty QB")
	noLeadingAnd(t, sql, "empty QB")
}

// 2-B Single WHERE condition – filter appended with AND
func TestSD_QB_SingleWhere(t *testing.T) {
	m := sdModel(t)
	sql := m.Query().Where("status", "active").Build().SQL()
	mustContain(t, sql, "AND deleted_at IS NULL", "single where")
	noLeadingAnd(t, sql, "single where")
}

// 2-C Multiple WHERE conditions – filter appended at correct position
func TestSD_QB_MultipleWhere(t *testing.T) {
	m := sdModel(t)
	sql := m.Query().Where("status", "active").Where("age", 30).Build().SQL()
	mustContain(t, sql, "AND deleted_at IS NULL", "multi where")
	noLeadingAnd(t, sql, "multi where")
}

// 2-D ORDER BY only – Bug-A: filter must be BEFORE ORDER BY
func TestSD_QB_OrderByOnly(t *testing.T) {
	m := sdModel(t)
	sql := m.Query().OrderBy("name", false).Build().SQL()
	mustContain(t, sql, "WHERE deleted_at IS NULL", "QB ORDER BY only")
	filterBeforeKeyword(t, sql, "deleted_at IS NULL", "ORDER BY", "QB ORDER BY only")
	noLeadingAnd(t, sql, "QB ORDER BY only")
}

// 2-E WHERE + ORDER BY – filter must be between WHERE and ORDER BY
func TestSD_QB_WhereAndOrderBy(t *testing.T) {
	m := sdModel(t)
	sql := m.Query().Where("status", "active").OrderBy("name", false).Build().SQL()
	mustContain(t, sql, "AND deleted_at IS NULL", "QB WHERE+ORDER")
	filterBeforeKeyword(t, sql, "deleted_at IS NULL", "ORDER BY", "QB WHERE+ORDER")
}

// 2-F WHERE + ORDER BY + LIMIT – filter before ORDER BY, before LIMIT
func TestSD_QB_WhereOrderByLimit(t *testing.T) {
	m := sdModel(t)
	sql := m.Query().
		Where("status", "active").
		OrderBy("name", false).
		Limit(10).
		Build().SQL()
	filterBeforeKeyword(t, sql, "deleted_at IS NULL", "ORDER BY", "QB WHERE+ORDER+LIMIT")
	filterBeforeKeyword(t, sql, "deleted_at IS NULL", "LIMIT", "QB WHERE+ORDER+LIMIT")
}

// 2-G Multiple conditions + ORDER BY + LIMIT + OFFSET
func TestSD_QB_FullChain(t *testing.T) {
	m := sdModel(t)
	sql := m.Query().
		Where("status", "active").
		Where("age", 30).
		OrderBy("name", false).
		Limit(5).
		Offset(10).
		Build().SQL()
	filterBeforeKeyword(t, sql, "deleted_at IS NULL", "ORDER BY", "full chain")
	filterBeforeKeyword(t, sql, "deleted_at IS NULL", "LIMIT", "full chain")
	filterBeforeKeyword(t, sql, "deleted_at IS NULL", "OFFSET", "full chain")
}

// 2-H WithTrashed – Bug-F: filter must NOT be injected
func TestSD_QB_WithTrashed_NoFilter(t *testing.T) {
	m := sdModel(t)
	sql := m.Query().WithTrashed().Build().SQL()
	mustNotContain(t, sql, "deleted_at", "WithTrashed no filter")
}

// 2-I WithTrashed + WHERE + ORDER BY – conditions work, no filter (Bug-F)
func TestSD_QB_WithTrashedAndConditions(t *testing.T) {
	m := sdModel(t)
	sql := m.Query().
		WithTrashed().
		Where("status", "active").
		OrderBy("name", false).
		Build().SQL()
	mustContain(t, sql, "WHERE status = $1", "WithTrashed conditions work")
	mustNotContain(t, sql, "deleted_at", "WithTrashed no filter with conditions")
}

// 2-J paginatableSQL() must inject filter identically to Build()
func TestSD_QB_PaginatableSQL_MatchesBuild(t *testing.T) {
	m := sdModel(t)
	qb := m.Query().Where("status", "active").OrderBy("name", false)
	buildSQL := qb.Clone().Build().SQL()
	paginatableSQL := qb.paginatableSQL()
	if buildSQL != paginatableSQL {
		t.Errorf("paginatableSQL() differs from Build().SQL()\n  Build:      %s\n  Paginatable: %s",
			buildSQL, paginatableSQL)
	}
}

// 2-K paginatableSQL() must NOT inject filter when WithTrashed
func TestSD_QB_PaginatableSQL_WithTrashed(t *testing.T) {
	m := sdModel(t)
	sql := m.Query().WithTrashed().paginatableSQL()
	mustNotContain(t, sql, "deleted_at", "paginatableSQL WithTrashed")
}

// 2-L QueryFromSQL with no existing clauses – filter added
func TestSD_QueryFromSQL_NoExistingClauses(t *testing.T) {
	m := sdModel(t)
	sql := m.QueryFromSQL("SELECT * FROM users").paginatableSQL()
	mustContain(t, sql, "WHERE deleted_at IS NULL", "QueryFromSQL no clauses")
	noLeadingAnd(t, sql, "QueryFromSQL no clauses")
}

// 2-M QueryFromSQL with existing WHERE – filter appended with AND
func TestSD_QueryFromSQL_ExistingWhere(t *testing.T) {
	m := sdModel(t)
	sql := m.QueryFromSQL("SELECT * FROM users WHERE tenant_id = $1", "t1").paginatableSQL()
	mustContain(t, sql, "AND deleted_at IS NULL", "QueryFromSQL existing WHERE")
	noLeadingAnd(t, sql, "QueryFromSQL existing WHERE")
}

// 2-N QueryFromSQL with existing ORDER BY – filter BEFORE ORDER BY (Bug-A)
func TestSD_QueryFromSQL_ExistingOrderBy(t *testing.T) {
	m := sdModel(t)
	sql := m.QueryFromSQL("SELECT * FROM users ORDER BY name").paginatableSQL()
	mustContain(t, sql, "WHERE deleted_at IS NULL", "QueryFromSQL existing ORDER BY")
	filterBeforeKeyword(t, sql, "deleted_at IS NULL", "ORDER BY", "QueryFromSQL existing ORDER BY")
}

// 2-O QueryFromSQL with WHERE + ORDER BY
func TestSD_QueryFromSQL_WhereAndOrderBy(t *testing.T) {
	m := sdModel(t)
	sql := m.QueryFromSQL(
		"SELECT * FROM users WHERE tenant_id = $1 ORDER BY name", "t1").
		paginatableSQL()
	mustContain(t, sql, "AND deleted_at IS NULL", "QueryFromSQL WHERE+ORDER")
	filterBeforeKeyword(t, sql, "deleted_at IS NULL", "ORDER BY", "QueryFromSQL WHERE+ORDER")
}

// 2-P QueryFromSQL + chained Where() + paginatableSQL
func TestSD_QueryFromSQL_ChainedWhere(t *testing.T) {
	m := sdModel(t)
	sql := m.QueryFromSQL("SELECT * FROM users WHERE tenant_id = $1", "t1").
		Where("status", "active").
		paginatableSQL()
	mustContain(t, sql, "deleted_at IS NULL", "QueryFromSQL chained WHERE")
}

// 2-Q ToSQL shows interpolated filter (no $N placeholders)
func TestSD_QB_ToSQL_Interpolated(t *testing.T) {
	m := sdModel(t)
	sql := m.Query().Where("status", "active").OrderBy("name", false).Build().ToSQL()
	// After Build() the filter is injected; ToSQL should reflect it
	mustNotContain(t, sql, "$1", "ToSQL no placeholders after interpolation")
	mustContain(t, sql, "deleted_at IS NULL", "ToSQL contains filter")
}

// 2-R No soft-delete column set – no filter injected
func TestSD_NoColumn_NoFilter(t *testing.T) {
	conn := NewPostgresConnection(__TestDBconfig)
	m := NewModel[User](NewConnector(conn, conn), "users") // NO WithSoftDelete
	sql := m.Query().Where("status", "active").OrderBy("name", false).Build().SQL()
	mustNotContain(t, sql, "IS NULL", "no soft delete column – no filter")
}

// 2-S Custom column name "removed_at"
func TestSD_CustomColumn_RemovedAt(t *testing.T) {
	conn := NewPostgresConnection(__TestDBconfig)
	m := NewModel[User](NewConnector(conn, conn), "users").WithSoftDelete("removed_at")
	sql := m.Query().OrderBy("name", false).Build().SQL()
	mustContain(t, sql, "WHERE removed_at IS NULL", "custom column removed_at")
	filterBeforeKeyword(t, sql, "removed_at IS NULL", "ORDER BY", "custom column removed_at")
}

// 2-T WhereLike + soft delete + ORDER BY
func TestSD_QB_WhereLikeOrderBy(t *testing.T) {
	m := sdModel(t)
	sql := m.Query().
		WhereLike("name", "Alice%").
		OrderBy("name", false).
		Build().SQL()
	filterBeforeKeyword(t, sql, "deleted_at IS NULL", "ORDER BY", "WhereLike+ORDER BY")
	noLeadingAnd(t, sql, "WhereLike+ORDER BY")
}

// 2-U WhereIn + soft delete + ORDER BY
func TestSD_QB_WhereInOrderBy(t *testing.T) {
	m := sdModel(t)
	sql := m.Query().
		WhereIn("status", []interface{}{"active", "inactive"}).
		OrderBy("age", true).
		Build().SQL()
	filterBeforeKeyword(t, sql, "deleted_at IS NULL", "ORDER BY", "WhereIn+ORDER BY")
}

// 2-V WhereNot + soft delete
func TestSD_QB_WhereNot(t *testing.T) {
	m := sdModel(t)
	sql := m.Query().WhereNot("status", "banned").Build().SQL()
	mustContain(t, sql, "AND deleted_at IS NULL", "WhereNot + soft delete")
	noLeadingAnd(t, sql, "WhereNot")
}

// 2-W OrWhere + soft delete
func TestSD_QB_OrWhere(t *testing.T) {
	m := sdModel(t)
	sql := m.Query().
		Where("status", "active").
		OrWhere(func(qb *QueryBuilder[User]) {
			qb.Where("age", 0)
		}).
		Build().SQL()
	mustContain(t, sql, "deleted_at IS NULL", "OrWhere + soft delete")
	noLeadingAnd(t, sql, "OrWhere")
}

// ─────────────────────────────────────────────────────────────────────────────
// Section 3 – Integration tests (DB required)
// ─────────────────────────────────────────────────────────────────────────────

// sdSetup creates a fresh table with a deleted_at column and a model
// configured with WithSoftDelete("deleted_at").
func sdSetup(t *testing.T) (m *Model[UserSD], cleanup func()) {
	t.Helper()
	conn := NewPostgresConnection(__TestDBconfig)
	if err := conn.Connect(context.Background()); err != nil {
		t.Skipf("no DB: %v", err)
	}
	db := conn.DB()
	table := "test_sd2_" + strings.ReplaceAll(t.Name(), "/", "_")
	_, err := db.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id         TEXT PRIMARY KEY,
			name       TEXT NOT NULL,
			deleted_at TIMESTAMPTZ
		)`, table))
	if err != nil {
		conn.Close()
		t.Fatalf("create table: %v", err)
	}
	m = NewModel[UserSD](NewConnector(conn, conn), table).WithSoftDelete("deleted_at")
	cleanup = func() {
		db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
		conn.Close()
	}
	return
}

// sdSetupCustomCol creates a table with a "removed_at" soft-delete column.
type UserRemoved struct {
	ID        string `db:"id"`
	Name      string `db:"name"`
	RemovedAt *string `db:"removed_at"`
}

func sdSetupCustomCol(t *testing.T) (m *Model[UserRemoved], cleanup func()) {
	t.Helper()
	conn := NewPostgresConnection(__TestDBconfig)
	if err := conn.Connect(context.Background()); err != nil {
		t.Skipf("no DB: %v", err)
	}
	db := conn.DB()
	table := "test_sdcol_" + strings.ReplaceAll(t.Name(), "/", "_")
	_, err := db.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id         TEXT PRIMARY KEY,
			name       TEXT NOT NULL,
			removed_at TIMESTAMPTZ
		)`, table))
	if err != nil {
		conn.Close()
		t.Fatalf("create table: %v", err)
	}
	m = NewModel[UserRemoved](NewConnector(conn, conn), table).WithSoftDelete("removed_at")
	cleanup = func() {
		db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
		conn.Close()
	}
	return
}

func insertSD(t *testing.T, m *Model[UserSD], id, name string) {
	t.Helper()
	_, err := m.writeConn.DB().ExecContext(context.Background(),
		fmt.Sprintf("INSERT INTO %s (id, name) VALUES ($1,$2)", m.tableName), id, name)
	if err != nil {
		t.Fatalf("insertSD %q: %v", id, err)
	}
}

func softDeleteRow(t *testing.T, m *Model[UserSD], id string) {
	t.Helper()
	_, err := m.writeConn.DB().ExecContext(context.Background(),
		fmt.Sprintf("UPDATE %s SET deleted_at=NOW() WHERE id=$1", m.tableName), id)
	if err != nil {
		t.Fatalf("softDeleteRow %q: %v", id, err)
	}
}

// 3-A All() excludes soft-deleted rows
func TestSD_Int_All_ExcludesSoftDeleted(t *testing.T) {
	m, cleanup := sdSetup(t)
	defer cleanup()
	ctx := context.Background()

	insertSD(t, m, "a1", "Alice")
	insertSD(t, m, "a2", "Bob")
	insertSD(t, m, "a3", "Carol")
	softDeleteRow(t, m, "a2")

	all, err := m.All().Exec(ctx)
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	if len(all) != 2 {
		t.Errorf("All: want 2 active rows, got %d (soft-deleted included?)", len(all))
	}
	for _, r := range all {
		if r.Name == "Bob" {
			t.Error("All: soft-deleted 'Bob' should not appear")
		}
	}
}

// 3-B Find(soft-deleted ID) must return sql.ErrNoRows
func TestSD_Int_Find_SoftDeletedReturnsNoRows(t *testing.T) {
	m, cleanup := sdSetup(t)
	defer cleanup()
	ctx := context.Background()

	insertSD(t, m, "b1", "Dave")
	softDeleteRow(t, m, "b1")

	_, err := m.Find("b1").Exec(ctx)
	if !errors.Is(err, sql.ErrNoRows) {
		t.Errorf("Find soft-deleted: want sql.ErrNoRows, got %v", err)
	}
}

// 3-C Find(active ID) must succeed
func TestSD_Int_Find_ActiveReturnsRow(t *testing.T) {
	m, cleanup := sdSetup(t)
	defer cleanup()
	ctx := context.Background()

	insertSD(t, m, "c1", "Eve")
	got, err := m.Find("c1").Exec(ctx)
	if err != nil {
		t.Fatalf("Find active: %v", err)
	}
	if got.Name != "Eve" {
		t.Errorf("Find active: want Eve, got %q", got.Name)
	}
}

// 3-D FindBy returns only active rows
func TestSD_Int_FindBy_ExcludesSoftDeleted(t *testing.T) {
	m, cleanup := sdSetup(t)
	defer cleanup()
	ctx := context.Background()

	insertSD(t, m, "d1", "Frank")
	softDeleteRow(t, m, "d1")
	insertSD(t, m, "d2", "Frank") // another Frank, active

	got, err := m.FindBy("name", "Frank").Exec(ctx)
	if err != nil {
		t.Fatalf("FindBy Frank: %v", err)
	}
	if got.ID != "d2" {
		t.Errorf("FindBy: want active d2, got %q", got.ID)
	}
}

// 3-E Count excludes soft-deleted
func TestSD_Int_Count_ExcludesSoftDeleted(t *testing.T) {
	m, cleanup := sdSetup(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		insertSD(t, m, fmt.Sprintf("e%d", i), fmt.Sprintf("User%d", i))
	}
	softDeleteRow(t, m, "e1")
	softDeleteRow(t, m, "e3")

	n, err := m.Count(ctx, nil)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n != 3 {
		t.Errorf("Count: want 3, got %d", n)
	}
}

// 3-F Exists returns false for soft-deleted rows
func TestSD_Int_Exists_SoftDeletedReturnsFalse(t *testing.T) {
	m, cleanup := sdSetup(t)
	defer cleanup()
	ctx := context.Background()

	insertSD(t, m, "f1", "George")
	softDeleteRow(t, m, "f1")

	ok, err := m.Exists(ctx, "f1")
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if ok {
		t.Error("Exists: soft-deleted row should return false")
	}
}

// 3-G Exists returns true for active rows
func TestSD_Int_Exists_ActiveReturnsTrue(t *testing.T) {
	m, cleanup := sdSetup(t)
	defer cleanup()
	ctx := context.Background()

	insertSD(t, m, "g1", "Hannah")

	ok, err := m.Exists(ctx, "g1")
	if err != nil || !ok {
		t.Errorf("Exists active: want true, got %v (err: %v)", ok, err)
	}
}

// 3-H ExistsBy excludes soft-deleted
func TestSD_Int_ExistsBy_ExcludesSoftDeleted(t *testing.T) {
	m, cleanup := sdSetup(t)
	defer cleanup()
	ctx := context.Background()

	insertSD(t, m, "h1", "Unique")
	softDeleteRow(t, m, "h1")

	ok, err := m.ExistsBy(ctx, map[string]interface{}{"name": "Unique"})
	if err != nil || ok {
		t.Errorf("ExistsBy soft-deleted: want false, got %v (err: %v)", ok, err)
	}
}

// 3-I QueryBuilder Where + Build excludes soft-deleted rows
func TestSD_Int_QB_Where_ExcludesSoftDeleted(t *testing.T) {
	m, cleanup := sdSetup(t)
	defer cleanup()
	ctx := context.Background()

	insertSD(t, m, "i1", "Ivan")
	insertSD(t, m, "i2", "Ivan") // duplicate name, will be soft-deleted
	softDeleteRow(t, m, "i2")

	results, err := m.Query().Where("name", "Ivan").Build().Exec(ctx)
	if err != nil {
		t.Fatalf("QB Where: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("QB Where: want 1 active Ivan, got %d", len(results))
	}
}

// 3-J QueryBuilder OrderBy + Build – soft-deleted excluded, ORDER BY preserved
func TestSD_Int_QB_OrderBy_ExcludesSoftDeleted(t *testing.T) {
	m, cleanup := sdSetup(t)
	defer cleanup()
	ctx := context.Background()

	for i, n := range []string{"Charlie", "Alice", "Bob"} {
		insertSD(t, m, fmt.Sprintf("j%d", i), n)
	}
	softDeleteRow(t, m, "j0") // Charlie soft-deleted

	results, err := m.Query().OrderBy("name", false).Build().Exec(ctx)
	if err != nil {
		t.Fatalf("QB OrderBy: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("QB OrderBy: want 2 active rows, got %d", len(results))
	}
	if results[0].Name != "Alice" {
		t.Errorf("QB OrderBy: want Alice first (ASC), got %q", results[0].Name)
	}
}

// 3-K QB Where + OrderBy + Limit – everything correct
func TestSD_Int_QB_WhereOrderByLimit(t *testing.T) {
	m, cleanup := sdSetup(t)
	defer cleanup()
	ctx := context.Background()

	names := []string{"Dave", "Alice", "Charlie", "Bob", "Eve"}
	for i, n := range names {
		insertSD(t, m, fmt.Sprintf("k%d", i), n)
	}
	softDeleteRow(t, m, "k4") // Eve soft-deleted

	results, err := m.Query().
		WhereNot("name", "Dave"). // exclude Dave too
		OrderBy("name", false).
		Limit(2).
		Build().Exec(ctx)
	if err != nil {
		t.Fatalf("QB Where+Order+Limit: %v", err)
	}
	// Remaining active non-Dave: Alice, Charlie, Bob → sorted → Alice, Bob, Charlie
	// Limit 2 → Alice, Bob
	if len(results) != 2 {
		t.Errorf("QB Where+Order+Limit: want 2, got %d", len(results))
	}
	if results[0].Name != "Alice" {
		t.Errorf("first row: want Alice, got %q", results[0].Name)
	}
}

// 3-L WithTrashed includes soft-deleted rows
func TestSD_Int_WithTrashed_IncludesSoftDeleted(t *testing.T) {
	m, cleanup := sdSetup(t)
	defer cleanup()
	ctx := context.Background()

	insertSD(t, m, "l1", "A")
	insertSD(t, m, "l2", "B")
	softDeleteRow(t, m, "l2")

	all, err := m.Query().WithTrashed().Build().Exec(ctx)
	if err != nil {
		t.Fatalf("WithTrashed: %v", err)
	}
	if len(all) != 2 {
		t.Errorf("WithTrashed: want 2, got %d", len(all))
	}
}

// 3-M Delete(id) – soft deletes the row; All() excludes it afterward
func TestSD_Int_Delete_SoftDeletesRow(t *testing.T) {
	m, cleanup := sdSetup(t)
	defer cleanup()
	ctx := context.Background()

	insertSD(t, m, "m1", "Mark")
	insertSD(t, m, "m2", "Mary")

	if err := m.Delete(ctx, "m1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	all, err := m.All().Exec(ctx)
	if err != nil {
		t.Fatalf("All after Delete: %v", err)
	}
	if len(all) != 1 || all[0].Name != "Mary" {
		t.Errorf("All after Delete: want [Mary], got %v", all)
	}

	// Find the deleted row must fail
	if _, err := m.Find("m1").Exec(ctx); !errors.Is(err, sql.ErrNoRows) {
		t.Errorf("Find after Delete: want ErrNoRows, got %v", err)
	}
}

// 3-N Delete is idempotent (second delete doesn't break anything)
func TestSD_Int_Delete_Idempotent(t *testing.T) {
	m, cleanup := sdSetup(t)
	defer cleanup()
	ctx := context.Background()

	insertSD(t, m, "n1", "Nick")

	if err := m.Delete(ctx, "n1"); err != nil {
		t.Fatalf("Delete 1st: %v", err)
	}
	if err := m.Delete(ctx, "n1"); err != nil {
		t.Fatalf("Delete 2nd (idempotent): %v", err)
	}
	all, err := m.All().Exec(ctx)
	if err != nil {
		t.Fatalf("All after idempotent delete: %v", err)
	}
	if len(all) != 0 {
		t.Errorf("expected 0 rows after idempotent delete, got %d", len(all))
	}
}

// 3-O DeleteBy soft-deletes all matching rows
func TestSD_Int_DeleteBy_SoftDeletesMatchingRows(t *testing.T) {
	m, cleanup := sdSetup(t)
	defer cleanup()
	ctx := context.Background()

	insertSD(t, m, "o1", "Alpha")
	insertSD(t, m, "o2", "Alpha")
	insertSD(t, m, "o3", "Beta")

	if err := m.DeleteBy(ctx, map[string]interface{}{"name": "Alpha"}); err != nil {
		t.Fatalf("DeleteBy: %v", err)
	}

	all, err := m.All().Exec(ctx)
	if err != nil {
		t.Fatalf("All after DeleteBy: %v", err)
	}
	if len(all) != 1 || all[0].Name != "Beta" {
		t.Errorf("All after DeleteBy: want [Beta], got %v", all)
	}
}

// 3-P Chunk excludes soft-deleted rows
func TestSD_Int_Chunk_ExcludesSoftDeleted(t *testing.T) {
	m, cleanup := sdSetup(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 6; i++ {
		insertSD(t, m, fmt.Sprintf("p%d", i), fmt.Sprintf("User%d", i))
	}
	softDeleteRow(t, m, "p1")
	softDeleteRow(t, m, "p4")

	var collected []UserSD
	err := m.Chunk(ctx, 2, nil, func(batch []UserSD) error {
		collected = append(collected, batch...)
		return nil
	})
	if err != nil {
		t.Fatalf("Chunk: %v", err)
	}
	if len(collected) != 4 {
		t.Errorf("Chunk: want 4 active, got %d", len(collected))
	}
	for _, r := range collected {
		if r.ID == "p1" || r.ID == "p4" {
			t.Errorf("Chunk: soft-deleted ID %q should not appear", r.ID)
		}
	}
}

// 3-Q Paginate QB excludes soft-deleted; total and pages are correct
func TestSD_Int_Paginate_QB_ExcludesSoftDeleted(t *testing.T) {
	m, cleanup := sdSetup(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 8; i++ {
		insertSD(t, m, fmt.Sprintf("q%d", i), fmt.Sprintf("U%d", i))
	}
	softDeleteRow(t, m, "q2")
	softDeleteRow(t, m, "q5")

	page, err := m.Paginate(ctx, 1, 3, m.Query().OrderBy("id", false))
	if err != nil {
		t.Fatalf("Paginate QB: %v", err)
	}
	if page.Total != 6 {
		t.Errorf("Paginate QB Total: want 6, got %d", page.Total)
	}
	if page.TotalPages != 2 {
		t.Errorf("Paginate QB TotalPages: want 2, got %d", page.TotalPages)
	}
	if len(page.Items) != 3 {
		t.Errorf("Paginate QB Items: want 3, got %d", len(page.Items))
	}
	for _, item := range page.Items {
		if item.ID == "q2" || item.ID == "q5" {
			t.Errorf("Paginate QB: soft-deleted %q should not appear", item.ID)
		}
	}
}

// 3-R Paginate WithTrashed includes soft-deleted in total
func TestSD_Int_Paginate_WithTrashed_Total(t *testing.T) {
	m, cleanup := sdSetup(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		insertSD(t, m, fmt.Sprintf("r%d", i), fmt.Sprintf("U%d", i))
	}
	softDeleteRow(t, m, "r2")

	page, err := m.Paginate(ctx, 1, 10, m.Query().WithTrashed())
	if err != nil {
		t.Fatalf("Paginate WithTrashed: %v", err)
	}
	if page.Total != 5 {
		t.Errorf("Paginate WithTrashed Total: want 5 (all), got %d", page.Total)
	}
}

// 3-S QueryFromSQL + soft delete in Paginate
func TestSD_Int_Paginate_QueryFromSQL_SD(t *testing.T) {
	m, cleanup := sdSetup(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 6; i++ {
		insertSD(t, m, fmt.Sprintf("s%d", i), fmt.Sprintf("U%d", i))
	}
	softDeleteRow(t, m, "s1")
	softDeleteRow(t, m, "s3")

	qb := m.QueryFromSQL(
		fmt.Sprintf("SELECT * FROM %s", m.tableName),
	).OrderBy("id", false)

	page, err := m.Paginate(ctx, 1, 10, qb)
	if err != nil {
		t.Fatalf("Paginate QueryFromSQL: %v", err)
	}
	if page.Total != 4 {
		t.Errorf("Paginate QueryFromSQL SD: want 4 active, got %d", page.Total)
	}
}

// 3-T Custom soft-delete column "removed_at"
func TestSD_Int_CustomColumn_RemovedAt(t *testing.T) {
	m, cleanup := sdSetupCustomCol(t)
	defer cleanup()
	ctx := context.Background()

	_, err := m.writeConn.DB().ExecContext(ctx,
		fmt.Sprintf("INSERT INTO %s (id,name) VALUES ($1,$2),($3,$4)", m.tableName),
		"t1", "Alice", "t2", "Bob")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	// Soft-delete Bob via the custom column
	m.writeConn.DB().ExecContext(ctx,
		fmt.Sprintf("UPDATE %s SET removed_at=NOW() WHERE id=$1", m.tableName), "t2")

	all, err := m.All().Exec(ctx)
	if err != nil {
		t.Fatalf("All custom col: %v", err)
	}
	if len(all) != 1 || all[0].Name != "Alice" {
		t.Errorf("custom column: want [Alice], got %v", all)
	}
}

// 3-U Paginate empty table with soft-delete → TotalPages = 0
func TestSD_Int_Paginate_EmptyTable(t *testing.T) {
	m, cleanup := sdSetup(t)
	defer cleanup()
	ctx := context.Background()

	page, err := m.Paginate(ctx, 1, 10, m.Query())
	if err != nil {
		t.Fatalf("Paginate empty: %v", err)
	}
	if page.Total != 0 || page.TotalPages != 0 {
		t.Errorf("empty: Total=%d TotalPages=%d", page.Total, page.TotalPages)
	}
}

// 3-V Table name that contains "where" – soft delete must still work (Bug-B regression)
func TestSD_Int_TableNameContainsWhere_SoftDelete(t *testing.T) {
	t.Helper()
	conn := NewPostgresConnection(__TestDBconfig)
	if err := conn.Connect(context.Background()); err != nil {
		t.Skipf("no DB: %v", err)
	}
	db := conn.DB()
	// Table name deliberately contains "where" to trigger the old Bug-B
	table := "test_somewhere_whereclause_sd"
	db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	_, err := db.Exec(fmt.Sprintf(
		`CREATE TABLE %s (id TEXT PRIMARY KEY, name TEXT NOT NULL, deleted_at TIMESTAMPTZ)`,
		table))
	if err != nil {
		conn.Close()
		t.Fatalf("create: %v", err)
	}
	defer func() {
		db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
		conn.Close()
	}()

	m := NewModel[UserSD](NewConnector(conn, conn), table).WithSoftDelete("deleted_at")
	ctx := context.Background()

	db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id,name) VALUES ($1,$2),($3,$4)",
		table), "w1", "Alice", "w2", "Bob")
	db.ExecContext(ctx, fmt.Sprintf("UPDATE %s SET deleted_at=NOW() WHERE id=$1", table), "w2")

	all, err := m.All().Exec(ctx)
	if err != nil {
		t.Fatalf("All on 'where'-named table: %v (Bug-B still present?)", err)
	}
	if len(all) != 1 || all[0].Name != "Alice" {
		t.Errorf("want [Alice], got %v (Bug-B? soft-delete filter malformed)", all)
	}
}

// 3-W Pluck excludes soft-deleted rows
func TestSD_Int_Pluck_ExcludesSoftDeleted(t *testing.T) {
	m, cleanup := sdSetup(t)
	defer cleanup()
	ctx := context.Background()

	insertSD(t, m, "v1", "Alpha")
	insertSD(t, m, "v2", "Beta")
	softDeleteRow(t, m, "v2")

	vals, err := m.Pluck(ctx, "name", nil)
	if err != nil {
		t.Fatalf("Pluck: %v", err)
	}
	if len(vals) != 1 {
		t.Errorf("Pluck: want 1 active name, got %d", len(vals))
	}
	if vals[0] != "Alpha" {
		t.Errorf("Pluck: want Alpha, got %v", vals[0])
	}
}

// 3-X GetBy excludes soft-deleted rows when conditions match both active and deleted
func TestSD_Int_GetBy_ExcludesSoftDeleted(t *testing.T) {
	m, cleanup := sdSetup(t)
	defer cleanup()
	ctx := context.Background()

	insertSD(t, m, "x1", "SameName")
	insertSD(t, m, "x2", "SameName")
	softDeleteRow(t, m, "x1")

	rows, err := m.GetBy(map[string]interface{}{"name": "SameName"}).Exec(ctx)
	if err != nil {
		t.Fatalf("GetBy: %v", err)
	}
	if len(rows) != 1 || rows[0].ID != "x2" {
		t.Errorf("GetBy: want 1 active SameName (x2), got %v", rows)
	}
}

// 3-Y After WithTrashed().Build().Exec(), soft-deleted rows ARE returned
func TestSD_Int_WithTrashed_Build_Exec(t *testing.T) {
	m, cleanup := sdSetup(t)
	defer cleanup()
	ctx := context.Background()

	insertSD(t, m, "y1", "Active")
	insertSD(t, m, "y2", "Deleted")
	softDeleteRow(t, m, "y2")

	all, err := m.Query().WithTrashed().Build().Exec(ctx)
	if err != nil {
		t.Fatalf("WithTrashed Build Exec: %v", err)
	}
	if len(all) != 2 {
		t.Errorf("WithTrashed: want 2 rows (active+deleted), got %d", len(all))
	}
}

// 3-Z Switching WithSoftDelete column at runtime
func TestSD_Int_SwitchSoftDeleteColumn(t *testing.T) {
	conn := NewPostgresConnection(__TestDBconfig)
	if err := conn.Connect(context.Background()); err != nil {
		t.Skipf("no DB: %v", err)
	}
	db := conn.DB()
	table := "test_sd_switch_" + strings.ReplaceAll(t.Name(), "/", "_")
	db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	_, err := db.Exec(fmt.Sprintf(
		`CREATE TABLE %s (id TEXT PRIMARY KEY, name TEXT, deleted_at TIMESTAMPTZ, archived_at TIMESTAMPTZ)`,
		table))
	if err != nil {
		conn.Close()
		t.Fatalf("create: %v", err)
	}
	defer func() {
		db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
		conn.Close()
	}()

	type UserFull struct {
		ID         string  `db:"id"`
		Name       string  `db:"name"`
		DeletedAt  *string `db:"deleted_at"`
		ArchivedAt *string `db:"archived_at"`
	}

	ctx := context.Background()
	db.ExecContext(ctx, fmt.Sprintf(
		"INSERT INTO %s (id,name) VALUES ($1,$2),($3,$4),($5,$6)", table),
		"z1", "Alice", "z2", "Bob", "z3", "Carol")
	db.ExecContext(ctx, fmt.Sprintf(
		"UPDATE %s SET deleted_at=NOW() WHERE id=$1", table), "z2")
	db.ExecContext(ctx, fmt.Sprintf(
		"UPDATE %s SET archived_at=NOW() WHERE id=$1", table), "z3")

	// Using deleted_at as soft-delete → z2 excluded
	mDel := NewModel[UserFull](NewConnector(conn, conn), table).WithSoftDelete("deleted_at")
	allDel, err := mDel.All().Exec(ctx)
	if err != nil {
		t.Fatalf("All deleted_at: %v", err)
	}
	if len(allDel) != 2 { // Alice + Carol
		t.Errorf("deleted_at filter: want 2, got %d", len(allDel))
	}

	// Using archived_at as soft-delete → z3 excluded
	mArch := NewModel[UserFull](NewConnector(conn, conn), table).WithSoftDelete("archived_at")
	allArch, err := mArch.All().Exec(ctx)
	if err != nil {
		t.Fatalf("All archived_at: %v", err)
	}
	if len(allArch) != 2 { // Alice + Bob
		t.Errorf("archived_at filter: want 2, got %d", len(allArch))
	}
}

