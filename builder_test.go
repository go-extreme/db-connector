package dbconnector

import (
	"testing"
)

type TestUser struct {
	ID   string `db:"id"`
	Name string `db:"name"`
	Age  int    `db:"age"`
}

func TestQueryBuilder_Where(t *testing.T) {
	model := &Model[TestUser]{tableName: "users"}
	qb := NewQueryBuilder(model)
	
	qb.Where("age", 30)
	
	sql := qb.SQL()
	expected := "SELECT * FROM users WHERE age = $1"
	if sql != expected {
		t.Errorf("Expected %q, got %q", expected, sql)
	}
	
	if len(qb.Args()) != 1 || qb.Args()[0] != 30 {
		t.Errorf("Expected args [30], got %v", qb.Args())
	}
}

func TestQueryBuilder_MultipleWhere(t *testing.T) {
	model := &Model[TestUser]{tableName: "users"}
	qb := NewQueryBuilder(model)
	
	qb.Where("age", 30).Where("name", "John")
	
	sql := qb.SQL()
	expected := "SELECT * FROM users WHERE age = $1 AND name = $2"
	if sql != expected {
		t.Errorf("Expected %q, got %q", expected, sql)
	}
}

func TestQueryBuilder_WhereNot(t *testing.T) {
	model := &Model[TestUser]{tableName: "users"}
	qb := NewQueryBuilder(model)
	
	qb.WhereNot("status", "deleted")
	
	sql := qb.SQL()
	expected := "SELECT * FROM users WHERE status != $1"
	if sql != expected {
		t.Errorf("Expected %q, got %q", expected, sql)
	}
}

func TestQueryBuilder_WhereIn(t *testing.T) {
	model := &Model[TestUser]{tableName: "users"}
	qb := NewQueryBuilder(model)
	
	qb.WhereIn("id", []interface{}{"1", "2", "3"})
	
	sql := qb.SQL()
	expected := "SELECT * FROM users WHERE id IN ($1,$2,$3)"
	if sql != expected {
		t.Errorf("Expected %q, got %q", expected, sql)
	}
	
	if len(qb.Args()) != 3 {
		t.Errorf("Expected 3 args, got %d", len(qb.Args()))
	}
}

func TestQueryBuilder_WhereNotIn(t *testing.T) {
	model := &Model[TestUser]{tableName: "users"}
	qb := NewQueryBuilder(model)
	
	qb.WhereNotIn("status", []interface{}{"deleted", "banned"})
	
	sql := qb.SQL()
	expected := "SELECT * FROM users WHERE status NOT IN ($1,$2)"
	if sql != expected {
		t.Errorf("Expected %q, got %q", expected, sql)
	}
}

func TestQueryBuilder_WhereLike(t *testing.T) {
	model := &Model[TestUser]{tableName: "users"}
	qb := NewQueryBuilder(model)
	
	qb.WhereLike("name", "%John%")
	
	sql := qb.SQL()
	expected := "SELECT * FROM users WHERE name LIKE $1"
	if sql != expected {
		t.Errorf("Expected %q, got %q", expected, sql)
	}
}

func TestQueryBuilder_WhereNull(t *testing.T) {
	model := &Model[TestUser]{tableName: "users"}
	qb := NewQueryBuilder(model)
	
	qb.WhereNull("deleted_at")
	
	sql := qb.SQL()
	expected := "SELECT * FROM users WHERE deleted_at IS NULL"
	if sql != expected {
		t.Errorf("Expected %q, got %q", expected, sql)
	}
}

func TestQueryBuilder_WhereNotNull(t *testing.T) {
	model := &Model[TestUser]{tableName: "users"}
	qb := NewQueryBuilder(model)
	
	qb.WhereNotNull("email")
	
	sql := qb.SQL()
	expected := "SELECT * FROM users WHERE email IS NOT NULL"
	if sql != expected {
		t.Errorf("Expected %q, got %q", expected, sql)
	}
}

func TestQueryBuilder_WhereBetween(t *testing.T) {
	model := &Model[TestUser]{tableName: "users"}
	qb := NewQueryBuilder(model)
	
	qb.WhereBetween("age", 18, 65)
	
	sql := qb.SQL()
	expected := "SELECT * FROM users WHERE age BETWEEN $1 AND $2"
	if sql != expected {
		t.Errorf("Expected %q, got %q", expected, sql)
	}
	
	if len(qb.Args()) != 2 {
		t.Errorf("Expected 2 args, got %d", len(qb.Args()))
	}
}

func TestQueryBuilder_WhereGreaterThan(t *testing.T) {
	model := &Model[TestUser]{tableName: "users"}
	qb := NewQueryBuilder(model)
	
	qb.WhereGreaterThan("age", 18)
	
	sql := qb.SQL()
	expected := "SELECT * FROM users WHERE age > $1"
	if sql != expected {
		t.Errorf("Expected %q, got %q", expected, sql)
	}
}

func TestQueryBuilder_WhereLessThan(t *testing.T) {
	model := &Model[TestUser]{tableName: "users"}
	qb := NewQueryBuilder(model)
	
	qb.WhereLessThan("age", 65)
	
	sql := qb.SQL()
	expected := "SELECT * FROM users WHERE age < $1"
	if sql != expected {
		t.Errorf("Expected %q, got %q", expected, sql)
	}
}

func TestQueryBuilder_OrderBy(t *testing.T) {
	model := &Model[TestUser]{tableName: "users"}
	qb := NewQueryBuilder(model)
	
	qb.Where("age", 30).OrderBy("created_at", true)
	
	sql := qb.SQL()
	expected := "SELECT * FROM users WHERE age = $1 ORDER BY created_at DESC"
	if sql != expected {
		t.Errorf("Expected %q, got %q", expected, sql)
	}
}

func TestQueryBuilder_GroupBy(t *testing.T) {
	model := &Model[TestUser]{tableName: "users"}
	qb := NewQueryBuilder(model)
	
	qb.GroupBy("age", "status")
	
	sql := qb.SQL()
	expected := "SELECT * FROM users GROUP BY age, status"
	if sql != expected {
		t.Errorf("Expected %q, got %q", expected, sql)
	}
}

func TestQueryBuilder_LimitOffset(t *testing.T) {
	model := &Model[TestUser]{tableName: "users"}
	qb := NewQueryBuilder(model)
	
	qb.Where("age", 30).Limit(10).Offset(20)
	
	sql := qb.SQL()
	expected := "SELECT * FROM users WHERE age = $1 LIMIT $2 OFFSET $3"
	if sql != expected {
		t.Errorf("Expected %q, got %q", expected, sql)
	}
	
	if len(qb.Args()) != 3 {
		t.Errorf("Expected 3 args, got %d", len(qb.Args()))
	}
}

func TestQueryBuilder_ComplexQuery(t *testing.T) {
	model := &Model[TestUser]{tableName: "users"}
	qb := NewQueryBuilder(model)
	
	qb.Where("status", "active").
		WhereGreaterThan("age", 18).
		WhereLessThan("age", 65).
		WhereIn("role", []interface{}{"admin", "user"}).
		WhereNotNull("email").
		OrderBy("created_at", true).
		Limit(10).
		Offset(0)
	
	sql := qb.SQL()
	expected := "SELECT * FROM users WHERE status = $1 AND age > $2 AND age < $3 AND role IN ($4,$5) AND email IS NOT NULL ORDER BY created_at DESC LIMIT $6 OFFSET $7"
	if sql != expected {
		t.Errorf("Expected %q, got %q", expected, sql)
	}
	
	if len(qb.Args()) != 7 {
		t.Errorf("Expected 7 args, got %d", len(qb.Args()))
	}
}

func TestQueryBuilder_Or(t *testing.T) {
	model := &Model[TestUser]{tableName: "users"}
	qb := NewQueryBuilder(model)
	
	qb.Where("status", "active").Or(func(q *QueryBuilder[TestUser]) {
		q.Where("age", 30).Where("name", "John")
	})
	
	sql := qb.SQL()
	expected := "SELECT * FROM users WHERE status = $1 OR (age = $2 AND name = $3)"
	if sql != expected {
		t.Errorf("Expected %q, got %q", expected, sql)
	}
}

func TestQueryBuilder_EmptyWhereIn(t *testing.T) {
	model := &Model[TestUser]{tableName: "users"}
	qb := NewQueryBuilder(model)
	
	qb.WhereIn("id", []interface{}{})
	
	sql := qb.SQL()
	expected := "SELECT * FROM users"
	if sql != expected {
		t.Errorf("Expected %q, got %q", expected, sql)
	}
}
