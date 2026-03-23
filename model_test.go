package dbconnector

import (
	"context"
	"strings"
	"testing"
)

type User struct {
	ID    string `db:"id"`
	Name  string `db:"name"`
	Email string `db:"email"`
	Age   int    `db:"age"`
}

// --- structInsertParts ---

func TestStructInsertParts(t *testing.T) {
	cols, placeholders := structInsertParts(User{})

	for _, col := range []string{"id", "name", "email", "age"} {
		if !strings.Contains(cols, col) {
			t.Errorf("expected cols to contain %q, got %q", col, cols)
		}
		if !strings.Contains(placeholders, ":"+col) {
			t.Errorf("expected placeholders to contain %q, got %q", ":"+col, placeholders)
		}
	}
}

func TestStructInsertPartsPointer(t *testing.T) {
	cols, _ := structInsertParts(&User{})
	if !strings.Contains(cols, "id") {
		t.Error("should handle pointer structs")
	}
}

type NoTagStruct struct {
	Ignored string
}

func TestStructInsertPartsNoTags(t *testing.T) {
	cols, placeholders := structInsertParts(NoTagStruct{})
	if cols != "" || placeholders != "" {
		t.Error("expected empty cols/placeholders for struct with no db tags")
	}
}

// --- selectColumns ---

func TestSelectColumnsEmpty(t *testing.T) {
	if selectColumns(nil) != "*" {
		t.Error("expected * for nil columns")
	}
	if selectColumns([]string{}) != "*" {
		t.Error("expected * for empty columns")
	}
}

func TestSelectColumnsProvided(t *testing.T) {
	result := selectColumns([]string{"id", "name"})
	if result != "id, name" {
		t.Errorf("expected 'id, name', got %q", result)
	}
}

// --- Model setup ---

func TestNewModel(t *testing.T) {
	conn := NewPostgresConnection(__TestDBconfig)
	conn.Connect(context.Background())
	defer conn.Close()

	model := NewModel[User](NewConnector(conn, conn), "users")

	if model == nil {
		t.Fatal("model should not be nil")
	}
	if model.tableName != "users" {
		t.Errorf("expected 'users', got %q", model.tableName)
	}
}

func TestModelWithCache(t *testing.T) {
	conn := NewPostgresConnection(__TestDBconfig)
	conn.Connect(context.Background())
	defer conn.Close()

	cache := NewInMemoryCache()
	model := NewModel[User](NewConnector(conn, conn), "users").WithCache(cache, 0)

	if model.cache == nil {
		t.Error("cache should be set")
	}
}

// --- Query SQL generation ---

func TestFindSQL(t *testing.T) {
	conn := NewPostgresConnection(__TestDBconfig)
	conn.Connect(context.Background())
	defer conn.Close()

	model := NewModel[User](NewConnector(conn, conn), "users")

	if sql := model.Find("1").SQL(); sql != "SELECT * FROM users WHERE id = $1" {
		t.Errorf("unexpected SQL: %q", sql)
	}
}

func TestFindWithColumns(t *testing.T) {
	conn := NewPostgresConnection(__TestDBconfig)
	conn.Connect(context.Background())
	defer conn.Close()

	model := NewModel[User](NewConnector(conn, conn), "users")
	sql := model.Find("1", "id", "name").SQL()

	if sql != "SELECT id, name FROM users WHERE id = $1" {
		t.Errorf("unexpected SQL: %q", sql)
	}
}

func TestFindBySQL(t *testing.T) {
	conn := NewPostgresConnection(__TestDBconfig)
	conn.Connect(context.Background())
	defer conn.Close()

	model := NewModel[User](NewConnector(conn, conn), "users")

	if sql := model.FindBy("email", "a@b.com").SQL(); sql != "SELECT * FROM users WHERE email = $1" {
		t.Errorf("unexpected SQL: %q", sql)
	}
}

func TestFindByWithColumns(t *testing.T) {
	conn := NewPostgresConnection(__TestDBconfig)
	conn.Connect(context.Background())
	defer conn.Close()

	model := NewModel[User](NewConnector(conn, conn), "users")
	sql := model.FindBy("email", "a@b.com", "id", "email").SQL()

	if sql != "SELECT id, email FROM users WHERE email = $1" {
		t.Errorf("unexpected SQL: %q", sql)
	}
}

func TestAllSQL(t *testing.T) {
	conn := NewPostgresConnection(__TestDBconfig)
	conn.Connect(context.Background())
	defer conn.Close()

	model := NewModel[User](NewConnector(conn, conn), "users")

	if sql := model.All().SQL(); sql != "SELECT * FROM users" {
		t.Errorf("unexpected SQL: %q", sql)
	}
}

func TestAllWithColumns(t *testing.T) {
	conn := NewPostgresConnection(__TestDBconfig)
	conn.Connect(context.Background())
	defer conn.Close()

	model := NewModel[User](NewConnector(conn, conn), "users")
	sql := model.All("id", "age").SQL()

	if sql != "SELECT id, age FROM users" {
		t.Errorf("unexpected SQL: %q", sql)
	}
}

func TestGetBySQL(t *testing.T) {
	conn := NewPostgresConnection(__TestDBconfig)
	conn.Connect(context.Background())
	defer conn.Close()

	model := NewModel[User](NewConnector(conn, conn), "users")
	sql := model.GetBy(map[string]interface{}{"age": 30}).SQL()

	if !strings.HasPrefix(sql, "SELECT * FROM users WHERE") {
		t.Errorf("unexpected SQL: %q", sql)
	}
}

func TestGetByWithColumns(t *testing.T) {
	conn := NewPostgresConnection(__TestDBconfig)
	conn.Connect(context.Background())
	defer conn.Close()

	model := NewModel[User](NewConnector(conn, conn), "users")
	sql := model.GetBy(map[string]interface{}{"age": 30}, "id", "name").SQL()

	if !strings.HasPrefix(sql, "SELECT id, name FROM users WHERE") {
		t.Errorf("unexpected SQL: %q", sql)
	}
}

// --- buildWhereQuery ---

func TestBuildWhereQuery(t *testing.T) {
	conn := NewPostgresConnection(__TestDBconfig)
	conn.Connect(context.Background())
	defer conn.Close()

	model := NewModel[User](NewConnector(conn, conn), "users")
	sql, args := model.buildWhereQuery("SELECT * FROM users", map[string]interface{}{
		"name": "John",
		"age":  30,
	})

	if len(args) != 2 {
		t.Errorf("expected 2 args, got %d", len(args))
	}
	if !strings.Contains(sql, "WHERE") {
		t.Errorf("expected WHERE clause in: %q", sql)
	}
}

func TestBuildWhereQueryEmpty(t *testing.T) {
	conn := NewPostgresConnection(__TestDBconfig)
	conn.Connect(context.Background())
	defer conn.Close()

	model := NewModel[User](NewConnector(conn, conn), "users")
	sql, args := model.buildWhereQuery("SELECT * FROM users", nil)

	if sql != "SELECT * FROM users" {
		t.Errorf("unexpected SQL: %q", sql)
	}
	if len(args) != 0 {
		t.Errorf("expected 0 args, got %d", len(args))
	}
}

// --- Transaction ---

func TestModelWriteTransaction(t *testing.T) {
	conn := NewPostgresConnection(__TestDBconfig)
	conn.Connect(context.Background())
	defer conn.Close()

	model := NewModel[User](NewConnector(conn, conn), "users")
	if model.WriteTransaction() == nil {
		t.Error("WriteTransaction should not be nil")
	}
}

func TestModelReadTransaction(t *testing.T) {
	conn := NewPostgresConnection(__TestDBconfig)
	conn.Connect(context.Background())
	defer conn.Close()

	model := NewModel[User](NewConnector(conn, conn), "users")
	if model.ReadTransaction() == nil {
		t.Error("ReadTransaction should not be nil")
	}
}
