package dbconnector

import (
	"context"
	"testing"
)

type User struct {
	ID   int    `db:"id"`
	Name string `db:"name"`
	Age  int    `db:"age"`
}

func TestModel(t *testing.T) {
	conn := NewPostgresConnection(__TestDBconfig)
	conn.Connect(context.Background())
	defer conn.Close()

	connector := NewConnector(conn, conn)
	model := NewModel[User](connector, "users")

	if model == nil {
		t.Error("model should not be nil")
	}

	if model.tableName != "users" {
		t.Errorf("expected table name 'users', got '%s'", model.tableName)
	}
}

func TestModelTransaction(t *testing.T) {
	conn := NewPostgresConnection(__TestDBconfig)
	conn.Connect(context.Background())
	defer conn.Close()

	connector := NewConnector(conn, conn)
	model := NewModel[User](connector, "users")

	tx := model.WriteTransaction()
	if tx == nil {
		t.Error("transaction should not be nil")
	}
}

func TestBuildWhereQuery(t *testing.T) {
	conn := NewPostgresConnection(__TestDBconfig)
	conn.Connect(context.Background())
	defer conn.Close()

	connector := NewConnector(conn, conn)
	model := NewModel[User](connector, "users")

	conditions := map[string]interface{}{
		"name": "John",
		"age":  30,
	}

	query, args := model.buildWhereQuery("SELECT * FROM users", conditions)

	if len(args) != 2 {
		t.Errorf("expected 2 args, got %d", len(args))
	}

	if query == "" {
		t.Error("query should not be empty")
	}
}
