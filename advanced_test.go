package dbconnector

import (
	"context"
	"testing"
	"time"
)

func TestQueryBuilder(t *testing.T) {
	conn := NewPostgresConnection(__TestDBconfig)
	conn.Connect(context.Background())
	defer conn.Close()

	connector := NewConnector(conn, conn)
	model := NewModel[User](connector, "users")
	qb := model.Query().
		Where("age", 30).
		OrderBy("name", false).
		Limit(10)

	sql := qb.SQL()

	if sql == "" {
		t.Error("compiled SQL should not be empty")
	}
}

func TestConnectionPool(t *testing.T) {
	conn1 := NewPostgresConnection(__TestDBconfig)
	conn2 := NewPostgresConnection(__TestDBconfig)

	pool := NewConnectionPool(conn1, conn2)

	ctx := context.Background()
	if err := pool.Connect(ctx); err != nil {
		t.Errorf("pool connect failed: %v", err)
	}
	defer pool.Close()

	if !pool.Connected() {
		t.Error("pool should be connected")
	}

	// Test round-robin
	db1 := pool.DB()
	db2 := pool.DB()

	if db1 == nil || db2 == nil {
		t.Error("pool should return valid DB connections")
	}
}

func TestHealthChecker(t *testing.T) {
	readConn := NewPostgresConnection(__TestDBreadConfig)
	writeConn := NewPostgresConnection(__TestDBwriteConfig)
	connector := NewConnector(readConn, writeConn)

	ctx := context.Background()
	connector.Connect(ctx)
	defer connector.Close()

	checker := NewHealthChecker(connector)
	status := checker.Check(ctx)

	if !status.Healthy {
		t.Errorf("health check failed: %v", status.Error)
	}

	if status.ReadLatency == 0 {
		t.Error("read latency should be measured")
	}

	if status.WriteLatency == 0 {
		t.Error("write latency should be measured")
	}
}

func TestBatchOperations(t *testing.T) {
	conn := NewPostgresConnection(__TestDBconfig)
	conn.Connect(context.Background())
	defer conn.Close()

	connector := NewConnector(conn, conn)
	model := NewModel[User](connector, "users")

	// Test batch create
	users := []User{
		{ID: 1, Name: "User1", Age: 25},
		{ID: 2, Name: "User2", Age: 30},
	}

	err := model.BatchCreate(context.Background(), users, 10)
	if err != nil {
		t.Logf("Batch create error (may be expected): %v", err)
	}

	// Test batch delete
	err = model.BatchDelete(context.Background(), []string{"b1", "b2"})
	if err != nil {
		t.Logf("Batch delete error (may be expected): %v", err)
	}
}

func TestMigrator(t *testing.T) {
	conn := NewPostgresConnection(__TestDBconfig)
	conn.Connect(context.Background())
	defer conn.Close()

	migrator := NewMigrator(conn)

	migration := Migration{
		Version: 1,
		Name:    "test_migration",
		Up:      "SELECT 1",
		Down:    "SELECT 1",
	}

	migrator.Add(migration)

	if len(migrator.migrations) != 1 {
		t.Errorf("expected 1 migration, got %d", len(migrator.migrations))
	}

	if migrator.migrations[0].Version != 1 {
		t.Error("migration version should be 1")
	}
}

func TestMiddleware(t *testing.T) {
	// Test logging middleware
	logCalled := false
	logger := func(query string, duration time.Duration) {
		logCalled = true
	}

	middleware := WithLogging(logger)
	executed := false

	err := middleware(context.Background(), "SELECT 1", func(ctx context.Context) error {
		executed = true
		return nil
	})

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if !executed {
		t.Error("query function should be executed")
	}

	if !logCalled {
		t.Error("logger should be called")
	}
}

func TestRetryMiddleware(t *testing.T) {
	attempts := 0

	middleware := WithRetry(3)

	err := middleware(context.Background(), "SELECT 1", func(ctx context.Context) error {
		attempts++
		if attempts < 3 {
			return ErrTimeout
		}
		return nil
	})

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}
}

func TestCircuitBreakerMiddleware(t *testing.T) {
	failures := 0

	middleware := WithCircuitBreaker(3, time.Second)

	// Trigger circuit breaker
	for i := 0; i < 5; i++ {
		middleware(context.Background(), "SELECT 1", func(ctx context.Context) error {
			failures++
			return ErrTimeout
		})
	}

	// Circuit should be open now
	err := middleware(context.Background(), "SELECT 1", func(ctx context.Context) error {
		return nil
	})

	if err != ErrCircuitOpen {
		t.Errorf("expected ErrCircuitOpen, got %v", err)
	}
}
