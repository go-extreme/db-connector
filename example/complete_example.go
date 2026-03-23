package main

import (
	"context"
	"fmt"
	"log"
	"time"

	dbconnector "github.com/go-extreme/db-connector/v3"
	"github.com/jmoiron/sqlx"
)

type TestUser struct {
	ID        string    `db:"id"`
	Name      string    `db:"name"`
	Email     string    `db:"email"`
	Age       int       `db:"age"`
	Status    string    `db:"status"`
	CreatedAt time.Time `db:"created_at"`
}

func main() {

	// Setup connector
	connector := setupConnector()
	defer connector.Close()

	ctx := context.Background()

	// Example 1: Basic CRUD without cache
	basicCRUD(ctx, connector)

	// Example 2: With Redis cache
	withRedisCache(ctx, connector)

	// Example 3: With in-memory cache
	withInMemoryCache(ctx, connector)

	// Example 4: Complex queries
	complexQueries(ctx, connector)

	// Example 5: Pagination
	paginationExample(ctx, connector)

	// Example 6: Batch operations
	batchOperations(ctx, connector)

	// Example 7: Transactions
	transactionExample(ctx, connector)
}

func setupConnector() dbconnector.Connector {
	readConfig := &dbconnector.Config{
		Host:              "localhost",
		Port:              5432,
		User:              "postgres",
		Password:          "Root1234",
		Database:          "test",
		SSLMode:           "disable",
		MaxOpenConnection: 25,
		MaxIdleConnection: 5,
		ConnMaxLifetime:   time.Hour,
	}

	writeConfig := &dbconnector.Config{
		Host:                 "localhost",
		Port:                 5432,
		User:                 "postgres",
		Password:             "Root1234",
		Database:             "test",
		SSLMode:              "disable",
		MaxOpenConnection:    10,
		MaxIdleConnection:    2,
		AutoDatabaseCreation: true,
	}

	readConn := dbconnector.NewPostgresConnection(readConfig)
	writeConn := dbconnector.NewPostgresConnection(writeConfig)
	connector := dbconnector.NewConnector(readConn, writeConn)

	ctx := context.Background()
	if err := connector.Connect(ctx); err != nil {
		log.Fatal(err)
	}

	return connector
}

func basicCRUD(ctx context.Context, connector dbconnector.Connector) {
	fmt.Println("=== Basic CRUD ===")

	// Create model - CQRS routing is automatic
	users := dbconnector.NewModel[TestUser](connector, "users")

	// Create
	newUser := TestUser{
		ID:     "u1",
		Name:   "John Doe",
		Email:  "john@example.com",
		Age:    30,
		Status: "active",
	}
	if err := users.Create(ctx, newUser); err != nil {
		log.Printf("Create error: %v", err)
	}

	// Read (uses read connection)
	user, err := users.Find("u1").Exec(ctx)
	if err != nil {
		log.Printf("Find error: %v", err)
	} else {
		fmt.Printf("Found user: %s\n", user.Name)
	}

	// Update (uses write connection)
	err = users.Update(ctx, "u1", map[string]interface{}{
		"age": 31,
	})
	if err != nil {
		log.Printf("Update error: %v", err)
	}

	// Check existence
	exists, _ := users.Exists(ctx, "u1")
	fmt.Printf("TestUser exists: %v\n", exists)

	// Count
	count, _ := users.Count(ctx, map[string]interface{}{"status": "active"})
	fmt.Printf("Active users: %d\n", count)

	// Delete
	err = users.Delete(ctx, "u1")
	if err != nil {
		log.Printf("Delete error: %v", err)
	}
}

func withRedisCache(ctx context.Context, connector dbconnector.Connector) {
	fmt.Println("\n=== With Redis Cache ===")

	// Setup Redis cache
	cache := dbconnector.NewRedisCache("localhost:6379", "", 0)

	// Create model with cache
	users := dbconnector.NewModel[TestUser](connector, "users").
		WithCache(cache, 5*time.Minute)

	// First call hits database
	user, _ := users.Find("u1").Exec(ctx)
	fmt.Printf("First call (DB): %s\n", user.Name)

	// Second call hits cache
	user, _ = users.Find("u1").Exec(ctx)
	fmt.Printf("Second call (Cache): %s\n", user.Name)

	// Write invalidates cache
	users.Update(ctx, "u1", map[string]interface{}{"age": 32})
	fmt.Println("Cache invalidated after write")
}

func withInMemoryCache(ctx context.Context, connector dbconnector.Connector) {
	fmt.Println("\n=== With In-Memory Cache ===")

	// Setup in-memory cache
	cache := dbconnector.NewInMemoryCache()

	users := dbconnector.NewModel[TestUser](connector, "users").
		WithCache(cache, 5*time.Minute)

	user, _ := users.Find("u1").Exec(ctx)
	fmt.Printf("Cached in memory: %s\n", user.Name)

	// Override cache TTL for specific query
	user, _ = users.Find("u1").WithCache(1 * time.Hour).Exec(ctx)
	fmt.Printf("Custom TTL: %s\n", user.Name)
}

func complexQueries(ctx context.Context, connector dbconnector.Connector) {
	fmt.Println("\n=== Complex Queries ===")

	users := dbconnector.NewModel[TestUser](connector, "users")

	// Query builder
	results, err := users.Query().
		Where("age", 30).
		Where("status", "active").
		WhereIn("role", []interface{}{"admin", "user"}).
		OrderBy("created_at", true).
		Limit(10).
		Offset(0).
		Build().
		Exec(ctx)

	if err != nil {
		log.Printf("Query error: %v", err)
	} else {
		fmt.Printf("Found %d users\n", len(results))
	}

	// Get by conditions
	activeUsers, _ := users.GetBy(map[string]interface{}{
		"status": "active",
	}).Exec(ctx)
	fmt.Printf("Active users: %d\n", len(activeUsers))
}

func paginationExample(ctx context.Context, connector dbconnector.Connector) {
	fmt.Println("\n=== Pagination ===")

	users := dbconnector.NewModel[TestUser](connector, "users")

	page, err := users.Paginate(ctx, 1, 20,
		users.Query().Where("status", "active"))

	if err != nil {
		log.Printf("Pagination error: %v", err)
		return
	}

	fmt.Printf("Page %d of %d\n", page.Page, page.TotalPages)
	fmt.Printf("Total users: %d\n", page.Total)
	fmt.Printf("Items on this page: %d\n", len(page.Items))
}

func batchOperations(ctx context.Context, connector dbconnector.Connector) {
	fmt.Println("\n=== Batch Operations ===")

	users := dbconnector.NewModel[TestUser](connector, "users")

	// Batch create
	newUsers := []TestUser{
		{ID: "b1", Name: "Alice", Email: "alice@example.com", Age: 25, Status: "active"},
		{ID: "b2", Name: "Bob", Email: "bob@example.com", Age: 28, Status: "active"},
		{ID: "b3", Name: "Charlie", Email: "charlie@example.com", Age: 32, Status: "active"},
	}

	err := users.BatchCreate(ctx, newUsers, 100)
	if err != nil {
		log.Printf("Batch create error: %v", err)
	} else {
		fmt.Printf("Created %d users in batch\n", len(newUsers))
	}

	// Batch delete
	ids := []string{"b1", "b2", "b3"}
	err = users.BatchDelete(ctx, ids)
	if err != nil {
		log.Printf("Batch delete error: %v", err)
	} else {
		fmt.Printf("Deleted %d users in batch\n", len(ids))
	}
}

func transactionExample(ctx context.Context, connector dbconnector.Connector) {
	fmt.Println("\n=== Transactions ===")

	users := dbconnector.NewModel[TestUser](connector, "users")

	err := users.WriteTransaction().Execute(ctx, func(ctx context.Context, tx *sqlx.Tx) error {
		// Multiple operations in transaction
		_, err := tx.Exec("INSERT INTO users (id, name, email) VALUES ($1, $2, $3)",
			"t1", "ReadTransaction TestUser", "tx@example.com")
		if err != nil {
			return err // Automatic rollback
		}

		_, err = tx.Exec("INSERT INTO logs (action) VALUES ($1)", "user_created")
		return err // Commits if nil, rolls back if error
	})

	if err != nil {
		log.Printf("ReadTransaction error: %v", err)
	} else {
		fmt.Println("ReadTransaction completed successfully")
	}
}
