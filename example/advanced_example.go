package main

import (
	"context"
	"fmt"
	"log"
	"time"

	dbconnector "github.com/go-extreme/db-connector/v2"
	"github.com/jmoiron/sqlx"
)

type User struct {
	ID        string     `db:"id"`
	Name      string     `db:"name"`
	Email     *string    `db:"email"`
	Age       int        `db:"age"`
	Status    *string    `db:"status"`
	CreatedAt *time.Time `db:"created_at"`
}

func main() {
	ctx := context.Background()

	// Configure connections
	readConfig := &dbconnector.Config{
		Host:              "localhost",
		Port:              5432,
		User:              "postgres",
		Password:          "Root1234",
		Database:          "test",
		SSLMode:           "disable",
		MaxIdleConnection: 25,
		MaxOpenConnection: 5,
		ConnMaxLifetime:   time.Hour,
		ConnMaxIdleTime:   10 * time.Minute,
	}

	writeConfig := &dbconnector.Config{
		Host:                 "localhost",
		Port:                 5432,
		User:                 "postgres",
		Password:             "Root1234",
		Database:             "test",
		SSLMode:              "disable",
		MaxIdleConnection:    10,
		MaxOpenConnection:    2,
		AutoDatabaseCreation: true,
	}

	// Setup connection pool for read replicas
	readConn1 := dbconnector.NewPostgresConnection(readConfig)
	readConn2 := dbconnector.NewPostgresConnection(readConfig)
	readPool := dbconnector.NewConnectionPool(readConn1, readConn2)

	writeConn := dbconnector.NewPostgresConnection(writeConfig)
	connector := dbconnector.NewConnector(readPool, writeConn)

	if err := connector.Connect(ctx); err != nil {
		log.Fatal(err)
	}
	defer connector.Close()

	// Run migrations
	migrator := dbconnector.NewMigrator(connector.Write())
	migrator.Add(dbconnector.Migration{
		Version: 1,
		Name:    "create_users_table",
		Up: `CREATE TABLE IF NOT EXISTS users (
			id VARCHAR(50) PRIMARY KEY,
			name VARCHAR(100),
			email VARCHAR(255),
			age INT,
			status VARCHAR(20),
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,
		Down: `DROP TABLE users`,
	})

	if err := migrator.Up(ctx); err != nil {
		log.Printf("Migration error: %v", err)
	}

	// Initialize unified model with Redis cache
	cache := dbconnector.NewRedisCache("localhost:6379", "", 0)
	users := dbconnector.NewModel[User](connector, "users").WithCache(cache, 5*time.Minute)

	// Health check
	healthChecker := dbconnector.NewHealthChecker(connector)
	status := healthChecker.Check(ctx)
	log.Printf("Health: %v, Read: %v, Write: %v",
		status.Healthy, status.ReadLatency, status.WriteLatency)

	// Clear old data first
	log.Println("Clearing old data...")
	_, _ = connector.Write().DB().Exec("DELETE FROM users")

	// Batch insert
	newUsers := make([]User, 250)
	for i := 0; i < 250; i++ {
		email := fmt.Sprintf("user%d@example.com", i+1)
		status := "active"
		newUsers[i] = User{
			ID:     fmt.Sprintf("user-%d", i+1),
			Name:   fmt.Sprintf("TestUser %d", i+1),
			Email:  &email,
			Age:    20 + (i % 50),
			Status: &status,
		}
	}
	if err := users.BatchCreate(ctx, newUsers, 100); err != nil {
		log.Printf("Batch create error: %v", err)
	} else {
		log.Println("Successfully created 250 users")
	}

	// Query with cache (auto-cached)
	user, err := users.Find("user-1").Exec(ctx)
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		log.Printf("TestUser: %+v", user)
	}

	// Advanced query builder
	results, err := users.Query().
		Where("age", 30).
		Where("status", "active").
		OrderBy("created_at", true).
		Limit(10).
		Build().
		Exec(ctx)

	if err != nil {
		log.Printf("Query error: %v", err)
	} else {
		log.Printf("Found %d users", len(results))
	}

	// Pagination
	page, err := users.Paginate(ctx, 1, 20, map[string]interface{}{"status": "active"})
	if err != nil {
		log.Printf("Pagination error: %v", err)
	} else {
		log.Printf("Page %d of %d, Total: %d", page.Page, page.TotalPages, page.Total)
	}

	// Transaction example
	err = users.WriteTransaction().Execute(ctx, func(ctx context.Context, tx *sqlx.Tx) error {
		_, err := tx.Exec("UPDATE users SET age = age + 1 WHERE status = $1", "active")
		return err
	})

	if err != nil {
		log.Printf("Transaction error: %v", err)
	}

	// Batch delete
	users.BatchDelete(ctx, []string{"user-1", "user-2", "user-3"})

	log.Println("All operations completed successfully")
}
