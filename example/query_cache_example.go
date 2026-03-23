package main

import (
	"context"
	"log"
	"time"

	dbconnector "github.com/go-extreme/db-connector/v3"
)

func main() {
	type User struct {
		ID   string `db:"id"`
		Name string `db:"name"`
		Age  int    `db:"age"`
	}

	readConfig := &dbconnector.Config{
		Host:              "localhost",
		Port:              5432,
		User:              "postgres",
		Password:          "Root1234",
		Database:          "test",
		SSLMode:           "disable",
		MaxOpenConnection: 25,
		MaxIdleConnection: 5,
	}

	writeConfig := &dbconnector.Config{
		Host:              "localhost",
		Port:              5432,
		User:              "postgres",
		Password:          "Root1234",
		Database:          "test",
		SSLMode:           "disable",
		MaxOpenConnection: 10,
		MaxIdleConnection: 2,
	}

	readConn := dbconnector.NewPostgresConnection(readConfig)
	writeConn := dbconnector.NewPostgresConnection(writeConfig)
	connector := dbconnector.NewConnector(readConn, writeConn)

	ctx := context.Background()

	if err := connector.Connect(ctx); err != nil {
		log.Fatal(err)
	}
	defer connector.Close()

	// Create table if not exists
	_, err := connector.Write().DB().Exec(`
		CREATE TABLE IF NOT EXISTS users (
			id VARCHAR(50) PRIMARY KEY,
			name VARCHAR(100),
			age INT
		)
	`)
	if err != nil {
		log.Fatal(err)
	}

	// Initialize unified model (without cache first to insert data)
	usersNoCache := dbconnector.NewModel[User](connector, "users")

	// Insert test data
	log.Println("Inserting test data...")
	testUsers := []User{
		{ID: "1", Name: "John Doe", Age: 30},
		{ID: "2", Name: "Jane Smith", Age: 25},
		{ID: "3", Name: "Bob Johnson", Age: 30},
	}
	for _, u := range testUsers {
		if err := usersNoCache.Create(ctx, u); err != nil {
			log.Printf("Insert error (may already exist): %v", err)
		}
	}

	// Initialize Redis cache
	redisCache := dbconnector.NewRedisCache("localhost:6379", "", 0)

	// Initialize unified model with cache
	users := dbconnector.NewModel[User](connector, "users").WithCache(redisCache, 5*time.Minute)

	log.Println("\n=== Starting Cache Examples ===")

	// Example 1: Direct query execution (auto-cached)
	log.Println("Example 1: Direct query execution with auto-caching")
	result, err := users.Find("1").Exec(ctx)
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		log.Printf("Result: %+v", result)
	}

	// Example 2: Query with caching (second call hits cache)
	log.Println("\nExample 2: Query with caching")

	// First call - will hit database
	result1, err := users.Find("1").Exec(ctx)
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		log.Printf("First call (from DB): %+v", result1)
	}

	// Second call - will hit cache
	result2, err := users.Find("1").Exec(ctx)
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		log.Printf("Second call (from cache): %+v", result2)
	}

	// Example 3: GetBy with cache
	log.Println("\nExample 3: GetBy with auto-caching")
	conditions := map[string]interface{}{"age": 30}

	results, err := users.GetBy(conditions).Exec(ctx)
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		log.Printf("Results: %+v", results)
	}

	// Example 4: Override cache TTL for specific query
	log.Println("\nExample 4: Override cache TTL")
	result3, err := users.Find("1").WithCache(1 * time.Hour).Exec(ctx)
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		log.Printf("Result with custom TTL: %+v", result3)
	}

	// Example 5: Get SQL query
	log.Println("\nExample 5: Get SQL query")
	query := users.Find("1")
	compiledSQL := query.SQL()
	log.Printf("Compiled SQL: %s", compiledSQL)

	// Example 6: In-memory cache
	log.Println("\nExample 6: Using in-memory cache")
	memCache := dbconnector.NewInMemoryCache()
	usersWithMemCache := dbconnector.NewModel[User](connector, "users").WithCache(memCache, 5*time.Minute)

	result4, err := usersWithMemCache.Find("1").Exec(ctx)
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		log.Printf("Result from in-memory cache: %+v", result4)
	}

	log.Println("\nAll operations completed successfully")
}
