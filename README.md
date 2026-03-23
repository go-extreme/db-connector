# DB Connector

A production-ready, type-safe Go package for PostgreSQL with CQRS pattern, pluggable caching, and connection pooling.

## Installation

```bash
go get github.com/go-extreme/db-connector/v3
```

## Features

- ✅ **CQRS Pattern** - Automatic read/write connection routing
- ✅ **Type-Safe Generics** - Compile-time type safety with Go generics
- ✅ **Pluggable Caching** - Redis, in-memory, or custom cache strategies
- ✅ **Connection Pooling** - Round-robin load balancing across read replicas
- ✅ **Query Builder** - Fluent API for complex queries
- ✅ **Pagination** - Built-in pagination support
- ✅ **Batch Operations** - High-throughput batch inserts/deletes
- ✅ **Transactions** - Simple transaction management
- ✅ **Migrations** - Database schema versioning
- ✅ **Health Checks** - Monitor connection health
- ✅ **Context Support** - Full context.Context integration

## Quick Start

### Basic Setup

```go
package main

import (
    "context"
    "time"
    dbconnector "github.com/go-extreme/db-connector/v3"
)

type User struct {
    ID        string    `db:"id"`
    Name      string    `db:"name"`
    Email     string    `db:"email"`
    Age       int       `db:"age"`
    CreatedAt time.Time `db:"created_at"`
}

func main() {
    // Configure connections
    readConfig := &dbconnector.Config{
        Host:            "localhost",
        Port:            5432,
        User:            "user",
        Password:        "password",
        Database:        "mydb",
        SSLMode:         "disable",
        MaxOpenConns:    25,
        MaxIdleConns:    5,
        ConnMaxLifetime: time.Hour,
    }

    writeConfig := &dbconnector.Config{
        Host:                 "localhost",
        Port:                 5432,
        User:                 "user",
        Password:             "password",
        Database:             "mydb",
        SSLMode:              "disable",
        MaxOpenConns:         10,
        MaxIdleConns:         2,
        AutoDatabaseCreation: true,
    }

    // Create connections
    readConn := dbconnector.NewPostgresConnection(readConfig)
    writeConn := dbconnector.NewPostgresConnection(writeConfig)
    connector := dbconnector.NewConnector(readConn, writeConn)

    ctx := context.Background()
    if err := connector.Connect(ctx); err != nil {
        panic(err)
    }
    defer connector.Close()

    // Create model - CQRS routing is automatic!
    users := dbconnector.NewModel[User](connector, "users")

    // Read operations automatically use read connection
    user, err := users.Find("123").Exec(ctx)
    
    // Write operations automatically use write connection
    err = users.Create(ctx, User{
        ID:    "123",
        Name:  "John Doe",
        Email: "john@example.com",
        Age:   30,
    })
}
```

## CQRS Pattern

The package automatically routes operations to the correct connection:

```go
users := dbconnector.NewModel[User](connector, "users")

// READ operations → Read connection (replica)
user, _ := users.Find("123").Exec(ctx)
users, _ := users.GetBy(map[string]interface{}{"age": 30}).Exec(ctx)
users, _ := users.All().Exec(ctx)
count, _ := users.Count(ctx, nil)
exists, _ := users.Exists(ctx, "123")

// WRITE operations → Write connection (primary)
users.Create(ctx, user)
users.Update(ctx, "123", map[string]interface{}{"age": 31})
users.Delete(ctx, "123")
```

## Caching Strategies

### Redis Cache

```go
// Initialize Redis cache
cache := dbconnector.NewRedisCache("localhost:6379", "", 0)

// Enable caching on model
users := dbconnector.NewModel[User](connector, "users").
    WithCache(cache, 5*time.Minute)

// All read operations are automatically cached
user, _ := users.Find("123").Exec(ctx)  // Hits database
user, _ := users.Find("123").Exec(ctx)  // Hits cache

// Write operations automatically invalidate cache
users.Update(ctx, "123", map[string]interface{}{"age": 31})
```

### In-Memory Cache

```go
// Use in-memory cache instead of Redis
cache := dbconnector.NewInMemoryCache()

users := dbconnector.NewModel[User](connector, "users").
    WithCache(cache, 5*time.Minute)
```

### Custom Cache Strategy

Implement the `Cache` interface:

```go
type Cache interface {
    Get(ctx context.Context, key string) ([]byte, error)
    Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
    Delete(ctx context.Context, key string) error
}

// Use your custom cache
users := dbconnector.NewModel[User](connector, "users").
    WithCache(myCustomCache, 5*time.Minute)
```

### Per-Query Cache Override

```go
// Model has default 5-minute cache
users := dbconnector.NewModel[User](connector, "users").
    WithCache(cache, 5*time.Minute)

// Override cache TTL for specific query
user, _ := users.Find("123").WithCache(1*time.Hour).Exec(ctx)
```

## Query Builder

Build complex queries with a fluent API:

```go
users := dbconnector.NewModel[User](connector, "users")

// Complex query
results, err := users.Query().
    Where("age", 30).
    Where("status", "active").
    WhereIn("role", []interface{}{"admin", "user"}).
    OrderBy("created_at", true).
    Limit(10).
    Offset(0).
    Build().
    Exec(ctx)
```

## Pagination

Built-in pagination support:

```go
users := dbconnector.NewModel[User](connector, "users")

page, err := users.Paginate(ctx, 1, 20, map[string]interface{}{
    "status": "active",
})

fmt.Printf("Page %d of %d\n", page.Page, page.TotalPages)
fmt.Printf("Total users: %d\n", page.Total)
for _, user := range page.Items {
    fmt.Println(user.Name)
}
```

## Batch Operations

High-throughput batch operations:

```go
users := dbconnector.NewModel[User](connector, "users")

// Batch insert with automatic chunking
newUsers := []User{
    {ID: "1", Name: "Alice"},
    {ID: "2", Name: "Bob"},
    // ... thousands more
}
err := users.BatchCreate(ctx, newUsers, 100) // 100 per batch

// Batch delete
ids := []string{"1", "2", "3", "4", "5"}
err = users.BatchDelete(ctx, ids)
```

## Transactions

Simple transaction management:

```go
users := dbconnector.NewModel[User](connector, "users")

err := users.Transaction().Execute(ctx, func(ctx context.Context, tx *sqlx.Tx) error {
    _, err := tx.Exec("INSERT INTO users (id, name) VALUES ($1, $2)", "1", "Alice")
    if err != nil {
        return err // Automatic rollback
    }
    
    _, err = tx.Exec("INSERT INTO logs (action) VALUES ($1)", "user_created")
    return err // Commits if nil, rolls back if error
})
```

## Connection Pooling

Use multiple read replicas with automatic load balancing:

```go
// Create read replicas
replica1 := dbconnector.NewPostgresConnection(config1)
replica2 := dbconnector.NewPostgresConnection(config2)
replica3 := dbconnector.NewPostgresConnection(config3)

// Create pool with round-robin load balancing
readPool := dbconnector.NewConnectionPool(replica1, replica2, replica3)

// Use pool as read connection
connector := dbconnector.NewConnector(readPool, writeConn)
```

## Health Checks

Monitor database health:

```go
healthChecker := dbconnector.NewHealthChecker(connector)

status := healthChecker.Check(ctx)
if !status.Healthy {
    log.Printf("Database unhealthy: %v", status.Error)
}
log.Printf("Read latency: %v, Write latency: %v", 
    status.ReadLatency, status.WriteLatency)
```

## Database Migrations

Version your database schema:

```go
migrator := dbconnector.NewMigrator(connector.Write())

migrator.Add(dbconnector.Migration{
    Version: 1,
    Name:    "create_users_table",
    Up: `CREATE TABLE users (
        id VARCHAR(50) PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        email VARCHAR(255) UNIQUE NOT NULL,
        age INT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )`,
    Down: `DROP TABLE users`,
})

migrator.Add(dbconnector.Migration{
    Version: 2,
    Name:    "add_status_to_users",
    Up:      `ALTER TABLE users ADD COLUMN status VARCHAR(20) DEFAULT 'active'`,
    Down:    `ALTER TABLE users DROP COLUMN status`,
})

// Run migrations
if err := migrator.Up(ctx); err != nil {
    log.Fatal(err)
}
```

## Advanced Examples

### Complete CRUD Application

```go
type User struct {
    ID        string    `db:"id"`
    Name      string    `db:"name"`
    Email     string    `db:"email"`
    Age       int       `db:"age"`
    Status    string    `db:"status"`
    CreatedAt time.Time `db:"created_at"`
}

func main() {
    // Setup
    connector := setupConnector()
    cache := dbconnector.NewRedisCache("localhost:6379", "", 0)
    
    users := dbconnector.NewModel[User](connector, "users").
        WithCache(cache, 5*time.Minute)
    
    ctx := context.Background()

    // Create
    newUser := User{
        ID:     "u123",
        Name:   "John Doe",
        Email:  "john@example.com",
        Age:    30,
        Status: "active",
    }
    users.Create(ctx, newUser)

    // Read (cached)
    user, _ := users.Find("u123").Exec(ctx)
    
    // Read with conditions
    activeUsers, _ := users.GetBy(map[string]interface{}{
        "status": "active",
    }).Exec(ctx)

    // Complex query
    results, _ := users.Query().
        Where("age", 30).
        Where("status", "active").
        OrderBy("created_at", true).
        Limit(10).
        Build().
        Exec(ctx)

    // Update
    users.Update(ctx, "u123", map[string]interface{}{
        "age": 31,
    })

    // Pagination
    page, _ := users.Paginate(ctx, 1, 20, map[string]interface{}{
        "status": "active",
    })

    // Count
    count, _ := users.Count(ctx, map[string]interface{}{
        "status": "active",
    })

    // Exists
    exists, _ := users.Exists(ctx, "u123")

    // Delete
    users.Delete(ctx, "u123")
}
```

## Architecture Benefits

### Scalability
- **Connection pooling** with round-robin load balancing
- **Batch operations** reduce database round trips
- **Pluggable caching** reduces database load
- **Read replicas** distribute query load

### Reliability
- **CQRS pattern** separates read/write concerns
- **Transaction support** with automatic rollback
- **Health checks** for monitoring
- **Context support** for cancellation and timeouts

### Maintainability
- **Type-safe generics** catch errors at compile time
- **Unified API** - one model for all operations
- **Automatic routing** - CQRS is transparent
- **Migration support** for schema evolution

## Best Practices

1. **Always use context**: Pass `context.Context` for cancellation and timeouts
2. **Enable caching for reads**: Use Redis or in-memory cache for frequently accessed data
3. **Use connection pooling**: Set up read replicas for high-traffic applications
4. **Batch operations**: Use `BatchCreate` and `BatchDelete` for bulk operations
5. **Monitor health**: Implement health checks in production
6. **Version your schema**: Use migrations for database changes

## Dependencies

- `github.com/jmoiron/sqlx` - SQL extensions
- `github.com/lib/pq` - PostgreSQL driver
- `github.com/redis/go-redis/v9` - Redis client (optional)

## License

MIT License
