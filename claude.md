# Claude.md - AI Assistant Context for db-connector

## Project Overview
Production-ready PostgreSQL connector implementing CQRS pattern with generics, pluggable caching, and connection pooling.

## Core Architecture

### CQRS Pattern (Automatic Routing)
- **Unified Model[T]**: Single model that automatically routes operations
- **Read operations** → Use readConn (replicas)
- **Write operations** → Use writeConn (primary)
- **Transparent**: Users don't manage separate read/write models

### Type-Safe Generics
- **Model[T]**: Unified CQRS-compliant model
- **Query[T]**: Immutable query execution (safe for concurrent use)
- **QueryBuilder[T]**: Fluent API for complex queries

### Pluggable Caching
- **Cache interface**: Supports Redis, in-memory, or custom strategies
- **RedisCache**: Production-ready Redis implementation
- **InMemoryCache**: Fast in-memory cache with TTL
- **Auto-invalidation**: Writes automatically invalidate cache

## Key Conventions

### Always Use Context
```go
// Correct
user, err := users.Find("1").Exec(ctx)

// Wrong - missing context
user, err := users.Find("1").Exec(nil)
```

### CQRS is Automatic
```go
// Single model for all operations
users := NewModel[User](connector, "users")

// Reads automatically use readConn
user, _ := users.Find("1").Exec(ctx)

// Writes automatically use writeConn
users.Create(ctx, user)
```

### Struct Tags
Use `db:"column_name"` tags for all struct fields:
```go
type User struct {
    ID   string `db:"id"`
    Name string `db:"name"`
}
```

### Error Handling
- Always check errors from Execute()
- Transactions auto-rollback on error
- Use context for cancellation

## File Structure
- `connector.go` - CQRS connector implementation
- `connection.go` - PostgreSQL connection
- `model.go` - Unified CQRS model with auto-routing
- `query.go` - Immutable query execution
- `builder.go` - Fluent query builder
- `cache.go` - Pluggable cache strategies (Redis, in-memory)
- `pool.go` - Connection pooling with load balancing
- `tx.go` - Simplified transaction API
- `migration.go` - Schema migrations
- `health.go` - Health checks
- `config.go` - Configuration

## Common Patterns

### Basic CRUD
```go
users := NewModel[User](connector, "users")

// Create
users.Create(ctx, User{ID: "1", Name: "John"})

// Read
user, _ := users.Find("1").Exec(ctx)

// Update
users.Update(ctx, "1", map[string]interface{}{"name": "Jane"})

// Delete
users.Delete(ctx, "1")
```

### Caching (Pluggable)
```go
// Redis cache
cache := NewRedisCache("localhost:6379", "", 0)
users := NewModel[User](connector, "users").WithCache(cache, 5*time.Minute)

// In-memory cache
cache := NewInMemoryCache()
users := NewModel[User](connector, "users").WithCache(cache, 5*time.Minute)

// All reads are auto-cached
user, _ := users.Find("1").Exec(ctx)
```

### Transactions
```go
users := NewModel[User](connector, "users")

users.Transaction().Execute(ctx, func(ctx context.Context, tx *sqlx.Tx) error {
    _, err := tx.Exec("INSERT INTO users (id, name) VALUES ($1, $2)", "1", "Alice")
    return err // Auto-rollback on error
})
```

### Pagination
```go
page, _ := users.Paginate(ctx, 1, 20, map[string]interface{}{"status": "active"})
fmt.Printf("Page %d of %d, Total: %d\n", page.Page, page.TotalPages, page.Total)
```

## Anti-Patterns to Avoid
- ❌ Don't skip context in Exec() calls
- ❌ Don't use raw SQL without parameterization
- ❌ Don't forget to defer Close() on connector
- ❌ Don't mutate query objects (they're immutable)
- ❌ Don't ignore cache invalidation on writes

## Testing
- Use `*_test.go` files
- Mock Connection interface for unit tests
- Integration tests require PostgreSQL

## Key Improvements
- **Unified Model**: Single model with automatic CQRS routing
- **Immutable Queries**: Safe for concurrent use
- **Pluggable Cache**: Redis, in-memory, or custom
- **Auto-invalidation**: Writes clear cache automatically
- **Pagination**: Built-in pagination support
- **Batch Operations**: High-throughput batch inserts/deletes
- **Type Safety**: Full generic support

## Dependencies
- `github.com/jmoiron/sqlx` - SQL extensions
- `github.com/lib/pq` - PostgreSQL driver
- `github.com/redis/go-redis/v9` - Redis client (optional)
