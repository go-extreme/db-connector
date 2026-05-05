package main

// pagination_example.go – demonstrates pagination with real data.
//
// Run with:
//   cd example && go run pagination_example.go
//
// (Remove the other *_example.go files from the build or compile this file alone.)

import (
	"context"
	"fmt"
	"log"
	"time"

	dbconnector "github.com/go-extreme/db-connector/v4"
)

// Product is the domain type used throughout the pagination demo.
type Product struct {
	ID        string    `db:"id"`
	Name      string    `db:"name"`
	Category  string    `db:"category"`
	Price     float64   `db:"price"`
	Stock     int       `db:"stock"`
	Active    bool      `db:"active"`
	CreatedAt time.Time `db:"created_at"`
}

// ProductSummary is a lightweight DTO used with PaginateAs to avoid scanning
// columns the caller doesn't need.
type ProductSummary struct {
	ID       string  `db:"id"`
	Name     string  `db:"name"`
	Category string  `db:"category"`
	Price    float64 `db:"price"`
}

func runPaginationExample() {
	ctx := context.Background()

	cfg := &dbconnector.Config{
		Host:                 "localhost",
		Port:                 5432,
		User:                 "postgres",
		Password:             "Root1234",
		Database:             "test",
		SSLMode:              "disable",
		MaxOpenConnection:    10,
		MaxIdleConnection:    5,
		AutoDatabaseCreation: true,
	}

	conn := dbconnector.NewPostgresConnection(cfg)
	connector := dbconnector.NewConnector(conn, conn)

	if err := connector.Connect(ctx); err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer connector.Close()

	// ── Schema ────────────────────────────────────────────────────────────────
	db := connector.Write().DB()
	db.MustExec(`DROP TABLE IF EXISTS demo_products`)
	db.MustExec(`
		CREATE TABLE demo_products (
			id         TEXT        PRIMARY KEY,
			name       TEXT        NOT NULL,
			category   TEXT        NOT NULL,
			price      NUMERIC     NOT NULL,
			stock      INT         NOT NULL DEFAULT 0,
			active     BOOLEAN     NOT NULL DEFAULT TRUE,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`)

	// ── Seed data ─────────────────────────────────────────────────────────────
	products := dbconnector.NewModel[Product](connector, "demo_products")

	catalog := []Product{
		{ID: "p01", Name: "Laptop Pro 15", Category: "electronics", Price: 1299.99, Stock: 12, Active: true},
		{ID: "p02", Name: "Wireless Mouse", Category: "electronics", Price: 29.99, Stock: 200, Active: true},
		{ID: "p03", Name: "USB-C Hub", Category: "electronics", Price: 49.99, Stock: 85, Active: true},
		{ID: "p04", Name: "Standing Desk", Category: "furniture", Price: 599.00, Stock: 8, Active: true},
		{ID: "p05", Name: "Ergonomic Chair", Category: "furniture", Price: 449.00, Stock: 14, Active: true},
		{ID: "p06", Name: "Desk Lamp", Category: "furniture", Price: 79.99, Stock: 60, Active: true},
		{ID: "p07", Name: "Noise-Cancelling Headphones", Category: "electronics", Price: 349.00, Stock: 30, Active: true},
		{ID: "p08", Name: "Mechanical Keyboard", Category: "electronics", Price: 149.99, Stock: 45, Active: true},
		{ID: "p09", Name: "4K Monitor", Category: "electronics", Price: 899.00, Stock: 9, Active: true},
		{ID: "p10", Name: "Bookshelf", Category: "furniture", Price: 199.00, Stock: 20, Active: true},
		{ID: "p11", Name: "Webcam HD", Category: "electronics", Price: 89.99, Stock: 55, Active: true},
		{ID: "p12", Name: "Office Plant", Category: "decor", Price: 24.99, Stock: 100, Active: true},
		{ID: "p13", Name: "Whiteboard", Category: "office", Price: 129.00, Stock: 17, Active: true},
		{ID: "p14", Name: "Sticky Notes", Category: "office", Price: 4.99, Stock: 500, Active: true},
		{ID: "p15", Name: "Legacy Fax Machine", Category: "electronics", Price: 0.01, Stock: 1, Active: false},
	}

	if err := products.BatchCreate(ctx, catalog, 50); err != nil {
		log.Fatalf("seed: %v", err)
	}
	fmt.Printf("Seeded %d products\n\n", len(catalog))

	// ═════════════════════════════════════════════════════════════════════════
	// Demo 1 – Basic pagination walking all pages
	// ═════════════════════════════════════════════════════════════════════════
	fmt.Println("━━━ Demo 1: walk all pages (pageSize=4, active only) ━━━")

	activeSrc := products.Query().
		Where("active", true).
		OrderBy("price", false) // ascending price

	pageNum := 1
	for {
		page, err := products.Paginate(ctx, pageNum, 4, activeSrc)
		if err != nil {
			log.Fatalf("paginate: %v", err)
		}

		fmt.Printf("  Page %d/%d  (total active: %d)\n", page.Page, page.TotalPages, page.Total)
		for _, p := range page.Items {
			fmt.Printf("    %-30s  $%8.2f  [%s]\n", p.Name, p.Price, p.Category)
		}

		if !page.HasNext() {
			break
		}
		pageNum = page.NextPage()
	}

	// ═════════════════════════════════════════════════════════════════════════
	// Demo 2 – Filter by category, sorted by price descending
	// ═════════════════════════════════════════════════════════════════════════
	fmt.Println("\n━━━ Demo 2: electronics, sorted by price desc (pageSize=3) ━━━")

	elecSrc := products.Query().
		Where("category", "electronics").
		Where("active", true).
		OrderBy("price", true) // descending

	page1, err := products.Paginate(ctx, 1, 3, elecSrc)
	if err != nil {
		log.Fatalf("paginate: %v", err)
	}
	printPage(page1)

	if page1.HasNext() {
		page2, err := products.Paginate(ctx, page1.NextPage(), 3, elecSrc)
		if err != nil {
			log.Fatalf("paginate: %v", err)
		}
		printPage(page2)
	}

	// ═════════════════════════════════════════════════════════════════════════
	// Demo 3 – PaginateAs: project into lightweight ProductSummary DTO
	// ═════════════════════════════════════════════════════════════════════════
	fmt.Println("\n━━━ Demo 3: PaginateAs – scan into ProductSummary DTO ━━━")

	dtoSrc := products.Query().
		Select("id", "name", "category", "price").
		Where("active", true).
		OrderBy("name", false)

	dtoPage, err := dbconnector.PaginateAs[Product, ProductSummary](
		ctx, connector.Read(), 1, 5, dtoSrc,
	)
	if err != nil {
		log.Fatalf("PaginateAs: %v", err)
	}
	fmt.Printf("  Total: %d  |  Pages: %d\n", dtoPage.Total, dtoPage.TotalPages)
	for _, s := range dtoPage.Items {
		fmt.Printf("  [%-12s]  %-30s  $%.2f\n", s.Category, s.Name, s.Price)
	}

	// ═════════════════════════════════════════════════════════════════════════
	// Demo 4 – RawQuery: price range with a hand-written SQL
	// ═════════════════════════════════════════════════════════════════════════
	fmt.Println("\n━━━ Demo 4: RawQuery – products $50–$500, ordered by price ━━━")

	rawSrc := dbconnector.NewRawQuery[Product](
		"demo_products",
		`SELECT * FROM demo_products
		  WHERE active = $1
		    AND price BETWEEN $2 AND $3
		  ORDER BY price ASC`,
		true, 50.0, 500.0,
	)

	rawPage, err := products.Paginate(ctx, 1, 4, rawSrc)
	if err != nil {
		log.Fatalf("RawQuery paginate: %v", err)
	}
	fmt.Printf("  Matched: %d products\n", rawPage.Total)
	for _, p := range rawPage.Items {
		fmt.Printf("  %-30s  $%8.2f\n", p.Name, p.Price)
	}

	// ═════════════════════════════════════════════════════════════════════════
	// Demo 5 – HasNext / HasPrev / NextPage / PrevPage navigation helpers
	// ═════════════════════════════════════════════════════════════════════════
	fmt.Println("\n━━━ Demo 5: navigation helpers (pageSize=5) ━━━")

	navSrc := products.Query().Where("active", true).OrderBy("id", false)

	for i := 1; ; i++ {
		p, err := products.Paginate(ctx, i, 5, navSrc)
		if err != nil {
			log.Fatalf("paginate: %v", err)
		}
		fmt.Printf("  Page %d  |  HasPrev=%v  HasNext=%v  PrevPage=%d  NextPage=%d\n",
			p.Page, p.HasPrev(), p.HasNext(), p.PrevPage(), p.NextPage())
		if !p.HasNext() {
			break
		}
	}

	// ═════════════════════════════════════════════════════════════════════════
	// Demo 6 – Edge case: page beyond last → empty Items, clamped TotalPages
	// ═════════════════════════════════════════════════════════════════════════
	fmt.Println("\n━━━ Demo 6: out-of-range page ━━━")

	oob, err := products.Paginate(ctx, 999, 10, products.Query().Where("active", true))
	if err != nil {
		log.Fatalf("paginate: %v", err)
	}
	fmt.Printf("  Page 999 requested  |  Items returned: %d  |  Total: %d  |  HasNext: %v\n",
		len(oob.Items), oob.Total, oob.HasNext())

	// Cleanup
	db.MustExec(`DROP TABLE IF EXISTS demo_products`)
	fmt.Println("\nDone – demo_products table dropped.")
}

// printPage is a small helper that pretty-prints a page of Products.
func printPage(page *dbconnector.Page[Product]) {
	fmt.Printf("  Page %d/%d  (total: %d)  HasPrev=%v  HasNext=%v\n",
		page.Page, page.TotalPages, page.Total, page.HasPrev(), page.HasNext())
	for _, p := range page.Items {
		fmt.Printf("    %-32s  $%8.2f\n", p.Name, p.Price)
	}
}

func main() {
	runPaginationExample()
}
