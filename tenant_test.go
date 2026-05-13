package dbconnector

// tenant_test.go
//
// Integration tests for the Tenant entity that uses REAL ddd-cqrs types:
//
//   - *domain.Aggregate   (embedded, no db tag → ignored by scanner)
//   - *utils.UniqueEntityID  db:"id"        (ptr struct, no sql.Scanner)
//   - TenantSlug           db:"slug"        (type TenantSlug string  – alias VO)
//   - Status               db:"status"      (reused string alias from ddd_cqrs_test.go)
//   - map[string]interface{} db:"metadata"  (JSONB column)
//   - domain.Date          db:"created_at"  (value struct, multi-field, no sql.Scanner)
//   - domain.Date          db:"updated_at"
//   - *domain.Date         db:"deleted_at"  (nullable pointer, soft-delete)
//
// domain.Date structure:
//   type Date struct {
//       *BaseValueObject          ← ptr, offset 0 → scanner skips (not convertible)
//       value time.Time           ← offset 8 → scanner sets this via unsafe
//   }
//   Value() time.Time has pointer receiver (*Date), so we use pointer-receiver
//   accessors in the test struct to keep things addressable.
//
// Covered scenarios:
//   - Scanner: domain.Date value field (createdAt / updatedAt)
//   - Scanner: *domain.Date pointer field NULL  → nil ptr
//   - Scanner: *domain.Date pointer field non-NULL → ptr with time.Time set
//   - Scanner: *domain.Aggregate embedding ignored
//   - Find / FindBy / All / GetBy / Raw / Count / ExistsBy
//   - QueryBuilder: Where / WhereLike / WhereIn / WhereGreaterThan / OrderBy+Limit
//   - ToSQL (interpolated)
//   - Paginate with *QueryBuilder (plain, filter, empty, page 2)
//   - Paginate with *RawQuery
//   - PaginateAs: Tenant → TenantDTO
//   - QueryFromSQL + chained + Paginate
//   - Soft-delete: All / Paginate / WithTrashed / QueryFromSQL
//   - domain.Date fields still readable after soft-delete
//   - Chunk streaming
//   - Page navigation helpers
//   - Multi-version (version field increment)

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	ddddomain "github.com/go-extreme/ddd-cqrs/domain"
	dddutils "github.com/go-extreme/ddd-cqrs/utils"
)

// ─────────────────────────────────────────────────────────────────────────────
// Value-object types for Tenant domain
// ─────────────────────────────────────────────────────────────────────────────

// TenantSlug is a validated URL-safe identifier (string alias VO).
type TenantSlug string

func NewTenantSlug(s string) (TenantSlug, error) {
	s = strings.TrimSpace(strings.ToLower(s))
	if s == "" {
		return "", fmt.Errorf("slug cannot be blank")
	}
	return TenantSlug(s), nil
}
func MustTenantSlug(s string) TenantSlug {
	sl, err := NewTenantSlug(s)
	if err != nil {
		panic(err)
	}
	return sl
}
func (s TenantSlug) String() string             { return string(s) }
func (s TenantSlug) Value() (driver.Value, error) { return string(s), nil }

// ─────────────────────────────────────────────────────────────────────────────
// Tenant entity — mirrors the user's real struct with db tags added
// ─────────────────────────────────────────────────────────────────────────────

// Tenant is a DDD aggregate with:
//   - *domain.Aggregate embedded (no db tag → ignored by scanner)
//   - domain.Date value fields (multi-field struct, scanned via unsafe field introspection)
//   - *domain.Date nullable pointer for soft-delete
type Tenant struct {
	*ddddomain.Aggregate // no db tag → scanner ignores this field

	id        *dddutils.UniqueEntityID `db:"id"`
	name      string                   `db:"name"`
	slug      TenantSlug               `db:"slug"`
	secret    string                   `db:"secret"`
	status    Status                   `db:"status"` // Status defined in ddd_cqrs_test.go
	metadata  map[string]interface{}   `db:"metadata"`
	version   int                      `db:"version"`
	createdAt ddddomain.Date           `db:"created_at"`
	updatedAt ddddomain.Date           `db:"updated_at"`
	deletedAt *ddddomain.Date          `db:"deleted_at"`
}

// Pointer-receiver accessors so callers can invoke pointer-receiver methods
// on domain.Date (e.g. Value(), String()) without addressability issues.
func (t *Tenant) ID() *dddutils.UniqueEntityID   { return t.id }
func (t *Tenant) Name() string                    { return t.name }
func (t *Tenant) Slug() TenantSlug               { return t.slug }
func (t *Tenant) Secret() string                  { return t.secret }
func (t *Tenant) Status() Status                  { return t.status }
func (t *Tenant) Metadata() map[string]interface{} { return t.metadata }
func (t *Tenant) Version() int                    { return t.version }
func (t *Tenant) CreatedAt() *ddddomain.Date      { return &t.createdAt }
func (t *Tenant) UpdatedAt() *ddddomain.Date      { return &t.updatedAt }
func (t *Tenant) DeletedAt() *ddddomain.Date      { return t.deletedAt }

// TenantDTO is a flat read-model for PaginateAs projection tests.
type TenantDTO struct {
	ID     string `db:"id"`
	Name   string `db:"name"`
	Slug   string `db:"slug"`
	Status string `db:"status"`
}

// ─────────────────────────────────────────────────────────────────────────────
// Table setup helpers
// ─────────────────────────────────────────────────────────────────────────────

func setupTenantTable(t *testing.T) (*Model[Tenant], func()) {
	t.Helper()
	conn := NewPostgresConnection(__TestDBconfig)
	if err := conn.Connect(context.Background()); err != nil {
		t.Skipf("no DB available: %v", err)
	}
	db := conn.DB()
	table := "test_tenant_" + strings.ReplaceAll(t.Name(), "/", "_")

	_, err := db.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id         TEXT        PRIMARY KEY,
			name       TEXT        NOT NULL,
			slug       TEXT        NOT NULL UNIQUE,
			secret     TEXT        NOT NULL DEFAULT '',
			status     TEXT        NOT NULL DEFAULT 'active',
			metadata   JSONB,
			version    INT         NOT NULL DEFAULT 0,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			deleted_at TIMESTAMPTZ
		)`, table))
	if err != nil {
		conn.Close()
		t.Fatalf("create table: %v", err)
	}

	m := NewModel[Tenant](NewConnector(conn, conn), table)
	return m, func() {
		db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
		conn.Close()
	}
}

func setupTenantTableSD(t *testing.T) (*Model[Tenant], func()) {
	t.Helper()
	conn := NewPostgresConnection(__TestDBconfig)
	if err := conn.Connect(context.Background()); err != nil {
		t.Skipf("no DB available: %v", err)
	}
	db := conn.DB()
	table := "test_tenantsd_" + strings.ReplaceAll(t.Name(), "/", "_")

	_, err := db.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id         TEXT        PRIMARY KEY,
			name       TEXT        NOT NULL,
			slug       TEXT        NOT NULL UNIQUE,
			secret     TEXT        NOT NULL DEFAULT '',
			status     TEXT        NOT NULL DEFAULT 'active',
			metadata   JSONB,
			version    INT         NOT NULL DEFAULT 0,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			deleted_at TIMESTAMPTZ
		)`, table))
	if err != nil {
		conn.Close()
		t.Fatalf("create table: %v", err)
	}

	m := NewModel[Tenant](NewConnector(conn, conn), table).WithSoftDelete("deleted_at")
	return m, func() {
		db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
		conn.Close()
	}
}

// insertTenant inserts via raw SQL, serialising each value-object manually.
func insertTenant(t *testing.T, m *Model[Tenant], ten Tenant) {
	t.Helper()

	metaJSON := []byte("{}")
	if ten.metadata != nil {
		b, err := json.Marshal(ten.metadata)
		if err != nil {
			t.Fatalf("marshal metadata: %v", err)
		}
		metaJSON = b
	}

	idStr := ""
	if ten.id != nil {
		idStr = ten.id.Value()
	}

	_, err := m.writeConn.DB().ExecContext(context.Background(), fmt.Sprintf(
		`INSERT INTO %s (id, name, slug, secret, status, metadata, version)
		 VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7)`, m.tableName),
		idStr,
		ten.name,
		string(ten.slug),
		ten.secret,
		string(ten.status),
		string(metaJSON),
		ten.version,
	)
	if err != nil {
		t.Fatalf("insertTenant %q: %v", idStr, err)
	}
}

// newTenant constructs a test Tenant value.
func newTenant(id, name, slug, secret string, status Status, metadata map[string]interface{}, version int) Tenant {
	return Tenant{
		id:      dddutils.NewUniqueID(id),
		name:    name,
		slug:    MustTenantSlug(slug),
		secret:  secret,
		status:  status,
		metadata: metadata,
		version: version,
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Scanner unit tests – domain.Date fields
// ─────────────────────────────────────────────────────────────────────────────

// TestTenant_DomainDate_ValueField proves that the unsafe scanner correctly
// populates domain.Date.value (an unexported time.Time field at offset 8)
// even though domain.Date does not implement sql.Scanner.
func TestTenant_DomainDate_ValueField(t *testing.T) {
	m, cleanup := setupTenantTable(t)
	defer cleanup()
	ctx := context.Background()

	before := time.Now().Add(-time.Second) // margin for DB NOW() precision
	insertTenant(t, m, newTenant("date-val-1", "Alpha Corp", "alpha-corp", "", StatusActiveAcc, nil, 0))

	got, err := m.Find("date-val-1").Exec(ctx)
	if err != nil {
		t.Fatalf("Find: %v", err)
	}

	// CreatedAt / UpdatedAt must be non-zero and after our 'before' marker
	ca := got.CreatedAt().Value()
	if ca.IsZero() {
		t.Error("createdAt should not be zero after scan")
	}
	if ca.Before(before) {
		t.Errorf("createdAt %v is before insert time %v", ca, before)
	}

	ua := got.UpdatedAt().Value()
	if ua.IsZero() {
		t.Error("updatedAt should not be zero after scan")
	}

	// deletedAt must be nil (NULL)
	if got.DeletedAt() != nil {
		t.Errorf("deletedAt should be nil, got %v", got.DeletedAt().Value())
	}
}

// TestTenant_DomainDate_PtrField_NonNull checks that *domain.Date is populated
// when the DB column is non-NULL.
func TestTenant_DomainDate_PtrField_NonNull(t *testing.T) {
	m, cleanup := setupTenantTable(t)
	defer cleanup()
	ctx := context.Background()

	insertTenant(t, m, newTenant("date-ptr-1", "Beta Corp", "beta-corp", "", StatusActiveAcc, nil, 0))

	// Manually soft-delete so deleted_at is filled
	before := time.Now().Add(-time.Second)
	_, err := m.writeConn.DB().ExecContext(ctx,
		fmt.Sprintf("UPDATE %s SET deleted_at = NOW() WHERE id = $1", m.tableName), "date-ptr-1")
	if err != nil {
		t.Fatalf("update deleted_at: %v", err)
	}

	// Use Raw so the soft-delete filter is NOT applied
	rows, err := m.Raw(ctx,
		fmt.Sprintf("SELECT * FROM %s WHERE id = $1", m.tableName), "date-ptr-1")
	if err != nil {
		t.Fatalf("Raw: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	got := rows[0]
	if got.DeletedAt() == nil {
		t.Fatal("deletedAt should not be nil after being set")
	}
	da := got.DeletedAt().Value()
	if da.IsZero() {
		t.Error("deletedAt value should not be zero")
	}
	if da.Before(before) {
		t.Errorf("deletedAt %v is before insert time %v", da, before)
	}
}

// TestTenant_DomainDate_PtrField_Null confirms *domain.Date stays nil for NULL DB values.
func TestTenant_DomainDate_PtrField_Null(t *testing.T) {
	m, cleanup := setupTenantTable(t)
	defer cleanup()
	ctx := context.Background()

	insertTenant(t, m, newTenant("date-null-1", "Gamma Corp", "gamma-corp", "", StatusActiveAcc, nil, 0))

	got, err := m.Find("date-null-1").Exec(ctx)
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if got.DeletedAt() != nil {
		t.Errorf("deletedAt should be nil for non-deleted row, got %v", got.DeletedAt())
	}
}

// TestTenant_Aggregate_EmbeddedIgnored confirms *domain.Aggregate embedding
// does not cause the scanner to panic or produce unexpected errors.
func TestTenant_Aggregate_EmbeddedIgnored(t *testing.T) {
	m, cleanup := setupTenantTable(t)
	defer cleanup()
	ctx := context.Background()

	insertTenant(t, m, newTenant("agg-1", "Agg Corp", "agg-corp", "", StatusActiveAcc, nil, 0))

	got, err := m.Find("agg-1").Exec(ctx)
	if err != nil {
		t.Fatalf("Find with embedded Aggregate: %v", err)
	}
	// The embedded *Aggregate should be nil (no db tag, never set by scanner).
	if got.Aggregate != nil {
		t.Error("embedded *Aggregate should remain nil (no db tag)")
	}
	// But all real fields should be populated.
	if got.ID() == nil || got.ID().Value() != "agg-1" {
		t.Errorf("ID mismatch after aggregate-embedded scan: %v", got.ID())
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// TenantSlug value-object unit tests (no DB)
// ─────────────────────────────────────────────────────────────────────────────

func TestTenantSlug_Normalisation(t *testing.T) {
	sl, err := NewTenantSlug("  My-Tenant  ")
	if err != nil {
		t.Fatalf("NewTenantSlug: %v", err)
	}
	if sl.String() != "my-tenant" {
		t.Errorf("want my-tenant, got %q", sl.String())
	}
}

func TestTenantSlug_BlankError(t *testing.T) {
	if _, err := NewTenantSlug("   "); err == nil {
		t.Error("expected error for blank slug")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// CRUD read operations
// ─────────────────────────────────────────────────────────────────────────────

func TestTenant_Find(t *testing.T) {
	m, cleanup := setupTenantTable(t)
	defer cleanup()
	ctx := context.Background()

	insertTenant(t, m, newTenant("find-1", "Acme Inc", "acme-inc", "s3cr3t", StatusActiveAcc, nil, 3))

	got, err := m.Find("find-1").Exec(ctx)
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if got.ID() == nil || got.ID().Value() != "find-1" {
		t.Errorf("ID: want find-1, got %v", got.ID())
	}
	if got.Name() != "Acme Inc" {
		t.Errorf("Name: want 'Acme Inc', got %q", got.Name())
	}
	if got.Slug() != "acme-inc" {
		t.Errorf("Slug: want acme-inc, got %q", got.Slug())
	}
	if got.Secret() != "s3cr3t" {
		t.Errorf("Secret: want s3cr3t, got %q", got.Secret())
	}
	if got.Status() != StatusActiveAcc {
		t.Errorf("Status: want active, got %q", got.Status())
	}
	if got.Version() != 3 {
		t.Errorf("Version: want 3, got %d", got.Version())
	}
}

func TestTenant_FindBy_Slug(t *testing.T) {
	m, cleanup := setupTenantTable(t)
	defer cleanup()
	ctx := context.Background()

	insertTenant(t, m, newTenant("fb-t-1", "Alpha", "alpha", "", StatusActiveAcc, nil, 0))
	insertTenant(t, m, newTenant("fb-t-2", "Beta", "beta", "", StatusInactiveAcc, nil, 0))

	got, err := m.FindBy("slug", "beta").Exec(ctx)
	if err != nil {
		t.Fatalf("FindBy slug: %v", err)
	}
	if got.Name() != "Beta" {
		t.Errorf("Name: want Beta, got %q", got.Name())
	}
	if got.Status() != StatusInactiveAcc {
		t.Errorf("Status: want inactive, got %q", got.Status())
	}
}

func TestTenant_All(t *testing.T) {
	m, cleanup := setupTenantTable(t)
	defer cleanup()
	ctx := context.Background()

	slugs := []string{"tenant-a", "tenant-b", "tenant-c"}
	for i, sl := range slugs {
		insertTenant(t, m, newTenant(
			fmt.Sprintf("all-t-%d", i), fmt.Sprintf("T%d", i), sl, "", StatusActiveAcc, nil, i))
	}

	all, err := m.All().Exec(ctx)
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(all))
	}
	for i := range all {
		if all[i].ID() == nil || all[i].ID().Value() == "" {
			t.Error("ID empty after All scan")
		}
		// Verify domain.Date fields are non-zero for every row
		if all[i].CreatedAt().Value().IsZero() {
			t.Error("createdAt zero after All scan")
		}
		if all[i].UpdatedAt().Value().IsZero() {
			t.Error("updatedAt zero after All scan")
		}
	}
}

func TestTenant_GetBy_Status(t *testing.T) {
	m, cleanup := setupTenantTable(t)
	defer cleanup()
	ctx := context.Background()

	insertTenant(t, m, newTenant("gb-t-1", "A", "slug-a", "", StatusActiveAcc, nil, 0))
	insertTenant(t, m, newTenant("gb-t-2", "B", "slug-b", "", StatusInactiveAcc, nil, 0))
	insertTenant(t, m, newTenant("gb-t-3", "C", "slug-c", "", StatusActiveAcc, nil, 0))

	rows, err := m.GetBy(map[string]interface{}{"status": string(StatusActiveAcc)}).Exec(ctx)
	if err != nil {
		t.Fatalf("GetBy: %v", err)
	}
	if len(rows) != 2 {
		t.Errorf("expected 2 active, got %d", len(rows))
	}
	for i := range rows {
		if rows[i].Status() != StatusActiveAcc {
			t.Errorf("unexpected status %q", rows[i].Status())
		}
		if rows[i].CreatedAt().Value().IsZero() {
			t.Error("createdAt zero in GetBy result")
		}
	}
}

func TestTenant_Raw(t *testing.T) {
	m, cleanup := setupTenantTable(t)
	defer cleanup()
	ctx := context.Background()

	insertTenant(t, m, newTenant("raw-t-1", "Raw Corp", "raw-corp", "sec", StatusSuspended, nil, 7))

	rows, err := m.Raw(ctx,
		fmt.Sprintf("SELECT * FROM %s WHERE id = $1", m.tableName), "raw-t-1")
	if err != nil {
		t.Fatalf("Raw: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	got := rows[0]
	if got.Version() != 7 {
		t.Errorf("Version: want 7, got %d", got.Version())
	}
	if got.Status() != StatusSuspended {
		t.Errorf("Status: want suspended, got %q", got.Status())
	}
	if got.CreatedAt().Value().IsZero() {
		t.Error("createdAt zero in Raw result")
	}
}

func TestTenant_Count_Exists(t *testing.T) {
	m, cleanup := setupTenantTable(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 4; i++ {
		insertTenant(t, m, newTenant(
			fmt.Sprintf("cnt-t-%d", i),
			fmt.Sprintf("T%d", i),
			fmt.Sprintf("slug-cnt-%d", i),
			"", StatusActiveAcc, nil, 0))
	}

	n, err := m.Count(ctx, nil)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n != 4 {
		t.Errorf("Count: want 4, got %d", n)
	}

	ok, err := m.Exists(ctx, "cnt-t-2")
	if err != nil || !ok {
		t.Errorf("Exists cnt-t-2: want true, got %v (err: %v)", ok, err)
	}
	ok, err = m.Exists(ctx, "ghost")
	if err != nil || ok {
		t.Errorf("Exists ghost: want false, got %v (err: %v)", ok, err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Metadata JSONB round-trip
// ─────────────────────────────────────────────────────────────────────────────

func TestTenant_Metadata_JSONB(t *testing.T) {
	m, cleanup := setupTenantTable(t)
	defer cleanup()
	ctx := context.Background()

	metadata := map[string]interface{}{
		"plan":    "enterprise",
		"seats":   float64(50),
		"regions": []interface{}{"us-east-1", "eu-west-1"},
		"flags": map[string]interface{}{
			"sso":    true,
			"saml":   true,
			"custom": false,
		},
	}

	insertTenant(t, m, newTenant("meta-1", "BigCorp", "bigcorp", "", StatusActiveAcc, metadata, 0))

	got, err := m.Find("meta-1").Exec(ctx)
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	md := got.Metadata()
	if md == nil {
		t.Fatal("metadata should not be nil")
	}
	if md["plan"] != "enterprise" {
		t.Errorf("plan: want enterprise, got %v", md["plan"])
	}
	if md["seats"] != float64(50) {
		t.Errorf("seats: want 50, got %v", md["seats"])
	}

	regions, ok := md["regions"].([]interface{})
	if !ok || len(regions) != 2 {
		t.Errorf("regions mismatch: %v", md["regions"])
	}

	flags, ok := md["flags"].(map[string]interface{})
	if !ok {
		t.Fatalf("flags not a map: %T", md["flags"])
	}
	if flags["sso"] != true {
		t.Errorf("flags.sso: want true, got %v", flags["sso"])
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// QueryBuilder fluent API
// ─────────────────────────────────────────────────────────────────────────────

func TestTenant_QB_WhereStatus(t *testing.T) {
	m, cleanup := setupTenantTable(t)
	defer cleanup()
	ctx := context.Background()

	insertTenant(t, m, newTenant("qs-t-1", "A", "qa", "", StatusActiveAcc, nil, 0))
	insertTenant(t, m, newTenant("qs-t-2", "B", "qb", "", StatusInactiveAcc, nil, 0))
	insertTenant(t, m, newTenant("qs-t-3", "C", "qc", "", StatusActiveAcc, nil, 0))

	results, err := m.Query().Where("status", string(StatusActiveAcc)).Build().Exec(ctx)
	if err != nil {
		t.Fatalf("QB.Where: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2, got %d", len(results))
	}
	for i := range results {
		if results[i].CreatedAt().Value().IsZero() {
			t.Error("createdAt zero in QB result")
		}
	}
}

func TestTenant_QB_WhereLike_Name(t *testing.T) {
	m, cleanup := setupTenantTable(t)
	defer cleanup()
	ctx := context.Background()

	insertTenant(t, m, newTenant("wl-t-1", "Acme Corp", "acme", "", StatusActiveAcc, nil, 0))
	insertTenant(t, m, newTenant("wl-t-2", "Acme Ltd", "acme-ltd", "", StatusActiveAcc, nil, 0))
	insertTenant(t, m, newTenant("wl-t-3", "Beta Inc", "beta-inc", "", StatusActiveAcc, nil, 0))

	results, err := m.Query().WhereLike("name", "Acme%").Build().Exec(ctx)
	if err != nil {
		t.Fatalf("WhereLike: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 Acme rows, got %d", len(results))
	}
}

func TestTenant_QB_WhereIn_Status(t *testing.T) {
	m, cleanup := setupTenantTable(t)
	defer cleanup()
	ctx := context.Background()

	insertTenant(t, m, newTenant("win-t-1", "A", "win-a", "", StatusActiveAcc, nil, 0))
	insertTenant(t, m, newTenant("win-t-2", "B", "win-b", "", StatusInactiveAcc, nil, 0))
	insertTenant(t, m, newTenant("win-t-3", "C", "win-c", "", StatusSuspended, nil, 0))

	results, err := m.Query().
		WhereIn("status", []interface{}{string(StatusActiveAcc), string(StatusSuspended)}).
		Build().Exec(ctx)
	if err != nil {
		t.Fatalf("WhereIn: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 (active+suspended), got %d", len(results))
	}
}

func TestTenant_QB_WhereGreaterThan_Version(t *testing.T) {
	m, cleanup := setupTenantTable(t)
	defer cleanup()
	ctx := context.Background()

	for _, ver := range []int{1, 5, 10, 3} {
		insertTenant(t, m, newTenant(
			fmt.Sprintf("ver-%d", ver),
			fmt.Sprintf("T%d", ver),
			fmt.Sprintf("slug-ver-%d", ver),
			"", StatusActiveAcc, nil, ver))
	}

	results, err := m.Query().WhereGreaterThan("version", 4).Build().Exec(ctx)
	if err != nil {
		t.Fatalf("WhereGreaterThan version: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 rows with version > 4, got %d", len(results))
	}
	for i := range results {
		if results[i].Version() <= 4 {
			t.Errorf("unexpected version %d (should be > 4)", results[i].Version())
		}
	}
}

func TestTenant_QB_OrderBy_Limit(t *testing.T) {
	m, cleanup := setupTenantTable(t)
	defer cleanup()
	ctx := context.Background()

	for i, ver := range []int{3, 1, 5, 2, 4} {
		insertTenant(t, m, newTenant(
			fmt.Sprintf("ol-t-%d", i),
			fmt.Sprintf("T%d", i),
			fmt.Sprintf("slug-ol-%d", i),
			"", StatusActiveAcc, nil, ver))
	}

	results, err := m.Query().OrderBy("version", true).Limit(3).Build().Exec(ctx)
	if err != nil {
		t.Fatalf("OrderBy+Limit: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("expected 3 rows, got %d", len(results))
	}
	// DESC: 5, 4, 3
	if results[0].Version() < results[1].Version() {
		t.Error("expected descending version order")
	}
}

func TestTenant_QB_ToSQL_Interpolated(t *testing.T) {
	m, cleanup := setupTenantTable(t)
	defer cleanup()

	sqlStr := m.Query().
		Where("status", string(StatusActiveAcc)).
		WhereGreaterThan("version", 2).
		OrderBy("name", false).
		Limit(5).
		ToSQL()

	if strings.Contains(sqlStr, "$1") || strings.Contains(sqlStr, "$2") {
		t.Errorf("ToSQL should not contain $N placeholders: %q", sqlStr)
	}
	if !strings.Contains(sqlStr, "'active'") {
		t.Errorf("ToSQL should contain 'active': %q", sqlStr)
	}
	if !strings.Contains(sqlStr, "2") {
		t.Errorf("ToSQL should contain 2: %q", sqlStr)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Paginate with *QueryBuilder
// ─────────────────────────────────────────────────────────────────────────────

func TestTenant_Paginate_QB(t *testing.T) {
	m, cleanup := setupTenantTable(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		insertTenant(t, m, newTenant(
			fmt.Sprintf("pqb-t-%d", i),
			fmt.Sprintf("Tenant%d", i),
			fmt.Sprintf("slug-pqb-%d", i),
			"", StatusActiveAcc, nil, i))
	}

	page, err := m.Paginate(ctx, 1, 4, m.Query().OrderBy("version", true))
	if err != nil {
		t.Fatalf("Paginate QB: %v", err)
	}
	if page.Total != 10 {
		t.Errorf("Total: want 10, got %d", page.Total)
	}
	if len(page.Items) != 4 {
		t.Errorf("Items: want 4, got %d", len(page.Items))
	}
	if page.TotalPages != 3 {
		t.Errorf("TotalPages: want 3, got %d", page.TotalPages)
	}
	for i := range page.Items {
		if page.Items[i].ID() == nil {
			t.Error("ID nil in paginated result")
		}
		if page.Items[i].CreatedAt().Value().IsZero() {
			t.Error("createdAt zero in paginated result")
		}
	}
}

func TestTenant_Paginate_QB_Page2(t *testing.T) {
	m, cleanup := setupTenantTable(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 7; i++ {
		insertTenant(t, m, newTenant(
			fmt.Sprintf("p2-t-%d", i),
			fmt.Sprintf("T%d", i),
			fmt.Sprintf("slug-p2-%d", i),
			"", StatusActiveAcc, nil, i))
	}

	page, err := m.Paginate(ctx, 2, 3, m.Query().OrderBy("version", true))
	if err != nil {
		t.Fatalf("Paginate page 2: %v", err)
	}
	if page.Page != 2 {
		t.Errorf("Page: want 2, got %d", page.Page)
	}
	if len(page.Items) != 3 {
		t.Errorf("Items: want 3 on page 2, got %d", len(page.Items))
	}
}

func TestTenant_Paginate_QB_Empty(t *testing.T) {
	m, cleanup := setupTenantTable(t)
	defer cleanup()
	ctx := context.Background()

	page, err := m.Paginate(ctx, 1, 10, m.Query())
	if err != nil {
		t.Fatalf("Paginate empty: %v", err)
	}
	if page.Total != 0 || page.TotalPages != 0 {
		t.Errorf("empty table: Total=%d TotalPages=%d", page.Total, page.TotalPages)
	}
}

func TestTenant_Paginate_QB_WithFilter(t *testing.T) {
	m, cleanup := setupTenantTable(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		status := StatusActiveAcc
		if i%2 == 0 {
			status = StatusInactiveAcc
		}
		insertTenant(t, m, newTenant(
			fmt.Sprintf("pf-t-%d", i),
			fmt.Sprintf("T%d", i),
			fmt.Sprintf("slug-pf-%d", i),
			"", status, nil, 0))
	}

	page, err := m.Paginate(ctx, 1, 10,
		m.Query().Where("status", string(StatusActiveAcc)))
	if err != nil {
		t.Fatalf("Paginate filter: %v", err)
	}
	if page.Total != 2 {
		t.Errorf("Total: want 2 active, got %d", page.Total)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Paginate with *RawQuery
// ─────────────────────────────────────────────────────────────────────────────

func TestTenant_Paginate_RawQuery(t *testing.T) {
	m, cleanup := setupTenantTable(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 9; i++ {
		insertTenant(t, m, newTenant(
			fmt.Sprintf("rq-t-%d", i),
			fmt.Sprintf("T%d", i),
			fmt.Sprintf("slug-rq-%d", i),
			"", StatusActiveAcc, nil, i))
	}

	rq := NewRawQuery[Tenant](m.tableName,
		fmt.Sprintf("SELECT * FROM %s WHERE version >= $1 ORDER BY version ASC", m.tableName),
		3,
	)
	// version >= 3 → 6 rows (3,4,5,6,7,8)
	page, err := m.Paginate(ctx, 1, 3, rq)
	if err != nil {
		t.Fatalf("Paginate RawQuery: %v", err)
	}
	if page.Total != 6 {
		t.Errorf("Total: want 6, got %d", page.Total)
	}
	if len(page.Items) != 3 {
		t.Errorf("Items: want 3, got %d", len(page.Items))
	}
	for i := range page.Items {
		if page.Items[i].Version() < 3 {
			t.Errorf("unexpected version %d (should be >= 3)", page.Items[i].Version())
		}
		if page.Items[i].CreatedAt().Value().IsZero() {
			t.Error("createdAt zero in RawQuery result")
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// PaginateAs – project Tenant → TenantDTO
// ─────────────────────────────────────────────────────────────────────────────

func TestTenant_PaginateAs_Projection(t *testing.T) {
	m, cleanup := setupTenantTable(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 6; i++ {
		insertTenant(t, m, newTenant(
			fmt.Sprintf("pa-t-%d", i),
			fmt.Sprintf("Tenant%d", i),
			fmt.Sprintf("slug-pa-%d", i),
			"", StatusActiveAcc, nil, 0))
	}

	rq := NewRawQuery[Tenant](m.tableName,
		fmt.Sprintf("SELECT id, name, slug, status FROM %s ORDER BY name", m.tableName),
	)
	page, err := PaginateAs[Tenant, TenantDTO](ctx, m.readConn, 1, 3, rq)
	if err != nil {
		t.Fatalf("PaginateAs: %v", err)
	}
	if page.Total != 6 {
		t.Errorf("Total: want 6, got %d", page.Total)
	}
	if len(page.Items) != 3 {
		t.Errorf("Items: want 3, got %d", len(page.Items))
	}
	for _, dto := range page.Items {
		if dto.ID == "" {
			t.Error("projected ID empty")
		}
		if dto.Name == "" {
			t.Error("projected Name empty")
		}
		if dto.Slug == "" {
			t.Error("projected Slug empty")
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// QueryFromSQL + chained + Paginate
// ─────────────────────────────────────────────────────────────────────────────

func TestTenant_QueryFromSQL_Chained_Paginate(t *testing.T) {
	m, cleanup := setupTenantTable(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 9; i++ {
		status := StatusActiveAcc
		if i >= 6 {
			status = StatusInactiveAcc
		}
		insertTenant(t, m, newTenant(
			fmt.Sprintf("qfs-t-%d", i),
			fmt.Sprintf("T%d", i),
			fmt.Sprintf("slug-qfs-%d", i),
			"", status, nil, i))
	}

	qb := m.QueryFromSQL(
		fmt.Sprintf("SELECT * FROM %s WHERE status = $1", m.tableName),
		string(StatusActiveAcc),
	).WhereGreaterThan("version", 2).OrderBy("version", true) // DESC

	// active AND version > 2 → rows 3,4,5 → 3 rows
	page, err := m.Paginate(ctx, 1, 2, qb)
	if err != nil {
		t.Fatalf("QueryFromSQL+Paginate: %v", err)
	}
	if page.Total != 3 {
		t.Errorf("Total: want 3, got %d", page.Total)
	}
	if len(page.Items) != 2 {
		t.Errorf("Items page 1: want 2, got %d", len(page.Items))
	}
	// DESC version: 5, 4 on page 1
	if page.Items[0].Version() < page.Items[1].Version() {
		t.Error("expected descending version order")
	}
}

func TestTenant_QueryFromSQL_ToSQL(t *testing.T) {
	m, cleanup := setupTenantTable(t)
	defer cleanup()

	sqlStr := m.QueryFromSQL(
		fmt.Sprintf("SELECT * FROM %s WHERE status = $1", m.tableName),
		string(StatusActiveAcc),
	).WhereGreaterThan("version", 5).ToSQL()

	if strings.Contains(sqlStr, "$1") || strings.Contains(sqlStr, "$2") {
		t.Errorf("ToSQL must not contain placeholders: %q", sqlStr)
	}
	if !strings.Contains(sqlStr, "'active'") {
		t.Errorf("ToSQL should contain 'active': %q", sqlStr)
	}
	if !strings.Contains(sqlStr, "5") {
		t.Errorf("ToSQL should contain 5: %q", sqlStr)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Soft-delete with domain.Date fields
// ─────────────────────────────────────────────────────────────────────────────

func TestTenant_SoftDelete_All(t *testing.T) {
	m, cleanup := setupTenantTableSD(t)
	defer cleanup()
	ctx := context.Background()

	insertTenant(t, m, newTenant("sd-t-1", "Alice", "alice-t", "", StatusActiveAcc, nil, 0))
	insertTenant(t, m, newTenant("sd-t-2", "Bob", "bob-t", "", StatusActiveAcc, nil, 0))

	if err := m.Delete(ctx, "sd-t-2"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	all, err := m.All().Exec(ctx)
	if err != nil {
		t.Fatalf("All after soft-delete: %v", err)
	}
	if len(all) != 1 {
		t.Errorf("expected 1 active tenant, got %d", len(all))
	}
	if all[0].Name() != "Alice" {
		t.Errorf("expected Alice, got %q", all[0].Name())
	}
	// CreatedAt should still be populated after soft-delete filter
	if all[0].CreatedAt().Value().IsZero() {
		t.Error("createdAt zero after soft-delete All scan")
	}
}

func TestTenant_SoftDelete_Paginate(t *testing.T) {
	m, cleanup := setupTenantTableSD(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 6; i++ {
		insertTenant(t, m, newTenant(
			fmt.Sprintf("sdp-t-%d", i),
			fmt.Sprintf("T%d", i),
			fmt.Sprintf("slug-sdp-%d", i),
			"", StatusActiveAcc, nil, 0))
	}
	m.writeConn.DB().ExecContext(ctx,
		fmt.Sprintf("UPDATE %s SET deleted_at=NOW() WHERE id IN ($1,$2)", m.tableName),
		"sdp-t-0", "sdp-t-1")

	page, err := m.Paginate(ctx, 1, 10, m.Query())
	if err != nil {
		t.Fatalf("Paginate with soft-delete: %v", err)
	}
	if page.Total != 4 {
		t.Errorf("Total: want 4 active, got %d", page.Total)
	}
	// Verify domain.Date fields are populated in paginated results
	for i := range page.Items {
		if page.Items[i].CreatedAt().Value().IsZero() {
			t.Error("createdAt zero in soft-deleted paginate result")
		}
	}
}

func TestTenant_SoftDelete_WithTrashed(t *testing.T) {
	m, cleanup := setupTenantTableSD(t)
	defer cleanup()
	ctx := context.Background()

	insertTenant(t, m, newTenant("wt-t-1", "A", "wt-a", "", StatusActiveAcc, nil, 0))
	insertTenant(t, m, newTenant("wt-t-2", "B", "wt-b", "", StatusActiveAcc, nil, 0))
	m.writeConn.DB().ExecContext(ctx,
		fmt.Sprintf("UPDATE %s SET deleted_at=NOW() WHERE id=$1", m.tableName), "wt-t-2")

	page, err := m.Paginate(ctx, 1, 10, m.Query().WithTrashed())
	if err != nil {
		t.Fatalf("WithTrashed: %v", err)
	}
	if page.Total != 2 {
		t.Errorf("WithTrashed Total: want 2, got %d", page.Total)
	}
}

// TestTenant_SoftDelete_DeletedAtPopulated proves that after soft-deletion,
// reading via WithTrashed shows a non-nil *domain.Date in deletedAt.
func TestTenant_SoftDelete_DeletedAtPopulated(t *testing.T) {
	m, cleanup := setupTenantTableSD(t)
	defer cleanup()
	ctx := context.Background()

	before := time.Now().Add(-time.Second)
	insertTenant(t, m, newTenant("del-at-1", "X", "del-x", "", StatusActiveAcc, nil, 0))
	m.Delete(ctx, "del-at-1")

	// Read back with WithTrashed so deleted row is visible
	all, err := m.Query().WithTrashed().Build().Exec(ctx)
	if err != nil {
		t.Fatalf("WithTrashed All: %v", err)
	}
	if len(all) != 1 {
		t.Fatalf("expected 1 row, got %d", len(all))
	}
	del := all[0]
	if del.DeletedAt() == nil {
		t.Fatal("deletedAt should be non-nil after soft-delete")
	}
	da := del.DeletedAt().Value()
	if da.IsZero() {
		t.Error("deletedAt value should not be zero")
	}
	if da.Before(before) {
		t.Errorf("deletedAt %v is before expected time %v", da, before)
	}
	// CreatedAt should still be valid
	if del.CreatedAt().Value().IsZero() {
		t.Error("createdAt should not be zero")
	}
}

func TestTenant_SoftDelete_QueryFromSQL_Paginate(t *testing.T) {
	m, cleanup := setupTenantTableSD(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		insertTenant(t, m, newTenant(
			fmt.Sprintf("sqd-t-%d", i),
			fmt.Sprintf("T%d", i),
			fmt.Sprintf("slug-sqd-%d", i),
			"", StatusActiveAcc, nil, i))
	}
	m.writeConn.DB().ExecContext(ctx,
		fmt.Sprintf("UPDATE %s SET deleted_at=NOW() WHERE id=$1", m.tableName), "sqd-t-0")

	qb := m.QueryFromSQL(
		fmt.Sprintf("SELECT * FROM %s WHERE status = $1", m.tableName),
		string(StatusActiveAcc),
	).OrderBy("version", false)

	page, err := m.Paginate(ctx, 1, 10, qb)
	if err != nil {
		t.Fatalf("QueryFromSQL SD Paginate: %v", err)
	}
	if page.Total != 4 {
		t.Errorf("Total: want 4 (1 soft-deleted), got %d", page.Total)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Complex multi-condition query with domain.Date in results
// ─────────────────────────────────────────────────────────────────────────────

func TestTenant_ComplexQuery(t *testing.T) {
	m, cleanup := setupTenantTable(t)
	defer cleanup()
	ctx := context.Background()

	data := []struct {
		id, name, sl string
		status       Status
		version      int
	}{
		{"cq-t-1", "Acme Corp", "acme-cq", StatusActiveAcc, 10},
		{"cq-t-2", "Beta Ltd", "beta-cq", StatusActiveAcc, 3},
		{"cq-t-3", "Carol Inc", "carol-cq", StatusSuspended, 15},
		{"cq-t-4", "Dave Co", "dave-cq", StatusActiveAcc, 8},
		{"cq-t-5", "Eve LLC", "eve-cq", StatusInactiveAcc, 12},
		{"cq-t-6", "Frank AG", "frank-cq", StatusActiveAcc, 1},
	}
	for _, d := range data {
		insertTenant(t, m, newTenant(d.id, d.name, d.sl, "", d.status, nil, d.version))
	}

	// active tenants with version > 5, ordered by version DESC, limit 3
	results, err := m.Query().
		Where("status", string(StatusActiveAcc)).
		WhereGreaterThan("version", 5).
		OrderBy("version", true).
		Limit(3).
		Build().Exec(ctx)
	if err != nil {
		t.Fatalf("ComplexQuery: %v", err)
	}
	// Matching: Acme(10), Dave(8) → 2 rows (Frank=1 excluded, Beta=3 excluded)
	if len(results) != 2 {
		t.Errorf("expected 2 rows, got %d", len(results))
	}
	// DESC: Acme(10), Dave(8)
	if results[0].Version() != 10 {
		t.Errorf("first row version: want 10, got %d", results[0].Version())
	}
	// All results must have non-zero domain.Date fields
	for i := range results {
		if results[i].CreatedAt().Value().IsZero() {
			t.Errorf("row %d: createdAt should not be zero", i)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Chunk streaming
// ─────────────────────────────────────────────────────────────────────────────

func TestTenant_Chunk(t *testing.T) {
	m, cleanup := setupTenantTable(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 7; i++ {
		insertTenant(t, m, newTenant(
			fmt.Sprintf("ch-t-%d", i),
			fmt.Sprintf("T%d", i),
			fmt.Sprintf("slug-ch-%d", i),
			"", StatusActiveAcc, nil, i))
	}

	var ids []string
	var datesAllSet = true
	err := m.Chunk(ctx, 3, nil, func(batch []Tenant) error {
		for i := range batch {
			ids = append(ids, batch[i].ID().Value())
			if batch[i].CreatedAt().Value().IsZero() {
				datesAllSet = false
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Chunk: %v", err)
	}
	if len(ids) != 7 {
		t.Errorf("expected 7 chunks, got %d", len(ids))
	}
	if !datesAllSet {
		t.Error("some createdAt values were zero in Chunk results")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Page navigation helpers
// ─────────────────────────────────────────────────────────────────────────────

func TestTenant_Page_Navigation(t *testing.T) {
	m, cleanup := setupTenantTable(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 9; i++ {
		insertTenant(t, m, newTenant(
			fmt.Sprintf("nav-t-%d", i),
			fmt.Sprintf("T%d", i),
			fmt.Sprintf("slug-nav-%d", i),
			"", StatusActiveAcc, nil, i))
	}

	p1, _ := m.Paginate(ctx, 1, 3, m.Query().OrderBy("version", true))
	if !p1.HasNext() {
		t.Error("p1 HasNext should be true")
	}
	if p1.HasPrev() {
		t.Error("p1 HasPrev should be false")
	}
	if p1.NextPage() != 2 {
		t.Errorf("p1.NextPage want 2, got %d", p1.NextPage())
	}

	p3, _ := m.Paginate(ctx, 3, 3, m.Query().OrderBy("version", true))
	if p3.HasNext() {
		t.Error("p3 HasNext should be false")
	}
	if !p3.HasPrev() {
		t.Error("p3 HasPrev should be true")
	}
	if p3.PrevPage() != 2 {
		t.Errorf("p3.PrevPage want 2, got %d", p3.PrevPage())
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// domain.Date Format / String helpers work after unsafe scan
// ─────────────────────────────────────────────────────────────────────────────

func TestTenant_DomainDate_Helpers(t *testing.T) {
	m, cleanup := setupTenantTable(t)
	defer cleanup()
	ctx := context.Background()

	insertTenant(t, m, newTenant("dh-1", "DateHelper Corp", "dh-corp", "", StatusActiveAcc, nil, 0))

	got, err := m.Find("dh-1").Exec(ctx)
	if err != nil {
		t.Fatalf("Find: %v", err)
	}

	ca := got.CreatedAt()
	if ca.Year() < 2024 {
		t.Errorf("Year: want >= 2024, got %d", ca.Year())
	}
	if ca.Unix() <= 0 {
		t.Errorf("Unix: want > 0, got %d", ca.Unix())
	}
	formatted := ca.Format(time.RFC3339)
	if formatted == "" {
		t.Error("Format(RFC3339) returned empty string")
	}
	if ca.String() == "" {
		t.Error("String() returned empty")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// IsBefore / IsAfter on domain.Date after unsafe scan
// ─────────────────────────────────────────────────────────────────────────────

func TestTenant_DomainDate_IsBefore_IsAfter(t *testing.T) {
	m, cleanup := setupTenantTable(t)
	defer cleanup()
	ctx := context.Background()

	insertTenant(t, m, newTenant("ia-1", "T1", "ia-slug-1", "", StatusActiveAcc, nil, 0))
	// Small sleep to ensure created_at differs
	time.Sleep(20 * time.Millisecond)
	insertTenant(t, m, newTenant("ia-2", "T2", "ia-slug-2", "", StatusActiveAcc, nil, 0))

	all, err := m.All().Exec(ctx)
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(all))
	}

	// Sort by created_at to find earlier/later
	var earlier, later Tenant
	if all[0].CreatedAt().Value().Before(all[1].CreatedAt().Value()) {
		earlier, later = all[0], all[1]
	} else {
		earlier, later = all[1], all[0]
	}

	if !earlier.CreatedAt().IsBefore(later.CreatedAt()) {
		t.Error("earlier.CreatedAt().IsBefore(later) should be true")
	}
	if !later.CreatedAt().IsAfter(earlier.CreatedAt()) {
		t.Error("later.CreatedAt().IsAfter(earlier) should be true")
	}
}

