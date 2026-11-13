package integration

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ranajahanzaib/zero-downtime-db-migrations/internal/app"
)

func TestRealPostgresMigrationLifecycle(t *testing.T) {
	if os.Getenv("RUN_POSTGRES_TESTS") != "1" {
		t.Skip("set RUN_POSTGRES_TESTS=1 with a disposable PostgreSQL database")
	}
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Fatal("DATABASE_URL required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := "zdmm_test_" + uuid.NewString()[:8]
	if _, err = admin.Exec(ctx, "create schema "+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = admin.Exec(context.Background(), "drop schema "+pgx.Identifier{schema}.Sanitize()+" cascade")
		_ = admin.Close(context.Background())
	}()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema + ",public"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	runSQL := func(path string) {
		b, e := os.ReadFile(filepath.Join("..", "..", path))
		if e != nil {
			t.Fatal(e)
		}
		if _, e = pool.Exec(ctx, string(b)); e != nil {
			t.Fatalf("apply %s: %v", path, e)
		}
	}
	runSQL("migrations/baseline/001_initial.sql")
	tenant := uuid.New()
	otherTenant := uuid.New()
	good := uuid.New()
	pendingRow := uuid.New()
	bad := uuid.New()
	if _, err = pool.Exec(ctx, "insert into customers(id,tenant_id,company_name,contact_details) values($1,$2,'Good',jsonb_build_object('email','before@example.test','phone','123','timezone','UTC','preferred_language','ur')),($3,$2,'Pending',jsonb_build_object('email','pending@example.test','preferred_language','en')),($4,$2,'Bad',jsonb_build_object('email',123,'preferred_language','fr'))", good, tenant, pendingRow, bad); err != nil {
		t.Fatal(err)
	}
	// Exercise the actual V1 HTTP application against the unexpanded schema and verify tenant isolation.
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	query := u.Query()
	query.Set("options", "-csearch_path="+schema+",public")
	u.RawQuery = query.Encode()
	legacy, err := app.New(app.Config{DSN: u.String(), Version: "v1", ReadMode: "legacy", TenantHeader: "X-Demo-Tenant", InstanceID: "integration-v1"})
	if err != nil {
		t.Fatalf("V1 did not start on original schema: %v", err)
	}
	api := httptest.NewServer(legacy.Handler)
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, api.URL+"/api/customers/"+good.String(), nil)
	request.Header.Set("X-Demo-Tenant", otherTenant.String())
	response, requestErr := http.DefaultClient.Do(request)
	if requestErr != nil {
		t.Fatal(requestErr)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("cross-tenant V1 read returned %d, want 404", response.StatusCode)
	}
	api.Close()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
	if err = legacy.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("V1 shutdown: %v", err)
	}
	shutdownCancel()
	runSQL("migrations/expand/002_expand_and_bridge.sql")
	var email string
	var phone, tz *string
	var migrated *time.Time
	var preferred string
	if err = pool.QueryRow(ctx, "select contact_email,contact_phone,contact_timezone,contact_migrated_at,contact_details->>'preferred_language' from customers where id=$1", good).Scan(&email, &phone, &tz, &migrated, &preferred); err != nil {
		t.Fatal(err)
	}
	if email != "before@example.test" || migrated != nil || preferred != "ur" {
		t.Fatalf("unexpected expand state email=%s migrated=%v language=%s", email, migrated, preferred)
	}
	if _, err = pool.Exec(ctx, "update customers set contact_details=jsonb_set(contact_details,'{email}',to_jsonb('v1@example.test'::text),true) where id=$1", good); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, "select contact_email from customers where id=$1", good).Scan(&email); err != nil || email != "v1@example.test" {
		t.Fatalf("V1 bridge failed: %s %v", email, err)
	}
	if _, err = pool.Exec(ctx, "update customers set contact_phone=NULL,contact_timezone=NULL where id=$1", good); err != nil {
		t.Fatal(err)
	}
	var hasPhone, hasTZ bool
	_ = pool.QueryRow(ctx, "select contact_details ? 'phone',contact_details ? 'timezone' from customers where id=$1", good).Scan(&hasPhone, &hasTZ)
	if hasPhone || hasTZ {
		t.Fatal("modern NULL values were not cleared from JSONB")
	}
	_ = pool.QueryRow(ctx, "select contact_details->>'preferred_language' from customers where id=$1", good).Scan(&preferred)
	if preferred != "ur" {
		t.Fatal("unrelated JSONB key was lost")
	}
	_, err = pool.Exec(ctx, "update customers set contact_email='modern@example.test',contact_details=jsonb_set(contact_details,'{email}',to_jsonb('conflict@example.test'::text),true) where id=$1", good)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23514" {
		t.Fatalf("divergent dual write was not rejected: %v", err)
	}
	var expected int64
	_ = pool.QueryRow(ctx, "select version from customers where id=$1", good).Scan(&expected)
	successes := make(chan int, 2)
	for i := 0; i < 2; i++ {
		go func(i int) {
			tag, e := pool.Exec(ctx, "update customers set contact_email=$1,version=version+1 where id=$2 and tenant_id=$3 and version=$4", fmt.Sprintf("writer%d@example.test", i), good, tenant, expected)
			if e == nil && tag.RowsAffected() == 1 {
				successes <- 1
			} else {
				successes <- 0
			}
		}(i)
	}
	wins := <-successes + <-successes
	if wins != 1 {
		t.Fatalf("optimistic concurrency accepted %d writers", wins)
	}
	// A valid row is backfilled while malformed source data is quarantined independently.
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := tx.Query(ctx, "select id,tenant_id from customers where contact_migrated_at is null order by id for update skip locked")
	if err != nil {
		t.Fatal(err)
	}
	type row struct{ id, tenant uuid.UUID }
	var pending []row
	for rows.Next() {
		var x row
		if err = rows.Scan(&x.id, &x.tenant); err != nil {
			t.Fatal(err)
		}
		pending = append(pending, x)
	}
	rows.Close()
	for _, x := range pending {
		var typ string
		_ = tx.QueryRow(ctx, "select coalesce(jsonb_typeof(contact_details->'email'),'missing') from customers where id=$1", x.id).Scan(&typ)
		if typ != "string" {
			_, err = tx.Exec(ctx, "insert into migration_exceptions(customer_id,tenant_id,category,detail,source_fingerprint,retry_count) values($1,$2,'invalid_email','fixture','fixture-checksum',1)", x.id, x.tenant)
			if err != nil {
				t.Fatal(err)
			}
			continue
		}
		_, err = tx.Exec(ctx, "update customers set contact_email=contact_details->>'email',contact_phone=contact_details->>'phone',contact_timezone=contact_details->>'timezone',contact_migrated_at=now() where id=$1", x.id)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var unresolved int
	if err = pool.QueryRow(ctx, "select count(*) from migration_exceptions where resolved_at is null").Scan(&unresolved); err != nil || unresolved != 1 {
		t.Fatalf("invalid record was not quarantined: n=%d err=%v", unresolved, err)
	}
	// Concurrent index creation must run outside a transaction and catalog validity is checked.
	if _, err = pool.Exec(ctx, "create index concurrently idx_customers_tenant_contact_email on customers(tenant_id,contact_email)"); err != nil {
		t.Fatal(err)
	}
	var valid bool
	_ = pool.QueryRow(ctx, "select indisvalid from pg_index where indexrelid='idx_customers_tenant_contact_email'::regclass").Scan(&valid)
	if !valid {
		t.Fatal("concurrent index is invalid")
	}
	// A conflicting DDL lock times out while ordinary MVCC reads remain possible.
	locker, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = locker.Exec(ctx, "begin; lock table customers in share mode"); err != nil {
		locker.Release()
		t.Fatal(err)
	}
	_, err = admin.Exec(ctx, "set lock_timeout='100ms'; alter table "+pgx.Identifier{schema, "customers"}.Sanitize()+" add column lock_test integer")
	if !errors.As(err, &pgErr) || pgErr.Code != "55P03" {
		locker.Exec(ctx, "rollback")
		locker.Release()
		t.Fatalf("expected lock timeout 55P03, got %v", err)
	}
	var count int
	_ = pool.QueryRow(ctx, "select count(*) from customers").Scan(&count)
	_, _ = locker.Exec(ctx, "rollback")
	locker.Release()
	// Repair the invalid fixture, then validate the staged constraint.
	if _, err = pool.Exec(ctx, "update customers set contact_details=jsonb_set(contact_details,'{email}',to_jsonb('fixed@example.test'::text),true) where id=$1", bad); err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(ctx, "update migration_exceptions set resolved_at=now(),resolution_status='resolved' where customer_id=$1", bad)
	if err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(ctx, "alter table customers add constraint customers_contact_email_present check(contact_email is not null) not valid; alter table customers validate constraint customers_contact_email_present; alter table customers alter column contact_email set not null")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, "drop trigger customers_contact_bridge on customers; drop function bridge_customer_contact(); alter table customers drop column contact_details,drop column contact_migrated_at"); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, "insert into customers(tenant_id,company_name,contact_email) values($1,'After contract','post@example.test')", tenant); err != nil {
		t.Fatalf("modern app cannot write contracted schema: %v", err)
	}
}
