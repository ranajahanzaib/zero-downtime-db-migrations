package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	if len(os.Args) < 2 || os.Args[1] != "seed" {
		log.Fatal("usage: demo seed --customers N")
	}
	fs := flag.NewFlagSet("seed", flag.ExitOnError)
	n := fs.Int("customers", 1000, "number of synthetic records")
	seed := fs.Int64("seed", 1, "deterministic dataset seed")
	_ = fs.Parse(os.Args[2:])
	if *n < 1 {
		log.Fatal("customers must be positive")
	}
	ctx := context.Background()
	p, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}
	defer p.Close()
	var existing int64
	if err = p.QueryRow(ctx, "select count(*) from customers").Scan(&existing); err != nil {
		log.Fatal(err)
	}
	if existing != 0 {
		log.Fatalf("demo seeding requires an empty disposable customers table (found %d); inspect data or run make reset to destroy the local volume", existing)
	}
	tenantIDs := []string{"11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222", "33333333-3333-4333-8333-333333333333"}
	timezones, languages := []string{"America/New_York", "Europe/London", "Asia/Karachi"}, []string{"en", "fr", "ur"}
	rotation := int((*seed%3 + 3) % 3)
	for start := 0; start < *n; start += 1000 {
		end := start + 1000
		if end > *n {
			end = *n
		}
		batch := make([][]any, 0, end-start)
		for i := start; i < end; i++ {
			var phone *string
			if i%3 != 0 {
				value := fmt.Sprintf("+1555%07d", i)
				phone = &value
			}
			contact := map[string]any{"email": fmt.Sprintf("customer-%06d@example.test", i), "phone": phone, "timezone": timezones[(i+rotation)%3], "preferred_language": languages[(i+rotation)%3]}
			doc, err := json.Marshal(contact)
			if err != nil {
				log.Fatal(err)
			}
			tenant, err := uuid.Parse(tenantIDs[(i+rotation)%len(tenantIDs)])
			if err != nil {
				log.Fatal(err)
			}
			id, err := uuid.Parse(fmt.Sprintf("%08x-0000-4000-8000-%012x", i+1, i+1))
			if err != nil {
				log.Fatal(err)
			}
			created := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(i) * time.Minute)
			batch = append(batch, []any{id, tenant, fmt.Sprintf("Fictional Company %06d", i), doc, created, created})
		}
		tx, err := p.Begin(ctx)
		if err != nil {
			log.Fatal(err)
		}
		_, err = tx.CopyFrom(ctx, pgx.Identifier{"customers"}, []string{"id", "tenant_id", "company_name", "contact_details", "created_at", "updated_at"}, pgx.CopyFromRows(batch))
		if err != nil {
			_ = tx.Rollback(ctx)
			log.Fatal(err)
		}
		if err = tx.Commit(ctx); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("seeded %d/%d\n", end, *n)
	}
	_, err = p.Exec(ctx, "insert into demo_datasets(id,seed,seeded_customers,tenant_count,config) values(true,$1,$2,$3,$4::jsonb) on conflict(id) do update set seed=excluded.seed,seeded_customers=excluded.seeded_customers,tenant_count=excluded.tenant_count,config=excluded.config,created_at=now()", *seed, *n, len(tenantIDs), fmt.Sprintf(`{"seed":%d,"customers":%d,"tenants":%d}`, *seed, *n, len(tenantIDs)))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("seeded %d fictional customers across %d tenants\n", *n, len(tenantIDs))
}
