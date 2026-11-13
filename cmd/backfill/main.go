package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	batch := flag.Int("batch", 250, "rows per bounded transaction")
	workers := flag.Int("workers", 4, "concurrent SKIP LOCKED workers")
	pause := flag.Duration("pause", 10*time.Millisecond, "delay between committed batches")
	flag.Parse()
	if *batch < 1 || *workers < 1 {
		log.Fatal("batch and workers must be positive")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	p, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}
	defer p.Close()
	var total int64
	if err = p.QueryRow(ctx, "select count(*) from customers").Scan(&total); err != nil {
		log.Fatal(err)
	}
	_, _ = p.Exec(ctx, "update migration_control set phase='backfill',status='running',updated_at=now() where id=true")
	_, _ = p.Exec(ctx, "insert into migration_events(event_type,status,details) values('backfill_started','running',jsonb_build_object('batch_size',$1,'workers',$2,'customers',$3))", *batch, *workers, total)
	started := time.Now()
	var wg sync.WaitGroup
	errs := make(chan error, *workers)
	for w := 0; w < *workers; w++ {
		var runID int64
		if err = p.QueryRow(ctx, "insert into backfill_runs(worker_id,batch_size,worker_count) values($1,$2,$3) returning id", fmt.Sprintf("pid-%d-worker-%d", os.Getpid(), w), *batch, *workers).Scan(&runID); err != nil {
			log.Fatal(err)
		}
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			if e := worker(ctx, p, id, *batch, *workers, *pause, total, started); e != nil {
				errs <- e
				cancel()
			}
		}(runID)
	}
	wg.Wait()
	close(errs)
	var runErr error
	for e := range errs {
		if e != nil && !errors.Is(e, context.Canceled) {
			runErr = e
		}
	}
	status := "completed"
	if runErr != nil {
		status = "failed"
	} else if ctx.Err() != nil {
		status = "interrupted"
	}
	_, _ = p.Exec(context.Background(), "update migration_control set status=$1,updated_at=now() where id=true", status)
	_, _ = p.Exec(context.Background(), "insert into migration_events(event_type,status,details) values('backfill_finished',$1,jsonb_build_object('status',$1,'error',$2))", status, errorString(runErr))
	if runErr != nil {
		log.Print(runErr)
		os.Exit(1)
	}
	if status == "interrupted" {
		log.Print("backfill interrupted; committed batches are safe to resume")
		os.Exit(2)
	}
}
func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
func worker(ctx context.Context, p *pgxpool.Pool, runID int64, batch, workers int, pause time.Duration, total int64, started time.Time) error {
	var batches, done, invalid int64
	retries := 0
	status := "completed"
	defer func() {
		_, _ = p.Exec(context.Background(), "update backfill_runs set finished_at=now(),batches=$2,rows_migrated=$3,rows_invalid=$4,status=$5 where id=$1", runID, batches, done, invalid, status)
	}()
	for {
		if ctx.Err() != nil {
			status = "interrupted"
			return ctx.Err()
		}
		tx, e := p.Begin(ctx)
		if e != nil {
			status = "failed"
			return e
		}
		if _, e = tx.Exec(ctx, "set local lock_timeout='2s'; set local statement_timeout='30s'"); e != nil {
			_ = tx.Rollback(ctx)
			status = "failed"
			return e
		}
		rows, e := tx.Query(ctx, "select id,tenant_id,contact_details from customers where contact_migrated_at is null and not exists(select 1 from migration_exceptions x where x.customer_id=customers.id and x.resolved_at is null) order by id limit $1 for update skip locked", batch)
		if e != nil {
			_ = tx.Rollback(ctx)
			status = "failed"
			return e
		}
		type item struct {
			id, tenant string
			doc        []byte
		}
		items := make([]item, 0, batch)
		for rows.Next() {
			var x item
			if e = rows.Scan(&x.id, &x.tenant, &x.doc); e != nil {
				break
			}
			items = append(items, x)
		}
		if e == nil {
			e = rows.Err()
		}
		rows.Close()
		if e != nil {
			_ = tx.Rollback(ctx)
			status = "failed"
			return e
		}
		if len(items) == 0 {
			_ = tx.Rollback(ctx)
			var pending int64
			if e = p.QueryRow(ctx, "select count(*) from customers where contact_migrated_at is null and not exists(select 1 from migration_exceptions x where x.customer_id=customers.id and x.resolved_at is null)").Scan(&pending); e != nil {
				status = "failed"
				return e
			}
			if pending == 0 {
				return nil
			}
			select {
			case <-ctx.Done():
				status = "interrupted"
				return ctx.Err()
			case <-time.After(100 * time.Millisecond):
				continue
			}
		}
		batchDone, batchInvalid := int64(0), int64(0)
		for _, x := range items {
			var objectType, emailType, phoneType, tzType, email string
			e = tx.QueryRow(ctx, "select coalesce(jsonb_typeof(contact_details),'missing'),coalesce(jsonb_typeof(contact_details->'email'),'missing'),coalesce(jsonb_typeof(contact_details->'phone'),'missing'),coalesce(jsonb_typeof(contact_details->'timezone'),'missing'),coalesce(contact_details->>'email','') from customers where id=$1", x.id).Scan(&objectType, &emailType, &phoneType, &tzType, &email)
			if e != nil {
				_ = tx.Rollback(ctx)
				status = "failed"
				return e
			}
			bad := objectType != "object" || emailType != "string" || strings.TrimSpace(email) == "" || !optionalType(phoneType) || !optionalType(tzType)
			if bad {
				sum := sha256.Sum256(x.doc)
				category, detail := "invalid_contact_document", "contact document has invalid structure or attribute types"
				if objectType == "object" && (emailType != "string" || strings.TrimSpace(email) == "") {
					category = "invalid_email"
					detail = "email must be a non-empty JSON string"
				}
				_, e = tx.Exec(ctx, "insert into migration_exceptions(customer_id,tenant_id,category,detail,source_fingerprint,retry_count,resolution_status) values($1,$2,$3,$4,$5,1,'open') on conflict(customer_id) do update set category=excluded.category,detail=excluded.detail,source_fingerprint=excluded.source_fingerprint,retry_count=migration_exceptions.retry_count+1,failed_at=now(),resolved_at=NULL,resolution_status='open'", x.id, x.tenant, category, detail, hex.EncodeToString(sum[:]))
				if e != nil {
					_ = tx.Rollback(ctx)
					status = "failed"
					return e
				}
				batchInvalid++
				continue
			}
			_, e = tx.Exec(ctx, "update customers set contact_email=nullif(contact_details->>'email',''),contact_phone=nullif(contact_details->>'phone',''),contact_timezone=nullif(contact_details->>'timezone',''),contact_migrated_at=now() where id=$1", x.id)
			if e != nil {
				_ = tx.Rollback(ctx)
				status = "failed"
				return e
			}
			batchDone++
		}
		if e = tx.Commit(ctx); e != nil {
			if retryable(e) && retries < 5 {
				retries++
				select {
				case <-ctx.Done():
					status = "interrupted"
					return ctx.Err()
				case <-time.After(time.Duration(retries) * 100 * time.Millisecond):
				}
				continue
			}
			status = "failed"
			return e
		}
		retries = 0
		batches++
		done += batchDone
		invalid += batchInvalid
		_, e = p.Exec(ctx, "update backfill_runs set batches=$2,rows_migrated=$3,rows_invalid=$4 where id=$1", runID, batches, done, invalid)
		if e != nil {
			status = "failed"
			return e
		}
		var migrated, pending, quarantined int64
		e = p.QueryRow(ctx, "select count(*) filter(where contact_migrated_at is not null),count(*) filter(where contact_migrated_at is null and not exists(select 1 from migration_exceptions x where x.customer_id=customers.id and x.resolved_at is null)),count(*) filter(where exists(select 1 from migration_exceptions x where x.customer_id=customers.id and x.resolved_at is null)) from customers").Scan(&migrated, &pending, &quarantined)
		if e != nil {
			status = "failed"
			return e
		}
		elapsed := time.Since(started).Seconds()
		throughput := float64(migrated) / elapsed
		if elapsed == 0 {
			throughput = 0
		}
		fmt.Printf("Backfill progress total=%d migrated=%d pending=%d invalid=%d batch=%d workers=%d throughput=%.1f/s status=RUNNING\n", total, migrated, pending, quarantined, batch, workers, throughput)
		select {
		case <-ctx.Done():
			status = "interrupted"
			return ctx.Err()
		case <-time.After(pause):
		}
	}
}
func optionalType(t string) bool { return t == "missing" || t == "string" || t == "null" }
func retryable(err error) bool {
	var pe *pgconn.PgError
	if !errors.As(err, &pe) {
		return false
	}
	return pe.Code == "40001" || pe.Code == "40P01" || pe.Code == "55P03"
}
