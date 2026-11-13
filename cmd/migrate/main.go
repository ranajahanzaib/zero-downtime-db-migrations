package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const advisoryKey int64 = 829104220

func main() {
	if len(os.Args) < 2 || os.Args[1] == "--help" || os.Args[1] == "-h" || os.Args[1] == "help" {
		usage()
		return
	}
	ctx := context.Background()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		fatal("DATABASE_URL is required")
	}
	c, err := pgx.Connect(ctx, dsn)
	if err != nil {
		fatal(err.Error())
	}
	defer c.Close(ctx)
	cmd := os.Args[1]
	if cmd == "status" || cmd == "history" {
		if cmd == "status" {
			status(ctx, c)
		} else {
			history(ctx, c)
		}
		return
	}
	if _, err = c.Exec(ctx, "select pg_advisory_lock($1)", advisoryKey); err != nil {
		fatal(err.Error())
	}
	defer c.Exec(context.Background(), "select pg_advisory_unlock($1)", advisoryKey)
	switch cmd {
	case "baseline":
		applySQL(ctx, c, "001_initial", "migrations/baseline/001_initial.sql", true)
	case "expand":
		applySQL(ctx, c, "002_expand_and_bridge", "migrations/expand/002_expand_and_bridge.sql", false)
		if err := setControl(ctx, c, "compatibility", "active"); err != nil {
			fatal(err.Error())
		}
		event(ctx, c, "expand_completed", "succeeded", `{"trigger":"customers_contact_bridge"}`)
	case "indexes":
		indexes(ctx, c)
	case "constraints":
		constraints(ctx, c)
	case "compatibility":
		if len(os.Args) < 3 || os.Args[2] != "enable" {
			fatal("usage: migrate compatibility enable")
		}
		var enabled bool
		if err := c.QueryRow(ctx, "select exists(select 1 from pg_trigger where tgname='customers_contact_bridge' and tgenabled='O')").Scan(&enabled); err != nil || !enabled {
			fatal("compatibility trigger is not enabled")
		}
		_ = setControl(ctx, c, "compatibility", "active")
		event(ctx, c, "compatibility_confirmed", "succeeded", `{}`)
		fmt.Println("compatibility bridge is active")
	case "verify":
		verifyGate(ctx, c)
	case "reads":
		if len(os.Args) < 3 || (os.Args[2] != "new" && os.Args[2] != "legacy" && os.Args[2] != "shadow") {
			fatal("usage: migrate reads legacy|shadow|new")
		}
		switchReads(ctx, c, os.Args[2])
	case "backfill":
		if len(os.Args) < 3 || os.Args[2] != "status" {
			fatal("run go run ./cmd/backfill for work; usage: migrate backfill status")
		}
		backfillStatus(ctx, c)
	case "contract":
		contract(ctx, c)
	default:
		fatal("unknown command: " + cmd)
	}
}
func usage() {
	fmt.Println(`usage: migrate <command>
  baseline | expand | indexes | constraints
  compatibility enable
  backfill status
  verify
  reads legacy|shadow|new
  status | history
  contract --approve`)
}
func fatal(s string) { fmt.Fprintln(os.Stderr, s); os.Exit(1) }
func applySQL(ctx context.Context, c *pgx.Conn, id, path string, baseline bool) {
	started := time.Now()
	if !baseline {
		_, err := c.Exec(ctx, "insert into migration_history(migration_id,started_at,status) values($1,$2,'running') on conflict(migration_id) do update set started_at=excluded.started_at,status='running',finished_at=NULL,error_code=NULL,error_detail=NULL", id, started)
		if err != nil {
			fatal(err.Error())
		}
	}
	b, err := os.ReadFile(path)
	if err != nil {
		fatal(err.Error())
	}
	_, err = c.Exec(ctx, "set lock_timeout='2s'; set statement_timeout='2min'")
	if err == nil {
		_, err = c.Exec(ctx, string(b))
	}
	status := "succeeded"
	var code any
	if err != nil {
		status = "failed"
		var pe *pgconn.PgError
		if errors.As(err, &pe) {
			code = pe.Code
		}
	}
	if baseline { // migration_history is created by the baseline itself
		if err == nil {
			_, err = c.Exec(ctx, "insert into migration_history(migration_id,started_at,finished_at,duration_ms,status) values($1,$2,now(),$3,'succeeded') on conflict(migration_id) do update set started_at=excluded.started_at,finished_at=excluded.finished_at,duration_ms=excluded.duration_ms,status='succeeded',error_code=NULL", id, started, time.Since(started).Milliseconds())
			if err == nil {
				_, err = c.Exec(ctx, "update migration_history set schema_fingerprint=(select md5(coalesce(string_agg(table_name||'.'||column_name||':'||data_type,',' order by table_name,column_name),'')) from information_schema.columns where table_schema=current_schema()) where migration_id=$1", id)
			}
		}
	} else {
		_, _ = c.Exec(ctx, "update migration_history set finished_at=now(),duration_ms=$2,status=$3,error_code=$4,error_detail=$5,schema_fingerprint=(select md5(coalesce(string_agg(table_name||'.'||column_name||':'||data_type,',' order by table_name,column_name),'')) from information_schema.columns where table_schema=current_schema()) where migration_id=$1", id, time.Since(started).Milliseconds(), status, code, errorText(err))
	}
	if err != nil {
		fatal(err.Error())
	}
	event(ctx, c, id, status, `{}`)
	fmt.Printf("%s %s (%s)\n", id, status, time.Since(started))
}
func beginHistory(ctx context.Context, c *pgx.Conn, id string) time.Time {
	now := time.Now()
	_, err := c.Exec(ctx, "insert into migration_history(migration_id,started_at,status) values($1,$2,'running') on conflict(migration_id) do update set started_at=excluded.started_at,finished_at=NULL,status='running',error_code=NULL,error_detail=NULL", id, now)
	if err != nil {
		fatal(err.Error())
	}
	return now
}
func finishHistory(ctx context.Context, c *pgx.Conn, id string, start time.Time, err error) {
	status := "succeeded"
	var code any
	if err != nil {
		status = "failed"
		var pe *pgconn.PgError
		if errors.As(err, &pe) {
			code = pe.Code
		}
	}
	_, _ = c.Exec(ctx, "update migration_history set finished_at=now(),duration_ms=$2,status=$3,error_code=$4,error_detail=$5,schema_fingerprint=(select md5(coalesce(string_agg(table_name||'.'||column_name||':'||data_type,',' order by table_name,column_name),'')) from information_schema.columns where table_schema=current_schema()) where migration_id=$1", id, time.Since(start).Milliseconds(), status, code, errorText(err))
	event(ctx, c, id, status, `{}`)
}
func errorText(err error) any {
	if err == nil {
		return nil
	}
	return err.Error()
}
func indexes(ctx context.Context, c *pgx.Conn) {
	id := "003_tenant_contact_email_idx"
	start := beginHistory(ctx, c, id)
	_, err := c.Exec(ctx, "set lock_timeout='2s'; set statement_timeout='10min'")
	var valid bool
	if err == nil {
		err = c.QueryRow(ctx, "select i.indisvalid from pg_index i join pg_class x on x.oid=i.indexrelid where x.relname='idx_customers_tenant_contact_email'").Scan(&valid)
	}
	if err == pgx.ErrNoRows {
		err = nil
	}
	if err == nil && !valid {
		_, err = c.Exec(ctx, "drop index concurrently if exists idx_customers_tenant_contact_email")
	}
	if err == nil && !valid {
		var ddl []byte
		ddl, err = os.ReadFile("migrations/indexes/003_tenant_contact_email.sql")
		if err == nil {
			_, err = c.Exec(ctx, string(ddl))
		}
	}
	if err == nil {
		err = c.QueryRow(ctx, "select i.indisvalid from pg_index i join pg_class x on x.oid=i.indexrelid where x.relname='idx_customers_tenant_contact_email'").Scan(&valid)
		if err == nil && !valid {
			err = fmt.Errorf("concurrent index exists but indisvalid=false")
		}
	}
	finishHistory(ctx, c, id, start, err)
	if err != nil {
		fatal(err.Error())
	}
	_ = setControl(ctx, c, "indexes", "succeeded")
	fmt.Println("tenant/email index is valid")
}
func constraints(ctx context.Context, c *pgx.Conn) {
	id := "004_contact_email_constraint"
	start := beginHistory(ctx, c, id)
	_, err := c.Exec(ctx, "set lock_timeout='2s'; set statement_timeout='10min'")
	var exists, validated bool
	if err == nil {
		err = c.QueryRow(ctx, "select convalidated from pg_constraint where conname='customers_contact_email_present'").Scan(&validated)
	}
	if err == pgx.ErrNoRows {
		exists = false
		err = nil
	} else {
		exists = true
	}
	if err == nil && !exists {
		var ddl []byte
		ddl, err = os.ReadFile("migrations/constraints/004_contact_email_present.sql")
		if err == nil {
			for _, statement := range strings.Split(string(ddl), ";") {
				if strings.TrimSpace(statement) == "" {
					continue
				}
				if _, err = c.Exec(ctx, statement); err != nil {
					break
				}
			}
		}
	} else if err == nil && !validated {
		_, err = c.Exec(ctx, "alter table customers validate constraint customers_contact_email_present; alter table customers alter column contact_email set not null")
	} else if err == nil {
		_, err = c.Exec(ctx, "alter table customers alter column contact_email set not null")
	}
	finishHistory(ctx, c, id, start, err)
	if err != nil {
		fatal(err.Error())
	}
	_ = setControl(ctx, c, "constraints", "succeeded")
	fmt.Println("contact email constraint validated and NOT NULL")
}
func verifyGate(ctx context.Context, c *pgx.Conn) {
	var evidence struct {
		Generated   time.Time `json:"generated_at"`
		Verdict     string    `json:"verdict"`
		Correctness struct {
			Customers int64 `json:"customers_verified"`
		} `json:"data_correctness"`
	}
	reportBytes, readErr := os.ReadFile("reports/verification.json")
	if readErr != nil || json.Unmarshal(reportBytes, &evidence) != nil || evidence.Verdict != "PASS" {
		_, _ = c.Exec(ctx, "update migration_control set verification_status='failed',status='failed',updated_at=now() where id=true")
		fatal("verification gate requires a current PASS in reports/verification.json")
	}
	if time.Since(evidence.Generated) > 10*time.Minute || evidence.Generated.After(time.Now().Add(time.Minute)) {
		_, _ = c.Exec(ctx, "update migration_control set verification_status='failed',status='failed',updated_at=now() where id=true")
		fatal("verification report is stale or has an invalid timestamp")
	}
	start := beginHistory(ctx, c, "006_verify")
	var currentCustomers int64
	var lastAudit time.Time
	if err := c.QueryRow(ctx, "select count(*) from customers").Scan(&currentCustomers); err != nil {
		fatal(err.Error())
	}
	if err := c.QueryRow(ctx, "select coalesce(max(created_at),'epoch'::timestamptz) from audit_events").Scan(&lastAudit); err != nil {
		fatal(err.Error())
	}
	if currentCustomers != evidence.Correctness.Customers || lastAudit.After(evidence.Generated) {
		_, _ = c.Exec(ctx, "update migration_control set verification_status='failed',status='failed',updated_at=now() where id=true")
		fatal("database changed after verification; rerun go run ./cmd/verify")
	}
	var total, pending, mismatch, exceptions int64
	var hasMarker, hasLegacy bool
	_ = c.QueryRow(ctx, "select exists(select 1 from information_schema.columns where table_schema=current_schema() and table_name='customers' and column_name='contact_migrated_at'),exists(select 1 from information_schema.columns where table_schema=current_schema() and table_name='customers' and column_name='contact_details')").Scan(&hasMarker, &hasLegacy)
	var err error
	if hasMarker && hasLegacy {
		err = c.QueryRow(ctx, `select count(*),count(*) filter(where contact_migrated_at is null),count(*) filter(where contact_email is distinct from nullif(contact_details->>'email','') or contact_phone is distinct from nullif(contact_details->>'phone','') or contact_timezone is distinct from nullif(contact_details->>'timezone','')) from customers`).Scan(&total, &pending, &mismatch)
	} else {
		err = c.QueryRow(ctx, "select count(*) from customers").Scan(&total)
	}
	if err != nil {
		fatal(err.Error())
	}
	if err = c.QueryRow(ctx, "select count(*) from migration_exceptions where resolved_at is null").Scan(&exceptions); err != nil {
		fatal(err.Error())
	}
	var idx, con bool
	_ = c.QueryRow(ctx, "select coalesce(bool_and(i.indisvalid),false) from pg_index i join pg_class x on x.oid=i.indexrelid where x.relname='idx_customers_tenant_contact_email'").Scan(&idx)
	_ = c.QueryRow(ctx, "select coalesce(bool_and(convalidated),false) from pg_constraint where conname='customers_contact_email_present'").Scan(&con)
	pass := pending == 0 && mismatch == 0 && exceptions == 0 && idx && con
	status := "failed"
	if pass {
		status = "passed"
	}
	_, _ = c.Exec(ctx, "update migration_control set verification_status=$1,verification_result=$2::jsonb,status=$1,updated_at=now() where id=true", status, reportBytes)
	details := fmt.Sprintf(`{"customers":%d,"unmigrated":%d,"mismatches":%d,"exceptions":%d,"index_valid":%t,"constraint_valid":%t}`, total, pending, mismatch, exceptions, idx, con)
	_, _ = c.Exec(ctx, "update migration_history set verification_result=$1::jsonb where migration_id='006_verify'", details)
	event(ctx, c, "verification", status, details)
	finishHistory(ctx, c, "006_verify", start, func() error {
		if pass {
			return nil
		}
		return errors.New("verification assertions failed")
	}())
	if !pass {
		fatal("verification gate failed; inspect verify report")
	}
	fmt.Printf("verified customers=%d index=valid constraint=valid\n", total)
}
func switchReads(ctx context.Context, c *pgx.Conn, mode string) {
	if mode == "new" {
		var state string
		_ = c.QueryRow(ctx, "select verification_status from migration_control where id=true").Scan(&state)
		if state != "passed" {
			fatal("read switch refused: verification must pass first")
		}
		var enabled bool
		_ = c.QueryRow(ctx, "select exists(select 1 from pg_trigger where tgname='customers_contact_bridge' and tgenabled='O')").Scan(&enabled)
		if !enabled {
			fatal("read switch refused: compatibility bridge is not active")
		}
	}
	window := os.Getenv("ROLLBACK_WINDOW")
	if window == "" {
		window = "10m"
	}
	if mode == "new" {
		d, err := time.ParseDuration(window)
		if err != nil || d < 0 {
			fatal("invalid ROLLBACK_WINDOW")
		}
		_, err = c.Exec(ctx, "update migration_control set read_mode=$1,rollback_after=now()+$2::interval,updated_at=now() where id=true", mode, d.String())
		if err != nil {
			fatal(err.Error())
		}
	} else {
		_, err := c.Exec(ctx, "update migration_control set read_mode=$1,rollback_after=NULL,updated_at=now() where id=true", mode)
		if err != nil {
			fatal(err.Error())
		}
	}
	if mode == "new" {
		_ = setControl(ctx, c, "observe", "active")
	}
	event(ctx, c, "read_mode_changed", "succeeded", fmt.Sprintf(`{"read_mode":%q}`, mode))
	fmt.Printf("V2 read mode set to %s; restart/roll instances with matching READ_MODE\n", mode)
}
func contract(ctx context.Context, c *pgx.Conn) {
	if len(os.Args) < 3 || os.Args[2] != "--approve" {
		fatal("contract requires explicit --approve")
	}
	var hasLegacy, hasEmail bool
	_ = c.QueryRow(ctx, "select exists(select 1 from information_schema.columns where table_schema=current_schema() and table_name='customers' and column_name='contact_details'),exists(select 1 from information_schema.columns where table_schema=current_schema() and table_name='customers' and column_name='contact_email')").Scan(&hasLegacy, &hasEmail)
	if !hasLegacy && hasEmail {
		now := time.Now()
		_, _ = c.Exec(ctx, "insert into migration_history(migration_id,started_at,finished_at,duration_ms,status,operator_approval,error_detail) values('005_contract',$1,$1,0,'succeeded',true,'reconciled from contracted catalog state') on conflict(migration_id) do update set finished_at=coalesce(migration_history.finished_at,excluded.finished_at),status='succeeded',operator_approval=true,error_detail='reconciled from contracted catalog state'", now)
		_ = setControl(ctx, c, "contract", "succeeded")
		event(ctx, c, "contract_reconciled", "succeeded", `{"catalog":"legacy_columns_absent"}`)
		fmt.Println("contracted schema detected and migration state reconciled")
		return
	}
	var evidence struct {
		Generated   time.Time `json:"generated_at"`
		Verdict     string    `json:"verdict"`
		Correctness struct {
			Customers int64 `json:"customers_verified"`
		} `json:"data_correctness"`
	}
	reportBytes, reportErr := os.ReadFile("reports/verification.json")
	if reportErr != nil || json.Unmarshal(reportBytes, &evidence) != nil || evidence.Verdict != "PASS" || time.Since(evidence.Generated) > 10*time.Minute {
		fatal("contract refused: a fresh PASS verification report is required")
	}
	var reportCustomers int64
	var lastAudit time.Time
	if err := c.QueryRow(ctx, "select count(*) from customers").Scan(&reportCustomers); err != nil {
		fatal(err.Error())
	}
	if err := c.QueryRow(ctx, "select coalesce(max(created_at),'epoch'::timestamptz) from audit_events").Scan(&lastAudit); err != nil {
		fatal(err.Error())
	}
	if reportCustomers != evidence.Correctness.Customers || lastAudit.After(evidence.Generated) {
		fatal("contract refused: database changed after verification; rerun the verifier")
	}
	var v1, v2old, exceptions, pending int
	err := c.QueryRow(ctx, "select count(*) filter(where app_version='v1'),count(*) filter(where app_version='v2' and read_mode<>'new') from application_instances where deployment_status='active' and last_seen>now()-interval '30 seconds'").Scan(&v1, &v2old)
	if err != nil {
		fatal(err.Error())
	}
	if v1 > 0 {
		fatal("contract refused: active V1 instances remain")
	}
	if v2old > 0 {
		fatal("contract refused: active V2 instances must use new reads")
	}
	if err = c.QueryRow(ctx, "select count(*) from migration_exceptions where resolved_at is null").Scan(&exceptions); err != nil {
		fatal(err.Error())
	}
	if err = c.QueryRow(ctx, "select count(*) from customers where contact_migrated_at is null or contact_email is distinct from nullif(contact_details->>'email','') or contact_phone is distinct from nullif(contact_details->>'phone','') or contact_timezone is distinct from nullif(contact_details->>'timezone','')").Scan(&pending); err != nil {
		fatal(err.Error())
	}
	if exceptions > 0 || pending > 0 {
		fatal("contract refused: unresolved exceptions or consistency mismatches")
	}
	var verification, phase string
	var rollback *time.Time
	var readMode string
	_ = c.QueryRow(ctx, "select verification_status,phase,read_mode,rollback_after from migration_control where id=true").Scan(&verification, &phase, &readMode, &rollback)
	if verification != "passed" {
		fatal("contract refused: verification has not passed")
	}
	if readMode != "new" {
		fatal("contract refused: migration read mode is not new")
	}
	if phase != "constraints" && phase != "observe" {
		fatal("contract refused: required constraints/read phase incomplete")
	}
	if rollback == nil || time.Now().Before(*rollback) {
		fatal("contract refused: configured rollback window has not ended")
	}
	start := beginHistory(ctx, c, "005_contract")
	_, _ = c.Exec(ctx, "update migration_history set operator_approval=true where migration_id='005_contract'")
	_, _ = c.Exec(ctx, "update migration_control set contract_approved_at=now() where id=true")
	tx, err := c.Begin(ctx)
	if err == nil {
		_, err = tx.Exec(ctx, "set local lock_timeout='2s'; set local statement_timeout='2min'; drop trigger if exists customers_contact_bridge on customers; drop function if exists bridge_customer_contact(); alter table customers drop column contact_details, drop column contact_migrated_at")
	}
	if err == nil {
		err = tx.Commit(ctx)
	} else if tx != nil {
		_ = tx.Rollback(ctx)
	}
	finishHistory(ctx, c, "005_contract", start, err)
	if err != nil {
		fatal(err.Error())
	}
	_ = setControl(ctx, c, "contract", "succeeded")
	event(ctx, c, "contract_completed", "irreversible", `{"legacy_data_dropped":true}`)
	fmt.Println("contract completed; V1 is incompatible and legacy JSONB data was dropped")
}
func setControl(ctx context.Context, c *pgx.Conn, phase, status string) error {
	_, err := c.Exec(ctx, "update migration_control set phase=$1,status=$2,updated_at=now() where id=true", phase, status)
	return err
}
func event(ctx context.Context, c *pgx.Conn, name, status, details string) {
	_, _ = c.Exec(ctx, "insert into migration_events(event_type,status,details) values($1,$2,$3::jsonb)", name, status, details)
}
func status(ctx context.Context, c *pgx.Conn) {
	var phase, state, mode, verification string
	var updated time.Time
	err := c.QueryRow(ctx, "select phase,status,read_mode,verification_status,updated_at from migration_control where id=true").Scan(&phase, &state, &mode, &verification, &updated)
	if err != nil {
		fatal(err.Error())
	}
	fmt.Printf("phase=%s status=%s read_mode=%s verification=%s updated_at=%s\n", phase, state, mode, verification, updated.Format(time.RFC3339))
}
func history(ctx context.Context, c *pgx.Conn) {
	rows, err := c.Query(ctx, "select migration_id,status,started_at,finished_at,duration_ms,coalesce(error_code,'') from migration_history order by id")
	if err != nil {
		fatal(err.Error())
	}
	defer rows.Close()
	for rows.Next() {
		var id, status, code string
		var start time.Time
		var end *time.Time
		var duration *int64
		_ = rows.Scan(&id, &status, &start, &end, &duration, &code)
		fmt.Printf("%s %s started=%s ended=%v duration_ms=%v error_code=%s\n", id, status, start.Format(time.RFC3339), end, duration, code)
	}
}
func backfillStatus(ctx context.Context, c *pgx.Conn) {
	var total, migrated, pending, invalid int64
	err := c.QueryRow(ctx, "select count(*),count(*) filter(where contact_migrated_at is not null),count(*) filter(where contact_migrated_at is null),count(*) filter(where exists(select 1 from migration_exceptions e where e.customer_id=customers.id and e.resolved_at is null)) from customers").Scan(&total, &migrated, &pending, &invalid)
	if err != nil {
		fatal(err.Error())
	}
	fmt.Printf("total=%d migrated=%d pending=%d invalid=%d progress=%.2f%%\n", total, migrated, pending, invalid, percent(migrated, total))
}
func percent(a, b int64) float64 {
	if b == 0 {
		return 100
	}
	return float64(a) * 100 / float64(b)
}
