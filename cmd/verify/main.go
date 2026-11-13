package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type contact struct {
	Email             string  `json:"email"`
	Phone             *string `json:"phone"`
	Timezone          *string `json:"timezone"`
	PreferredLanguage string  `json:"preferred_language,omitempty"`
}
type journalEntry struct {
	OperationID             string    `json:"operation_id"`
	OperationType           string    `json:"operation_type"`
	CustomerID              string    `json:"customer_id"`
	TenantID                string    `json:"tenant_id"`
	ExpectedVersion         int64     `json:"expected_version"`
	ReturnedVersion         int64     `json:"returned_version"`
	Contact                 contact   `json:"contact"`
	RequestTimestamp        time.Time `json:"request_timestamp"`
	AcknowledgmentTimestamp time.Time `json:"acknowledgment_timestamp"`
	ApplicationVersion      string    `json:"application_version"`
}
type phase struct {
	ID         string     `json:"migration_id"`
	Status     string     `json:"status"`
	Started    *time.Time `json:"started_at,omitempty"`
	Finished   *time.Time `json:"finished_at,omitempty"`
	DurationMS *int64     `json:"duration_ms,omitempty"`
	ErrorCode  string     `json:"postgres_error_code,omitempty"`
}
type correctness struct {
	CustomersVerified         int64 `json:"customers_verified"`
	ExpectedCustomers         int64 `json:"expected_customers"`
	MissingOrDuplicateRecords int64 `json:"missing_or_duplicate_records"`
	UnmigratedRecords         int64 `json:"unmigrated_records"`
	FieldMismatches           int64 `json:"field_mismatches"`
	LostAcknowledgedWrites    int64 `json:"lost_acknowledged_writes"`
	UnresolvedExceptions      int64 `json:"unresolved_exceptions"`
	TenantIsolationViolations int64 `json:"tenant_isolation_violations"`
	VersionViolations         int64 `json:"version_violations"`
	JournalAvailable          bool  `json:"journal_available"`
	JournalEntries            int64 `json:"journal_entries"`
	AuditEntriesVerified      int64 `json:"audit_entries_verified"`
}
type database struct {
	LockWaits             int64   `json:"lock_waiting_sessions_at_verification"`
	LockTimeoutEvents     int64   `json:"lock_timeout_events"`
	BackfillRowsPerSecond float64 `json:"backfill_rows_per_second"`
	InvalidIndexes        int64   `json:"invalid_indexes"`
	RequiredIndexValid    bool    `json:"required_index_valid"`
	ConstraintValidated   bool    `json:"constraint_validated"`
}
type report struct {
	Generated         time.Time       `json:"generated_at"`
	Verdict           string          `json:"verdict"`
	IncompleteReasons []string        `json:"incomplete_reasons,omitempty"`
	Environment       map[string]any  `json:"environment"`
	Phases            []phase         `json:"migration_phases"`
	Traffic           json.RawMessage `json:"traffic_outcomes,omitempty"`
	Correctness       correctness     `json:"data_correctness"`
	Database          database        `json:"database_behavior"`
}

func main() {
	journalPath := flag.String("journal", "reports/write-journal.jsonl", "independent acknowledged-write journal")
	trafficPath := flag.String("traffic", "reports/traffic-results.json", "traffic results JSON")
	jsonPath := flag.String("json", "reports/verification.json", "machine-readable report")
	mdPath := flag.String("markdown", "reports/verification.md", "readable report")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	p, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		fatal(2, err.Error())
	}
	defer p.Close()
	tx, err := p.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		fatal(2, err.Error())
	}
	defer tx.Rollback(ctx)
	r := report{Generated: time.Now().UTC(), Verdict: "PASS", Environment: map[string]any{"go_version": runtime.Version()}}
	var pgVersion string
	if err = tx.QueryRow(ctx, "select version()").Scan(&pgVersion); err != nil {
		fatal(2, err.Error())
	}
	r.Environment["postgres_version"] = pgVersion
	var hasLegacy, hasMarker bool
	_ = tx.QueryRow(ctx, "select exists(select 1 from information_schema.columns where table_schema=current_schema() and table_name='customers' and column_name='contact_details'),exists(select 1 from information_schema.columns where table_schema=current_schema() and table_name='customers' and column_name='contact_migrated_at')").Scan(&hasLegacy, &hasMarker)
	var seed int64
	var seeded int64
	var tenants int
	var dataset []byte
	err = tx.QueryRow(ctx, "select seed,seeded_customers,tenant_count,config from demo_datasets where id=true").Scan(&seed, &seeded, &tenants, &dataset)
	if err == nil {
		r.Environment["dataset_seed"] = seed
		r.Environment["dataset_size"] = seeded
		r.Environment["tenant_count"] = tenants
		r.Environment["dataset_config"] = json.RawMessage(dataset)
	} else {
		r.IncompleteReasons = append(r.IncompleteReasons, "dataset configuration is unavailable")
	}
	if !hasLegacy && !hasMarker {
		var emailColumn bool
		_ = tx.QueryRow(ctx, "select exists(select 1 from information_schema.columns where table_schema=current_schema() and table_name='customers' and column_name='contact_email')").Scan(&emailColumn)
		if !emailColumn {
			r.IncompleteReasons = append(r.IncompleteReasons, "schema has not been expanded")
		}
	}
	var mismatchSQL string
	if hasLegacy && hasMarker {
		mismatchSQL = `count(*) filter(where contact_email is distinct from nullif(contact_details->>'email','') or contact_phone is distinct from nullif(contact_details->>'phone','') or contact_timezone is distinct from nullif(contact_details->>'timezone',''))`
	}
	var total, pending, mismatches, versionBad int64
	if hasLegacy && hasMarker {
		err = tx.QueryRow(ctx, "select count(*),count(*) filter(where contact_migrated_at is null),"+mismatchSQL+",count(*) filter(where version<1) from customers").Scan(&total, &pending, &mismatches, &versionBad)
	} else {
		err = tx.QueryRow(ctx, "select count(*),0,0,count(*) filter(where version<1) from customers").Scan(&total, &pending, &mismatches, &versionBad)
	}
	if err != nil {
		fatal(2, err.Error())
	}
	r.Correctness.CustomersVerified = total
	r.Correctness.UnmigratedRecords = pending
	r.Correctness.FieldMismatches = mismatches
	r.Correctness.VersionViolations = versionBad
	var expectedBase int64
	if err = tx.QueryRow(ctx, "select coalesce((select seeded_customers from demo_datasets where id=true),0)").Scan(&expectedBase); err != nil {
		fatal(2, err.Error())
	}
	if hasLegacy && hasMarker {
		if err = tx.QueryRow(ctx, "select count(*) from migration_exceptions where resolved_at is null").Scan(&r.Correctness.UnresolvedExceptions); err != nil {
			fatal(2, err.Error())
		}
	} else {
		_ = tx.QueryRow(ctx, "select count(*) from migration_exceptions where resolved_at is null").Scan(&r.Correctness.UnresolvedExceptions)
	}
	if err = tx.QueryRow(ctx, "select count(*) from audit_events a join customers c on c.id=a.customer_id where c.tenant_id<>a.tenant_id").Scan(&r.Correctness.TenantIsolationViolations); err != nil {
		fatal(2, err.Error())
	}
	entries, readErr := readJournal(*journalPath)
	if readErr == nil {
		r.Correctness.JournalAvailable = true
		r.Correctness.JournalEntries = int64(len(entries))
		expected := expectedBase
		for _, e := range entries {
			if e.OperationType == "create" {
				expected++
			}
		}
		r.Correctness.ExpectedCustomers = expected
		if expected != 0 && expected != total {
			r.Correctness.MissingOrDuplicateRecords = abs(expected - total)
		}
		auditCount, latest, missingAudit := checkJournal(ctx, tx, entries)
		r.Correctness.AuditEntriesVerified = auditCount
		r.Correctness.LostAcknowledgedWrites = checkCurrent(ctx, tx, latest) + missingAudit
	} else {
		r.IncompleteReasons = append(r.IncompleteReasons, "independent write journal unavailable: "+readErr.Error())
	}
	err = tx.QueryRow(ctx, "select count(*) filter(where not i.indisvalid),coalesce(bool_or(x.relname='idx_customers_tenant_contact_email' and i.indisvalid),false) from pg_index i join pg_class x on x.oid=i.indexrelid where x.relname like 'idx_customers_%'").Scan(&r.Database.InvalidIndexes, &r.Database.RequiredIndexValid)
	if err != nil {
		fatal(2, err.Error())
	}
	var constraintExists bool
	_ = tx.QueryRow(ctx, "select coalesce(bool_and(convalidated),false) from pg_constraint where conname='customers_contact_email_present'").Scan(&constraintExists)
	r.Database.ConstraintValidated = constraintExists
	_ = tx.QueryRow(ctx, "select count(*) from pg_stat_activity where datname=current_database() and wait_event_type='Lock'").Scan(&r.Database.LockWaits)
	_ = tx.QueryRow(ctx, "select count(*) from migration_history where error_code='55P03'").Scan(&r.Database.LockTimeoutEvents)
	var durationSeconds float64
	var rowsMigrated int64
	_ = tx.QueryRow(ctx, "select coalesce(sum(rows_migrated),0),coalesce(extract(epoch from (max(coalesce(finished_at,now()))-min(started_at))),0) from backfill_runs").Scan(&rowsMigrated, &durationSeconds)
	if durationSeconds > 0 {
		r.Database.BackfillRowsPerSecond = float64(rowsMigrated) / durationSeconds
	}
	rows, err := tx.Query(ctx, "select migration_id,status,started_at,finished_at,duration_ms,coalesce(error_code,'') from migration_history order by id")
	if err == nil {
		for rows.Next() {
			var x phase
			_ = rows.Scan(&x.ID, &x.Status, &x.Started, &x.Finished, &x.DurationMS, &x.ErrorCode)
			r.Phases = append(r.Phases, x)
		}
		rows.Close()
	}
	var backfillConfig []byte
	_ = tx.QueryRow(ctx, "select coalesce(jsonb_agg(jsonb_build_object('batch_size',batch_size,'worker_count',worker_count,'status',status)), '[]'::jsonb) from backfill_runs").Scan(&backfillConfig)
	r.Environment["backfill_configuration"] = json.RawMessage(backfillConfig)
	var fleet []byte
	_ = tx.QueryRow(ctx, "select coalesce(jsonb_agg(jsonb_build_object('instance_id',instance_id,'app_version',app_version,'read_mode',read_mode,'write_mode',write_mode,'deployment_status',deployment_status) order by instance_id),'[]'::jsonb) from application_instances where last_seen>now()-interval '30 seconds'").Scan(&fleet)
	r.Environment["active_application_instances"] = json.RawMessage(fleet)
	traffic, e := os.ReadFile(*trafficPath)
	if e == nil {
		r.Traffic = traffic
		var tr struct {
			Ack           int64          `json:"acknowledged_writes"`
			Started       time.Time      `json:"started_at"`
			Duration      float64        `json:"duration_seconds"`
			Configuration map[string]any `json:"configuration"`
		}
		if json.Unmarshal(traffic, &tr) == nil {
			r.Environment["traffic_configuration"] = tr.Configuration
			var inTraffic int64
			end := tr.Started.Add(time.Duration(tr.Duration * float64(time.Second)))
			for _, entry := range entries {
				if (entry.ApplicationVersion == "v1" || entry.ApplicationVersion == "v2") && (entry.OperationType == "create" || entry.OperationType == "update") && !entry.AcknowledgmentTimestamp.Before(tr.Started) && !entry.AcknowledgmentTimestamp.After(end) {
					inTraffic++
				}
			}
			if r.Correctness.JournalAvailable && tr.Ack != inTraffic {
				r.Correctness.LostAcknowledgedWrites += abs(tr.Ack - inTraffic)
			}
		}
	} else {
		r.IncompleteReasons = append(r.IncompleteReasons, "traffic result file unavailable")
	}
	if err = tx.Commit(ctx); err != nil {
		fatal(2, err.Error())
	}
	dataFailure := r.Correctness.FieldMismatches+r.Correctness.LostAcknowledgedWrites+r.Correctness.UnresolvedExceptions+r.Correctness.TenantIsolationViolations+r.Correctness.VersionViolations+r.Correctness.MissingOrDuplicateRecords > 0 || r.Database.InvalidIndexes > 0
	var trafficEvidence struct {
		HTTP5xx         int64 `json:"http_5xx"`
		Timeouts        int64 `json:"timeouts"`
		NetworkFailures int64 `json:"network_failures"`
		ClientErrors    int64 `json:"unexpected_client_errors"`
	}
	if len(r.Traffic) > 0 {
		if json.Unmarshal(r.Traffic, &trafficEvidence) == nil && (trafficEvidence.HTTP5xx+trafficEvidence.Timeouts+trafficEvidence.NetworkFailures+trafficEvidence.ClientErrors > 0) {
			dataFailure = true
		}
	}
	if dataFailure {
		r.Verdict = "FAIL"
	}
	if r.Verdict == "PASS" && (r.Correctness.UnmigratedRecords > 0 || !r.Database.RequiredIndexValid || !r.Database.ConstraintValidated) {
		r.IncompleteReasons = append(r.IncompleteReasons, "migration verification stages are incomplete")
	}
	if r.Verdict == "PASS" && len(r.IncompleteReasons) > 0 {
		r.Verdict = "INCOMPLETE"
	}
	if e = writeReport(*jsonPath, *mdPath, r); e != nil {
		fatal(2, e.Error())
	}
	b, _ := json.MarshalIndent(r, "", "  ")
	fmt.Println(string(b))
	if r.Verdict == "FAIL" {
		os.Exit(1)
	}
	if r.Verdict == "INCOMPLETE" {
		os.Exit(2)
	}
}
func readJournal(path string) ([]journalEntry, error) {
	f, e := os.Open(path)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	out := []journalEntry{}
	s := bufio.NewScanner(f)
	s.Buffer(make([]byte, 4096), 2<<20)
	for s.Scan() {
		var x journalEntry
		if e = json.Unmarshal(s.Bytes(), &x); e != nil {
			return nil, e
		}
		out = append(out, x)
	}
	return out, s.Err()
}
func checkJournal(ctx context.Context, tx pgx.Tx, entries []journalEntry) (int64, map[string]journalEntry, int64) {
	latest := map[string]journalEntry{}
	var verified int64
	var missing int64
	for _, e := range entries {
		if old, ok := latest[e.CustomerID]; !ok || e.ReturnedVersion > old.ReturnedVersion {
			latest[e.CustomerID] = e
		}
		id, err := uuid.Parse(e.OperationID)
		if err != nil {
			missing++
			continue
		}
		var tenant, customer, version, action, appVersion, checksum string
		var prev int64
		err = tx.QueryRow(ctx, "select tenant_id::text,customer_id::text,record_version::text,action,previous_version,app_version,contact_checksum from audit_events where operation_id=$1", id).Scan(&tenant, &customer, &version, &action, &prev, &appVersion, &checksum)
		if err != nil {
			missing++
			continue
		}
		var contactJSON []byte
		contactJSON, _ = json.Marshal(e.Contact)
		sum := sha256.Sum256(contactJSON)
		if tenant == e.TenantID && customer == e.CustomerID && prev == e.ExpectedVersion && version == fmt.Sprint(e.ReturnedVersion) && action == e.OperationType && appVersion == e.ApplicationVersion && checksum == hex.EncodeToString(sum[:]) {
			verified++
		} else {
			missing++
		}
	}
	return verified, latest, missing
}

func checkCurrent(ctx context.Context, tx pgx.Tx, latest map[string]journalEntry) int64 {
	var lost int64
	for id, e := range latest {
		var tenant, email string
		var phone, tz *string
		var version int64
		err := tx.QueryRow(ctx, "select tenant_id::text,contact_email,contact_phone,contact_timezone,version from customers where id=$1", id).Scan(&tenant, &email, &phone, &tz, &version)
		if err != nil || tenant != e.TenantID || version < e.ReturnedVersion {
			lost++
			continue
		}
		if version == e.ReturnedVersion && (email != e.Contact.Email || !same(phone, e.Contact.Phone) || !same(tz, e.Contact.Timezone)) {
			lost++
		}
		if version > e.ReturnedVersion {
			lost++
		}
	}
	return lost
}
func same(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}
func abs(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}
func writeReport(j, m string, r report) error {
	if e := os.MkdirAll(filepath.Dir(j), 0755); e != nil {
		return e
	}
	b, e := json.MarshalIndent(r, "", "  ")
	if e != nil {
		return e
	}
	if e = os.WriteFile(j, b, 0644); e != nil {
		return e
	}
	if filepath.Base(j) == "verification.json" {
		if e = os.WriteFile(filepath.Join(filepath.Dir(j), "migration-report.json"), b, 0644); e != nil {
			return e
		}
	}
	md := fmt.Sprintf("# Migration verification\n\n- **Verdict:** %s\n- Generated: %s\n- PostgreSQL: %v\n- Go: %v\n- Customers verified: %d\n- Expected customers: %d\n- Unmigrated: %d\n- Field mismatches: %d\n- Lost acknowledged writes: %d\n- Unresolved exceptions: %d\n- Tenant isolation violations: %d\n- Required index valid: %t\n- Constraint validated: %t\n- Backfill throughput: %.1f rows/s\n", r.Verdict, r.Generated.Format(time.RFC3339), r.Environment["postgres_version"], r.Environment["go_version"], r.Correctness.CustomersVerified, r.Correctness.ExpectedCustomers, r.Correctness.UnmigratedRecords, r.Correctness.FieldMismatches, r.Correctness.LostAcknowledgedWrites, r.Correctness.UnresolvedExceptions, r.Correctness.TenantIsolationViolations, r.Database.RequiredIndexValid, r.Database.ConstraintValidated, r.Database.BackfillRowsPerSecond)
	if len(r.IncompleteReasons) > 0 {
		md += "\n## Incomplete evidence\n\n"
		for _, reason := range r.IncompleteReasons {
			md += "- " + reason + "\n"
		}
	}
	if e = os.MkdirAll(filepath.Dir(m), 0755); e != nil {
		return e
	}
	if e = os.WriteFile(m, []byte(md), 0644); e != nil {
		return e
	}
	if filepath.Base(m) == "verification.md" {
		return os.WriteFile(filepath.Join(filepath.Dir(m), "migration-report.md"), []byte(md), 0644)
	}
	return nil
}
func fatal(code int, msg string) { fmt.Fprintln(os.Stderr, msg); os.Exit(code) }
