package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/mail"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	id := flag.String("customer", "", "customer UUID in the local demo")
	email := flag.String("email", "", "replacement email value")
	flag.Parse()
	if *id == "" || *email == "" {
		log.Fatal("usage: remediate --customer UUID --email address")
	}
	if _, e := mail.ParseAddress(*email); e != nil {
		log.Fatal("email is invalid")
	}
	customerID, e := uuid.Parse(*id)
	if e != nil {
		log.Fatal(e)
	}
	ctx := context.Background()
	requestAt := time.Now().UTC()
	p, e := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if e != nil {
		log.Fatal(e)
	}
	defer p.Close()
	tx, e := p.Begin(ctx)
	if e != nil {
		log.Fatal(e)
	}
	defer tx.Rollback(ctx)
	var tenant uuid.UUID
	var version int64
	var c struct {
		Email             string  `json:"email"`
		Phone             *string `json:"phone"`
		Timezone          *string `json:"timezone"`
		PreferredLanguage string  `json:"preferred_language,omitempty"`
	}
	e = tx.QueryRow(ctx, `update customers set contact_details=jsonb_set(
 (case when jsonb_typeof(contact_details)='object' then contact_details-'email'-'phone'-'timezone' else '{}'::jsonb end)
 || (case when jsonb_typeof(contact_details->'phone') in ('string','null') then jsonb_build_object('phone',contact_details->'phone') else '{}'::jsonb end)
 || (case when jsonb_typeof(contact_details->'timezone') in ('string','null') then jsonb_build_object('timezone',contact_details->'timezone') else '{}'::jsonb end),
	'{email}',to_jsonb($1::text),true),version=version+1,updated_at=now() where id=$2 returning tenant_id,version,contact_email,contact_phone,contact_timezone,coalesce(contact_details->>'preferred_language','')`, *email, customerID).Scan(&tenant, &version, &c.Email, &c.Phone, &c.Timezone, &c.PreferredLanguage)
	if e != nil {
		log.Fatal(e)
	}
	encoded, _ := json.Marshal(c)
	sum := sha256.Sum256(encoded)
	operationID := uuid.New()
	_, e = tx.Exec(ctx, "insert into audit_events(operation_id,tenant_id,customer_id,app_version,action,previous_version,record_version,contact_checksum) values($1,$2,$3,'remediation','remediate',$4,$5,$6)", operationID, tenant, customerID, version-1, version, hex.EncodeToString(sum[:]))
	if e != nil {
		log.Fatal(e)
	}
	tag, e := tx.Exec(ctx, "update migration_exceptions set resolved_at=now(),resolution_status='resolved' where customer_id=$1 and resolved_at is null", customerID)
	if e != nil {
		log.Fatal(e)
	}
	if tag.RowsAffected() != 1 {
		log.Fatal("customer does not have an unresolved migration exception")
	}
	if e = tx.Commit(ctx); e != nil {
		log.Fatal(e)
	}
	entry := struct {
		OperationID             string    `json:"operation_id"`
		OperationType           string    `json:"operation_type"`
		CustomerID              string    `json:"customer_id"`
		TenantID                string    `json:"tenant_id"`
		ExpectedVersion         int64     `json:"expected_version"`
		ReturnedVersion         int64     `json:"returned_version"`
		Contact                 any       `json:"contact"`
		RequestTimestamp        time.Time `json:"request_timestamp"`
		AcknowledgmentTimestamp time.Time `json:"acknowledgment_timestamp"`
		ApplicationVersion      string    `json:"application_version"`
	}{operationID.String(), "remediate", customerID.String(), tenant.String(), version - 1, version, c, requestAt, time.Now().UTC(), "remediation"}
	journal, err := os.OpenFile("reports/write-journal.jsonl", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		log.Fatal(err)
	}
	payload, err := json.Marshal(entry)
	if err == nil {
		writer := bufio.NewWriter(journal)
		_, err = writer.Write(append(payload, '\n'))
		if err == nil {
			err = writer.Flush()
		}
	}
	if err == nil {
		err = journal.Sync()
	}
	_ = journal.Close()
	if err != nil {
		log.Fatal("customer remediation committed but independent journal append failed: ", err)
	}
	_, _ = p.Exec(ctx, "insert into migration_events(event_type,status,details) values('exception_remediated','succeeded',jsonb_build_object('customer_id',$1,'tenant_id',$2,'record_version',$3))", customerID, tenant, version)
	fmt.Printf("remediated customer %s at version %d; rerun backfill and verify\n", customerID, version)
}
