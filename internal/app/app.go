package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Config struct{ DSN, Addr, Version, ReadMode, TenantHeader, InstanceID string }

func ConfigFromEnv() (Config, error) {
	c := Config{DSN: os.Getenv("DATABASE_URL"), Addr: env("HTTP_ADDR", ":8080"), Version: env("APP_VERSION", "v1"), ReadMode: env("READ_MODE", "legacy"), TenantHeader: env("TENANT_HEADER", "X-Demo-Tenant"), InstanceID: env("INSTANCE_ID", "local")}
	if c.DSN == "" {
		return c, errors.New("DATABASE_URL is required")
	}
	if c.Version != "v1" && c.Version != "v2" {
		return c, errors.New("APP_VERSION must be v1 or v2")
	}
	if c.ReadMode != "legacy" && c.ReadMode != "shadow" && c.ReadMode != "new" {
		return c, errors.New("READ_MODE must be legacy, shadow, or new")
	}
	return c, nil
}
func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

type Server struct {
	*http.Server
	stop       context.CancelFunc
	pool       *pgxpool.Pool
	instanceID string
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.stop()
	err := s.Server.Shutdown(ctx)
	_, _ = s.pool.Exec(ctx, "update application_instances set deployment_status='drained',last_seen=now() where instance_id=$1", s.instanceID)
	s.pool.Close()
	return err
}

func New(c Config) (*Server, error) {
	p, e := pgxpool.New(context.Background(), c.DSN)
	if e != nil {
		return nil, e
	}
	if e = p.Ping(context.Background()); e != nil {
		p.Close()
		return nil, e
	}
	s := &service{pool: p, cfg: c}
	_ = p.QueryRow(context.Background(), "select exists(select 1 from information_schema.columns where table_schema=current_schema() and table_name='customers' and column_name='contact_details')").Scan(&s.legacyColumn)
	var expanded bool
	_ = p.QueryRow(context.Background(), "select exists(select 1 from information_schema.columns where table_schema=current_schema() and table_name='customers' and column_name='contact_email')").Scan(&expanded)
	if c.Version == "v2" && !expanded {
		p.Close()
		return nil, errors.New("V2 requires the schema expansion migration")
	}
	if c.Version == "v2" && !s.legacyColumn && c.ReadMode != "new" {
		p.Close()
		return nil, errors.New("contracted schema requires V2 READ_MODE=new")
	}
	if c.Version == "v2" && c.ReadMode == "new" && s.legacyColumn {
		var pending int64
		if e = p.QueryRow(context.Background(), "select count(*) from customers where contact_migrated_at is null or contact_email is distinct from nullif(contact_details->>'email','') or contact_phone is distinct from nullif(contact_details->>'phone','') or contact_timezone is distinct from nullif(contact_details->>'timezone','')").Scan(&pending); e != nil || pending != 0 {
			p.Close()
			return nil, fmt.Errorf("new reads require completed consistency verification (pending or query error: %v)", e)
		}
	}
	if c.Version == "v2" && c.ReadMode == "new" {
		var verification string
		var indexValid, constraintValid bool
		if e = p.QueryRow(context.Background(), "select verification_status from migration_control where id=true").Scan(&verification); e != nil || verification != "passed" {
			p.Close()
			return nil, errors.New("new reads require an approved verification report")
		}
		_ = p.QueryRow(context.Background(), "select coalesce(bool_or(i.indisvalid),false) from pg_index i join pg_class x on x.oid=i.indexrelid where x.relname='idx_customers_tenant_contact_email'").Scan(&indexValid)
		_ = p.QueryRow(context.Background(), "select coalesce(bool_and(convalidated),false) from pg_constraint where conname='customers_contact_email_present'").Scan(&constraintValid)
		if !indexValid || !constraintValid {
			p.Close()
			return nil, errors.New("new reads require the tenant/email index and validated email constraint")
		}
	}
	readMode, writeMode := c.ReadMode, "jsonb"
	if c.Version == "v1" {
		readMode = "legacy"
	} else {
		writeMode = "relational"
	}
	if _, e = p.Exec(context.Background(), "insert into application_instances(instance_id,app_version,read_mode,write_mode,deployment_status,last_seen) values($1,$2,$3,$4,'active',now()) on conflict(instance_id) do update set app_version=excluded.app_version,read_mode=excluded.read_mode,write_mode=excluded.write_mode,deployment_status='active',last_seen=now()", c.InstanceID, c.Version, readMode, writeMode); e != nil {
		p.Close()
		return nil, fmt.Errorf("register app instance (apply baseline first): %w", e)
	}
	hbctx, stop := context.WithCancel(context.Background())
	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-hbctx.Done():
				return
			case <-t.C:
				_, _ = p.Exec(hbctx, "update application_instances set last_seen=now(),read_mode=$2,write_mode=$3,deployment_status='active' where instance_id=$1", c.InstanceID, readMode, writeMode)
			}
		}
	}()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.health)
	mux.HandleFunc("GET /ready", s.ready)
	mux.HandleFunc("GET /metrics", s.metrics)
	mux.HandleFunc("GET /api/customers", s.list)
	mux.HandleFunc("GET /api/customers/search", s.search)
	mux.HandleFunc("GET /api/customers/{id}", s.get)
	mux.HandleFunc("POST /api/customers", s.create)
	mux.HandleFunc("PATCH /api/customers/{id}", s.update)
	return &Server{Server: &http.Server{Addr: c.Addr, Handler: requestLog(mux, p, c.Version), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second}, stop: stop, pool: p, instanceID: c.InstanceID}, nil
}

type service struct {
	pool         *pgxpool.Pool
	cfg          Config
	legacyColumn bool
}
type contact struct {
	Email             string  `json:"email"`
	Phone             *string `json:"phone"`
	Timezone          *string `json:"timezone"`
	PreferredLanguage string  `json:"preferred_language,omitempty"`
}
type customer struct {
	ID       string    `json:"id"`
	TenantID string    `json:"tenant_id"`
	Company  string    `json:"company_name"`
	Contact  contact   `json:"contact"`
	Version  int64     `json:"version"`
	Created  time.Time `json:"created_at"`
	Updated  time.Time `json:"updated_at"`
}

func (s *service) tenant(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, e := uuid.Parse(r.Header.Get(s.cfg.TenantHeader))
	if e != nil {
		http.Error(w, "valid demonstration tenant header required", 400)
		return uuid.Nil, false
	}
	return id, true
}
func (s *service) health(w http.ResponseWriter, _ *http.Request) {
	write(w, 200, map[string]string{"status": "ok", "version": s.cfg.Version})
}
func (s *service) ready(w http.ResponseWriter, r *http.Request) {
	if e := s.pool.Ping(r.Context()); e != nil {
		http.Error(w, "database unavailable", 503)
		return
	}
	write(w, 200, map[string]string{"status": "ready"})
}
func (s *service) metrics(w http.ResponseWriter, _ *http.Request) {
	var n int64
	_ = s.pool.QueryRow(context.Background(), "select count(*) from audit_events").Scan(&n)
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "migration_demo_audit_events_total %d\n", n)
}
func (s *service) list(w http.ResponseWriter, r *http.Request) {
	t, ok := s.tenant(w, r)
	if !ok {
		return
	}
	limit := 100
	if n := r.URL.Query().Get("limit"); n != "" {
		parsed, e := strconv.Atoi(n)
		if e != nil {
			http.Error(w, "limit must be an integer", 400)
			return
		}
		limit = parsed
	}
	if limit < 1 || limit > 500 {
		http.Error(w, "limit must be 1..500", 400)
		return
	}
	offset, e := pageOffset(r)
	if e != nil {
		http.Error(w, e.Error(), 400)
		return
	}
	rows, e := s.pool.Query(r.Context(), "select id,tenant_id,company_name,"+s.contactJSON()+",version,created_at,updated_at from customers where tenant_id=$1 order by created_at desc,id limit $2 offset $3", t, limit, offset)
	if e != nil {
		http.Error(w, "database error", 500)
		return
	}
	defer rows.Close()
	out := []customer{}
	for rows.Next() {
		c, e := decodeRow(rows)
		if e != nil {
			http.Error(w, "database error", 500)
			return
		}
		out = append(out, c)
	}
	write(w, 200, out)
}
func (s *service) search(w http.ResponseWriter, r *http.Request) {
	t, ok := s.tenant(w, r)
	if !ok {
		return
	}
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	if q == "" {
		http.Error(w, "q is required", 400)
		return
	}
	limit := 100
	if n := r.URL.Query().Get("limit"); n != "" {
		var parseErr error
		limit, parseErr = strconv.Atoi(n)
		if parseErr != nil || limit < 1 || limit > 500 {
			http.Error(w, "limit must be an integer in 1..500", 400)
			return
		}
	}
	offset, e := pageOffset(r)
	if e != nil {
		http.Error(w, e.Error(), 400)
		return
	}
	rows, e := s.pool.Query(r.Context(), "select id,tenant_id,company_name,"+s.contactJSON()+",version,created_at,updated_at from customers where tenant_id=$1 and (company_name ilike '%'||$2||'%' or "+s.emailExpression()+" ilike '%'||$2||'%') order by company_name,id limit $3 offset $4", t, q, limit, offset)
	if e != nil {
		http.Error(w, "database error", 500)
		return
	}
	defer rows.Close()
	out := []customer{}
	for rows.Next() {
		c, e := decodeRow(rows)
		if e != nil {
			http.Error(w, "database error", 500)
			return
		}
		out = append(out, c)
	}
	write(w, 200, out)
}

func pageOffset(r *http.Request) (int, error) {
	n := r.URL.Query().Get("offset")
	if n == "" {
		return 0, nil
	}
	var offset int
	var err error
	if offset, err = strconv.Atoi(n); err != nil || offset < 0 || offset > 1_000_000 {
		return 0, errors.New("offset must be an integer in 0..1000000")
	}
	return offset, nil
}

func (s *service) contactJSON() string {
	if s.cfg.Version == "v2" && s.cfg.ReadMode == "new" {
		if s.legacyColumn {
			return "jsonb_set(jsonb_set(jsonb_set(contact_details,'{email}',to_jsonb(contact_email),true),'{phone}',coalesce(to_jsonb(contact_phone),'null'::jsonb),true),'{timezone}',coalesce(to_jsonb(contact_timezone),'null'::jsonb),true)"
		}
		return "jsonb_build_object('email',contact_email,'phone',contact_phone,'timezone',contact_timezone)"
	}
	return "contact_details"
}
func (s *service) emailExpression() string {
	if s.cfg.Version == "v2" && (!s.legacyColumn || s.cfg.ReadMode == "new") {
		return "contact_email"
	}
	return "contact_details->>'email'"
}

type scanner interface{ Scan(...any) error }

func decodeRow(row scanner) (customer, error) {
	var c customer
	var b []byte
	e := row.Scan(&c.ID, &c.TenantID, &c.Company, &b, &c.Version, &c.Created, &c.Updated)
	if e != nil {
		return c, e
	}
	var x map[string]any
	if e = json.Unmarshal(b, &x); e != nil {
		return c, e
	}
	c.Contact.Email, _ = x["email"].(string)
	c.Contact.Phone = stringPtr(x["phone"])
	c.Contact.Timezone = stringPtr(x["timezone"])
	c.Contact.PreferredLanguage, _ = x["preferred_language"].(string)
	return c, nil
}
func stringPtr(v any) *string {
	if x, ok := v.(string); ok {
		return &x
	}
	return nil
}
func (s *service) get(w http.ResponseWriter, r *http.Request) {
	t, ok := s.tenant(w, r)
	if !ok {
		return
	}
	id, e := uuid.Parse(r.PathValue("id"))
	if e != nil {
		http.Error(w, "invalid customer id", 400)
		return
	}
	var c customer
	var legacy []byte
	e = s.pool.QueryRow(r.Context(), "select id,tenant_id,company_name,"+s.contactJSON()+",version,created_at,updated_at from customers where id=$1 and tenant_id=$2", id, t).Scan(&c.ID, &c.TenantID, &c.Company, &legacy, &c.Version, &c.Created, &c.Updated)
	if errors.Is(e, pgx.ErrNoRows) {
		http.NotFound(w, r)
		return
	}
	if e != nil {
		http.Error(w, "database error", 500)
		return
	}
	var data map[string]any
	_ = json.Unmarshal(legacy, &data)
	if s.cfg.Version == "v2" && s.cfg.ReadMode != "legacy" {
		var email string
		var phone, tz *string
		e = s.pool.QueryRow(r.Context(), "select contact_email,contact_phone,contact_timezone from customers where id=$1 and tenant_id=$2", id, t).Scan(&email, &phone, &tz)
		if e != nil {
			http.Error(w, "new contact columns unavailable", 503)
			return
		}
		if s.cfg.ReadMode == "shadow" {
			if fmt.Sprint(email) != fmt.Sprint(data["email"]) || fmt.Sprint(phone) != fmt.Sprint(data["phone"]) || fmt.Sprint(tz) != fmt.Sprint(data["timezone"]) {
				slog.Warn("shadow contact mismatch", "customer_id", id)
			}
		} else {
			data["email"] = email
			data["phone"] = phone
			data["timezone"] = tz
		}
	}
	c.Contact.Email, _ = data["email"].(string)
	c.Contact.Phone = stringPtr(data["phone"])
	c.Contact.Timezone = stringPtr(data["timezone"])
	c.Contact.PreferredLanguage, _ = data["preferred_language"].(string)
	write(w, 200, c)
}
func (s *service) create(w http.ResponseWriter, r *http.Request) {
	t, ok := s.tenant(w, r)
	if !ok {
		return
	}
	var in struct {
		Company string  `json:"company_name"`
		Contact contact `json:"contact"`
	}
	if json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&in) != nil || strings.TrimSpace(in.Company) == "" || strings.TrimSpace(in.Contact.Email) == "" {
		http.Error(w, "company_name and contact.email are required", 400)
		return
	}
	var id string
	operationID := uuid.New()
	tx, e := s.pool.Begin(r.Context())
	if e != nil {
		http.Error(w, "database unavailable", 503)
		return
	}
	if s.cfg.Version == "v2" {
		if s.legacyColumn {
			doc, _ := json.Marshal(in.Contact)
			e = tx.QueryRow(r.Context(), "insert into customers(tenant_id,company_name,contact_details,contact_email,contact_phone,contact_timezone) values($1,$2,$3,$4,$5,$6) returning id", t, in.Company, doc, in.Contact.Email, in.Contact.Phone, in.Contact.Timezone).Scan(&id)
		} else {
			e = tx.QueryRow(r.Context(), "insert into customers(tenant_id,company_name,contact_email,contact_phone,contact_timezone) values($1,$2,$3,$4,$5) returning id", t, in.Company, in.Contact.Email, in.Contact.Phone, in.Contact.Timezone).Scan(&id)
		}
	} else {
		doc, _ := json.Marshal(in.Contact)
		e = tx.QueryRow(r.Context(), "insert into customers(tenant_id,company_name,contact_details) values($1,$2,$3) returning id", t, in.Company, doc).Scan(&id)
	}
	if e != nil {
		_ = tx.Rollback(r.Context())
		http.Error(w, "database write failed", 500)
		return
	}
	if e = s.audit(r.Context(), tx, operationID, t, id, "create", 0, 1, in.Contact); e != nil {
		_ = tx.Rollback(r.Context())
		http.Error(w, "audit write failed", 500)
		return
	}
	if e = tx.Commit(r.Context()); e != nil {
		http.Error(w, "database commit failed", 500)
		return
	}
	w.Header().Set("Location", "/api/customers/"+id)
	write(w, 201, map[string]any{"id": id, "version": 1, "operation_id": operationID})
}
func (s *service) update(w http.ResponseWriter, r *http.Request) {
	t, ok := s.tenant(w, r)
	if !ok {
		return
	}
	id, e := uuid.Parse(r.PathValue("id"))
	if e != nil {
		http.Error(w, "invalid customer id", 400)
		return
	}
	var in struct {
		Expected int64   `json:"expected_version"`
		Contact  contact `json:"contact"`
	}
	if json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&in) != nil || in.Expected < 1 || strings.TrimSpace(in.Contact.Email) == "" {
		http.Error(w, "expected_version and contact.email are required", 400)
		return
	}
	operationID := uuid.New()
	tx, e := s.pool.Begin(r.Context())
	if e != nil {
		http.Error(w, "database unavailable", 503)
		return
	}
	doc, _ := json.Marshal(in.Contact)
	var version int64
	if s.cfg.Version == "v2" {
		e = tx.QueryRow(r.Context(), "update customers set contact_email=$1,contact_phone=$2,contact_timezone=$3,version=version+1,updated_at=now() where id=$4 and tenant_id=$5 and version=$6 returning version", in.Contact.Email, in.Contact.Phone, in.Contact.Timezone, id, t, in.Expected).Scan(&version)
	} else {
		e = tx.QueryRow(r.Context(), "update customers set contact_details=jsonb_set(jsonb_set(jsonb_set(contact_details,'{email}',$1->'email',true),'{phone}',coalesce($1->'phone','null'::jsonb),true),'{timezone}',coalesce($1->'timezone','null'::jsonb),true),version=version+1,updated_at=now() where id=$2 and tenant_id=$3 and version=$4 returning version", doc, id, t, in.Expected).Scan(&version)
	}
	if errors.Is(e, pgx.ErrNoRows) {
		_ = tx.Rollback(r.Context())
		http.Error(w, "customer missing or version conflict", 409)
		return
	}
	if e != nil {
		_ = tx.Rollback(r.Context())
		http.Error(w, "database write failed", 500)
		return
	}
	if e = s.audit(r.Context(), tx, operationID, t, id.String(), "update", in.Expected, version, in.Contact); e != nil {
		_ = tx.Rollback(r.Context())
		http.Error(w, "audit write failed", 500)
		return
	}
	if e = tx.Commit(r.Context()); e != nil {
		http.Error(w, "database commit failed", 500)
		return
	}
	write(w, 200, map[string]any{"version": version, "operation_id": operationID})
}
func (s *service) audit(ctx context.Context, tx pgx.Tx, op, tenant uuid.UUID, id, action string, previous, next int64, c contact) error {
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(b)
	_, err = tx.Exec(ctx, "insert into audit_events(operation_id,tenant_id,customer_id,app_version,action,previous_version,record_version,contact_checksum) values($1,$2,$3,$4,$5,$6,$7,$8)", op, tenant, id, s.cfg.Version, action, previous, next, hex.EncodeToString(sum[:]))
	return err
}
func write(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}
func requestLog(next http.Handler, p *pgxpool.Pool, version string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		requestID := r.Header.Get("X-Request-ID")
		if requestID == "" {
			requestID = uuid.NewString()
		}
		w.Header().Set("X-Request-ID", requestID)
		sw := &statusWriter{ResponseWriter: w}
		next.ServeHTTP(sw, r)
		if sw.status == 0 {
			sw.status = http.StatusOK
		}
		duration := time.Since(start)
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		defer cancel()
		_, _ = p.Exec(ctx, "insert into http_request_metrics(app_version,method,route,status_code,duration_ms) values($1,$2,$3,$4,$5)", version, r.Method, r.URL.Path, sw.status, float64(duration.Microseconds())/1000)
		slog.Info("request", "request_id", requestID, "version", version, "method", r.Method, "path", r.URL.Path, "status", sw.status, "duration_ms", duration.Milliseconds())
	})
}
