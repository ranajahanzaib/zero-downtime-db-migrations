package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type snapshot struct {
	Generated      time.Time      `json:"generated_at"`
	Customers      int64          `json:"customers"`
	Migrated       int64          `json:"migrated"`
	Pending        int64          `json:"pending"`
	WaitingQueries int64          `json:"waiting_queries"`
	Migrations     []migration    `json:"migrations"`
	Instances      []instance     `json:"instances"`
	Control        control        `json:"migration_control"`
	Traffic        []trafficPoint `json:"traffic"`
	Backfill       backfill       `json:"backfill"`
	Consistency    consistency    `json:"consistency"`
	Events         []event        `json:"events"`
	Locks          []lockWait     `json:"locks"`
}
type migration struct {
	ID         string     `json:"id"`
	Status     string     `json:"status"`
	Started    time.Time  `json:"started_at"`
	Finished   *time.Time `json:"finished_at"`
	DurationMS *int64     `json:"duration_ms"`
}
type instance struct {
	ID        string    `json:"instance_id"`
	Version   string    `json:"app_version"`
	ReadMode  string    `json:"read_mode"`
	WriteMode string    `json:"write_mode"`
	Status    string    `json:"deployment_status"`
	Seen      time.Time `json:"last_seen"`
}
type control struct {
	Phase         string     `json:"phase"`
	Status        string     `json:"status"`
	ReadMode      string     `json:"read_mode"`
	Verification  string     `json:"verification_status"`
	RollbackAfter *time.Time `json:"rollback_after"`
}
type trafficPoint struct {
	Minute   time.Time `json:"minute"`
	Version  string    `json:"app_version"`
	Requests int64     `json:"requests"`
	Errors   int64     `json:"errors"`
	Writes   int64     `json:"successful_writes"`
	P95      float64   `json:"p95_latency_ms"`
	P99      float64   `json:"p99_latency_ms"`
}
type event struct {
	Type    string          `json:"event_type"`
	Status  string          `json:"status"`
	At      time.Time       `json:"occurred_at"`
	Details json.RawMessage `json:"details"`
}
type lockWait struct {
	PID             int32   `json:"pid"`
	Mode            string  `json:"lock_mode"`
	DurationSeconds float64 `json:"duration_seconds"`
	Query           string  `json:"query"`
}
type backfill struct {
	Total         int64   `json:"total"`
	Migrated      int64   `json:"migrated"`
	Pending       int64   `json:"pending"`
	Failed        int64   `json:"failed"`
	Workers       int64   `json:"active_workers"`
	RowsPerSecond float64 `json:"rows_per_second"`
	Percent       float64 `json:"percent"`
}
type consistency struct {
	Mismatches       int64  `json:"field_mismatches"`
	Exceptions       int64  `json:"unresolved_exceptions"`
	TenantViolations int64  `json:"tenant_isolation_violations"`
	Verification     string `json:"verification_status"`
}

const page = `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>Migration dashboard</title>
<style>body{font:16px system-ui;max-width:1100px;margin:2rem auto;padding:0 1rem;color:#17212b}h1{font-size:2rem}section{border:1px solid #ccd4dc;border-radius:12px;padding:1rem;margin:1rem 0}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(145px,1fr));gap:1rem}.num{font-size:1.7rem;font-weight:700}pre{white-space:pre-wrap;max-height:24rem;overflow:auto}svg{width:100%;height:230px}.legend{color:#4c5964}</style></head>
<body><h1>Database migration status</h1><p>Live measurements from this synthetic demonstration.</p>
<section><h2>Migration lifecycle</h2><div class="grid" id="stats"></div><p id="state"></p></section>
<section><h2>Live traffic by application version</h2><svg id="chart" viewBox="0 0 900 210" role="img" aria-label="Requests per minute chart"></svg><p class="legend">V1 blue · V2 orange</p><pre id="traffic"></pre></section>
<section><h2>Backfill progress</h2><pre id="backfill"></pre></section><section><h2>Data consistency</h2><pre id="consistency"></pre></section>
<section><h2>Database locking</h2><p>Waiting locks are observations and do not necessarily block application requests.</p><pre id="locks"></pre></section>
<section><h2>Application fleet</h2><pre id="instances"></pre></section><section><h2>Migration event timeline</h2><pre id="events"></pre></section><section><h2>Migration history</h2><pre id="migrations"></pre></section>
<script>function draw(points){const s=document.querySelector('#chart');s.innerHTML='';const colors={v1:'#1769aa',v2:'#bc4b16'};['v1','v2'].forEach(v=>{const a=points.filter(x=>x.app_version===v);if(!a.length)return;const max=Math.max(1,...a.map(x=>x.requests));const p=document.createElementNS('http://www.w3.org/2000/svg','polyline');p.setAttribute('fill','none');p.setAttribute('stroke',colors[v]);p.setAttribute('stroke-width','3');p.setAttribute('points',a.map((x,i)=>(20+i*850/Math.max(1,a.length-1))+','+(195-x.requests/max*175)).join(' '));s.appendChild(p)})}
async function refresh(){try{const d=await(await fetch('/api/status')).json();document.querySelector('#stats').innerHTML=[['Customers',d.customers],['Migrated',d.migrated],['Pending',d.pending],['Phase',d.migration_control.phase],['Read mode',d.migration_control.read_mode],['Lock waiters',d.waiting_queries]].map(x=>'<div><div>'+x[0]+'</div><div class="num">'+x[1]+'</div></div>').join('');document.querySelector('#state').textContent='Status: '+d.migration_control.status+' · Verification: '+d.migration_control.verification_status;draw(d.traffic);for(const k of ['traffic','backfill','consistency','locks','instances','events','migrations'])document.querySelector('#'+k).textContent=JSON.stringify(d[k],null,2)}catch(e){document.querySelector('#stats').textContent='Database status unavailable'}}refresh();setInterval(refresh,2000)</script></body></html>`

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("DATABASE_URL required")
	}
	p, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer p.Close()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(page))
	})
	mux.HandleFunc("GET /api/status", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		d := snapshot{Generated: time.Now().UTC()}
		var hasMarker, hasLegacy bool
		_ = p.QueryRow(ctx, "select exists(select 1 from information_schema.columns where table_schema=current_schema() and table_name='customers' and column_name='contact_migrated_at'),exists(select 1 from information_schema.columns where table_schema=current_schema() and table_name='customers' and column_name='contact_details')").Scan(&hasMarker, &hasLegacy)
		var err error
		if hasMarker {
			err = p.QueryRow(ctx, "select count(*),count(*) filter(where contact_migrated_at is not null),count(*) filter(where contact_migrated_at is null) from customers").Scan(&d.Customers, &d.Migrated, &d.Pending)
		} else {
			err = p.QueryRow(ctx, "select count(*) from customers").Scan(&d.Customers)
			d.Pending = d.Customers
			var newCols bool
			_ = p.QueryRow(ctx, "select exists(select 1 from information_schema.columns where table_schema=current_schema() and table_name='customers' and column_name='contact_email')").Scan(&newCols)
			if newCols {
				d.Migrated = d.Customers
				d.Pending = 0
			}
		}
		if err != nil {
			http.Error(w, "customer schema unavailable", 503)
			return
		}
		var controlErr error
		controlErr = p.QueryRow(ctx, "select phase,status,read_mode,verification_status,rollback_after from migration_control where id=true").Scan(&d.Control.Phase, &d.Control.Status, &d.Control.ReadMode, &d.Control.Verification, &d.Control.RollbackAfter)
		if controlErr != nil {
			http.Error(w, "migration control unavailable", 503)
			return
		}
		_ = p.QueryRow(ctx, "select count(*) from pg_stat_activity where datname=current_database() and wait_event_type='Lock'").Scan(&d.WaitingQueries)
		if hasLegacy {
			_ = p.QueryRow(ctx, `select count(*) from customers where contact_email is distinct from nullif(contact_details->>'email','') or contact_phone is distinct from nullif(contact_details->>'phone','') or contact_timezone is distinct from nullif(contact_details->>'timezone','')`).Scan(&d.Consistency.Mismatches)
		}
		_ = p.QueryRow(ctx, "select count(*) from migration_exceptions where resolved_at is null").Scan(&d.Consistency.Exceptions)
		_ = p.QueryRow(ctx, "select count(*) from audit_events a join customers c on c.id=a.customer_id where c.tenant_id<>a.tenant_id").Scan(&d.Consistency.TenantViolations)
		d.Consistency.Verification = d.Control.Verification
		if hasMarker {
			_ = p.QueryRow(ctx, "select count(*),count(*) filter(where contact_migrated_at is not null),count(*) filter(where contact_migrated_at is null),count(*) filter(where exists(select 1 from migration_exceptions e where e.customer_id=customers.id and e.resolved_at is null)) from customers").Scan(&d.Backfill.Total, &d.Backfill.Migrated, &d.Backfill.Pending, &d.Backfill.Failed)
			_ = p.QueryRow(ctx, "select count(*) from backfill_runs where status='running'").Scan(&d.Backfill.Workers)
		} else {
			d.Backfill.Total = d.Customers
			d.Backfill.Pending = d.Customers
			if !hasLegacy {
				d.Backfill.Migrated = d.Customers
				d.Backfill.Pending = 0
			}
		}
		var migrated, seconds int64
		_ = p.QueryRow(ctx, "select coalesce(sum(rows_migrated),0),coalesce(sum(extract(epoch from (coalesce(finished_at,now())-started_at))::bigint),0) from backfill_runs").Scan(&migrated, &seconds)
		if seconds > 0 {
			d.Backfill.RowsPerSecond = float64(migrated) / float64(seconds)
		}
		if d.Backfill.Total > 0 {
			d.Backfill.Percent = 100 * float64(d.Backfill.Migrated) / float64(d.Backfill.Total)
		}
		rows, e := p.Query(ctx, `select date_trunc('minute',occurred_at),app_version,count(*),count(*) filter(where status_code>=500),count(*) filter(where method in ('POST','PATCH') and status_code between 200 and 299),percentile_cont(.95) within group(order by duration_ms),percentile_cont(.99) within group(order by duration_ms) from http_request_metrics where occurred_at>now()-interval '15 minutes' group by 1,2 order by 1,2`)
		if e == nil {
			for rows.Next() {
				var x trafficPoint
				_ = rows.Scan(&x.Minute, &x.Version, &x.Requests, &x.Errors, &x.Writes, &x.P95, &x.P99)
				d.Traffic = append(d.Traffic, x)
			}
			rows.Close()
		}
		rows, e = p.Query(ctx, "select pid,mode,extract(epoch from now()-query_start),left(query,300) from pg_locks l join pg_stat_activity a using(pid) where not granted order by query_start limit 30")
		if e == nil {
			for rows.Next() {
				var x lockWait
				_ = rows.Scan(&x.PID, &x.Mode, &x.DurationSeconds, &x.Query)
				d.Locks = append(d.Locks, x)
			}
			rows.Close()
		}
		rows, e = p.Query(ctx, "select migration_id,status,started_at,finished_at,duration_ms from migration_history order by id desc limit 30")
		if e == nil {
			for rows.Next() {
				var x migration
				_ = rows.Scan(&x.ID, &x.Status, &x.Started, &x.Finished, &x.DurationMS)
				d.Migrations = append(d.Migrations, x)
			}
			rows.Close()
		}
		rows, e = p.Query(ctx, "select instance_id,app_version,read_mode,write_mode,deployment_status,last_seen from application_instances order by last_seen desc")
		if e == nil {
			for rows.Next() {
				var x instance
				_ = rows.Scan(&x.ID, &x.Version, &x.ReadMode, &x.WriteMode, &x.Status, &x.Seen)
				d.Instances = append(d.Instances, x)
			}
			rows.Close()
		}
		rows, e = p.Query(ctx, "select event_type,status,occurred_at,details from migration_events order by id desc limit 50")
		if e == nil {
			for rows.Next() {
				var x event
				_ = rows.Scan(&x.Type, &x.Status, &x.At, &x.Details)
				d.Events = append(d.Events, x)
			}
			rows.Close()
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(d)
	})
	addr := os.Getenv("HTTP_ADDR")
	if addr == "" {
		addr = ":8090"
	}
	log.Fatal(http.ListenAndServe(addr, mux))
}
