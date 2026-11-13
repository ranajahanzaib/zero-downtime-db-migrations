package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
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
type result struct {
	Configuration       map[string]any `json:"configuration"`
	Started             time.Time      `json:"started_at"`
	DurationSeconds     float64        `json:"duration_seconds"`
	Requests            int64          `json:"requests_sent"`
	SuccessfulResponses int64          `json:"successful_responses"`
	ExpectedConflicts   int64          `json:"expected_conflicts"`
	ClientErrors        int64          `json:"unexpected_client_errors"`
	ServerErrors        int64          `json:"http_5xx"`
	NetworkFailures     int64          `json:"network_failures"`
	Timeouts            int64          `json:"timeouts"`
	JournalErrors       int64          `json:"journal_errors"`
	Retries             int64          `json:"retry_attempts"`
	AcknowledgedWrites  int64          `json:"acknowledged_writes"`
	P50MS               float64        `json:"p50_latency_ms"`
	P95MS               float64        `json:"p95_latency_ms"`
	P99MS               float64        `json:"p99_latency_ms"`
	Throughput          float64        `json:"requests_per_second"`
}
type stats struct {
	requests, success, conflicts, clientErr, serverErr, network, timeouts, retries, writes, journalErrors atomic.Int64
	mu                                                                                                    sync.Mutex
	latency                                                                                               []time.Duration
	journalMu                                                                                             sync.Mutex
	journal                                                                                               *os.File
}

func main() {
	base := flag.String("url", "http://localhost:8081", "default application URL")
	v1 := flag.String("v1-url", "", "V1 target URL (mixed target) ")
	v2 := flag.String("v2-url", "", "V2 target URL (mixed target)")
	tenantArg := flag.String("tenants", "11111111-1111-4111-8111-111111111111,22222222-2222-4222-8222-222222222222,33333333-3333-4333-8333-333333333333", "comma separated demo tenant UUIDs")
	requests := flag.Int("requests", 1000, "total customer cycles; 0 runs for duration")
	duration := flag.Duration("duration", 10*time.Second, "traffic duration")
	rate := flag.Int("rate", 0, "cycles per second; zero means maximum throughput")
	workers := flag.Int("concurrency", 8, "concurrent workers")
	timeout := flag.Duration("timeout", 3*time.Second, "per-request timeout")
	retries := flag.Int("retries", 1, "bounded retries for network/5xx errors")
	seed := flag.Int64("seed", 1, "deterministic target and tenant seed")
	journalPath := flag.String("journal", "reports/write-journal.jsonl", "durable acknowledged-write journal")
	out := flag.String("out", "reports/traffic-results.json", "result JSON path")
	sharedID := flag.String("shared-customer", "", "optional customer UUID for overlapping updates")
	sharedTenant := flag.String("shared-tenant", "11111111-1111-4111-8111-111111111111", "tenant UUID owning shared customer")
	flag.Parse()
	if *workers < 1 || *requests < 0 || *rate < 0 || *retries < 0 {
		fatal("workers must be positive; requests, rate, and retries cannot be negative")
	}
	tenants := strings.Split(*tenantArg, ",")
	if len(tenants) == 0 {
		fatal("at least one tenant required")
	}
	targets := []struct{ url, version string }{}
	if *v1 != "" || *v2 != "" {
		if *v1 != "" {
			for _, url := range strings.Split(*v1, ",") {
				if strings.TrimSpace(url) != "" {
					targets = append(targets, struct{ url, version string }{strings.TrimSpace(url), "v1"})
				}
			}
		}
		if *v2 != "" {
			for _, url := range strings.Split(*v2, ",") {
				if strings.TrimSpace(url) != "" {
					targets = append(targets, struct{ url, version string }{strings.TrimSpace(url), "v2"})
				}
			}
		}
	} else {
		targets = append(targets, struct{ url, version string }{*base, "unknown"})
	}
	if len(targets) == 0 {
		fatal("at least one application target URL is required")
	}
	if err := os.MkdirAll(filepath.Dir(*journalPath), 0755); err != nil {
		fatal(err.Error())
	}
	jf, err := os.OpenFile(*journalPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		fatal(err.Error())
	}
	defer jf.Close()
	st := &stats{latency: make([]time.Duration, 0, max(*requests, 1000)), journal: jf}
	client := &http.Client{Timeout: *timeout}
	start := time.Now().UTC()
	deadline := time.Now().Add(*duration)
	jobs := make(chan int, (*workers)*4)
	var wg sync.WaitGroup
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(*seed + int64(worker)))
			for seq := range jobs {
				target := targets[rng.Intn(len(targets))]
				tenant := tenants[rng.Intn(len(tenants))]
				if *sharedID != "" {
					tenant = *sharedTenant
				}
				runCycle(ctx, client, st, target.url, target.version, tenant, seq, worker, *sharedID, *retries)
			}
		}(w)
	}
	issued := 0
	if *rate > 0 {
		ticker := time.NewTicker(time.Second / time.Duration(*rate))
		defer ticker.Stop()
		for (*requests == 0 || issued < *requests) && time.Now().Before(deadline) {
			select {
			case <-ctx.Done():
				goto finish
			case <-ticker.C:
				jobs <- issued
				issued++
			}
		}
	} else {
		for (*requests == 0 || issued < *requests) && time.Now().Before(deadline) {
			jobs <- issued
			issued++
		}
	}
finish:
	close(jobs)
	wg.Wait()
	elapsed := time.Since(start)
	st.journalMu.Lock()
	_ = st.journal.Sync()
	st.journalMu.Unlock()
	sort.Slice(st.latency, func(i, j int) bool { return st.latency[i] < st.latency[j] })
	quantile := func(p float64) float64 {
		if len(st.latency) == 0 {
			return 0
		}
		i := int(float64(len(st.latency)-1) * p)
		return float64(st.latency[i].Microseconds()) / 1000
	}
	targetVersions := []string{}
	for _, t := range targets {
		targetVersions = append(targetVersions, t.version)
	}
	r := result{Configuration: map[string]any{"targets": targetVersions, "rate_cycles_per_second": *rate, "duration_limit_seconds": duration.Seconds(), "concurrency": *workers, "tenant_count": len(tenants), "random_seed": *seed, "request_timeout_ms": timeout.Milliseconds(), "retry_limit": *retries, "shared_customer": *sharedID != ""}, Started: start, DurationSeconds: elapsed.Seconds(), Requests: st.requests.Load(), SuccessfulResponses: st.success.Load(), ExpectedConflicts: st.conflicts.Load(), ClientErrors: st.clientErr.Load(), ServerErrors: st.serverErr.Load(), NetworkFailures: st.network.Load(), Timeouts: st.timeouts.Load(), JournalErrors: st.journalErrors.Load(), Retries: st.retries.Load(), AcknowledgedWrites: st.writes.Load(), P50MS: quantile(.50), P95MS: quantile(.95), P99MS: quantile(.99)}
	if elapsed.Seconds() > 0 {
		r.Throughput = float64(r.Requests) / elapsed.Seconds()
	}
	b, _ := json.MarshalIndent(r, "", "  ")
	if e := os.MkdirAll(filepath.Dir(*out), 0755); e != nil {
		fatal(e.Error())
	}
	if e := os.WriteFile(*out, b, 0644); e != nil {
		fatal(e.Error())
	}
	fmt.Println(string(b))
	if r.ServerErrors+r.NetworkFailures+r.ClientErrors+r.JournalErrors > 0 {
		os.Exit(1)
	}
}
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
func fatal(s string) { fmt.Fprintln(os.Stderr, s); os.Exit(2) }
func runCycle(ctx context.Context, c *http.Client, st *stats, base, version, tenant string, seq, worker int, shared string, maxRetries int) {
	customerID := shared
	expected := int64(1)
	var createOp string
	var state contact
	if customerID != "" {
		status, body, err := do(ctx, c, http.MethodGet, base+"/api/customers/"+customerID, tenant, nil, maxRetries, st)
		if err != nil {
			classify(err, st)
			return
		}
		if status != 200 {
			classifyStatus(status, st)
			return
		}
		var existing struct {
			Version int64   `json:"version"`
			Contact contact `json:"contact"`
		}
		if json.Unmarshal(body, &existing) != nil {
			st.clientErr.Add(1)
			return
		}
		expected = existing.Version
		state = existing.Contact
		st.success.Add(1)
	}
	if customerID == "" {
		email := fmt.Sprintf("traffic-%d-%d@example.test", worker, seq)
		phone := fmt.Sprintf("+1555%07d", seq%10000000)
		state = contact{Email: email, Phone: &phone, Timezone: strptr("UTC"), PreferredLanguage: "en"}
		requestAt := time.Now().UTC()
		status, body, err := do(ctx, c, http.MethodPost, base+"/api/customers", tenant, map[string]any{"company_name": fmt.Sprintf("Traffic Company %d", seq), "contact": state}, 0, st)
		if err != nil {
			classify(err, st)
			return
		}
		if status != 201 {
			classifyStatus(status, st)
			return
		}
		var created struct {
			ID          string `json:"id"`
			Version     int64  `json:"version"`
			OperationID string `json:"operation_id"`
		}
		if json.Unmarshal(body, &created) != nil {
			st.clientErr.Add(1)
			return
		}
		customerID = created.ID
		expected = created.Version
		createOp = created.OperationID
		st.success.Add(1)
		st.writes.Add(1)
		if err := appendJournal(st, journalEntry{OperationID: createOp, OperationType: "create", CustomerID: customerID, TenantID: tenant, ExpectedVersion: 0, ReturnedVersion: expected, Contact: state, RequestTimestamp: requestAt, AcknowledgmentTimestamp: time.Now().UTC(), ApplicationVersion: version}); err != nil {
			st.clientErr.Add(1)
		}
	}
	status, _, err := do(ctx, c, http.MethodGet, base+"/api/customers/"+customerID, tenant, nil, maxRetries, st)
	if err != nil {
		classify(err, st)
	} else if status == 200 {
		st.success.Add(1)
	} else {
		classifyStatus(status, st)
	}
	if seq%5 == 0 {
		code, _, err := do(ctx, c, http.MethodGet, base+"/api/customers/search?q=Traffic", tenant, nil, maxRetries, st)
		if err != nil {
			classify(err, st)
		} else if code >= 300 {
			classifyStatus(code, st)
		} else {
			st.success.Add(1)
		}
	} else {
		code, _, err := do(ctx, c, http.MethodGet, base+"/api/customers?limit=20", tenant, nil, maxRetries, st)
		if err != nil {
			classify(err, st)
		} else if code >= 300 {
			classifyStatus(code, st)
		} else {
			st.success.Add(1)
		}
	}
	state.Email = fmt.Sprintf("updated-%d-%d@example.test", worker, seq)
	state.Phone = nil
	requestAt := time.Now().UTC()
	status, body, err := do(ctx, c, http.MethodPatch, base+"/api/customers/"+customerID, tenant, map[string]any{"expected_version": expected, "contact": state}, 0, st)
	if err != nil {
		classify(err, st)
	} else if status == 409 {
		st.conflicts.Add(1)
	} else if status == 200 {
		var updated struct {
			Version     int64  `json:"version"`
			OperationID string `json:"operation_id"`
		}
		if json.Unmarshal(body, &updated) != nil {
			st.clientErr.Add(1)
		} else {
			st.success.Add(1)
			st.writes.Add(1)
			if err := appendJournal(st, journalEntry{OperationID: updated.OperationID, OperationType: "update", CustomerID: customerID, TenantID: tenant, ExpectedVersion: expected, ReturnedVersion: updated.Version, Contact: state, RequestTimestamp: requestAt, AcknowledgmentTimestamp: time.Now().UTC(), ApplicationVersion: version}); err != nil {
				st.clientErr.Add(1)
			}
		}
	} else {
		classifyStatus(status, st)
	}
}
func do(ctx context.Context, c *http.Client, method, url, tenant string, body any, retries int, st *stats) (int, []byte, error) {
	for attempt := 0; ; attempt++ {
		st.requests.Add(1)
		started := time.Now()
		var data bytes.Buffer
		if body != nil {
			_ = json.NewEncoder(&data).Encode(body)
		}
		req, e := http.NewRequestWithContext(ctx, method, url, &data)
		if e != nil {
			return 0, nil, e
		}
		req.Header.Set("X-Demo-Tenant", tenant)
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, e := c.Do(req)
		if e != nil {
			recordLatency(st, time.Since(started))
			if attempt < retries {
				st.retries.Add(1)
				continue
			}
			return 0, nil, e
		}
		out := new(bytes.Buffer)
		_, _ = out.ReadFrom(resp.Body)
		_ = resp.Body.Close()
		recordLatency(st, time.Since(started))
		if resp.StatusCode >= 500 && attempt < retries {
			st.retries.Add(1)
			continue
		}
		return resp.StatusCode, out.Bytes(), nil
	}
}
func recordLatency(st *stats, d time.Duration) {
	st.mu.Lock()
	st.latency = append(st.latency, d)
	st.mu.Unlock()
}
func classify(err error, st *stats) {
	if os.IsTimeout(err) {
		st.timeouts.Add(1)
	}
	st.network.Add(1)
}
func classifyStatus(code int, st *stats) {
	if code >= 500 {
		st.serverErr.Add(1)
	} else {
		st.clientErr.Add(1)
	}
}
func appendJournal(st *stats, e journalEntry) error {
	b, err := json.Marshal(e)
	if err != nil {
		st.journalErrors.Add(1)
		return err
	}
	st.journalMu.Lock()
	defer st.journalMu.Unlock()
	if _, err = st.journal.Write(append(b, '\n')); err == nil {
		err = st.journal.Sync()
	}
	if err != nil {
		st.journalErrors.Add(1)
	}
	return err
}
func strptr(s string) *string { return &s }
