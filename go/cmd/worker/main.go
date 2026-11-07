// Worker: consumes requests from Kafka, streams a completion from a
// llama-server sidecar, and publishes every token to inference-results.
//
// Delivery is at-least-once: offsets are committed only after the whole
// response has been produced, so a worker killed mid-request never commits and
// the message is redelivered to another group member.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kgo"

	"llmplatform/internal/config"
)

const (
	requestsTopic = "inference-requests"
	resultsTopic  = "inference-results"
	workerGroup   = "inference-workers"
)

// Same metric names as the Python worker, so dashboards work for both.
var (
	requestCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "inference_requests_total", Help: "Total requests received"})
	requestLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "inference_request_latency_seconds", Help: "End-to-end request latency",
		Buckets: []float64{0.5, 1, 2, 5, 10, 20, 30, 60, 120}})
	tokensTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "inference_tokens_total", Help: "Tokens generated"})
	expiredTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "inference_requests_expired_total", Help: "Requests skipped because their deadline passed"})
)

type request struct {
	RequestID string   `json:"request_id"`
	Prompt    string   `json:"prompt"`
	MaxTokens int      `json:"max_tokens"`
	Deadline  *float64 `json:"deadline"`
}

type result struct {
	RequestID string `json:"request_id"`
	Token     string `json:"token"`
	WorkerID  string `json:"worker_id"`
	Done      bool   `json:"done"`
	Error     string `json:"error,omitempty"`
}

type worker struct {
	id          string
	llama       *llamaClient
	kc          *kgo.Client
	concurrency int
	ready       atomic.Bool
}

func main() {
	log.SetFlags(0) // plain lines, like the Python worker's print()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	host, _ := os.Hostname()
	w := &worker{
		id:          config.Str("HOSTNAME", host),
		llama:       newLlamaClient(config.Str("LLAMA_SERVER_URL", "http://localhost:8080")),
		concurrency: max(1, config.Int("WORKER_CONCURRENCY", 1)),
	}

	srv := w.httpServer(config.Str("PORT", "8000"))
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http server: %v", err)
		}
	}()
	defer srv.Close()

	// Like the Python worker loading its model at startup: don't join the
	// consumer group until the model is actually ready to serve.
	log.Printf("[%s] waiting for llama-server", w.id)
	if err := w.llama.waitReady(ctx); err != nil {
		log.Printf("[%s] shutting down before llama-server was ready", w.id)
		return
	}
	w.ready.Store(true)
	log.Printf("[%s] llama-server ready, concurrency=%d", w.id, w.concurrency)

	brokers := strings.Split(config.Str("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"), ",")
	kc, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(workerGroup),
		kgo.ConsumeTopics(requestsTopic),
		kgo.DisableAutoCommit(),                                // commit manually, after full completion
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),      // a brand-new group reads from the beginning
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, m map[string][]int32) {
			log.Printf("[%s] ASSIGNED partitions %s", w.id, fmtParts(m[requestsTopic]))
		}),
		kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, m map[string][]int32) {
			log.Printf("[%s] REVOKED partitions %s", w.id, fmtParts(m[requestsTopic]))
		}),
		kgo.OnPartitionsLost(func(_ context.Context, _ *kgo.Client, m map[string][]int32) {
			log.Printf("[%s] REVOKED partitions %s (lost)", w.id, fmtParts(m[requestsTopic]))
		}),
	)
	if err != nil {
		log.Fatalf("kafka client: %v", err)
	}
	w.kc = kc
	defer kc.Close() // leaves the group so partitions move quickly

	w.run(ctx)
}

func (w *worker) run(ctx context.Context) {
	for ctx.Err() == nil {
		// Pull at most `concurrency` records; with the default of 1 this is
		// the same one-at-a-time behaviour as the Python worker.
		fetches := w.kc.PollRecords(ctx, w.concurrency)
		if fetches.IsClientClosed() {
			return
		}
		fetches.EachError(func(t string, p int32, err error) {
			if !errors.Is(err, context.Canceled) {
				log.Printf("[%s] fetch error %s/%d: %v", w.id, t, p, err)
			}
		})
		var recs []*kgo.Record
		fetches.EachRecord(func(r *kgo.Record) { recs = append(recs, r) })
		if len(recs) > 0 {
			w.processBatch(ctx, recs)
		}
	}
}

func (w *worker) processBatch(ctx context.Context, recs []*kgo.Record) {
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		done []*kgo.Record
	)
	for _, r := range recs {
		wg.Add(1)
		go func(r *kgo.Record) {
			defer wg.Done()
			if w.handle(ctx, r) {
				mu.Lock()
				done = append(done, r)
				mu.Unlock()
			}
		}(r)
	}
	wg.Wait()
	if len(done) == 0 {
		return
	}
	// Commit only after every response in `done` is fully produced. (As in the
	// Python worker, a later commit in a partition also covers an earlier
	// record that was not committed; per-partition ordering is not tracked.)
	cctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := w.kc.CommitRecords(cctx, done...); err != nil {
		log.Printf("[%s] commit failed: %v", w.id, err)
	}
}

// handle processes one request. It returns true when the record may be committed.
func (w *worker) handle(ctx context.Context, rec *kgo.Record) bool {
	var req request
	if err := json.Unmarshal(rec.Value, &req); err != nil || req.RequestID == "" {
		log.Printf("[%s] skipping malformed message partition=%d offset=%d", w.id, rec.Partition, rec.Offset)
		return true // poison pill: commit and move on
	}

	if req.Deadline != nil {
		now := float64(time.Now().UnixNano()) / 1e9
		if now > *req.Deadline {
			expiredTotal.Inc()
			log.Printf("[%s] EXPIRED %s late_by=%.1fs", w.id, req.RequestID, now-*req.Deadline)
			return true
		}
	}

	requestCount.Inc()
	log.Printf("[%s] RECEIVED %s partition=%d offset=%d", w.id, req.RequestID, rec.Partition, rec.Offset)

	timer := prometheus.NewTimer(requestLatency)
	defer timer.ObserveDuration()

	maxTokens := req.MaxTokens
	if maxTokens <= 0 {
		maxTokens = 256
	}

	var produceFailed atomic.Bool
	send := func(res result) {
		res.RequestID = req.RequestID
		res.WorkerID = w.id
		b, _ := json.Marshal(res)
		w.kc.Produce(ctx, &kgo.Record{Topic: resultsTopic, Key: []byte(req.RequestID), Value: b},
			func(_ *kgo.Record, err error) {
				if err != nil {
					produceFailed.Store(true)
					log.Printf("[%s] produce failed for %s: %v", w.id, req.RequestID, err)
				}
			})
	}

	err := w.llama.stream(ctx, req.Prompt, maxTokens, func(tok string) {
		tokensTotal.Inc()
		send(result{Token: tok})
	})
	if ctx.Err() != nil {
		return false // shutting down mid-request: leave uncommitted so it is redelivered
	}
	if err != nil {
		// Tell the client instead of leaving it hanging until the router times out.
		log.Printf("[%s] generation failed for %s: %v", w.id, req.RequestID, err)
		send(result{Done: true, Error: err.Error()})
	} else {
		send(result{Done: true})
	}

	fctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := w.kc.Flush(fctx); err != nil || produceFailed.Load() {
		log.Printf("[%s] results for %s not fully delivered; not committing", w.id, req.RequestID)
		return false
	}
	return true
}

func (w *worker) httpServer(port string) *http.Server {
	mux := http.NewServeMux()
	ok := func(body string) http.HandlerFunc {
		return func(rw http.ResponseWriter, _ *http.Request) {
			rw.Header().Set("Content-Type", "application/json")
			fmt.Fprint(rw, body)
		}
	}
	mux.HandleFunc("GET /health", ok(`{"status":"ok"}`))
	mux.HandleFunc("GET /healthz/live", ok(`{"status":"alive"}`)) // is the process up
	mux.HandleFunc("GET /healthz/ready", func(rw http.ResponseWriter, r *http.Request) { // is the model ready
		if !w.ready.Load() || !w.llama.healthy(r.Context()) {
			http.Error(rw, `{"detail":"Model not loaded"}`, http.StatusServiceUnavailable)
			return
		}
		rw.Header().Set("Content-Type", "application/json")
		fmt.Fprint(rw, `{"status":"ready"}`)
	})
	mux.Handle("/metrics", promhttp.Handler())
	mux.Handle("/metrics/", promhttp.Handler())
	return &http.Server{Addr: ":" + port, Handler: mux}
}

func fmtParts(ps []int32) string {
	s := make([]string, len(ps))
	for i, p := range ps {
		s[i] = strconv.Itoa(int(p))
	}
	return "[" + strings.Join(s, ", ") + "]"
}
