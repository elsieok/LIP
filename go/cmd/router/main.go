// Router: accepts POST /generate, puts the request on Kafka, and streams the
// tokens the workers publish back to the client as Server-Sent Events.
//
// Wire-compatible with the Python router/worker: same topics, same JSON
// payloads, same metric names, so the two implementations can be mixed.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"os"
	"os/signal"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"llmplatform/internal/config"
)

const (
	requestsTopic = "inference-requests"
	resultsTopic  = "inference-results"
	workerGroup   = "inference-workers" // must match the workers' group
	tokenTimeout  = 60 * time.Second    // max silence between tokens
)

var (
	requestCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "inference_requests_total", Help: "Total requests received"})
	requestLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "inference_request_latency_seconds", Help: "End-to-end request_latency",
		Buckets: []float64{0.5, 1, 2, 5, 10, 20, 30, 60, 120}})
	consumerLag = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "kafka_consumer_lag", Help: "Current consumer lag on inference-requests"})
)

type generateRequest struct {
	Prompt    string `json:"prompt"`
	MaxTokens int    `json:"max_tokens"`
}

// requestMsg is what goes on the inference-requests topic.
type requestMsg struct {
	RequestID string  `json:"request_id"`
	Prompt    string  `json:"prompt"`
	MaxTokens int     `json:"max_tokens"`
	Deadline  float64 `json:"deadline"` // unix seconds
}

// result is what workers put on the inference-results topic.
type result struct {
	RequestID string `json:"request_id"`
	Token     string `json:"token"`
	WorkerID  string `json:"worker_id"`
	Done      bool   `json:"done"`
	Error     string `json:"error,omitempty"`
}

type router struct {
	producer     *kgo.Client
	lagThreshold int64
	ttl          time.Duration

	lag atomic.Int64

	mu      sync.Mutex
	waiters map[string]chan result // request_id -> mailbox
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	brokers := strings.Split(config.Str("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"), ",")

	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		log.Fatalf("producer: %v", err)
	}
	defer producer.Close()

	// No consumer group: every router instance reads every result from the end
	// of the topic. This avoids the Python version's fixed-group-id problem
	// (multiple router replicas would split the partitions between them).
	results, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(resultsTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	)
	if err != nil {
		log.Fatalf("results consumer: %v", err)
	}
	defer results.Close()

	rt := &router{
		producer:     producer,
		lagThreshold: int64(config.Int("LAG_THRESHOLD", 200)),
		ttl:          time.Duration(config.Float("REQUEST_TTL_S", 55) * float64(time.Second)),
		waiters:      make(map[string]chan result),
	}
	go rt.resultsLoop(ctx, results)
	go rt.lagLoop(ctx, kadm.NewClient(producer))

	mux := http.NewServeMux()
	mux.HandleFunc("POST /generate", rt.handleGenerate)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"status":"ok"}`)
	})
	mux.Handle("/metrics", promhttp.Handler())
	mux.Handle("/metrics/", promhttp.Handler()) // Python version served /metrics/

	// No WriteTimeout: responses are long-lived streams.
	srv := &http.Server{Addr: ":" + config.Str("PORT", "8000"), Handler: mux}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()

	log.Printf("router listening on %s (lag threshold %d, ttl %s)", srv.Addr, rt.lagThreshold, rt.ttl)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func (rt *router) handleGenerate(w http.ResponseWriter, r *http.Request) {
	req := generateRequest{MaxTokens: 256}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Prompt == "" {
		http.Error(w, `{"detail":"invalid request body: need {\"prompt\": string, \"max_tokens\": int}"}`, http.StatusUnprocessableEntity)
		return
	}
	requestCount.Inc()

	// Backpressure: fail fast instead of growing the queue.
	if lag := rt.lag.Load(); lag > rt.lagThreshold {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprintf(w, `{"detail":"system overloaded (lag=%d), try again shortly"}`, lag)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	id := newID()
	ch := make(chan result, 8192)
	rt.mu.Lock()
	rt.waiters[id] = ch
	rt.mu.Unlock()
	defer func() {
		rt.mu.Lock()
		delete(rt.waiters, id)
		rt.mu.Unlock()
	}()

	if err := rt.produce(r.Context(), id, req); err != nil {
		log.Printf("produce %s failed: %v", id, err)
		http.Error(w, `{"detail":"could not enqueue request"}`, http.StatusBadGateway)
		return
	}

	start := time.Now()
	defer func() { requestLatency.Observe(time.Since(start).Seconds()) }()

	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	timeout := time.NewTimer(tokenTimeout)
	defer timeout.Stop()
	for {
		select {
		case res, ok := <-ch:
			if !ok { // mailbox overflowed; the results loop gave up on us
				panic(http.ErrAbortHandler)
			}
			if res.Error != "" {
				writeSSE(w, "error", res.Error)
				flusher.Flush()
				return
			}
			if res.Done {
				writeSSE(w, "done", res.WorkerID)
				flusher.Flush()
				return
			}
			writeSSE(w, "token", res.Token)
			flusher.Flush()
			if !timeout.Stop() {
				select {
				case <-timeout.C:
				default:
				}
			}
			timeout.Reset(tokenTimeout)
		case <-timeout.C:
			// Deliberately abort the connection (no clean chunked terminator) so
			// the client sees a broken stream, exactly like the Python router's
			// queue.Empty after 60 s. A clean close would look like a success
			// to Locust and skew the benchmarks.
			panic(http.ErrAbortHandler)
		case <-r.Context().Done():
			return
		}
	}
}

func (rt *router) produce(ctx context.Context, id string, req generateRequest) error {
	payload, err := json.Marshal(requestMsg{
		RequestID: id,
		Prompt:    req.Prompt,
		MaxTokens: req.MaxTokens,
		Deadline:  float64(time.Now().Add(rt.ttl).UnixNano()) / 1e9,
	})
	if err != nil {
		return err
	}
	return rt.producer.ProduceSync(ctx, &kgo.Record{
		Topic: requestsTopic, Key: []byte(id), Value: payload,
	}).FirstErr()
}

// resultsLoop routes every message from inference-results to its waiting handler.
func (rt *router) resultsLoop(ctx context.Context, cl *kgo.Client) {
	for {
		fetches := cl.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return
		}
		fetches.EachError(func(t string, p int32, err error) {
			log.Printf("results fetch error %s/%d: %v", t, p, err)
		})
		fetches.EachRecord(func(rec *kgo.Record) {
			var res result
			if err := json.Unmarshal(rec.Value, &res); err != nil {
				log.Printf("bad result message: %v", err)
				return
			}
			rt.mu.Lock()
			if ch, ok := rt.waiters[res.RequestID]; ok {
				select {
				case ch <- res:
				default: // client is far too slow; drop the waiter, handler aborts
					delete(rt.waiters, res.RequestID)
					close(ch)
				}
			}
			rt.mu.Unlock()
		})
	}
}

// lagLoop refreshes consumer lag every 5 s, off the request path.
func (rt *router) lagLoop(ctx context.Context, adm *kadm.Client) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		if lag, err := computeLag(ctx, adm); err != nil {
			log.Printf("lag check failed: %v", err)
		} else {
			rt.lag.Store(lag)
			consumerLag.Set(float64(lag))
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func computeLag(ctx context.Context, adm *kadm.Client) (int64, error) {
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	lags, err := adm.Lag(cctx, workerGroup)
	if err != nil {
		return 0, err
	}
	gl, ok := lags[workerGroup]
	if !ok {
		return 0, nil
	}
	if err := gl.Error(); err != nil {
		return 0, err
	}
	return gl.Lag.Total(), nil
}

func writeSSE(w io.Writer, event, data string) {
	fmt.Fprintf(w, "event: %s\n", event)
	for _, line := range strings.Split(data, "\n") {
		fmt.Fprintf(w, "data: %s\n", line)
	}
	fmt.Fprint(w, "\n")
}

// newID returns a random UUIDv4.
func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	h := hex.EncodeToString(b[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}
