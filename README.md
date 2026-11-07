# Distributed LLM Inference Platform

A streaming LLM inference service built to learn distributed-systems fundamentals end to end: a FastAPI **router** accepts prompts and streams tokens back over **Server-Sent Events**, a Kafka-compatible **queue (Redpanda)** decouples it from a pool of **llama.cpp workers**, and the whole thing runs on Docker Compose locally and on **k3s** (Kubernetes) with health probes, Prometheus/Grafana monitoring, backpressure, and autoscaling.

Everything runs on CPU with a small quantized model (Qwen2.5-1.5B-Instruct, Q4_K_M GGUF), so the entire stack is reproducible on a laptop or a cheap ARM cloud instance.

**Highlights**

- Queue-based architecture with **at-least-once delivery**: workers commit Kafka offsets only after a request is fully generated, so a killed worker's request is redelivered to another worker (demonstrated below with real logs).
- **Token streaming** from llama.cpp through Kafka and SSE to the client.
- **Kubernetes (k3s)** deployment: Deployments, a StatefulSet for the broker, Services, Traefik Ingress, readiness/liveness probes (readiness gated on model load), HPA.
- **Observability**: Prometheus metrics from router and workers, Grafana via kube-prometheus-stack.
- **Backpressure**: the router rejects requests with `503` when consumer lag exceeds a threshold.
- **Request deadlines**: every request carries a deadline (55 s by default); a worker skips and commits requests whose client has already given up, instead of burning compute on them.
- **Load-test harness** (Locust) with scripted experiments and per-Pod CPU/memory sampling. That sampling exposed a thread-oversubscription bug in my first benchmark run; after fixing it, throughput scaled with workers (details below).

---

## Architecture

```
                              HTTP + SSE
  client ──────────────────────────────────────────────┐
    │  POST /generate                                  │ event: token / done
    ▼                                                  │
┌──────────┐  produce   ┌──────────────────────┐       │
│  router  │ ─────────▶ │ topic:               │       │
│ (FastAPI)│            │ inference-requests   │       │
│          │            │ (3 partitions)       │       │
│          │            └──────────┬───────────┘       │
│          │                       │ consume (group: inference-workers)
│          │                       ▼
│          │            ┌──────────────────────┐
│          │            │ worker-1 … worker-N  │  llama.cpp (CPU)
│          │            │ (llama-cpp-python)   │
│          │            └──────────┬───────────┘
│          │   consume             │ produce tokens (tagged with worker_id)
│          │ ◀──────── ┌───────────▼──────────┐
└──────────┘           │ topic:               │
                       │ inference-results    │
                       └──────────────────────┘
```

**Request flow**

1. The client `POST`s a prompt to the router. The router assigns a UUID `request_id`, registers an in-memory queue for it, and produces the request to `inference-requests` (message key = `request_id`).
2. One worker in the `inference-workers` consumer group receives the message (each partition is owned by exactly one group member) and streams tokens from llama.cpp.
3. For every token the worker produces a message to `inference-results` containing `request_id`, the token, and its own `worker_id` (the Pod name on Kubernetes). A final message with `done: true` closes the stream.
4. A background thread in the router consumes `inference-results` and routes each token to the waiting request's queue. The HTTP handler yields them to the client as SSE events.
5. Only after the full generation succeeds does the worker commit its offset.

## Key design decisions

| Decision | Why | Trade-off |
|---|---|---|
| Queue (Kafka API) between router and workers | Decouples intake from compute, lets workers die or scale without the router noticing, enables lag-based backpressure | Extra hop; the router needs a results consumer and per-request bookkeeping |
| Redpanda instead of Apache Kafka | Single binary, Kafka-protocol compatible, light enough for a laptop and one small VM | Single broker, no replication (see limitations) |
| 3 partitions, message key = `request_id` | Parallelism across workers; all tokens of one request stay ordered in one partition | At most 3 workers in a group do useful work |
| Manual offset commit **after** full generation | At-least-once delivery: a worker that dies mid-request never commits, so the message is redelivered | Duplicates are possible; partial output already streamed is not de-duplicated |
| SSE for streaming | One-directional, works with plain `curl`, simple on both sides | Not bidirectional |
| `worker_id` in every result message | Makes "which Pod served this request" observable end to end | Slightly larger messages |
| Readiness gated on model load (`/healthz/ready`) | A worker that has not loaded the model must not look healthy | Slow-loading Pods need a startup probe or generous delays |
| Lag-based backpressure (`503` above `LAG_THRESHOLD`) | Fail fast under overload instead of building an unbounded queue | Needs a threshold tuned to the real queue depth (see Experiment B) |
| Per-request deadline stamped by the router | Stops workers from generating answers nobody is waiting for | Relies on router and workers sharing a clock (true on a single node) |
| Lag computed in a background thread | Keeps Kafka metadata calls off the request path | Value can lag behind a sudden burst |

## Quick start (Docker Compose)

Requires Docker and the model file at `models/qwen2.5-1.5b-instruct-q4_k_m.gguf`.

**Python**

```bash
docker compose up -d --build
./kafka/create_topics.sh
docker compose logs -f worker      # each worker logs its consumer-group membership
```

**Go** (workers talk to a llama-server sidecar; topics are created automatically)

```bash
docker compose -f docker-compose.go.yml up -d --build
docker compose -f docker-compose.go.yml logs -f worker-1 worker-2 worker-3
```

In another terminal:

```bash
curl -N -X POST localhost:8000/generate -H "Content-Type: application/json" \
  -d '{"prompt": "Explain partitions in one sentence."}'
```

You should see `event: token` lines stream in, ending with `event: done` whose data is the worker that served the request. Run it several times to see different workers.

Before switching stacks, 
```bash
run docker compose down # add -f docker-compose.go.yml if you’re stopping the Go one.
```

## Kubernetes (k3s)

The same images run on k3s, locally in a VM or on a cloud instance.

```bash
# 1. build arm64 or amd64 images to match the node, then import them into k3s's containerd
docker save llm-router:dev | gzip > llm-router.tar.gz
docker save llm-worker:dev | gzip > llm-worker.tar.gz
gunzip -c llm-router.tar.gz | sudo k3s ctr images import -
gunzip -c llm-worker.tar.gz | sudo k3s ctr images import -

# 2. copy the model to the node (mounted into workers with hostPath: /home/ubuntu/models)

# 3. deploy: broker first, create topics, then the rest
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/redpanda-statefulset.yaml
kubectl exec -n llm-platform redpanda-0 -- rpk topic create inference-requests --partitions 3 --replicas 1
kubectl exec -n llm-platform redpanda-0 -- rpk topic create inference-results  --partitions 3 --replicas 1
kubectl apply -f k8s/router-deployment.yaml -f k8s/router-service.yaml
kubectl apply -f k8s/worker-deployment.yaml
kubectl apply -f k8s/ingress.yaml
```

`deploy/provision-ec2.sh`, `deploy/install-k3s.sh`, and `deploy/teardown.sh` automate the EC2 path (a short-lived Graviton instance, torn down after benchmarking).

Images are imported directly into containerd (`imagePullPolicy: IfNotPresent`) instead of using a registry, which keeps a single-node setup free of extra infrastructure.

## Observability

| Metric | Type | Source |
|---|---|---|
| `inference_requests_total` | counter | router (requests received), workers (requests consumed) |
| `inference_request_latency_seconds` | histogram | router (end-to-end), workers (generation) |
| `inference_tokens_total` | counter | workers |
| `kafka_consumer_lag` | gauge | router (backlog of `inference-requests` for the worker group) |

Each service exposes `/metrics/` (note the trailing slash). Install the monitoring stack with:

```bash
helm install monitoring prometheus-community/kube-prometheus-stack \
  -n monitoring --create-namespace -f monitoring/prometheus-values.yaml \
  --set grafana.adminPassword='<choose-a-password>'
```

Prometheus discovers router and worker Pods by label and attaches `app` and `pod` labels, so queries can be broken down per worker.

---

## Fault tolerance: killing a worker mid-request

**Goal:** show that an in-flight request survives a hard worker kill (Kafka-level recovery, not just the Pod restarting).

**Test** (run on a local k3s cluster): start a long generation, identify the worker that received it from its `RECEIVED` log line, save that worker's logs, then force-delete its Pod mid-generation.

```bash
curl -sN -X POST "http://${INGRESS_IP}/generate" -H "Content-Type: application/json" \
  -d '{"prompt": "...", "max_tokens": 6000}' > /tmp/gen_output.log &
sleep 5
VICTIM=$(kubectl logs -n llm-platform -l app=worker --prefix --tail=-1 --since=1m \
  | grep RECEIVED | tail -1 | sed -E 's#^\[pod/([^/]+)/.*#\1#')
kubectl logs -n llm-platform "$VICTIM" > /tmp/victim.log
kubectl delete pod -n llm-platform "$VICTIM" --grace-period=0 --force
sleep 70    # wait for the consumer-group session timeout
kubectl logs -n llm-platform -l app=worker --prefix --tail=-1 --since=5m | grep -E "RECEIVED|ASSIGNED|REVOKED"
```

**Evidence**

The victim received the request, then was killed before committing its offset (from `/tmp/victim.log`):

```
[worker-86cf45f5db-rbkrq] RECEIVED 240f5a74-2cd3-414b-ba07-5c4a63ef34d3 partition=0 offset=18
```

The consumer group rebalanced:

```
[worker-86cf45f5db-dqcr5] REVOKED partitions [2]
[worker-86cf45f5db-dqcr5] ASSIGNED partitions [2]
[worker-86cf45f5db-bq4xt] ASSIGNED partitions [0, 1]
```

A different worker received the **same message** (same request ID, partition, and offset):

```
[worker-86cf45f5db-bq4xt] RECEIVED 240f5a74-2cd3-414b-ba07-5c4a63ef34d3 partition=0 offset=18
```

The client's stream ended with the surviving worker's name:

```
event: done
data: worker-86cf45f5db-bq4xt
```

**Result:** the killed worker never committed, so Kafka redelivered the message to another group member after the rebalance, and the client received a complete response.

**Limitation:** delivery is at-least-once. The second worker regenerates from the beginning, so tokens already streamed by the failed worker can be repeated in the client's stream, and a request can be processed more than once.

---

## Benchmarks

### Setup

- **Node:** one AWS EC2 Graviton (arm64) instance, `c7g.xlarge` (4 vCPU, 8 GB), single-node k3s, CPU-only inference. The instance existed for a few hours per run and was terminated afterwards.
- **Model:** Qwen2.5-1.5B-Instruct, Q4_K_M, via llama-cpp-python. Each worker runs llama.cpp with **one compute thread** and requests one CPU core (`LLAMA_N_THREADS=1`, `cpu: 1`).
- **Load:** Locust run from a laptop, prompt `"Explain photosynthesis briefly."`, `max_tokens=100`, think time 0.5–2 s between requests, 3 minutes per run unless noted. Before every run the topics were deleted and recreated and the router and workers restarted, so no run inherits another's backlog.
- **Sampling:** per-Pod CPU and node memory were recorded every 15 s with `kubectl top` / `free` (metrics-server averages over a window, so these are smoothed readings).
- **Latency** is the full time until the stream ends, not time-to-first-token, and the latency columns cover **successful requests only**. Locust rounds large percentile values, so read medians, not the last digit of p95.
- **Failures** are either `HTTP 503` (rejected by backpressure) or a stream cut after 60 s without a token (`ChunkedEncodingError`).
- **Sample sizes are small:** 17–120 successful requests per run, one run per configuration. The same configuration (6 users, 3 workers) ran twice, as part of experiments A and E, and gave 0.231 and 0.243 req/s. Read the trends, not the decimals.
- Raw data for the final run is in `benchmarks/results_v3/`. An earlier, misconfigured run is in `benchmarks/results_v2/` (see finding 3).

![Successful throughput, latency, and failures per run](benchmarks/results_v3/summary.png)

### Results (final run)

| Run | Users | Workers | OK | Failed | Median (s) | p95 (s) | OK req/s |
|---|---:|---:|---:|---:|---:|---:|---:|
| E: single user | 1 | 3 | 17 | 0 | 8.8 | 11.0 | 0.097 |
| A: 1 worker | 6 | 1 | 20 | 0 | 52.0 | 53.0 | 0.112 |
| A: 2 workers | 6 | 2 | 28 | 1 | 36.0 | 45.0 | 0.165 |
| A: 3 workers | 6 | 3 | 41 | 2 | 19.0 | 34.0 | 0.231 |
| E: 6 users | 6 | 3 | 43 | 0 | 18.0 | 46.0 | 0.243 |
| E: 12 users | 12 | 3 | 48 | 6 | 25.0 | 63.0 | 0.270 |
| E: 24 users | 24 | 3 | 52 | 15 | 52.0 | 64.0 | 0.293 |
| C: batch window 0 ms | 6 | 3 | 42 | 0 | 19.0 | 54.0 | 0.237 |
| C: batch window 30 ms | 6 | 3 | 47 | 0 | 18.0 | 37.0 | 0.272 |
| B: backpressure, threshold 10 | 40 | 3 | 45 | 1,100 (1,056 × 503) | 29.0 | 58.0 | 0.252 |
| B: no backpressure | 40 | 3 | 44 | 49 | 56.0 | 64.0 | 0.248 |
| D: kill a worker at t=120 s (5 min) | 6 | 3 | 49 | 6 | 20.0 | 51.0 | 0.166 |
| H: HPA, 1→3 workers (8 min) | 12 | 1→3 | 120 | 10 | 39.0 | 63.0 | 0.251 |

### What the data shows

**1. A single-thread worker answers one request in about 9 s.** With one user and no queueing, all 17 requests completed in 8.7–10.8 s. Responses carried roughly 100 tokens (about 2.9 KB), so one thread generates about 11 tokens/s. In the first run, with 4 threads, the same request took about 3.4 s: four times the threads gave about 2.6 times the speed.

**2. Throughput scales with workers.**

![Throughput, CPU used, and fastest request versus worker count, before and after the thread fix](benchmarks/results_v3/scaling.png)

| Workers | OK req/s | Speed-up | Total worker CPU (cores) | Worker utilization | Fastest request (s) |
|---:|---:|---:|---:|---:|---:|
| 1 | 0.112 | 1.0× | 1.00 | 100% | 10.4 |
| 2 | 0.165 | 1.5× | 1.63 | 82% | 9.5 |
| 3 | 0.231 | 2.1× | 2.18 | 73% | 9.5 |

The fastest request stays at about 10 s however many workers run, so workers no longer slow each other down. Throughput per busy core is nearly constant at roughly 0.10–0.11 req/s (0.112, 0.101, 0.106). The gap to ideal 2× and 3× comes from workers sitting idle, not from contention. Each worker only consumes its own partitions and request keys are random, so with 6 users one partition can be empty while another has a queue; the CPU samples show one worker near 0% while another sits at 100%. More concurrent users reduce this: with 3 workers, utilization rose from 73% at 6 users to 86% at 12 and 93% at 24, and throughput rose to 0.293 req/s, 2.6× a single worker.

**3. The first benchmark run was invalid, and per-Pod CPU sampling is how I found out.** In my first run, throughput *fell* as workers were added (0.306, 0.133, and 0.099 req/s for 1, 2, and 3 workers), the fastest request grew from 3.8 s to 28 s, and total worker CPU sat at 3.5–3.8 cores in every configuration. A lone worker was using about 3.5 cores, which meant each worker ran several compute threads, and three workers meant 12 threads fighting over 4 vCPUs. I had set `LLAMA_N_THREADS=1` in the manifest, but the worker code read a differently named variable (`N_THREADS`, default 4), so the setting was silently ignored. After fixing the variable name and rerunning everything, one worker used 1.0 core and scaling turned positive. The pattern in the first run reproduced across two separate instances before I found the cause.

**4. On a single 4-vCPU node, scale-out does not beat one multi-threaded worker.** In the first run, one 4-thread worker reached 0.306 req/s at 6 users with an 18 s median; three single-thread workers reached 0.231–0.243 req/s with an 18–19 s median, and 0.293 req/s at 24 users. (This compares separate runs on separate instances.) The node's CPU is the limit either way. What the multi-worker design adds is failover, isolation, and the ability to add nodes, at the cost of one copy of the model state per worker (about 1.1 GiB of memory each; at least 2.9 GB stayed free in every run).

**5. Load beyond capacity only adds latency and timeouts.** With 3 workers, throughput plateaued at 0.27–0.29 req/s from 12 users up, while the median latency went from 18 s (6 users) to 25 s (12) to 52 s (24) and timeouts from 0 to 6 to 15.

**6. Backpressure protected latency but did not add capacity.** With 40 users ramping up at 2 per second, a lag threshold of 10 rejected 1,056 requests immediately with `503`, completed 45 successfully with a 29 s median (fastest 9.8 s), and left 44 timeouts. With no threshold, 44 completed with a 56 s median (fastest 33 s, because every request sat in a long queue) and 49 timed out. Goodput was the same (0.252 vs 0.248 req/s). So the check halved the median latency of admitted requests and turned most of the excess load into instant, explicit rejections, but a threshold of 10 was still loose enough to admit about as many doomed requests as before. A threshold near `throughput × acceptable wait` (about 5 for a 20 s target at 0.25 req/s) would be tighter.

**7. No measurable benefit from the batch window.** Windows of 0 and 30 ms gave 0.237 and 0.272 req/s. The worker CPU samples show the 0 ms run's workers were less busy (1.95 vs 2.44 cores on average), meaning the two runs saw different amounts of the partition imbalance from finding 2, which explains the gap better than the batch setting does. This also matches the implementation: the "batch" groups up to 8 Kafka messages but generates them one after another.

**8. Killing a worker slowed service for about a minute, and no request failed afterwards.** In the 5-minute kill run (6 users, 3 workers), one worker was force-deleted at t=120 s. Completions ran at about 0.22/s before the kill, dropped to about 0.1/s over the next 40–50 s, and returned to about 0.24/s once the group had rebalanced; a replacement Pod was running within about 26 s. No request failed after the kill. All six failures in this run occurred in the first 70 s (each user's first request), before the kill. I did not isolate that cause; it is consistent with early results being missed while the router's results consumer rejoined its group after the harness restarted the router. The interrupted request is consistent with redelivery, but I only saved the victim's logs, so the proof of redelivery is the log-based test above and not these CSVs.

**9. The autoscaler worked, and CPU is now a valid signal.** With 12 users, the HPA (min 1, max 3, target 70% CPU utilization, one new Pod per 90 s) went from 1 to 2 replicas about 60 s after load started (utilization 100% of the Pod's CPU request) and to 3 about 90 s later, then stayed at 3 with each worker saturated at about 100%. About 100 s after utilization fell to zero it scaled back down (3 → 2 → 1). Successful throughput rose with each step: about 0.10 req/s with one worker, 0.21 with two, and 0.29 with three. In the first run, CPU was a misleading signal because added workers added no capacity; with one-thread workers, CPU utilization tracks saturation. Scaling on queue lag would still measure unmet demand more directly.

## Go rewrite: Python vs Go comparison

The router and worker are also implemented in Go (`go/`). The Go services are drop-in compatible with the Python ones: same Kafka topics, JSON payloads, metric names and log lines, so either implementation can be swapped in (or mixed) on the same cluster.

**What changed**

| | Python | Go |
|---|---|---|
| Kafka client | confluent-kafka (librdkafka) | franz-go (pure Go, no cgo) |
| Inference | llama-cpp-python, in-process | `llama-server` sidecar container per worker (HTTP, streaming `/v1/chat/completions`) |
| Router results consumer | consumer group, starts at latest offset | no group, reads from the end of every partition (safe for multiple router replicas) |
| Worker concurrency | one request at a time | one at a time by default (`WORKER_CONCURRENCY`) |
| On inference error | request is not committed, client waits for timeout | client gets `event: error`, request is committed |

The Go worker needs a sidecar because llama-cpp-python is Python-only. Each Pod runs the Go worker plus `ghcr.io/ggml-org/llama.cpp:server`.

### Method

- **Node:** one AWS EC2 `c7g.xlarge` (Graviton, 4 vCPU, 8 GB), single-node k3s. Both stacks ran on the same instance, one after the other.
- **Model:** Qwen2.5-1.5B-Instruct Q4_K_M, one llama.cpp compute thread per worker, one CPU core requested per worker.
- **Load:** Locust, `"Explain photosynthesis briefly."`, `max_tokens=100`, 0.5–2 s think time, 3 minutes per run unless noted. Topics and Pods are reset before every run.
- **Resources:** `kubectl top pods --containers` and `free -m` every 15 s (metrics-server smooths over a window).
- **One run per configuration.** The same configuration (6 users, 3 workers) ran twice per stack and gave 0.183 and 0.262 req/s on Go, 0.201 and 0.248 on Python. Differences of that size are noise.

### Resources (3 workers, 6 users, 3 min)

| | Python | Go |
|---|---|---|
| Worker Pod memory, idle | 1,101 MiB | 1,052 MiB (llama-server 1,049 + worker 3) |
| Worker Pod memory, under load (mean / peak) | 1,118 / 1,138 MiB | 1,079 / 1,116 MiB |
| Router memory, idle | 45 MiB | 3 MiB |
| Router memory, under load (peak) | 48 MiB | 11 MiB |
| Node memory used, peak | 4,613 MiB | 4,523 MiB |
| Router image | 426 MB | 9 MB |
| Worker-side images | 490 MB | 306 MB (9 MB worker + 297 MB llama.cpp, pulled once per node) |
| Pod cold start (single sample) | 6 s | 7 s |

### Throughput and latency

| Run | Users | Workers | req/s (Py / Go) | Median s (Py / Go) | Failed (Py / Go) |
|---|---:|---:|---|---|---|
| Scaling | 6 | 1 | 0.114 / 0.108 | 50 / 42 | 0 / 2 |
| Scaling | 6 | 2 | 0.129 / 0.192 | 35 / 27 | 2 / 0 |
| Scaling | 6 | 3 | 0.247 / 0.243 | 20 / 20 | 1 / 0 |
| Load | 1 | 3 | 0.097 / 0.095 | 8.8 / 9.2 | 0 / 0 |
| Load | 6 | 3 | 0.201 / 0.183 | 20 / 28 | 0 / 0 |
| Load | 12 | 3 | 0.247 / 0.283 | 28 / 31 | 7 / 2 |
| Load | 24 | 3 | 0.287 / 0.291 | 50 / 49 | 17 / 17 |
| Backpressure on (threshold 10) | 40 | 3 | 0.234 / 0.268 | 37 / 31 | 1,026 / 1,024 (979 / 980 × 503) |
| Backpressure off | 40 | 3 | 0.252 / 0.295 | 56 / 49 | 49 / 49 |
| Kill a worker at t=120 s (5 min) | 6 | 3 | 0.182 / 0.251 | 19 / 20 | 7 / 0 |
| HPA 1→3 (8 min) | 12 | 1→3 | 0.241 / 0.258 | 45 / 44 | 13 / 10 |
| Compare run | 6 | 3 | 0.248 / 0.262 | 20 / 19 | 0 / 1 |

The batch-window experiment ran on Python only (0 ms vs 30 ms: 0.254 vs 0.265 req/s, no real effect). The Go worker has no batch window.

### What the data shows

1. **Go's advantage is footprint, not speed.** The router needs 3 MiB instead of 45 MiB at idle and its image is 98% smaller. At 24 users it also used about half the CPU (about 10 m vs 19 m). The worker Pod is only 2–4% smaller, because the model dominates memory and llama-server holds the same model as llama-cpp-python.
2. **Throughput and latency are indistinguishable.** With one user the HTTP hop to llama-server costs about 0.4 s (9.2 s vs 8.8 s per request). At 24 users both plateau at about 0.29 req/s with a 49–50 s median. Every per-run gap above is smaller than the run-to-run variation.
3. **Scaling is limited by partition imbalance, not language.** Both stacks go from about 0.11 req/s with one worker to about 0.24 with three (about 2.2×). The Python two-worker run (0.129) had one worker idle while the other was saturated: with 3 partitions and random request keys, a worker can own a partition that stays empty.
4. **Backpressure works the same on both.** A lag threshold of 10 rejects about 980 requests immediately with `503` and cuts the median latency of admitted requests by about a third (Go 49→31 s, Python 56→37 s). Goodput does not change, so it protects latency but adds no capacity.
5. **A killed worker is survivable under load.** In both stacks a replacement Pod was visible about 26 s after the kill. Go showed no visible stall and no failed requests. Python showed a gap of about 30 s in completions and one timeout after the kill; its other six failures were first-request timeouts at about 60 s, consistent with the known router results-consumer gap after a router restart. Similar first-request timeouts appeared in four other Python runs (2 workers, 3 workers, 12 users, and the kill run).
6. **Autoscaling behaves the same.** With 12 users both stacks scaled 1→2 within about 60 s and →3 within about 150 s, held 3 replicas at 95–100% of the CPU request, and scaled back to 1 about 100–120 s after the load stopped.
7. **Memory keeps growing during a run.** Over the 8-minute HPA run the oldest Go Pod grew from 1,053 to 1,227 MiB (+174) and the oldest Python Pod from 1,101 to 1,191 MiB (+90). Neither had plateaued. Go's idle advantage disappears on longer runs, and I have not yet checked why (page cache of the mapped model is one candidate).

### Fault tolerance (Go): killing a worker mid-request

Run locally with Docker Compose (`docker-compose.go.yml`, 3 workers, `REQUEST_TTL_S=300`): start a 1,500-token generation, hard-kill the worker that received it with `docker kill`, wait 75 s for the group session timeout.

The victim received the request:

```
[worker-2] RECEIVED 150b4513-9fbc-4623-ae4c-ede42c609196 partition=0 offset=26
```

Another worker took over the partition and received the **same message** (same request ID, partition and offset):

```
[worker-3] ASSIGNED partitions [0]
[worker-3] RECEIVED 150b4513-9fbc-4623-ae4c-ede42c609196 partition=0 offset=26
```

The client's stream still ended cleanly, with the surviving worker's name:

```
event: done
data: worker-3
```

**Result:** the killed worker never committed, so Kafka redelivered the message to a surviving group member and the client received a complete response. As with the Python version, delivery is at-least-once: the second worker regenerates from the start, so a client can see repeated tokens.

### Problems found

- **Switching stacks:** Go workers failed to join the `inference-workers` group with `INCONSISTENT_GROUP_PROTOCOL` for about 45 s after Python workers were deleted, because the old members had not left the group. Wait about 60 s after swapping stacks.
- **Request TTL vs hard kills:** the default request deadline (55 s) leaves only about 10 s of margin over the consumer-group session timeout (about 45 s). After a hard kill, a redelivered request can therefore be skipped as `EXPIRED`. The cluster kill test did not capture a redelivery line; the local run below used `REQUEST_TTL_S=300` and did. Raise the TTL for fault-tolerance tests.
- **Failure semantics differ:** if `llama-server` dies mid-request, the Go worker reports `event: error` and commits instead of redelivering. A hard Pod kill (worker and sidecar together) still redelivers.
- **Small samples, one run each:** treat trends, not decimals, as results.
- **Node disk used** is shared by both stacks (both sets of images are on the node), so only image sizes are compared.

### Reproducing

```bash
# Go stack: swap the Deployments (same names and labels as the Python ones)
kubectl delete deployment router worker -n llm-platform
kubectl apply -f k8s/go/router-deployment.yaml -f k8s/go/worker-deployment.yaml

# experiments (results directory is set with OUT); skip C for Go
OUT=benchmarks/results_go bash benchmarks/run_experiments.sh A      # A E B D H
python benchmarks/plot.py benchmarks/results_go

# resource comparison: swaps stacks itself and prints the table above
bash benchmarks/compare_stacks.sh both

# kill the worker serving an in-flight request and check redelivery
bash benchmarks/kill_test.sh
```

## Known limitations

- **At-least-once, not exactly-once:** a request interrupted mid-generation is regenerated, and the client may see repeated tokens.
- **Partition-level imbalance:** a worker only serves its own partitions, so at low concurrency some workers idle while others have a backlog (finding 2).
- **Small samples:** each run completed 17–120 requests; there are no confidence intervals, and experiments such as the batch-window comparison are dominated by imbalance noise.
- **Single node:** all workers share 4 vCPUs, so scale-out cannot beat the node's CPU (finding 4).
- **Router results consumer:** it uses a fixed group ID and starts from the latest offset, so after a router restart early results can be missed, and multiple router replicas would need a unique group ID each.
- **Single broker, no replication:** a corrupted data volume (I hit this after force-stopping a VM) means wiping Redpanda and recreating topics.
- **Router streaming concurrency is probably capped** by the thread pool that runs the blocking SSE generator (not measured).
- **Batching is grouping only**, not true continuous batching.
- **Latency is total time, not time-to-first-token.**

## Next steps

1. **Reduce idle workers:** use more partitions than workers, or a pull-based shared queue, so work is spread evenly at low concurrency.
2. **Fix the router's results consumer:** unique group ID per instance, and don't report ready until partitions are assigned.
3. **Run across multiple nodes** to test scale-out where each worker gets its own cores.
4. **Autoscale on queue lag** (for example with KEDA), and calibrate `LAG_THRESHOLD` from measured throughput and a target wait.
5. **Capture survivor logs in the kill test** so redelivery is shown on the same run as the latency data.
6. **Repeat runs** to get usable error bars, and **measure time-to-first-token**.
7. **Try a real continuous-batching engine** (for example llama.cpp's server mode) and compare tokens/s.

## Reproducing the benchmarks

```bash
bash benchmarks/run_experiments.sh 
# A is worker scaling (1, 2, 3 workers)
# E is load levels (1, 6, 12, 24 users)
# C is batch window 0 vs 30 ms
# B is backpressure threshold 10 vs disabled
# D is kill a worker mid-run
# H is HPA scale-up and scale-down
python benchmarks/plot.py benchmarks/results_v3
```

Each run writes Locust CSVs plus a `*_top.txt` file with per-Pod CPU and memory samples to the results directory set by `OUT` in the runner. The runner expects an instance provisioned by `deploy/provision-ec2.sh`.

## Debugging notes

A few problems from building this that shaped the design:

- **A config knob that silently did nothing:** the manifest set `LLAMA_N_THREADS`, but the code read `N_THREADS` (default 4), so every worker ran four threads. The first benchmark showed throughput falling as workers were added; per-Pod CPU samples (a lone worker using 3.5 cores) pointed at the cause. After the fix, the same experiment scaled from 0.112 to 0.231 req/s across 1 to 3 workers. A one-line startup log of the effective thread count would have caught it before the first run.
- **Probe 404s caused a restart loop:** readiness/liveness probes pointed at paths the worker did not serve, so the kubelet kept killing healthy containers (exit code 0, `Completed`). Fixed by serving `/healthz/live` and `/healthz/ready`, the latter gated on model load.
- **Force-stopping a VM corrupted Redpanda's volume** (a Raft assertion on startup). Recovery was deleting the StatefulSet and PVC and recreating topics.
- **Silent "no data" in Grafana** came from a VM clock 71 minutes behind the laptop (the VM froze while the laptop slept), so Prometheus timestamps fell outside the query window.
- **A benchmark runner that stopped at the first failure:** Locust exits non-zero when requests fail, which killed the script under `set -e` and silently skipped later experiments.
