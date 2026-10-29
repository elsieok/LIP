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

```bash
docker compose up -d --build
./kafka/create_topics.sh
docker compose logs -f worker      # each worker logs its consumer-group membership
```

In another terminal:

```bash
curl -N -X POST localhost:8000/generate -H "Content-Type: application/json" \
  -d '{"prompt": "Explain partitions in one sentence."}'
```

You should see `event: token` lines stream in, ending with `event: done` whose data is the worker that served the request. Run it several times to see different workers.

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
bash benchmarks/run_experiments.sh A    # worker scaling (1, 2, 3 workers)
bash benchmarks/run_experiments.sh E    # load levels (1, 6, 12, 24 users)
bash benchmarks/run_experiments.sh C    # batch window 0 vs 30 ms
bash benchmarks/run_experiments.sh B    # backpressure threshold 10 vs disabled
bash benchmarks/run_experiments.sh D    # kill a worker mid-run
bash benchmarks/run_experiments.sh H    # HPA scale-up and scale-down
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
