#!/usr/bin/env bash
# Compare the Python stack and the Go stack on the SAME k3s node: memory, CPU, image size on
# disk, Pod cold start, and throughput/latency under identical load.
#
#   bash benchmarks/compare_stacks.sh [py|go|both]      (default: both)
#
# env (all optional):
#   WORKERS   worker Pods for the load run   (default: node vCPUs - 1)
#   USERS     Locust users                   (default: 2 x WORKERS)
#   DURATION  Locust run time                (default: 3m)
#   OUT       results directory              (default: benchmarks/results_compare)
#
# Needs: the instance from deploy/provision-ec2.sh, k3s + Redpanda running, the four images
# imported (llm-router, llm-worker, llm-router-go, llm-worker-go), and k8s/ (including
# k8s/go/) copied to ~/k8s on the instance.
set -euo pipefail

KEY=${KEY:-$HOME/.ssh/llm-platform-key.pem}
IP=$(cat deploy/.instance_ip)
OUT=${OUT:-benchmarks/results_compare}
SSH="ssh -o ServerAliveInterval=30 -i $KEY ubuntu@$IP"
rk() { $SSH "export KUBECONFIG=~/.kube/config; $*"; }

CPUS=$(rk "nproc" | tail -1)
WORKERS=${WORKERS:-$(( CPUS > 1 ? CPUS - 1 : 1 ))}
USERS=${USERS:-$(( WORKERS * 2 ))}
DURATION=${DURATION:-3m}
if [ "$WORKERS" -ge "$CPUS" ]; then
  echo "WARNING: $WORKERS workers on $CPUS vCPUs. Each worker Pod requests ~1 CPU, so some may stay Pending." >&2
fi

wait_ready() {
  rk "kubectl rollout status deployment/worker -n llm-platform --timeout=300s"
  rk "kubectl rollout status deployment/router -n llm-platform --timeout=120s"
}

# Replace the running router/worker Deployments with the python (k8s/) or go (k8s/go/) ones.
use_stack() {
  local dir=k8s
  [ "$1" = go ] && dir=k8s/go
  rk "kubectl delete deployment router worker -n llm-platform --ignore-not-found"
  rk "kubectl apply -f ~/$dir/router-deployment.yaml -f ~/$dir/worker-deployment.yaml"
  wait_ready
}

# Fresh topics, fresh Pods, empty queue. $1 = number of workers.  (same as run_experiments.sh)
reset() {
  rk "kubectl scale deployment worker -n llm-platform --replicas=0"
  rk "kubectl exec -n llm-platform redpanda-0 -- rpk topic delete inference-requests inference-results || true"
  rk "kubectl exec -n llm-platform redpanda-0 -- rpk topic create inference-requests --partitions 3 --replicas 1"
  rk "kubectl exec -n llm-platform redpanda-0 -- rpk topic create inference-results --partitions 3 --replicas 1"
  rk "kubectl rollout restart deployment/router -n llm-platform"
  rk "kubectl scale deployment worker -n llm-platform --replicas=$1"
  wait_ready
  sleep 45
}

# Seconds from "scale 0 -> 1" until the worker Pod is Ready (image already local, model file warm).
cold_start() {
  rk "kubectl scale deployment worker -n llm-platform --replicas=0 >/dev/null; kubectl wait --for=delete pod -l app=worker -n llm-platform --timeout=120s >/dev/null 2>&1 || true"
  rk "s=\$(date +%s); kubectl scale deployment worker -n llm-platform --replicas=1 >/dev/null; kubectl rollout status deployment/worker -n llm-platform --timeout=300s >/dev/null; e=\$(date +%s); echo \$((e - s))" | tail -1
}

# Background sampler: per-container CPU/memory (kubectl top) + node memory. $1 = file, $2 = interval s
SAMPLER=""
start_sampler() {
  ( while true; do
      echo "== $(date +%s)"
      $SSH 'export KUBECONFIG=~/.kube/config; kubectl top pods -n llm-platform --containers --no-headers; free -m | sed -n 2p' || true
      sleep "$2"
    done ) > "$1" 2>&1 &
  SAMPLER=$!
}
stop_sampler() { kill "$SAMPLER" 2>/dev/null || true; }

run_stack() {
  local s=$1 d="$OUT/$1"
  mkdir -p "$d"
  echo "=============== stack: $s  (workers=$WORKERS users=$USERS duration=$DURATION) ==============="

  use_stack "$s"
  echo "cold_start_s=$(cold_start)" > "$d/meta.txt"
  reset "$WORKERS"

  # disk: image sizes as the node sees them (the summary picks out this stack's images)
  rk "sudo k3s crictl images" > "$d/images.txt" || true
  echo "node_disk_used_mb=$(rk "df -m --output=used / | tail -1" | tr -d ' ' | tail -1)" >> "$d/meta.txt"

  # idle memory/CPU (workers up, no traffic)
  start_sampler "$d/idle_top.txt" 15
  sleep 50
  stop_sampler

  # identical load
  start_sampler "$d/load_top.txt" 15
  locust -f benchmarks/locustfile.py --host "http://$IP" --headless \
    -u "$USERS" -r 1 -t "$DURATION" --csv "$d/load" --only-summary || true
  stop_sampler

  echo "redpanda_data_mb=$(rk "kubectl exec -n llm-platform redpanda-0 -- du -sm /var/lib/redpanda/data" | awk '{print $1}' | tail -1)" >> "$d/meta.txt"
  echo "workers=$WORKERS users=$USERS duration=$DURATION" >> "$d/meta.txt"
}

case "${1:-both}" in
  py)   run_stack py ;;
  go)   run_stack go ;;
  both) run_stack py; run_stack go ;;
  *)    echo "usage: $0 [py|go|both]"; exit 1 ;;
esac

python3 benchmarks/compare_summary.py "$OUT"
