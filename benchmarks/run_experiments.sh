#!/usr/bin/env bash
set -euo pipefail

KEY=~/.ssh/llm-platform-key.pem
IP=$(cat deploy/.instance_ip)
OUT=benchmarks/results_v3
mkdir -p "$OUT"
SSH="ssh -o ServerAliveInterval=30 -i $KEY ubuntu@$IP"

rk() { $SSH "export KUBECONFIG=~/.kube/config; $*"; }

wait_ready() {
  rk "kubectl rollout status deployment/worker -n llm-platform --timeout=300s"
  rk "kubectl rollout status deployment/router -n llm-platform --timeout=120s"
}

# Fresh topics, fresh Pods, empty queue. $1 = number of workers.
reset() {
  rk "kubectl scale deployment worker -n llm-platform --replicas=0"
  rk "kubectl exec -n llm-platform redpanda-0 -- rpk topic delete inference-requests inference-results || true"
  rk "kubectl exec -n llm-platform redpanda-0 -- rpk topic create inference-requests --partitions 3 --replicas 1"
  rk "kubectl exec -n llm-platform redpanda-0 -- rpk topic create inference-results --partitions 3 --replicas 1"
  rk "kubectl rollout restart deployment/router -n llm-platform"
  rk "kubectl scale deployment worker -n llm-platform --replicas=$1"
  wait_ready
  sleep 45   # consumers join their groups, rebalance settles, model pages in
}

# name users spawn_rate duration; also samples per-pod CPU and memory every 15 s
run() {
  local name=$1 users=$2 rate=$3 dur=$4
  ( while true; do
      echo "== $(date +%s)"
      $SSH "export KUBECONFIG=~/.kube/config; kubectl top pods -n llm-platform --no-headers; free -m | sed -n 2p" || true
      sleep 15
    done ) > "$OUT/${name}_top.txt" 2>&1 &
  local sampler=$!
  locust -f benchmarks/locustfile.py --host "http://$IP" --headless \
    -u "$users" -r "$rate" -t "$dur" --csv "$OUT/$name" --only-summary || true
  kill "$sampler" 2>/dev/null || true
}

case "${1:-}" in
  A)  # worker scaling
    for N in 1 2 3; do reset $N; run A_workers$N 6 1 3m; done ;;
  E)  # load levels, 3 workers
    for U in 1 6 12 24; do reset 3; run E_users$U $U 2 3m; done ;;
  C)  # batch window off vs on
    for W in 0 30; do
      rk "kubectl set env deployment/worker BATCH_WINDOW_MS=$W -n llm-platform"
      reset 3; run C_batch$W 6 1 3m
    done
    rk "kubectl set env deployment/worker BATCH_WINDOW_MS=30 -n llm-platform" ;;
  B)  # backpressure on (threshold 10) vs effectively off
    for T in 10 1000000000; do
      rk "kubectl set env deployment/router LAG_THRESHOLD=$T -n llm-platform"
      reset 3; run B_lag$T 40 2 3m
    done
    rk "kubectl set env deployment/router LAG_THRESHOLD=200 -n llm-platform" ;;
  D)  # kill a worker mid-run
    reset 3
    run D_kill 6 1 5m &
    LPID=$!
    sleep 120
    VICTIM=$(rk "kubectl get pods -n llm-platform -l app=worker -o jsonpath='{.items[0].metadata.name}'")
    echo "kill_epoch=$(date +%s) victim=$VICTIM" | tee "$OUT/D_kill_event.txt"
    rk "kubectl logs -n llm-platform $VICTIM --tail=-1" > "$OUT/D_victim.log" || true
    rk "kubectl delete pod -n llm-platform $VICTIM --grace-period=0 --force"
    wait $LPID ;;
  H)  # HPA: start at 1 worker, sustained load, capture scale-up and scale-down
    rk "kubectl delete hpa --all -n llm-platform || true"
    reset 1
    rk "kubectl apply -f ~/k8s/hpa.yaml"
    ( $SSH 'export KUBECONFIG=~/.kube/config; while true; do echo "$(date +%s) $(kubectl get hpa worker-hpa -n llm-platform --no-headers)"; sleep 10; done' > "$OUT/hpa_watch.txt" ) &
    WPID=$!
    run H_hpa 12 2 8m
    sleep 300   # keep watching after load stops so scale-down is captured
    kill $WPID 2>/dev/null || true
    rk "kubectl delete hpa --all -n llm-platform || true" ;;
  *) echo "usage: $0 {A|E|C|B|D|H}"; exit 1 ;;
esac