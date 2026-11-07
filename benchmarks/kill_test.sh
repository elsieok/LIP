#!/usr/bin/env bash
# Kill the worker that is serving an in-flight request and show it being redelivered.
# Works for either stack (Python or Go): both log the same RECEIVED/ASSIGNED/REVOKED lines.
#
#   bash benchmarks/kill_test.sh [max_tokens]     (default 1500; needs >= 2 running worker Pods,
#                                                  and no other traffic while it runs)
set -euo pipefail

KEY=${KEY:-$HOME/.ssh/llm-platform-key.pem}
IP=$(cat deploy/.instance_ip)
OUT=${OUT:-benchmarks/results_kill}
TOKENS=${1:-1500}
mkdir -p "$OUT"
SSH="ssh -o ServerAliveInterval=30 -i $KEY ubuntu@$IP"
rk() { $SSH "export KUBECONFIG=~/.kube/config; $*"; }
# worker-container logs of every worker Pod, prefixed with [pod/<name>/worker]
wlogs() { rk "kubectl logs -n llm-platform -l app=worker -c worker --prefix --tail=-1 --since=$1"; }

RUNNING=$(rk "kubectl get pods -n llm-platform -l app=worker --field-selector=status.phase=Running --no-headers | wc -l" | tail -1)
if [ "$RUNNING" -lt 2 ]; then
  echo "Need at least 2 running worker Pods (found $RUNNING)." >&2
  exit 1
fi

echo "Starting a $TOKENS-token generation..."
curl -sN -X POST "http://$IP/generate" -H "Content-Type: application/json" \
  -d "{\"prompt\":\"Write a very long, detailed story about a lighthouse keeper.\",\"max_tokens\":$TOKENS}" \
  > "$OUT/client.log" &
CURL=$!
sleep 8

LINE=$(wlogs 30s | grep RECEIVED | tail -1 || true)
if [ -z "$LINE" ]; then
  echo "No RECEIVED line in the last 30 s; is the request reaching a worker?" >&2
  kill "$CURL" 2>/dev/null || true
  exit 1
fi
VICTIM=$(echo "$LINE" | sed -E 's#^\[pod/([^/]+)/.*#\1#')
REQ=$(echo "$LINE" | grep -oE '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}')
echo "Victim: $VICTIM   request: $REQ"
echo "$LINE" | tee "$OUT/victim_received.txt"

rk "kubectl logs -n llm-platform $VICTIM -c worker --tail=-1" > "$OUT/victim.log" || true

KILL_AT=$(date +%s)
echo "Force-deleting $VICTIM (no graceful shutdown, so it never leaves the consumer group)..."
rk "kubectl delete pod -n llm-platform $VICTIM --grace-period=0 --force"

echo "Waiting for another worker to pick up the same request (group session timeout ~45 s)..."
FOUND=""
for _ in $(seq 1 36); do
  sleep 5
  FOUND=$(wlogs 10m | grep "RECEIVED $REQ" || true)
  [ -n "$FOUND" ] && break
done
ELAPSED=$(( $(date +%s) - KILL_AT ))

echo
echo "=== Consumer-group events ==="
wlogs 5m | grep -E 'ASSIGNED|REVOKED' | tee "$OUT/group_events.txt" || true
echo
if [ -n "$FOUND" ]; then
  echo "=== REDELIVERED after ~${ELAPSED}s ==="
  echo "$FOUND" | tee "$OUT/redelivered.txt"
else
  echo "=== NOT redelivered within ${ELAPSED}s. Check: kubectl logs -l app=worker -c worker ===" >&2
fi

echo
echo "Waiting for the client stream to finish (Ctrl-C to skip)..."
wait "$CURL" || true
echo "token events received (includes any repeated tokens): $(grep -c '^event: token' "$OUT/client.log" || true)"
if grep -q '^event: done' "$OUT/client.log"; then
  echo "Client stream finished:"; tail -n 3 "$OUT/client.log"
else
  echo "Client stream ended WITHOUT 'event: done'. The router's 60 s no-token timeout probably fired"
  echo "before redelivery finished; the RECEIVED lines above still show redelivery happened."
fi
