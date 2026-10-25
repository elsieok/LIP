#!/usr/bin/env bash
while true; do
  LAG=$(kubectl exec -n llm-platform redpanda-0 -- rpk group describe inference-workers -f json | jq '[.[] .lag] | add')
  if [ "$LAG" -gt 300 ]; then
    kubectl scale deployment worker -n llm-platform --replicas=6
  elif [ "$LAG" -lt 50 ]; then
    kubectl scale deployment worker -n llm-platform --replicas=2
  fi
  sleep 15
done