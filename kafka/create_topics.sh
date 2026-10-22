#!/usr/bin/env bash
set -euo pipefail
docker compose exec redpanda rpk topic create inference-requests --partitions 3 --replicas 1
docker compose exec redpanda rpk topic create inference-results --partitions 3 --replicas 1
docker compose exec redpanda rpk topic list