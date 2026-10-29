#!/usr/bin/env bash
set -euo pipefail
curl -sfL https://get.k3s.io | sh -
sudo k3s kubectl get nodes
mkdir -p ~/models
# scp your GGUF file here separately: scp -i key.pem models/*.gguf ubuntu@<ip>:~/models/