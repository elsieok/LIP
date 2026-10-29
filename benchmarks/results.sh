for N in 1 2 3; do
  ssh -i $KEY ubuntu@$IP "export KUBECONFIG=~/.kube/config; kubectl scale deployment worker -n llm-platform --replicas=$N; kubectl rollout status deployment/worker -n llm-platform"
  sleep 30
  locust -f benchmarks/locustfile.py --host "http://$IP" --headless -u 6 -r 1 -t 3m --csv "benchmarks/results/workers${N}_u6"
done