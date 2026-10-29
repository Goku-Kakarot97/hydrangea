#!/usr/bin/env bash
# Usage: benchmark/run-k8s-grpc-compare.sh [rounds=3]
# Needs k8s/grpc-compare-servers.yaml applied plus the ConfigMaps hydrangea-gj-jars (hydrangea.jar, gbench.jar) and hydrangea-gj-urls (urls.txt).
# Runs the same load Job against the Hydrangea and grpc-java servers (0.5 CPU each, one running at a time, order alternating per round).
set -euo pipefail
NS=${NS:-default}          # the namespace the manifests of k8s/ were applied in
cd "$(dirname "$0")"
ROUNDS=${1:-3}
mkdir -p k8s-results/grpc; rm -f k8s-results/grpc/*.log
for r in $(seq 1 "$ROUNDS"); do
  if (( r % 2 == 1 )); then ORDER="hydrangea java"; else ORDER="java hydrangea"; fi
  for impl in $ORDER; do
    echo "== round $r: $impl"
    kubectl -n "$NS" scale deploy/bench-grpc-$impl --replicas=1 >/dev/null
    kubectl -n "$NS" rollout status deploy/bench-grpc-$impl --timeout=300s >/dev/null
    job=bench-grpc-load-$impl-r$r
    sed -e "s/__NAME__/$job/" -e "s/__TARGET__/bench-grpc-$impl/" ../k8s/grpc-compare-load.yaml.tmpl | kubectl apply -f - >/dev/null
    kubectl -n "$NS" wait --for=condition=complete job/$job --timeout=600s >/dev/null
    kubectl -n "$NS" logs job/$job > k8s-results/grpc/$impl-r$r.log
    kubectl -n "$NS" delete job/$job >/dev/null
    kubectl -n "$NS" scale deploy/bench-grpc-$impl --replicas=0 >/dev/null
    kubectl -n "$NS" wait --for=delete pod -l app=bench-grpc-$impl --timeout=90s >/dev/null 2>&1 || true
  done
done
echo FINISHED
