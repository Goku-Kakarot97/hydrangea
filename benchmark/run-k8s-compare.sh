#!/usr/bin/env bash
# Usage: benchmark/run-k8s-compare.sh [rounds=3]
# Needs k8s/compare-servers.yaml applied and the ConfigMaps from k8s/README.md. Runs the same load Job against Hydrangea and Netty
# servers (one running at a time, order alternating per round) and writes logs to benchmark/k8s-results/.
set -euo pipefail
NS=${NS:-default}          # the namespace the manifests of k8s/ were applied in
cd "$(dirname "$0")"
ROUNDS=${1:-3}
mkdir -p k8s-results; rm -f k8s-results/*.log
for r in $(seq 1 "$ROUNDS"); do
  if (( r % 2 == 1 )); then ORDER="hydrangea netty"; else ORDER="netty hydrangea"; fi
  for impl in $ORDER; do
    echo "== round $r: $impl"
    kubectl -n "$NS" scale deploy/bench-$impl --replicas=1 >/dev/null
    kubectl -n "$NS" rollout status deploy/bench-$impl --timeout=180s >/dev/null
    kubectl -n "$NS" get pod -l app=bench-$impl -o jsonpath='{.items[0].spec.nodeName}' > k8s-results/$impl-r$r.node
    job=bench-load-$impl-r$r
    sed -e "s/__NAME__/$job/" -e "s/__TARGET__/bench-$impl/" ../k8s/compare-load.yaml.tmpl | kubectl apply -f - >/dev/null
    kubectl -n "$NS" wait --for=condition=complete job/$job --timeout=600s >/dev/null
    kubectl -n "$NS" logs job/$job > k8s-results/$impl-r$r.log
    kubectl -n "$NS" delete job/$job >/dev/null
    kubectl -n "$NS" scale deploy/bench-$impl --replicas=0 >/dev/null
    kubectl -n "$NS" wait --for=delete pod -l app=bench-$impl --timeout=60s >/dev/null 2>&1 || true
  done
done
echo FINISHED
