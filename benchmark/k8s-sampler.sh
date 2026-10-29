#!/usr/bin/env bash
NS=${NS:-default}          # the namespace the manifests of k8s/ were applied in
# Usage: benchmark/k8s-sampler.sh out.csv [interval=120]   pod CPU / memory and restart counts of the hydrangea-bench pods, until killed
OUT=$1; EVERY=${2:-120}
echo "time,pod,cpu_m,mem_mi,restarts" > "$OUT"
while true; do
  T=$(date -u +%H:%M:%S)
  kubectl -n "$NS" top pod -l app.kubernetes.io/part-of=hydrangea-bench --no-headers 2>/dev/null | while read -r pod cpu mem; do
    R=$(kubectl -n "$NS" get pod "$pod" -o jsonpath='{.status.containerStatuses[0].restartCount}' 2>/dev/null)
    echo "$T,$pod,${cpu%m},${mem%Mi},$R" >> "$OUT"
  done
  sleep "$EVERY"
done
