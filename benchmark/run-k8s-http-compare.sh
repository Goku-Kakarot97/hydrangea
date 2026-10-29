#!/usr/bin/env bash
# Usage: IMPLS="hydrangea utuned udispatch" benchmark/run-k8s-http-compare.sh [rounds=3]
# Needs a k8s/http-compare-servers*.yaml applied (one Deployment bench-http-<impl> per implementation, replicas 0) and the ConfigMaps hydrangea-ut-jars and hydrangea-ut-urls.
# Runs the same load Job against each implementation (0.5 CPU each, one running at a time); the order rotates every round.
set -euo pipefail
NS=${NS:-default}          # the namespace the manifests of k8s/ were applied in
cd "$(dirname "$0")"
ROUNDS=${1:-3}
read -r -a ALL <<< "${IMPLS:-hydrangea undertow}"
OUTDIR=${OUTDIR:-k8s-results/http}
PREFIX=${PREFIX:-bench-http}                       # Deployments, Services and Jobs are named $PREFIX-<impl>
TEMPLATE=${TEMPLATE:-http-compare-load.yaml.tmpl}  # in k8s/
mkdir -p "$OUTDIR"; rm -f "$OUTDIR"/*.log
for r in $(seq 1 "$ROUNDS"); do
  for k in $(seq 0 $(( ${#ALL[@]} - 1 ))); do
    impl=${ALL[$(( (k + r - 1) % ${#ALL[@]} ))]}
    echo "== round $r: $impl"
    kubectl -n "$NS" scale deploy/$PREFIX-$impl --replicas=1 >/dev/null
    kubectl -n "$NS" rollout status deploy/$PREFIX-$impl --timeout=300s >/dev/null
    job=$PREFIX-load-$impl-r$r
    sed -e "s/__NAME__/$job/" -e "s/__TARGET__/$PREFIX-$impl/" ../k8s/$TEMPLATE | kubectl apply -f - >/dev/null
    kubectl -n "$NS" wait --for=condition=complete job/$job --timeout=600s >/dev/null
    kubectl -n "$NS" logs job/$job > "$OUTDIR/$impl-r$r.log"
    kubectl -n "$NS" delete job/$job >/dev/null
    kubectl -n "$NS" scale deploy/$PREFIX-$impl --replicas=0 >/dev/null
    kubectl -n "$NS" wait --for=delete pod -l app=$PREFIX-$impl --timeout=90s >/dev/null 2>&1 || true
  done
done
echo FINISHED
