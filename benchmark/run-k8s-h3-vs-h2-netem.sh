#!/usr/bin/env bash
# Needs k8s/h3-vs-h2-netem.yaml applied (ConfigMaps hydrangea-cmp-jars/-certs/-load). For each network condition, `tc netem` is set on the egress of the client and of both
# servers (so delay and loss apply in both directions), then the same Python client runs HTTP/2 and HTTP/3, twice with the order swapped. Results: k8s-results/h3-netem/results.jsonl
set -uo pipefail
cd "$(dirname "$0")"
CTX=${CTX:-$(kubectl config current-context)}
k() { kubectl --context "$CTX" -n "${NS:-default}" "$@"; }
OUT=k8s-results/h3-netem; mkdir -p "$OUT"; : > "$OUT/results.jsonl"
H2=bench-cmp-h2.iris.svc.cluster.local; H3=bench-cmp-h3.iris.svc.cluster.local
setnet() { for d in bench-cmp-client bench-cmp-h2 bench-cmp-h3; do k exec deploy/$d -- sh -c "tc qdisc del dev eth0 root 2>/dev/null; [ -z '$1' ] || tc qdisc add dev eth0 root netem $1"; done; }
run() { # scenario rep mode
  local host=$H2 port=8443; [ "$3" = h3 ] && host=$H3 && port=4433
  for what in "streams 10 rounds 15 size 100000" "connect 12"; do
    line=$(k exec deploy/bench-cmp-client -- python3 /load/cmp_client.py "$3" "$host" "$port" $what 2>&1 | tail -1)
    echo "{\"scenario\":\"$1\",\"rep\":$2,\"result\":$line}" 2>/dev/null | tee -a "$OUT/results.jsonl"
  done
}
trap 'setnet ""' EXIT
echo "== warm-up (not recorded)"; setnet ""
for m in h2 h3; do host=$H2; port=8443; [ $m = h3 ] && host=$H3 && port=4433; k exec deploy/bench-cmp-client -- python3 /load/cmp_client.py $m $host $port streams 10 rounds 20 size 100000 >/dev/null 2>&1; done
for sc in "base:" "rtt50:delay 25ms" "loss1:delay 25ms loss 1%" "loss3:delay 25ms loss 3%"; do
  name=${sc%%:*}; args=${sc#*:}
  echo "== $name ($args)"; setnet "$args"
  for rep in 1 2; do
    if [ $rep = 1 ]; then order="h2 h3"; else order="h3 h2"; fi
    for m in $order; do run "$name" $rep $m; done
  done
done
echo FINISHED
