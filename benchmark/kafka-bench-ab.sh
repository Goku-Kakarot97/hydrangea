#!/usr/bin/env bash
# Controlled A/B of produce throughput: Hydrangea and kafka-clients, idempotent (IDEM) and not (FAST), strictly alternating, with a pause before every run so that the
# broker has finished deleting the previous topic. Prints one line per run: "<h|k>-<IDEM|FAST> records/s p99_ms client_cpu_s_per_100k".
# usage: CLASSES=<compiled core + kafka> benchmark/kafka-bench-ab.sh KAFKA_JARS_DIR [rounds]
set -u
cd "$(dirname "$0")/.."
J=$1; ROUNDS=${2:-8}
CLASSES=${CLASSES:?set CLASSES}
CP="benchmark/out-kafka:$CLASSES:$J/kafka-clients-3.9.0.jar:$J/slf4j-api-1.7.36.jar"
IDEM="acks=-1 idempotent=true inflight=5"; FAST="acks=1 idempotent=false inflight=5"
for r in $(seq 1 $ROUNDS); do
  if [ $((r % 2)) = 1 ]; then seq="h-IDEM k-IDEM h-FAST k-FAST"; else seq="k-FAST h-FAST k-IDEM h-IDEM"; fi
  for v in $seq; do
    case $v in h-*) lib=hydrangea;; k-*) lib=kafka-clients;; esac
    case $v in *IDEM) cfg=$IDEM;; *FAST) cfg=$FAST;; esac
    sleep 6
    echo -n "$v "
    java -Xmx2g -cp "$CP" kafka.KafkaBench $lib produce size=100 partitions=3 $cfg seconds=8 warmup=3 2>&1 | grep RESULT | sed 's/.* per_s=\([0-9.]*\) .*p99_ms=\([0-9.]*\) client_cpu_s_per_100k=\([0-9.]*\).*/\1 \2 \3/'
  done
done
