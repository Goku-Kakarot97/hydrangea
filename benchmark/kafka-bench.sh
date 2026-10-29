#!/usr/bin/env bash
# Hydrangea Kafka client vs kafka-clients 3.9.0 against one broker (apache/kafka 3.9.0 in Docker, hydrangea-kafka-test, 127.0.0.1:29092).
# usage: CLASSES=<dir with compiled hydrangea-core + hydrangea-kafka> benchmark/kafka-bench.sh KAFKA_JARS_DIR OUT_DIR [rounds]
# KAFKA_JARS_DIR holds kafka-clients-3.9.0.jar and slf4j-api-1.7.36.jar; benchmark/out-kafka holds the compiled KafkaBench.
# Three producer configurations, the same for both libraries:
#   IDEM   idempotent, acks=-1, 5 batches in flight per partition (the default of both clients)
#   FAST   not idempotent, acks=1, 5 in flight (a retry may reorder or duplicate; what the Java client did before 3.0)
#   SAFE   not idempotent, acks=1, 1 in flight (a retry cannot overtake)
set -u
cd "$(dirname "$0")/.."
J=$1; OUT=$2; ROUNDS=${3:-3}
CLASSES=${CLASSES:?set CLASSES}
CP="benchmark/out-kafka:$CLASSES:$J/kafka-clients-3.9.0.jar:$J/slf4j-api-1.7.36.jar"
mkdir -p "$OUT"
run() { java -Xmx2g -cp "$CP" kafka.KafkaBench "$@" 2>&1 | grep -E '^RESULT' || echo "FAILED $*"; }
IDEM="acks=-1 idempotent=true inflight=5"
FAST="acks=1 idempotent=false inflight=5"
SAFE="acks=1 idempotent=false inflight=1"
LIBS=(hydrangea kafka-clients)
for r in $(seq 1 $ROUNDS); do
  f="$OUT/round$r.txt"; : > "$f"
  order=("${LIBS[@]:$(((r-1)%2)):2}" "${LIBS[@]:0:$(((r-1)%2))}")
  for lib in "${order[@]}"; do
    echo "== $lib round $r"
    { run $lib connect samples=30 $IDEM
      run $lib produce size=100 partitions=3 $IDEM seconds=10 warmup=3
      run $lib produce size=100 partitions=1 $IDEM seconds=10 warmup=3
      run $lib produce size=1024 partitions=3 $IDEM seconds=10 warmup=3
      run $lib produce size=100 partitions=3 $FAST seconds=10 warmup=3
      run $lib produce size=100 partitions=3 $SAFE seconds=10 warmup=3
      run $lib produce-threads threads=1000 $IDEM seconds=10 warmup=3
      run $lib produce-threads threads=1000 $FAST seconds=10 warmup=3
      run $lib consume size=100 records=3000000
      run $lib consume size=1024 records=500000
      run $lib consume-group size=100 records=3000000
      run $lib e2e samples=3000 $IDEM
    } >> "$f"
  done
done
