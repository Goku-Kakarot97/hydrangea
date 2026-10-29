#!/bin/sh
# Hydrangea AMQP client vs the official com.rabbitmq:amqp-client against one RabbitMQ container.
#   sh benchmark/amqp-bench.sh [rounds]        results land in benchmark/amqp-results/, summary: python3 benchmark/aggregate-amqp.py
# Needs: the broker from hydrangea-amqp/src/test/resources/amqp/start-broker.sh, a compiled project (mvn compile), and amqp-client + slf4j-api in ~/.m2.
set -e
cd "$(dirname "$0")/.."
ROUNDS=${1:-3}
M=$HOME/.m2/repository
CP=target/classes:$M/com/rabbitmq/amqp-client/5.26.0/amqp-client-5.26.0.jar:$M/org/slf4j/slf4j-api/2.0.17/slf4j-api-2.0.17.jar
OUT=benchmark/amqp-results
mkdir -p "$OUT" benchmark/out-amqp
javac -cp "$CP" -d benchmark/out-amqp benchmark/src/amqp/AmqpBench.java

# name|scenario|args
SCENARIOS='publish-noconfirm|publish-noconfirm|producers=2 size=128 seconds=10 warmup=3
publish-confirm|publish-confirm|producers=2 window=1000 size=128 seconds=10 warmup=3
publish-confirm-64k|publish-confirm|producers=1 window=16 size=65536 seconds=10 warmup=3
consume|consume|messages=300000 consumers=2 prefetch=250 size=128
latency|latency|samples=20000 warmup=3000 size=128
connections-100|connections|connections=100 window=10 drivers=4 size=128 seconds=10 warmup=3'

run_one() { # round lib name scenario args
  f="$OUT/r$1-$2-$3.log"
  ( while :; do docker stats --no-stream --format 'BROKERCPU {{.CPUPerc}}' hydrangea-amqp-test; done ) > "$f.cpu" 2>/dev/null &
  sampler=$!
  java -cp "benchmark/out-amqp:$CP" amqp.AmqpBench "$2" "$4" $5 > "$f" 2>&1 || echo "FAILED" >> "$f"
  kill $sampler 2>/dev/null || true
  wait $sampler 2>/dev/null || true
  sleep 3
}

r=1
while [ "$r" -le "$ROUNDS" ]; do
  if [ $((r % 2)) -eq 1 ]; then LIBS="hydrangea official"; else LIBS="official hydrangea"; fi
  echo "$SCENARIOS" | while IFS='|' read -r name scenario sargs; do
    for lib in $LIBS; do
      echo "round $r  $name  $lib"
      run_one "$r" "$lib" "$name" "$scenario" "$sargs"
    done
  done
  r=$((r + 1))
done
