#!/bin/sh
# Hydrangea MQTT client vs Eclipse Paho (as it ships, and with TCP_NODELAY set) against one Mosquitto container.
#   sh benchmark/mqtt-bench.sh [rounds]        (ONLY="publish-qos1 publish-qos2" runs just those)
#   GROUP=v3 (default)  MQTT 3.1.1 over TCP,        Paho org.eclipse.paho.client.mqttv3
#   GROUP=v5            MQTT 5 over TCP,            Paho org.eclipse.paho.mqttv5.client
#   GROUP=v5ws          MQTT 5 over WebSocket,      the same Paho (ws://host:9001/mqtt)
#          results land in benchmark/mqtt-results/, summary: python3 benchmark/aggregate-mqtt.py
# Needs: the broker from hydrangea-mqtt/src/test/resources/mqtt/start-broker.sh, a built project (mvn compile: target/classes at the root), and
# Paho 1.2.5 (mqttv3, and mqttv5 for the other groups) in ~/.m2; the mqttv5 jar is downloaded from Maven Central if it is missing. The client JVM runs in a second container that shares the broker's network namespace (loopback inside the Docker VM, Linux),
# because through Docker Desktop's port forwarding on macOS a one-message-at-a-time exchange stalls for 40 ms now and then (Nagle/delayed ACK in
# the forwarder), which says nothing about either library.
set -e
cd "$(dirname "$0")/.."
ROUNDS=${1:-3}
GROUP=${GROUP:-v3}
PAHO=$HOME/.m2/repository/org/eclipse/paho/org.eclipse.paho.client.mqttv3/1.2.5/org.eclipse.paho.client.mqttv3-1.2.5.jar
PAHO5=$HOME/.m2/repository/org/eclipse/paho/org.eclipse.paho.mqttv5.client/1.2.5/org.eclipse.paho.mqttv5.client-1.2.5.jar
if [ ! -f "$PAHO5" ]; then
  mkdir -p "$(dirname "$PAHO5")"
  curl -sSf -o "$PAHO5" https://repo.maven.apache.org/maven2/org/eclipse/paho/org.eclipse.paho.mqttv5.client/1.2.5/org.eclipse.paho.mqttv5.client-1.2.5.jar
fi
case "$GROUP" in
  v3)   H=hydrangea;     P=paho;     N=paho-nodelay;     W1=1000 ;;
  v5)   H=hydrangea5;    P=paho5;    N=paho5-nodelay;    W1=100 ;;   # an MQTT 5 broker's receive maximum (Mosquitto: 100) caps the QoS 1/2 window
  v5ws) H=hydrangea5-ws; P=paho5-ws; N=paho5-ws-nodelay; W1=100 ;;
  *) echo "GROUP must be v3, v5 or v5ws"; exit 1 ;;
esac
OUT=${OUT:-benchmark/mqtt-results}
mkdir -p "$OUT" benchmark/out-mqtt
javac --release 21 -cp "target/classes:$PAHO:$PAHO5" -d benchmark/out-mqtt benchmark/src/mqtt/MqttBench.java
docker rm -f hydrangea-mqtt-client >/dev/null 2>&1 || true
docker create --name hydrangea-mqtt-client --network container:hydrangea-mqtt-test --entrypoint sleep eclipse-temurin:21-jre 86400 >/dev/null
docker cp target/classes hydrangea-mqtt-client:/classes
docker cp benchmark/out-mqtt hydrangea-mqtt-client:/bench
docker cp "$PAHO" hydrangea-mqtt-client:/paho.jar
docker cp "$PAHO5" hydrangea-mqtt-client:/paho5.jar
docker start hydrangea-mqtt-client >/dev/null
trap 'docker rm -f hydrangea-mqtt-client >/dev/null 2>&1' EXIT

# name|scenario|args
SCENARIOS="publish-qos0|publish|qos=0 producers=2 window=1000 size=128 seconds=10 warmup=3
publish-qos1|publish|qos=1 producers=2 window=$W1 system=2000 size=128 seconds=10 warmup=3
publish-qos1-64k|publish|qos=1 producers=1 window=16 system=64 size=65536 seconds=10 warmup=3
publish-qos2|publish|qos=2 producers=2 window=100 system=400 size=128 seconds=10 warmup=3
latency-qos0|latency|qos=0 samples=20000 warmup=3000 size=128
latency-qos1|latency|qos=1 samples=20000 warmup=3000 size=128
connections-100|connections|connections=100 window=10 drivers=4 size=128 seconds=10 warmup=3"

run_one() { # round lib name scenario args
  f="$OUT/r$1-$2-$3.log"
  ( while :; do docker stats --no-stream --format 'BROKERCPU {{.CPUPerc}}' hydrangea-mqtt-test; done ) > "$f.cpu" 2>/dev/null &
  sampler=$!
  docker exec hydrangea-mqtt-client java -cp /bench:/classes:/paho.jar:/paho5.jar mqtt.MqttBench "$2" "$4" port=1883 wsport=9001 $5 > "$f" 2>&1 || echo "FAILED" >> "$f"
  kill $sampler 2>/dev/null || true
  wait $sampler 2>/dev/null || true
  sleep 3
}

r=1
while [ "$r" -le "$ROUNDS" ]; do
  case $((r % 3)) in
    1) LIBS="$H $P $N" ;;
    2) LIBS="$P $N $H" ;;
    0) LIBS="$N $H $P" ;;
  esac
  echo "$SCENARIOS" | while IFS='|' read -r name scenario sargs; do
    [ -n "$ONLY" ] && case " $ONLY " in *" $name "*) ;; *) continue ;; esac
    for lib in $LIBS; do
      echo "round $r  $name  $lib"
      run_one "$r" "$lib" "$name" "$scenario" "$sargs"
    done
  done
  r=$((r + 1))
done
