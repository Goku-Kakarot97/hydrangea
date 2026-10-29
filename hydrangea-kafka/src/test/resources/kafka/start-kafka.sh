#!/usr/bin/env bash
# One Kafka 3.9 broker (KRaft) for the tests of hydrangea-kafka: PLAINTEXT on 127.0.0.1:29092, SASL/PLAIN on 29094
# (user alice / alicepw). Kafka needs the advertised port to equal the published port, hence 29092:29092.
set -e
NAME=${NAME:-hydrangea-kafka-test}
docker rm -f $NAME >/dev/null 2>&1 || true
docker run -d --name $NAME -p 29092:29092 -p 29094:29094 \
  -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://:29092,SASL://:29094,CONTROLLER://:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:29092,SASL://127.0.0.1:29094 \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,SASL:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_SASL_ENABLED_MECHANISMS=PLAIN \
  -e KAFKA_LISTENER_NAME_SASL_PLAIN_SASL_JAAS_CONFIG='org.apache.kafka.common.security.plain.PlainLoginModule required user_alice="alicepw";' \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=false \
  apache/kafka:3.9.0
for i in $(seq 1 60); do
  if docker exec $NAME /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:29092 >/dev/null 2>&1; then echo "kafka up"; exit 0; fi
  sleep 1
done
echo "kafka did not start"; docker logs $NAME | tail -20; exit 1
