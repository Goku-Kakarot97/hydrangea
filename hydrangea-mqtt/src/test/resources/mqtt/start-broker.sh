#!/bin/sh
# Mosquitto for the MQTT tests, on 127.0.0.1:
#   21883  anonymous, plain
#   21884  user alice / password secret, and an ACL: alice may read and write only "allowed/#"
#   28883  anonymous, TLS (certificates next to this script)
#   29001  anonymous, MQTT over WebSocket
# Remove with: docker rm -f hydrangea-mqtt-test
set -e
cd "$(dirname "$0")"
W=$(mktemp -d)
docker run --rm eclipse-mosquitto:2 sh -c 'mosquitto_passwd -b -c /tmp/p alice secret >/dev/null 2>&1; cat /tmp/p' > "$W/passwd"
printf 'user alice\ntopic readwrite allowed/#\n' > "$W/acl"
cp ca.pem server.pem server.key "$W/"
cat > "$W/mosquitto.conf" <<X
per_listener_settings true
persistence false
max_queued_messages 1000000
max_inflight_messages 100
message_size_limit 100000000
log_type error
log_type warning

listener 1883
allow_anonymous true

listener 1884
allow_anonymous false
password_file /mosquitto/config/passwd
acl_file /mosquitto/config/acl

listener 8883
allow_anonymous true
cafile /mosquitto/config/ca.pem
certfile /mosquitto/config/server.pem
keyfile /mosquitto/config/server.key

listener 9001
protocol websockets
allow_anonymous true
X
chmod 644 "$W"/*
docker rm -f hydrangea-mqtt-test >/dev/null 2>&1 || true
docker create --name hydrangea-mqtt-test -p 127.0.0.1:21883:1883 -p 127.0.0.1:21884:1884 -p 127.0.0.1:28883:8883 -p 127.0.0.1:29001:9001 eclipse-mosquitto:2 >/dev/null
for f in mosquitto.conf passwd acl ca.pem server.pem server.key; do docker cp "$W/$f" hydrangea-mqtt-test:/mosquitto/config/$f; done
rm -rf "$W"
docker start hydrangea-mqtt-test >/dev/null
echo "waiting for Mosquitto..."
for i in $(seq 1 30); do
  if (exec 3<>/dev/tcp/127.0.0.1/21883) 2>/dev/null; then echo ready; exit 0; fi
  sleep 1
done
echo "Mosquitto did not become ready" >&2; docker logs hydrangea-mqtt-test 2>&1 | tail -20 >&2; exit 1
