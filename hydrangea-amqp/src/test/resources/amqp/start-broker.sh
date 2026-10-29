#!/bin/sh
# RabbitMQ for the AMQP tests: plain AMQP on 127.0.0.1:25672 and AMQP over TLS on 127.0.0.1:25671.
# Remove with: docker rm -f hydrangea-amqp-test
set -e
cd "$(dirname "$0")"
W=$(mktemp -d)
mkdir "$W/certs"
cp ca.pem server.pem server.key "$W/certs/"
chmod 644 "$W"/certs/*
cat > "$W/20-tls.conf" <<X
listeners.ssl.default = 5671
ssl_options.cacertfile = /etc/rabbitmq/certs/ca.pem
ssl_options.certfile = /etc/rabbitmq/certs/server.pem
ssl_options.keyfile = /etc/rabbitmq/certs/server.key
ssl_options.verify = verify_none
ssl_options.fail_if_no_peer_cert = false
X
docker rm -f hydrangea-amqp-test >/dev/null 2>&1 || true
docker create --name hydrangea-amqp-test -p 127.0.0.1:25672:5672 -p 127.0.0.1:25671:5671 rabbitmq:3-management >/dev/null
docker cp "$W/certs" hydrangea-amqp-test:/etc/rabbitmq/certs
docker cp "$W/20-tls.conf" hydrangea-amqp-test:/etc/rabbitmq/conf.d/20-tls.conf
rm -rf "$W"
docker start hydrangea-amqp-test >/dev/null
echo "waiting for RabbitMQ..."
for i in $(seq 1 90); do
  if docker exec -u rabbitmq hydrangea-amqp-test rabbitmq-diagnostics -q check_port_connectivity >/dev/null 2>&1; then echo ready; exit 0; fi
  sleep 1
done
echo "RabbitMQ did not become ready" >&2; docker logs hydrangea-amqp-test 2>&1 | tail -20 >&2; exit 1
