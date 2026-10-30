#!/usr/bin/env bash
# An Elasticsearch for the tests of hydrangea-elasticsearch, in Docker:
#   hydrangea-es-test  29200  Elasticsearch 8.17, single node, security off (no TLS, no password)
# usage: start-elasticsearch.sh [stop]
# ES_IMAGE=opensearchproject/opensearch:2.19.0 runs the same tests against OpenSearch (it needs DISABLE_SECURITY_PLUGIN=true, set below).
set -eu
if [ "${1:-}" = stop ]; then docker rm -f hydrangea-es-test >/dev/null 2>&1 || true; exit 0; fi
IMG=${ES_IMAGE:-docker.elastic.co/elasticsearch/elasticsearch:8.17.0}
docker rm -f hydrangea-es-test >/dev/null 2>&1 || true
docker run -d --name hydrangea-es-test -p 29200:9200 \
  -e discovery.type=single-node -e xpack.security.enabled=false -e DISABLE_SECURITY_PLUGIN=true -e DISABLE_INSTALL_DEMO_CONFIG=true \
  -e ES_JAVA_OPTS="-Xms512m -Xmx512m" -e OPENSEARCH_JAVA_OPTS="-Xms512m -Xmx512m" \
  -e cluster.routing.allocation.disk.threshold_enabled=false "$IMG" >/dev/null
for i in $(seq 1 90); do
  if curl -fs localhost:29200/_cluster/health >/dev/null 2>&1; then echo "Elasticsearch up on 29200"; exit 0; fi
  sleep 1
done
echo "Elasticsearch did not come up in 90 s" >&2
exit 1
