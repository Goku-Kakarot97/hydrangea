#!/usr/bin/env bash
# ProxySQL in front of the MySQL of start-mysql.sh, for the compatibility tests:  hydrangea-proxysql  23311  (the MySQL port of ProxySQL; user hydra / hydra)
# The two containers share the network hydra-net (created if missing); ProxySQL is configured through its admin interface after it starts.
# usage: start-proxysql.sh [stop]
set -eu
if [ "${1:-}" = stop ]; then docker rm -f hydrangea-proxysql >/dev/null 2>&1 || true; exit 0; fi
IMG=${PROXYSQL_IMAGE:-proxysql/proxysql:2.7.3}
docker network inspect hydra-net >/dev/null 2>&1 || docker network create hydra-net >/dev/null
docker network connect hydra-net hydrangea-my-test >/dev/null 2>&1 || true
docker rm -f hydrangea-proxysql >/dev/null 2>&1 || true
docker run -d --name hydrangea-proxysql --network hydra-net -p 127.0.0.1:23311:6033 "$IMG" >/dev/null
sleep 3
admin() { docker exec hydrangea-proxysql mysql -h127.0.0.1 -P6032 -uadmin -padmin -e "$1" 2>/dev/null; }
for i in $(seq 1 30); do admin "select 1" >/dev/null 2>&1 && break; sleep 1; done
admin "INSERT INTO mysql_servers (hostgroup_id, hostname, port) VALUES (0, 'hydrangea-my-test', 3306);
INSERT INTO mysql_users (username, password, default_hostgroup) VALUES ('hydra', 'hydra', 0);
UPDATE global_variables SET variable_value='caching_sha2_password' WHERE variable_name='mysql-default_authentication_plugin';
UPDATE global_variables SET variable_value='hydra' WHERE variable_name='mysql-monitor_username';
UPDATE global_variables SET variable_value='hydra' WHERE variable_name='mysql-monitor_password';
LOAD MYSQL SERVERS TO RUNTIME; LOAD MYSQL USERS TO RUNTIME; LOAD MYSQL VARIABLES TO RUNTIME;
SAVE MYSQL SERVERS TO DISK; SAVE MYSQL USERS TO DISK; SAVE MYSQL VARIABLES TO DISK;"
echo "ProxySQL up on 23311 (in front of hydrangea-my-test)"
