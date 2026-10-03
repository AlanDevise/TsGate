#!/bin/sh
# SPDX-License-Identifier: Apache-2.0
# Each cluster container runs a separate meta, store and SQL process.
set -eu
node_ip=$(hostname -i | awk '{print $1}')
mode=${OPENGEMINI_MODE:-single}
if [ "$mode" = cluster ]; then
    meta_join=${OPENGEMINI_META_JOIN:?Missing cluster meta endpoints}
    gossip_members=${OPENGEMINI_GOSSIP_MEMBERS:?Missing cluster gossip endpoints}
    meta_domain=${OPENGEMINI_NODE_NAME:?Missing cluster node name}
    ha_policy=replication
    gossip_enabled=true
else
    meta_join="\"${node_ip}:8092\""
    gossip_members="\"${node_ip}:8010\""
    meta_domain=${node_ip}
    ha_policy=write-available-first
    gossip_enabled=false
fi
mkdir -p /var/lib/opengemini/meta /var/lib/opengemini/data /var/lib/opengemini/wal /var/log/opengemini
cat > /tmp/opengemini.conf <<CONFIG
[common]
  meta-join = [${meta_join}]
  ha-policy = "${ha_policy}"
  cpu-num = 2
  memory-size = "768m"
  report-enable = false
  ignore-empty-tag = false
[meta]
  domain = "${meta_domain}"
  bind-address = "${node_ip}:8088"
  http-bind-address = "${node_ip}:8091"
  rpc-bind-address = "${node_ip}:8092"
  dir = "/var/lib/opengemini/meta"
  ptnum-pernode = 1
[http]
  bind-address = "0.0.0.0:8086"
  flight-enabled = false
  auth-enabled = false
  weakpwd-path = "/opt/opengemini/etc/weakpasswd.properties"
[data]
  store-ingest-addr = "${node_ip}:8400"
  store-select-addr = "${node_ip}:8401"
  store-data-dir = "/var/lib/opengemini/data"
  store-wal-dir = "/var/lib/opengemini/wal"
  store-meta-dir = "/var/lib/opengemini/meta"
  enable-mmap-read = false
[logging]
  path = "/var/log/opengemini/"
[gossip]
  enabled = ${gossip_enabled}
  bind-address = "${node_ip}"
  store-bind-port = 8011
  meta-bind-port = 8010
  sql-bind-port = 8012
  members = [${gossip_members}]
[monitor]
  store-enabled = false
CONFIG
if [ "$mode" = single ]; then
    exec ts-server -config /tmp/opengemini.conf
fi
# The shell remains PID 1 and forwards termination to all three processes.
trap 'kill -TERM "$meta_pid" "$store_pid" "$sql_pid" 2>/dev/null || true; wait; exit' TERM INT
ts-meta -config /tmp/opengemini.conf > /var/log/opengemini/meta.stdout 2>&1 &
meta_pid=$!
ts-store -config /tmp/opengemini.conf > /var/log/opengemini/store.stdout 2>&1 &
store_pid=$!
ts-sql -config /tmp/opengemini.conf > /var/log/opengemini/sql.stdout 2>&1 &
sql_pid=$!
while kill -0 "$meta_pid" && kill -0 "$store_pid" && kill -0 "$sql_pid"; do sleep 1; done
kill -TERM "$meta_pid" "$store_pid" "$sql_pid" 2>/dev/null || true
wait || true
exit 1
