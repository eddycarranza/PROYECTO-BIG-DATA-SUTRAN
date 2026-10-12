#!/bin/bash
set -euo pipefail
export HIVE_CONF_DIR=/opt/hive/conf
export HADOOP_CONF_DIR=/opt/hive/conf
export HADOOP_CLIENT_OPTS="-Xmx512m ${HADOOP_CLIENT_OPTS:-}"
for conf in /hive_custom_conf/*.xml; do
  ln -sfn "$conf" "$HIVE_CONF_DIR/$(basename "$conf")"
done
if [ -d /opt/hive/data/metastore_db ]; then
  "$HIVE_HOME/bin/schematool" -dbType derby -info
else
  "$HIVE_HOME/bin/schematool" -dbType derby -initSchema
fi
exec "$HIVE_HOME/bin/hive" --service metastore
