#!/bin/bash
set -euo pipefail
python /opt/role_config.py
export HADOOP_CONF_DIR=/tmp/hadoop-conf
export SPARK_DIST_CLASSPATH="$(hadoop classpath --glob)"
case "${1:-}" in
 init-nn)
  if [ -f /data/name/current/VERSION ]; then echo 'NameNode existente: no se formatea'; exit 0; fi
  exec hdfs namenode -format -nonInteractive ;;
 init-standby)
  if [ -f /data/name/current/VERSION ]; then echo 'Standby existente: no se reinicializa'; exit 0; fi
  exec hdfs namenode -bootstrapStandby -nonInteractive ;;
 init-zk)
  if python -c "from kazoo.client import KazooClient; z=KazooClient(hosts='zk1:2181,zk2:2181,zk3:2181'); z.start(timeout=30); present=z.exists('/hadoop-ha/sutran'); z.stop(); z.close(); raise SystemExit(0 if present else 1)"; then echo 'Estado HA existente en ZooKeeper'; exit 0; fi
  exec hdfs zkfc -formatZK -nonInteractive ;;
 namenode)
  test -f /data/name/current/VERSION || { echo 'Inicializar con iniciar.ps1 antes de arrancar'; exit 1; }
  exec hdfs namenode ;;
 journalnode) exec hdfs journalnode ;;
 datanode) exec hdfs datanode ;;
 zkfc) exec hdfs zkfc ;;
 resourcemanager) exec yarn resourcemanager ;;
 nodemanager) exec yarn nodemanager ;;
 jupyter) exec jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --ServerApp.root_dir=/workspace ;;
 *) exec "$@" ;;
esac
