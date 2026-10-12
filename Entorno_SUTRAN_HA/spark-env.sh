#!/bin/bash
export HADOOP_CONF_DIR=/tmp/hadoop-conf
export SPARK_DIST_CLASSPATH="$(/opt/hadoop/bin/hadoop classpath --glob)"
