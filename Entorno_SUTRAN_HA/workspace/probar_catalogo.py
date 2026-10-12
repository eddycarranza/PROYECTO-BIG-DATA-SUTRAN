"""Separate persistence test: invoke once with --crear, then after restarting HMS."""

import argparse

from pyspark.sql import SparkSession

p = argparse.ArgumentParser()
p.add_argument("--crear", action="store_true")
a = p.parse_args()
s = (
    SparkSession.builder.appName("SUTRAN-prueba-catalogo")
    .config("spark.sql.hive.metastore.version", "3.1.3")
    .config("spark.sql.hive.metastore.jars", "maven")
    .config("spark.hadoop.hive.metastore.uris", "thrift://metastore:9083")
    .enableHiveSupport()
    .getOrCreate()
)
try:
    if a.crear:
        s.sql("CREATE DATABASE IF NOT EXISTS prueba_entorno")
        s.range(10).write.mode("errorifexists").saveAsTable("prueba_entorno.persistencia")
    assert s.table("prueba_entorno.persistencia").count() == 10
    print("CATALOGO_OK: tabla registrada y accesible desde Spark")
finally:
    s.stop()
