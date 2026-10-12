from uuid import uuid4

from pyspark.sql import SparkSession

s = SparkSession.builder.appName("SUTRAN-HA-prueba").getOrCreate()
try:
    assert s.sparkContext.master == "yarn"
    path = "hdfs://sutran/sutran/pruebas/" + uuid4().hex
    s.range(1000).repartition(3).write.mode("errorifexists").parquet(path)
    assert s.read.parquet(path).count() == 1000
    print("PRUEBA_OK", s.sparkContext.applicationId, path)
finally:
    s.stop()
