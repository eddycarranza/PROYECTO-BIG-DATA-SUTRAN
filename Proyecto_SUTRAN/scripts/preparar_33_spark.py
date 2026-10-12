"""Draft Spark audit and route-presence join. Run only after environment checks.

This does not integrate speed/traffic or certify historical route validity.
Exact duplicates are flagged, not automatically removed.
"""

import argparse
import json
from pathlib import Path
from uuid import uuid4

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--accidentes", required=True, help="Exact HDFS URI from the ingest manifest"
    )
    parser.add_argument("--rutas", default="/workspace/dim_rutas_referencia_2018.csv")
    args = parser.parse_args()
    spark = SparkSession.builder.appName("SUTRAN-preparacion-3.3").getOrCreate()
    spark.conf.set("spark.sql.ansi.enabled", "false")
    try:
        if spark.sparkContext.master != "yarn":
            raise RuntimeError("Esta validacion requiere Spark sobre YARN")
        raw = (
            spark.read.option("header", True)
            .option("sep", ";")
            .option("encoding", "windows-1252")
            .option("mode", "FAILFAST")
            .csv(args.accidentes)
        )
        expected = [
            "FECHA_CORTE",
            "FECHA",
            "HORA",
            "DEPARTAMENTO",
            "CODIGO_VÍA",
            "KILOMETRO",
            "MODALIDAD",
            "FALLECIDOS",
            "HERIDOS",
        ]
        if raw.columns != expected:
            raise ValueError(f"Esquema inesperado: {raw.columns}")
        frame = raw.withColumnRenamed("CODIGO_VÍA", "codigo_via_original")
        frame = frame.withColumn("fecha", F.to_date("FECHA", "yyyyMMdd")).withColumn(
            "mes", F.date_format("fecha", "yyyy-MM")
        )
        frame = frame.withColumn(
            "codigo_ruta", F.regexp_replace(F.upper(F.trim("codigo_via_original")), r"\s+", "")
        )
        frame = frame.withColumn(
            "kilometro", F.regexp_replace(F.trim("KILOMETRO"), ",", ".").cast("double")
        )
        frame = frame.withColumn("fallecidos", F.col("FALLECIDOS").cast("int")).withColumn(
            "heridos", F.col("HERIDOS").cast("int")
        )
        invalid = frame.filter(
            F.col("fecha").isNull()
            | (F.col("fallecidos") < 0)
            | (F.col("heridos") < 0)
            | (F.col("kilometro") < 0)
        )
        unknown = frame.filter(
            F.col("fallecidos").isNull() | F.col("heridos").isNull() | F.col("kilometro").isNull()
        )
        duplicates = raw.groupBy(raw.columns).count().filter("count > 1")
        period = frame.filter(F.col("fecha").between("2021-01-01", "2021-09-30"))
        routes = spark.read.option("header", True).csv(args.rutas)
        if (
            routes.filter(F.col("codigo_ruta").isNull()).count()
            or routes.count() != routes.select("codigo_ruta").distinct().count()
        ):
            raise ValueError("Dimension de rutas sin clave unica")
        # Una sola coincidencia por registro evita multiplicar accidentes.
        joined = period.join(routes.select("codigo_ruta"), ["codigo_ruta"], "left_semi")
        unmatched = period.join(routes.select("codigo_ruta"), ["codigo_ruta"], "left_anti")
        total = period.count()
        matched = joined.count()
        notmatched = unmatched.count()
        assert matched + notmatched == total
        result = {
            "aplicacion_yarn": spark.sparkContext.applicationId,
            "filas_raw": raw.count(),
            "filas_periodo": total,
            "filas_coinciden_codigo_2018": matched,
            "filas_sin_codigo_2018": notmatched,
            "grupos_duplicados_exactos": duplicates.count(),
            "filas_con_fecha_o_numeros_invalidos": invalid.count(),
            "filas_con_medidas_no_informadas": unknown.count(),
            "nota": "Union por presencia de codigo historico; no certifica tramo ni vigencia en 2021. Repeticiones no eliminadas.",
        }
        destination = "hdfs://namenode:8020/sutran/preparacion_33/" + uuid4().hex
        unmatched.write.mode("errorifexists").parquet(destination + "/sin_correspondencia")
        duplicates.write.mode("errorifexists").parquet(destination + "/repeticiones")
        result["salida_auditoria_hdfs"] = destination
        Path("/workspace").mkdir(exist_ok=True)
        Path("/workspace/resultado_preparacion_33.json").write_text(
            json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
