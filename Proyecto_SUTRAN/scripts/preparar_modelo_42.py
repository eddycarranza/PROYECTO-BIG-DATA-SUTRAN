"""Prepara pares consecutivos de meses reales. No ajusta ningún estimador."""

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from contrato_modelo import NUMERIC_FEATURES
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    project_root = Path("/project")

    def read(name):
        return json.loads((project_root / "config" / name).read_text(encoding="utf-8-sig"))

    layout, quality = (read("ultimo_almacenamiento.json"), read("ultimo_control_calidad.json"))
    assert (
        layout["status"] == "success"
        and quality["status"] == "passed"
        and (layout["pipeline_run"] == quality["run_id_pipeline"])
    )
    run = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex[:8]
    output_dir = project_root / "documentation/modelo_42/preparacion" / run
    output_dir.mkdir(parents=True, exist_ok=False)
    spark = SparkSession.builder.appName("SUTRAN-preparacion-4.2-sin-entrenar").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "America/Lima")
    root = f"hdfs://sutran/sutran/modeling_42/dataset={run}"
    report = {
        "status": "running",
        "dataset_id": run,
        "application_id": spark.sparkContext.applicationId,
        "pipeline_run": layout["pipeline_run"],
        "layout_id": layout["layout_id"],
        "fit_executed": False,
        "synthetic_data_used": False,
        "accidents_2022_used": False,
        "split_rule": {
            "train_target_start": "2020-02-01",
            "train_target_end": "2021-06-01",
            "test_target_start": "2021-07-01",
            "test_target_end": "2021-09-01",
        },
        "evaluation": "Pronóstico de un mes, con información del mes anterior en cada origen; no pronóstico simultáneo de tres meses.",
        "publication_caveat": "La fecha del evento no acredita la fecha de disponibilidad pública. Backtest retrospectivo, sujeto a verificar latencia de publicación antes de uso operativo.",
    }
    numeric = NUMERIC_FEATURES.copy()
    try:

        def table(key):
            return spark.read.parquet(layout["outputs"][key]["path"])

        acc = table("standardized/accidentes").filter(
            F.col("fecha").between("2020-01-01", "2021-09-30")
        )
        month = (
            acc.withColumn("mes_fecha", F.trunc("fecha", "month"))
            .groupBy("departamento", "mes_fecha")
            .agg(
                F.count("*").alias("accidentes_previos"),
                F.sum("fallecidos").alias("fallecidos_previos"),
                F.sum("heridos").alias("heridos_previos"),
                F.sum(F.col("duplicado_sospechoso").cast("int")).alias("filas_duplicadas_marcadas"),
            )
        )
        speed = (
            table("standardized/velocidad_2019_2021")
            .filter(F.col("fecha").between("2020-01-01", "2021-08-31"))
            .withColumn("mes_fecha", F.trunc("fecha", "month"))
            .groupBy("departamento", "mes_fecha")
            .agg(
                F.avg("exceso_kmh").alias("exceso_previo_kmh"),
                F.count("*").alias("detecciones_previas"),
            )
        )
        flow = (
            table("standardized/flujo_peajes")
            .filter(F.col("fecha").between("2020-01-01", "2021-08-31"))
            .withColumn("mes_fecha", F.trunc("fecha", "month"))
            .groupBy("departamento", "mes_fecha")
            .agg(F.sum("veh_total").alias("pasos_peajes_previos"))
        )
        features = month.join(speed, ["departamento", "mes_fecha"], "left").join(
            flow, ["departamento", "mes_fecha"], "left"
        )
        assert features.count() == month.count()
        target = month.select(
            "departamento",
            F.col("mes_fecha").alias("mes_objetivo"),
            F.col("accidentes_previos").cast("double").alias("label"),
        )
        dataset = features.withColumn("mes_objetivo", F.add_months("mes_fecha", 1)).join(
            target, ["departamento", "mes_objetivo"], "inner"
        )
        dataset = (
            dataset.withColumn("log_detecciones_previas", F.log1p("detecciones_previas"))
            .withColumn("log_pasos_peajes_previos", F.log1p("pasos_peajes_previos"))
            .withColumn(
                "cobertura_velocidad_previa",
                F.col("detecciones_previas").isNotNull().cast("double"),
            )
            .withColumn(
                "cobertura_peajes_previa", F.col("pasos_peajes_previos").isNotNull().cast("double")
            )
            .withColumn(
                "mes_seno", F.sin(F.month("mes_objetivo") * F.lit(2 * 3.141592653589793 / 12))
            )
            .withColumn(
                "mes_coseno", F.cos(F.month("mes_objetivo") * F.lit(2 * 3.141592653589793 / 12))
            )
        )
        dataset = dataset.withColumn(
            "particion",
            F.when(
                F.col("mes_objetivo").between("2020-02-01", "2021-06-01"), F.lit("train")
            ).otherwise(F.lit("test")),
        )
        dataset = dataset.filter(F.col("mes_objetivo").between("2020-02-01", "2021-09-01"))
        for col in numeric:
            dataset = dataset.withColumn(col, F.col(col).cast("double"))
        columns = [
            "departamento",
            "mes_fecha",
            "mes_objetivo",
            "label",
            "particion",
            "filas_duplicadas_marcadas",
        ] + numeric
        dataset = dataset.select(*columns).cache()
        n = dataset.count()
        assert n == dataset.select("departamento", "mes_objetivo").distinct().count()
        assert dataset.filter(F.col("mes_fecha") != F.add_months("mes_objetivo", -1)).count() == 0
        train = dataset.filter(F.col("particion") == "train")
        test = dataset.filter(F.col("particion") == "test")
        assert train.count() > 50 and test.count() > 0
        assert (
            train.agg(F.max("mes_objetivo")).first()[0] < test.agg(F.min("mes_objetivo")).first()[0]
        )
        for col in numeric:
            assert train.filter(F.col(col).isNotNull()).count() > 0, (
                "Columna sin observaciones: " + col
            )
        monthly = [
            r.asDict()
            for r in dataset.groupBy("particion", "mes_objetivo")
            .agg(F.count("*").alias("unidades"), F.sum("label").alias("registros_accidentes"))
            .orderBy("mes_objetivo")
            .collect()
        ]
        missing = (
            dataset.agg(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in numeric])
            .first()
            .asDict()
        )
        dataset.write.mode("errorifexists").option("compression", "snappy").parquet(root)
        assert spark.read.parquet(root).count() == n
        report.update(
            status="prepared",
            path=root,
            rows=n,
            train_rows=train.count(),
            test_rows=test.count(),
            numeric_features=numeric,
            monthly=monthly,
            missing=missing,
            selection="Solo pares consecutivos con registro de accidentes observado en ambos meses. No se convierten meses ausentes en ceros; evaluación condicionada a esa cobertura.",
            model_plan={
                "algorithm": "Spark MLlib LinearRegression",
                "regParam": 0.1,
                "elasticNetParam": 0.0,
                "maxIter": 100,
                "imputation": "Mediana ajustada solo con train",
                "categorical": "Departamento con StringIndexer y OneHotEncoder",
                "prediction": "Máximo entre cero y salida de regresión; no se redondea",
                "baseline": "Accidentes del mes inmediatamente anterior",
                "primary_metric": "MAE",
                "secondary_metric": "RMSE",
                "ranking_k": 5,
            },
        )
        dataset.unpersist()
    finally:
        (output_dir / "dataset_modelo.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8"
        )
        spark.stop()
    assert report["status"] == "prepared"
    subprocess.run(
        [
            "hdfs",
            "--config",
            "/tmp/hadoop-conf",
            "dfs",
            "-put",
            str(output_dir / "dataset_modelo.json"),
            f"hdfs://sutran/sutran/metadata/dataset_modelo_{run}.json",
        ],
        check=True,
    )
    (project_root / "config/ultimo_dataset_modelo.json").write_text(
        json.dumps(
            {
                "status": "prepared",
                "dataset_id": run,
                "path": root,
                "report_relative_path": (output_dir / "dataset_modelo.json")
                .relative_to(project_root)
                .as_posix(),
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    print(
        "DATASET_PREPARADO_SIN_ENTRENAR "
        + json.dumps({"train": report["train_rows"], "test": report["test_rows"], "path": root}),
        flush=True,
    )


if __name__ == "__main__":
    main()
