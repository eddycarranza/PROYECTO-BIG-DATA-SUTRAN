"""Entrena una regresión lineal regularizada y la evalúa frente al mes anterior."""

import argparse
import csv
import json
import math
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from pyspark.ml import Pipeline
from pyspark.ml.feature import Imputer, OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_pipeline(numeric_columns):
    """Imputa y codifica las entradas antes de ajustar la regresión."""
    imputed_columns = [column + "_imputado" for column in numeric_columns]
    stages = [
        Imputer(strategy="median", inputCols=numeric_columns, outputCols=imputed_columns),
        StringIndexer(
            inputCol="departamento", outputCol="departamento_indice", handleInvalid="keep"
        ),
        OneHotEncoder(
            inputCols=["departamento_indice"],
            outputCols=["departamento_vector"],
            handleInvalid="keep",
        ),
        VectorAssembler(inputCols=imputed_columns + ["departamento_vector"], outputCol="features"),
        LinearRegression(
            featuresCol="features",
            labelCol="label",
            predictionCol="prediction",
            regParam=0.1,
            elasticNetParam=0.0,
            maxIter=100,
            standardization=True,
        ),
    ]
    return Pipeline(stages=stages)


def regression_metrics(frame, prediction_column):
    """Calcula MAE y RMSE sobre las mismas filas de prueba."""
    r = (
        frame.agg(
            F.count("*").alias("n"),
            F.avg(F.abs(F.col(prediction_column) - F.col("label"))).alias("mae"),
            F.sqrt(F.avg(F.pow(F.col(prediction_column) - F.col("label"), 2))).alias("rmse"),
        )
        .first()
        .asDict()
    )
    if r["n"] == 0 or not all(math.isfinite(r[key]) for key in ("mae", "rmse")):
        raise ValueError("La evaluación requiere filas y métricas finitas.")
    return r


def department_rankings(predictions, k):
    """Compara los departamentos seleccionados con el ranking observado."""
    rankings = []
    for row in predictions.select("mes_objetivo").distinct().orderBy("mes_objetivo").collect():
        date = row["mes_objetivo"]
        part = predictions.filter(F.col("mes_objetivo") == F.lit(date))
        total = part.agg(F.sum("label")).first()[0]
        actual = part.orderBy(F.desc("label"), "departamento").limit(k)
        actual_set = {r["departamento"] for r in actual.select("departamento").collect()}
        for name, col in [
            ("modelo", "prediccion_modelo"),
            ("referencia", "prediccion_referencia"),
        ]:
            selected = part.orderBy(F.desc(col), "departamento").limit(k)
            chosen = [r.asDict() for r in selected.select("departamento", "label", col).collect()]
            rankings.append(
                {
                    "mes_objetivo": str(date),
                    "metodo": name,
                    "k": k,
                    "departamentos": [r["departamento"] for r in chosen],
                    "fraccion_registros_capturados": sum((r["label"] for r in chosen)) / total,
                    "coincidencias_top_real": len(actual_set & {r["departamento"] for r in chosen}),
                    "total_registros_evaluados_mes": total,
                }
            )
    return rankings


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--entrenar", action="store_true")
    args = parser.parse_args()
    if not args.entrenar:
        parser.error("Usar --entrenar para iniciar el ajuste del modelo.")
    project_root = Path("/project")
    dataset_pointer = json.loads(
        (project_root / "config/ultimo_dataset_modelo.json").read_text(encoding="utf-8-sig")
    )
    dataset_report = json.loads(
        (project_root / dataset_pointer["report_relative_path"]).read_text(encoding="utf-8")
    )
    assert (
        dataset_pointer["status"] == "prepared"
        and dataset_report["status"] == "prepared"
        and (not dataset_report["fit_executed"])
    )
    assert "/benchmark" not in dataset_pointer["path"] and (
        not dataset_report["synthetic_data_used"]
    )
    run = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex[:8]
    output_dir = project_root / "documentation/modelo_42/entrenamiento" / run
    output_dir.mkdir(parents=True, exist_ok=False)
    root = f"hdfs://sutran/sutran/modeling_42/training={run}"
    spark = SparkSession.builder.appName("SUTRAN-modelo-base-4.2").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "America/Lima")
    report = {
        "status": "running",
        "run_id": run,
        "dataset_id": dataset_pointer["dataset_id"],
        "application_id": spark.sparkContext.applicationId,
        "model_path": root + "/modelo",
        "dataset_plan": dataset_report,
        "baseline": "Conteo del mes anterior",
        "metrics": {},
        "test_used_for_tuning": False,
    }
    try:
        data = spark.read.parquet(dataset_pointer["path"])
        train = data.filter(F.col("particion") == "train")
        test = data.filter(F.col("particion") == "test")
        assert (
            train.count() == dataset_report["train_rows"]
            and test.count() == dataset_report["test_rows"]
        )
        assert (
            train.agg(F.max("mes_objetivo")).first()[0] < test.agg(F.min("mes_objetivo")).first()[0]
        )
        cols = dataset_report["numeric_features"]
        imputed = [c + "_imputado" for c in cols]
        print(
            "ENTRENANDO: imputaciones y coeficientes se ajustan solo con meses anteriores a julio de 2021.",
            flush=True,
        )
        model = build_pipeline(cols).fit(train)
        model.write().save(root + "/modelo")
        pred = (
            model.transform(test)
            .withColumn("prediccion_modelo", F.greatest(F.lit(0.0), F.col("prediction")))
            .withColumn("prediccion_referencia", F.col("accidentes_previos"))
        )
        negative_count = pred.filter(F.col("prediction") < 0).count()
        pred = pred.select(
            "departamento",
            "mes_fecha",
            "mes_objetivo",
            "label",
            "prediccion_modelo",
            "prediccion_referencia",
        ).cache()

        for name, col in [("modelo", "prediccion_modelo"), ("referencia", "prediccion_referencia")]:
            report["metrics"][name] = regression_metrics(pred, col)
        k = dataset_report["model_plan"]["ranking_k"]
        rankings = department_rankings(pred, k)
        output = [r.asDict() for r in pred.orderBy("mes_objetivo", "departamento").collect()]
        with (output_dir / "predicciones_prueba.csv").open(
            "w", newline="", encoding="utf-8-sig"
        ) as f:
            w = csv.DictWriter(f, fieldnames=list(output[0]))
            w.writeheader()
            w.writerows(output)
        pred.write.mode("errorifexists").option("compression", "snappy").parquet(
            root + "/predicciones_prueba"
        )
        lr = model.stages[-1]
        summary = lr.summary
        report.update(
            status="success",
            train_rows=dataset_report["train_rows"],
            test_rows=dataset_report["test_rows"],
            ranking=rankings,
            model_beats_baseline_mae=report["metrics"]["modelo"]["mae"]
            < report["metrics"]["referencia"]["mae"],
            intercept=lr.intercept,
            coefficients=lr.coefficients.toArray().tolist(),
            total_iterations=summary.totalIterations,
            training_objective_history=summary.objectiveHistory,
            imputation_values=model.stages[0].surrogateDF.first().asDict(),
            department_labels=model.stages[1].labels,
            negative_predictions_clipped=negative_count,
            final_numeric_features=imputed,
            model_feature_count=len(lr.coefficients),
        )
        pred.unpersist()
    except Exception as exc:
        report["status"] = "failed"
        report["error"] = str(exc)
        raise
    finally:
        (output_dir / "resultado_modelo.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8"
        )
        spark.stop()
    subprocess.run(
        [
            "hdfs",
            "--config",
            "/tmp/hadoop-conf",
            "dfs",
            "-put",
            str(output_dir / "resultado_modelo.json"),
            f"hdfs://sutran/sutran/metadata/modelo_{run}.json",
        ],
        check=True,
    )
    (project_root / "config/ultimo_modelo.json").write_text(
        json.dumps(
            {
                "status": "success",
                "run_id": run,
                "report_relative_path": (output_dir / "resultado_modelo.json")
                .relative_to(project_root)
                .as_posix(),
                "model_path": root + "/modelo",
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    print("ENTRENAMIENTO_COMPLETADO " + json.dumps(report["metrics"]), flush=True)
    print("El resultado está en " + str(output_dir / "resultado_modelo.json"), flush=True)


if __name__ == "__main__":
    main()
