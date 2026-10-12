"""Carga el PipelineModel guardado y exporta predicciones sin ajustar parámetros."""

import argparse
import csv
import json
from pathlib import Path

from contrato_modelo import NUMERIC_FEATURES, read_input


def resolve_model(args):
    """Obtiene el modelo y su ficha desde argumentos o el último entrenamiento."""
    pointer_path = args.proyecto / "config/ultimo_modelo.json"
    if args.modelo:
        if not args.ficha or not args.entrada:
            raise ValueError("Con --modelo indicar --ficha y --entrada.")
        model_path = args.modelo
        report = json.loads(args.ficha.read_text(encoding="utf-8"))
    else:
        if not pointer_path.is_file():
            raise ValueError("Primero entrenar o indicar --modelo, --ficha y --entrada.")
        pointer = json.loads(pointer_path.read_text(encoding="utf-8-sig"))
        if pointer.get("status") != "success":
            raise ValueError("El último entrenamiento no terminó correctamente.")
        model_path = pointer["model_path"]
        report = json.loads(
            (args.proyecto / pointer["report_relative_path"]).read_text(encoding="utf-8")
        )
        if report["status"] != "success":
            raise ValueError("El informe del modelo no acredita un entrenamiento exitoso.")
    return model_path, report


def export_predictions(path, columns, rows):
    """Escribe el CSV sin sobrescribir una ejecución anterior."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns)
        writer.writeheader()
        writer.writerows(row.asDict() for row in rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--proyecto", type=Path, default=Path("/project"))
    parser.add_argument("--modelo", help="URI del PipelineModel; permite usar la copia incluida.")
    parser.add_argument("--ficha", type=Path, help="Ficha JSON correspondiente a --modelo.")
    parser.add_argument(
        "--entrada", type=Path, help="CSV de características; si se omite, usa test."
    )
    parser.add_argument(
        "--salida", type=Path, default=Path("/project/documentation/predicciones.csv")
    )
    args = parser.parse_args()
    try:
        model_path, report = resolve_model(args)
    except ValueError as exc:
        parser.error(str(exc))
    if args.salida.exists():
        parser.error(
            "La salida ya existe. Elegir otro nombre para conservar resultados anteriores."
        )
    rows = read_input(args.entrada) if args.entrada else None
    if rows:
        unknown = {r["departamento"] for r in rows} - set(report["department_labels"])
        if unknown:
            parser.error(
                "Departamentos sin ejemplos de entrenamiento: " + ", ".join(sorted(unknown))
            )

    # El CSV se valida antes de abrir Spark; las medianas se cargan del modelo entrenado.
    from pyspark.ml import PipelineModel
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType, StringType, StructField, StructType

    spark = SparkSession.builder.appName("SUTRAN-predicciones-modelo-base").getOrCreate()
    try:
        spark.conf.set("spark.sql.session.timeZone", "America/Lima")
        model = PipelineModel.load(model_path)
        if rows:
            schema = StructType(
                [StructField("departamento", StringType(), False)]
                + [StructField(column, DoubleType(), True) for column in NUMERIC_FEATURES]
            )
            data = spark.createDataFrame(rows, schema)
            output_columns = ["departamento"]
        else:
            data = spark.read.parquet(report["dataset_plan"]["path"]).filter(
                F.col("particion") == "test"
            )
            output_columns = ["departamento", "mes_objetivo", "label"]
        result = (
            model.transform(data)
            .withColumn("prediccion_modelo", F.greatest(F.lit(0.0), F.col("prediction")))
            .select(*output_columns, "prediccion_modelo")
        )
        output = result.orderBy(*output_columns[:2]).collect()
        if not output:
            raise ValueError("No hay registros para predecir.")
        export_predictions(args.salida, result.columns, output)
        print(f"PREDICCIONES_OK: {len(output)} filas en {args.salida}", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
