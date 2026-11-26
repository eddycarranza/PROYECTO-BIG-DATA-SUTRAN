"""Audita en Glue una corrida del pipeline SUTRAN publicada en S3."""

import argparse
import csv
import json
import re
from datetime import datetime, timezone
from io import StringIO
from urllib.parse import urlsplit
from uuid import uuid4

import boto3
from awsglue.context import GlueContext
from calidad_reglas import inspect_quality
from pyspark import SparkContext


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--pipeline-report", required=True, help="URI S3 del resultado_pipeline.json"
    )
    parser.add_argument("--bucket-owner", required=True)
    args, _ = parser.parse_known_args()
    if not re.fullmatch(r"[0-9]{12}", args.bucket_owner):
        parser.error("--bucket-owner requiere el ID de 12 digitos de la cuenta AWS")
    report_uri = urlsplit(args.pipeline_report)
    report_key = report_uri.path.lstrip("/")
    if (
        report_uri.scheme != "s3"
        or not report_uri.netloc
        or not report_key.startswith("resultados/sutran/")
        or not report_key.endswith("/resultado_pipeline.json")
        or report_uri.query
        or report_uri.fragment
    ):
        parser.error("--pipeline-report debe identificar un resultado_pipeline.json de SUTRAN")
    s3 = boto3.client("s3")
    with s3.get_object(
        Bucket=report_uri.netloc, Key=report_key, ExpectedBucketOwner=args.bucket_owner
    )["Body"] as stream:
        previous = json.load(stream)
    if previous.get("status") != "success":
        raise ValueError("El informe de entrada debe corresponder a una corrida exitosa")
    run_id = previous["run_id"]
    if report_key != f"resultados/sutran/{run_id}/resultado_pipeline.json":
        raise ValueError("La ruta del informe no coincide con el identificador de la corrida")
    outputs = previous["outputs"]
    for name, table in outputs.items():
        expected_path = f"s3://{report_uri.netloc}/resultados/sutran/{run_id}/{name}"
        if table["path"] != expected_path or ".." in name.split("/"):
            raise ValueError(f"Ruta de tabla fuera de la corrida: {name}")
    output_prefix = f"resultados/calidad/{run_id}/{uuid4().hex}/"
    spark = GlueContext(SparkContext.getOrCreate()).spark_session
    profiles = []
    rules = []
    outliers = []
    rejections = []
    result = {
        "run_id_pipeline": run_id,
        "application_id": spark.sparkContext.applicationId,
        "started_utc": datetime.now(timezone.utc).isoformat(),
        "tables": {},
        "pipeline_report": args.pipeline_report,
        "status": "running",
    }
    try:
        inspect_quality(
            spark,
            previous,
            outputs,
            result,
            profiles,
            rules,
            outliers,
            rejections,
            include_batches=True,
        )
    except Exception as exc:
        result["status"] = "failed"
        result["error"] = str(exc)
        raise
    finally:
        result["finished_utc"] = datetime.now(timezone.utc).isoformat()
        report_key = output_prefix + "resultado_calidad.json"
        try:
            s3.put_object(
                Bucket=report_uri.netloc,
                Key=report_key,
                ExpectedBucketOwner=args.bucket_owner,
                Body=json.dumps(result, ensure_ascii=False, indent=2).encode("utf-8"),
                ContentType="application/json",
            )
            for name, rows in [
                ("completitud_campos", profiles),
                ("reglas_calidad", rules),
                ("atipicos_revision", outliers),
                ("motivos_rechazo", rejections),
            ]:
                if rows:
                    stream = StringIO(newline="")
                    writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
                    writer.writeheader()
                    writer.writerows(rows)
                    s3.put_object(
                        Bucket=report_uri.netloc,
                        Key=output_prefix + name + ".csv",
                        ExpectedBucketOwner=args.bucket_owner,
                        Body=stream.getvalue().encode("utf-8-sig"),
                        ContentType="text/csv",
                    )
        finally:
            spark.stop()
        print(f"REPORTE_CALIDAD s3://{report_uri.netloc}/{report_key}", flush=True)
    if result["blocking_failures"]:
        raise SystemExit("CALIDAD_NO_CUMPLE")
    print("CALIDAD_OK", flush=True)


if __name__ == "__main__":
    main()
