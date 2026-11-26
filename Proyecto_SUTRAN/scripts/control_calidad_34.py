"""Comprueba las reglas de calidad sobre las salidas del pipeline."""

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path

from calidad_reglas import inspect_quality
from pyspark.sql import SparkSession


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pointer", default="/project/config/ultimo_pipeline.json")
    args = ap.parse_args()
    pointer = json.loads(Path(args.pointer).read_text(encoding="utf-8"))
    previous = json.loads(
        (Path("/project") / pointer["report_relative_path"]).read_text(encoding="utf-8")
    )
    assert previous["status"] == "success"
    out = Path("/project/documentation/calidad_34") / pointer["run_id"]
    out.mkdir(parents=True, exist_ok=True)
    spark = SparkSession.builder.appName("SUTRAN-control-calidad-3.4").getOrCreate()
    profiles = []
    rules = []
    outliers = []
    rejections = []
    result = {
        "run_id_pipeline": pointer["run_id"],
        "application_id": spark.sparkContext.applicationId,
        "started_utc": datetime.now(timezone.utc).isoformat(),
        "tables": {},
    }
    try:
        inspect_quality(
            spark, previous, pointer["outputs"], result, profiles, rules, outliers, rejections
        )
    finally:
        result["finished_utc"] = datetime.now(timezone.utc).isoformat()
        (out / "resultado_calidad.json").write_text(
            json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        for name, rows in [
            ("completitud_campos", profiles),
            ("reglas_calidad", rules),
            ("atipicos_revision", outliers),
            ("motivos_rechazo", rejections),
        ]:
            if rows:
                with (out / (name + ".csv")).open("w", encoding="utf-8-sig", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=list(rows[0]))
                    writer.writeheader()
                    writer.writerows(rows)
        spark.stop()
    if result["blocking_failures"]:
        raise SystemExit("CALIDAD_NO_CUMPLE: revisar resultado_calidad.json")
    print("CALIDAD_OK " + str(out / "resultado_calidad.json"))


if __name__ == "__main__":
    main()
