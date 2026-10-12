"""Paired warm-run measurements of equivalent annual speed aggregations."""

import csv
import json
import math
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from time import perf_counter
from uuid import uuid4

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

p = Path("/project")


def load(name):
    return json.loads((p / "config" / name).read_text(encoding="utf-8-sig"))


old, new, quality = (
    load("ultimo_pipeline.json"),
    load("ultimo_almacenamiento.json"),
    load("ultimo_control_calidad.json"),
)
assert new["status"] == "success" and quality["status"] == "passed"
assert new["pipeline_run"] == old["run_id"] == quality["run_id_pipeline"]
key = "standardized/velocidad_2019_2021"
paths = {"antes": old["outputs"][key]["path"], "despues": new["outputs"][key]["path"]}
run = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex[:8]
out = p / "documentation/optimizacion_36" / run
out.mkdir(parents=True, exist_ok=False)
s = SparkSession.builder.appName("SUTRAN-optimizacion-3.6").getOrCreate()
s.conf.set("spark.sql.session.timeZone", "America/Lima")
s.conf.set("spark.sql.adaptive.enabled", "true")
report = {
    "status": "running",
    "run_id": run,
    "application_id": s.sparkContext.applicationId,
    "pipeline_run": old["run_id"],
    "storage_layout": new["layout_id"],
    "paths": paths,
    "method": "One warm-up per variant/year, five paired repetitions; alternating order; no Spark cache; wall time includes read/schema, query planning and collect. OS/HDFS caches not cleared.",
    "spark_version": s.version,
    "spark_config": {},
    "measurements": [],
    "summaries": [],
}
for name in [
    "spark.executor.instances",
    "spark.executor.cores",
    "spark.executor.memory",
    "spark.sql.shuffle.partitions",
    "spark.sql.adaptive.enabled",
]:
    report["spark_config"][name] = (
        s.sparkContext.getConf().get(name, "")
        if name.startswith("spark.executor")
        else s.conf.get(name)
    )


def query(variant, year):
    df = s.read.parquet(paths[variant])
    cond = F.year("fecha") == year if variant == "antes" else F.col("anio_particion") == year
    return (
        df.filter(cond)
        .select("departamento", "velocidad_kmh", "exceso_kmh")
        .groupBy("departamento")
        .agg(
            F.count("*").alias("registros"),
            F.sum("velocidad_kmh").alias("suma_velocidad"),
            F.sum("exceso_kmh").alias("suma_exceso"),
        )
        .orderBy("departamento")
    )


def same(a, b):
    assert len(a) == len(b)
    for left, right in zip(a, b):
        assert (
            left["departamento"] == right["departamento"]
            and left["registros"] == right["registros"]
        )
        for k in ["suma_velocidad", "suma_exceso"]:
            assert (left[k] is None and right[k] is None) or math.isclose(
                left[k], right[k], rel_tol=1e-10, abs_tol=1e-6
            )


try:
    for year in [2019, 2021]:
        reference = None
        for variant in ["antes", "despues"]:
            s.catalog.clearCache()
            q = query(variant, year)
            result = [x.asDict() for x in q.collect()]
            if reference is None:
                reference = result
            else:
                same(reference, result)
            plan = q._jdf.queryExecution().executedPlan().toString()
            (out / f"plan_{variant}_{year}.txt").write_text(plan, encoding="utf-8")
            if variant == "despues":
                assert "PartitionFilters:" in plan and str(year) in plan
        for repetition in range(1, 6):
            order = ["antes", "despues"] if repetition % 2 else ["despues", "antes"]
            for position, variant in enumerate(order, 1):
                s.catalog.clearCache()
                t = perf_counter()
                result = [x.asDict() for x in query(variant, year).collect()]
                elapsed = perf_counter() - t
                same(reference, result)
                item = {
                    "year": year,
                    "repetition": repetition,
                    "position": position,
                    "variant": variant,
                    "seconds": elapsed,
                    "rows_aggregated": sum(x["registros"] for x in result),
                    "departments": len(result),
                    "equivalent": True,
                }
                report["measurements"].append(item)
                print("MEDICION " + json.dumps(item), flush=True)
        before = [
            x["seconds"]
            for x in report["measurements"]
            if x["year"] == year and x["variant"] == "antes"
        ]
        after = [
            x["seconds"]
            for x in report["measurements"]
            if x["year"] == year and x["variant"] == "despues"
        ]
        report["summaries"].append(
            {
                "year": year,
                "before_median_seconds": median(before),
                "after_median_seconds": median(after),
                "before_min_seconds": min(before),
                "before_max_seconds": max(before),
                "after_min_seconds": min(after),
                "after_max_seconds": max(after),
                "time_reduction_percent": 100 * (median(before) - median(after)) / median(before),
                "speedup": median(before) / median(after),
                "rows": sum(x["registros"] for x in reference),
            }
        )
        (out / f"resultado_agregado_{year}.json").write_text(
            json.dumps(reference, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    report["status"] = "success"
finally:
    (out / "resultado_optimizacion.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    if report["measurements"]:
        with (out / "tiempos_consultas.csv").open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(report["measurements"][0]))
            writer.writeheader()
            writer.writerows(report["measurements"])
    s.stop()
subprocess.run(
    [
        "hdfs",
        "--config",
        "/tmp/hadoop-conf",
        "dfs",
        "-put",
        str(out / "resultado_optimizacion.json"),
        f"hdfs://sutran/sutran/metadata/optimizacion_{run}.json",
    ],
    check=True,
)
(p / "config/ultima_optimizacion.json").write_text(
    json.dumps(
        {
            "status": report["status"],
            "run_id": run,
            "report_relative_path": str((out / "resultado_optimizacion.json").relative_to(p)),
        },
        indent=2,
    ),
    encoding="utf-8",
)
print("OPTIMIZACION_OK " + json.dumps(report["summaries"]), flush=True)
