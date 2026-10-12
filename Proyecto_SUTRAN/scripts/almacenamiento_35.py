"""Create and validate an immutable Parquet storage layout from audited tables."""

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

p = Path("/project")
pointer = json.loads((p / "config/ultimo_pipeline.json").read_text(encoding="utf-8-sig"))
quality = json.loads((p / "config/ultimo_control_calidad.json").read_text(encoding="utf-8-sig"))
assert quality["status"] == "passed" and quality["run_id_pipeline"] == pointer["run_id"]
layout = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex[:8]
out = p / "documentation/almacenamiento_35" / layout
out.mkdir(parents=True, exist_ok=False)
s = SparkSession.builder.appName("SUTRAN-almacenamiento-3.5").getOrCreate()
report = {
    "status": "running",
    "layout_id": layout,
    "pipeline_run": pointer["run_id"],
    "application_id": s.sparkContext.applicationId,
    "tables": {},
    "format": "parquet",
    "compression": "snappy",
}


def inventory(path):
    jpath = s._jvm.org.apache.hadoop.fs.Path(path)
    fs = jpath.getFileSystem(s.sparkContext._jsc.hadoopConfiguration())
    files = fs.listFiles(jpath, True)
    result = []
    while files.hasNext():
        f = files.next()
        if str(f.getPath()).endswith(".parquet"):
            result.append(
                {"path": str(f.getPath()), "bytes": f.getLen(), "replication": f.getReplication()}
            )
    return {
        "parquet_files": len(result),
        "logical_bytes": sum(f["bytes"] for f in result),
        "replicated_bytes": sum(f["bytes"] * f["replication"] for f in result),
        "files": result,
    }


try:
    for name, entry in pointer["outputs"].items():
        if name.startswith("quarantine/"):
            continue
        print("ALMACENANDO " + name, flush=True)
        zone, table = name.split("/")
        df = s.read.parquet(entry["path"]).cache()
        before = df.count()
        assert before == entry["rows"]
        dest = f"hdfs://sutran/sutran/{zone}/run={pointer['run_id']}/layout={layout}/{table}"
        # Se particiona solo el histórico de varios años; las tablas pequeñas
        # y la fuente de un solo año se conservan sin particiones adicionales.
        partitioned = table == "velocidad_2019_2021"
        if partitioned:
            written = df.withColumn("anio_particion", F.year("fecha"))
            assert written.filter(F.col("anio_particion").isNull()).count() == 0
            written.repartition("anio_particion").write.mode("errorifexists").option(
                "compression", "snappy"
            ).partitionBy("anio_particion").parquet(dest)
        else:
            df.coalesce(1).write.mode("errorifexists").option("compression", "snappy").parquet(dest)
        read = s.read.parquet(dest)
        restored = read.select(*df.columns)
        assert restored.count() == before
        assert dict(restored.dtypes) == dict(df.dtypes)
        assert df.exceptAll(restored).limit(1).count() == 0
        assert restored.exceptAll(df).limit(1).count() == 0
        item = {
            "source": entry["path"],
            "path": dest,
            "rows": before,
            "partition_columns": ["anio_particion"] if partitioned else [],
            "count_verified": True,
            "schema_verified": True,
            "multiset_verified": True,
            "before": inventory(entry["path"]),
            "after": inventory(dest),
        }
        if partitioned:
            item["partitions"] = [
                r.asDict()
                for r in read.groupBy("anio_particion").count().orderBy("anio_particion").collect()
            ]
            query = read.filter(F.col("anio_particion") == 2021).select(
                "fecha", "departamento", "velocidad_kmh"
            )
            actual = query.count()
            expected = df.filter(F.year("fecha") == 2021).count()
            assert actual == expected
            plan = query._jdf.queryExecution().executedPlan().toString()
            assert "PartitionFilters:" in plan and "2021" in plan
            (out / "consulta_particion_2021.txt").write_text(plan, encoding="utf-8")
            item["query_2021_rows"] = actual
            item["partition_filter_verified"] = True
        report["tables"][name] = item
        df.unpersist()
    report["status"] = "success"
finally:
    (out / "resultado_almacenamiento.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    s.stop()
result = subprocess.run(
    [
        "hdfs",
        "--config",
        "/tmp/hadoop-conf",
        "dfs",
        "-put",
        str(out / "resultado_almacenamiento.json"),
        f"hdfs://sutran/sutran/metadata/almacenamiento_{layout}.json",
    ],
    capture_output=True,
    text=True,
)
if result.returncode:
    raise RuntimeError("No se pudo publicar el manifiesto de almacenamiento: " + result.stderr)
summary = {
    "status": report["status"],
    "layout_id": layout,
    "pipeline_run": pointer["run_id"],
    "report_relative_path": str((out / "resultado_almacenamiento.json").relative_to(p)),
    "outputs": {k: {"path": v["path"], "rows": v["rows"]} for k, v in report["tables"].items()},
}
(p / "config/ultimo_almacenamiento.json").write_text(
    json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
)
print("ALMACENAMIENTO_OK " + str(out / "resultado_almacenamiento.json"))
