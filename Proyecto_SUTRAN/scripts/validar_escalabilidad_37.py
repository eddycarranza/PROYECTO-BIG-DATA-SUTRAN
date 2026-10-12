"""Independent validation of all replicated serving values against real data."""

import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

p = Path("/project")
pointer = json.loads((p / "config/ultima_escalabilidad.json").read_text(encoding="utf-8-sig"))
out = (p / pointer["report_relative_path"].replace("\\", "/")).parent
result = json.loads((out / "resultado_escalabilidad.json").read_text(encoding="utf-8"))
base_pointer = json.loads((p / "config/ultimo_pipeline.json").read_text(encoding="utf-8-sig"))
s = SparkSession.builder.appName("SUTRAN-verificacion-replicas-3.7").getOrCreate()
ref = s.read.parquet(base_pointer["outputs"]["serving/indicadores_departamento_mes_2021"]["path"])
keys = ["replica_id", "departamento", "mes"]
fields = [x for x in ref.columns if x not in keys]
checks = []
try:
    for item in result["results"]:
        factor = item["factor"]
        pipeline = json.loads(
            next((out / f"factor_{factor}").glob("*/resultado_pipeline.json")).read_text(
                encoding="utf-8"
            )
        )
        actual = s.read.parquet(
            pipeline["outputs"]["serving/indicadores_departamento_mes_2021"]["path"]
        )
        expected = ref.crossJoin(
            s.range(factor).select(F.col("id").cast("string").alias("replica_id"))
        )
        expected = expected.select(*keys, *[F.col(x).alias("ref_" + x) for x in fields]).withColumn(
            "reference_present", F.lit(True)
        )
        actual = actual.withColumn("actual_present", F.lit(True))
        joined = actual.join(expected, keys, "full")
        invalid = F.col("actual_present").isNull() | F.col("reference_present").isNull()
        types = dict(ref.dtypes)
        for field in fields:
            a, b = F.col(field), F.col("ref_" + field)
            if types[field] in ["float", "double"]:
                equal = (a.isNull() & b.isNull()) | (
                    a.isNotNull()
                    & b.isNotNull()
                    & (F.abs(a - b) <= F.greatest(F.lit(1e-7), F.abs(b) * F.lit(1e-10)))
                )
            else:
                equal = a.eqNullSafe(b)
            invalid = invalid | ~F.coalesce(equal, F.lit(False))
        checked = joined.count()
        bad = joined.filter(invalid).count()
        assert checked == 218 * factor and bad == 0, (factor, checked, bad)
        checks.append(
            {
                "factor": factor,
                "rows_verified": checked,
                "different_rows": bad,
                "matches_real_serving": True,
            }
        )
    report = {
        "status": "passed",
        "application_id": s.sparkContext.applicationId,
        "checks": checks,
        "float_absolute_tolerance": 1e-7,
        "float_relative_tolerance": 1e-10,
    }
    (out / "verificacion_indicadores.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print("INDICADORES_VERIFICADOS " + json.dumps(checks), flush=True)
finally:
    s.stop()
