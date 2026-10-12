"""Ejecuta las transformaciones sobre réplicas para medir escalabilidad.

Las salidas de esta prueba se mantienen fuera del análisis y del entrenamiento.
"""

import argparse
import csv
import hashlib
import json
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from transformaciones import header, norm, number


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", required=True)
    ap.add_argument("--output", default="/project/documentation/pipeline_33")
    args = ap.parse_args()
    manifest = json.loads(Path(args.manifest).read_text(encoding="utf-8"))
    assert manifest["status"] == "success" and manifest["target"] == "hdfs"
    sources = {s["id"]: s for s in manifest["records"]}
    assert len(sources) == 7 and all(s["integrity_verified"] for s in sources.values())
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex[:8]
    benchmark = manifest["benchmark"]
    factor = benchmark["factor"]
    root = benchmark["root"] + "/factor=" + str(factor)
    out = Path(args.output) / run_id
    out.mkdir(parents=True, exist_ok=False)
    report = {
        "run_id": run_id,
        "status": "running",
        "sources": {},
        "outputs": {},
        "periodo_integracion": "2021-01-01 a 2021-09-30",
        "limitations": [
            "Registros repetidos de accidentes retenidos y señalados: no hay ID de evento.",
            "Unión de velocidad y flujo solo por departamento-mes, no por tramo.",
            "Flujo representa conteos de paso en peajes; no vehículos únicos ni exposición exacta a accidentes.",
            "Rutas 2018 son referencia histórica, no certifican vigencia en 2021.",
            "Estaciones con corte octubre 2021 y datos 2022 no se unen como variables históricas de enero-septiembre 2021.",
            "Sin clima ni modelo entrenado en esta etapa.",
        ],
    }
    staged_inputs = tempfile.TemporaryDirectory(prefix="sutran_convert_")
    for sid in []:
        local = Path(staged_inputs.name) / sources[sid]["stored_name"]
        result = subprocess.run(
            [
                "hdfs",
                "--config",
                "/tmp/hadoop-conf",
                "dfs",
                "-get",
                sources[sid]["destination"],
                str(local),
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode:
            raise RuntimeError(f"Descarga HDFS {sid}: {result.stderr}")
        with local.open("rb") as f:
            assert hashlib.file_digest(f, "sha256").hexdigest() == sources[sid]["sha256"]
    spark = SparkSession.builder.appName(
        "SUTRAN-escalabilidad-3.7-" + str(factor) + "x"
    ).getOrCreate()
    spark.conf.set("spark.sql.ansi.enabled", "false")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.session.timeZone", "America/Lima")
    report["benchmark_factor"] = factor
    report["base_pipeline_sha256"] = (
        "7fde4afdeca15bb6433abd9ebdf041e1664e36909d64f43a293a00abbafabd66"
    )
    report["scope"] = (
        "Seven physically replicated raw CSV sources -> validation, cleaning, duplicate checks, joins, Parquet writes and read-back counts"
    )
    report["application_id"] = spark.sparkContext.applicationId
    started = datetime.now(timezone.utc)

    def hdfs(*argv):
        result = subprocess.run(
            ["hdfs", "--config", "/tmp/hadoop-conf", "dfs", *argv], capture_output=True, text=True
        )
        if result.returncode:
            raise RuntimeError(result.stderr)
        return result

    def save(df, zone, name):
        print("ESCRIBIENDO " + zone + "/" + name, flush=True)
        path = f"{root}/{zone}/run={run_id}/{name}"
        rows = df.count()
        df.write.mode("errorifexists").parquet(path)
        actual = spark.read.parquet(path).count()
        assert actual == rows, f"Conteo diferente después de escribir {name}"
        report["outputs"][f"{zone}/{name}"] = {"path": path, "rows": rows}
        return path

    def tag(df, sid):
        return (
            df.withColumn("fuente_id", F.lit(sid))
            .withColumn("sha256_fuente", F.lit(sources[sid]["sha256"]))
            .withColumn("fecha_obtencion", F.lit(sources[sid]["acquisition_date"]))
            .withColumn("fila_hash", F.sha2(F.to_json(F.struct(*df.columns)), 256))
        )

    def split(df, checks, sid):
        conditions = [
            F.when(F.coalesce(test, F.lit(False)), F.lit(reason)) for test, reason in checks
        ]
        df = df.withColumn("motivos_rechazo", F.concat_ws("|", *conditions)).cache()
        bad = df.filter(F.col("motivos_rechazo") != "")
        good = df.filter(F.col("motivos_rechazo") == "").drop("motivos_rechazo").cache()
        total, accepted, rejected = df.count(), good.count(), bad.count()
        assert total == accepted + rejected
        save(bad, "quarantine", sid)
        report["sources"][sid].update(accepted=accepted, quarantined=rejected)
        df.unpersist()
        return good

    try:
        assert spark.sparkContext.master == "yarn"
        frames = {}
        for sid in ["F01", "F02", "F03", "F04", "F05", "F06", "F07"]:
            raw = (
                spark.read.option("header", True)
                .option("sep", ";")
                .option("encoding", "UTF-8")
                .option("multiLine", True)
                .option("escape", '"')
                .option("mode", "FAILFAST")
                .csv(sources[sid]["benchmark_paths"])
            )
            names = [header(c) or f"extra_{i}" for i, c in enumerate(raw.columns)]
            assert len(set(names)) == len(names), f"Encabezados ambiguos en {sid}"
            df = tag(raw.toDF(*names), sid).cache()
            n = df.count()
            assert n == sources[sid]["records_before_cleaning"], (
                f"Filas originales cambiaron: {sid}"
            )
            report["sources"][sid] = {"raw_rows": n, "input": sources[sid]["destination"]}
            frames[sid] = df

        a = frames["F01"]
        for sid, df in frames.items():
            duplicate_groups = df.groupBy("fila_hash").count()
            report["sources"][sid]["duplicates_extra"] = duplicate_groups.select(
                F.coalesce(F.sum(F.col("count") - 1), F.lit(0))
            ).first()[0]
            report["sources"][sid]["duplicate_policy"] = (
                "Retenidos para revisión; flujo excluye además claves peaje-periodo ambiguas."
            )
        duplicates = a.groupBy("fila_hash").agg(F.count("*").alias("repeticiones_exactas"))
        a = a.join(duplicates, "fila_hash", "left").withColumn(
            "duplicado_sospechoso", F.col("repeticiones_exactas") > 1
        )
        a = (
            a.withColumn("fecha", F.to_date("fecha", "yyyyMMdd"))
            .withColumn("mes", F.date_format("fecha", "yyyy-MM"))
            .withColumn("departamento", norm("departamento"))
            .withColumn("codigo_ruta", F.regexp_replace(norm("codigo_via"), r"\s+", ""))
            .withColumn("kilometro_original", F.col("kilometro"))
            .withColumn("kilometro", number("kilometro"))
            .withColumn("fallecidos_original", F.col("fallecidos"))
            .withColumn("fallecidos", number("fallecidos", "int"))
            .withColumn("heridos_original", F.col("heridos"))
            .withColumn("heridos", number("heridos", "int"))
        )
        a = split(
            a,
            [
                (F.col("fecha").isNull(), "fecha_invalida"),
                (
                    F.col("departamento").isNull() | F.col("departamento").isin("", "N.I.", "NI"),
                    "departamento_no_informado",
                ),
                (
                    (F.col("kilometro") < 0) | (F.col("fallecidos") < 0) | (F.col("heridos") < 0),
                    "medida_negativa",
                ),
            ],
            "F01",
        )
        report["sources"]["F01"]["duplicates_extra"] = duplicates.select(
            F.sum(F.col("repeticiones_exactas") - 1)
        ).first()[0]
        save(a, "standardized", "accidentes")

        speeds = {}
        for sid in ["F02", "F06"]:
            d = frames[sid]
            dept = "region" if sid == "F02" else "departamento"
            limit = "limite_velocidad" if sid == "F02" else "velocidad_fiscalizada"
            d = (
                d.withColumn("departamento", norm(dept))
                .withColumn("fecha", F.to_date("fecha_papeleta", "yyyyMMdd"))
                .withColumn("mes", F.date_format("fecha", "yyyy-MM"))
                .withColumn("limite_kmh", number(limit))
                .withColumn("velocidad_kmh", number("velocidad_detectada"))
                .withColumn("carretera_normalizada", norm("carretera"))
                .withColumn("exceso_kmh", F.col("velocidad_kmh") - F.col("limite_kmh"))
            )
            d = split(
                d,
                [
                    (F.col("fecha").isNull(), "fecha_invalida"),
                    (
                        F.col("limite_kmh").isNull()
                        | F.col("velocidad_kmh").isNull()
                        | (F.col("limite_kmh") <= 0)
                        | (F.col("velocidad_kmh") < 0),
                        "velocidad_invalida",
                    ),
                    (
                        F.col("departamento").isNull()
                        | F.col("departamento").isin("", "N.I.", "NI"),
                        "departamento_no_informado",
                    ),
                ],
                sid,
            )
            save(
                d,
                "standardized",
                "velocidad_2019_2021" if sid == "F02" else "velocidad_2022_reserva",
            )
            speeds[sid] = d

        flow = frames["F03"].withColumn("departamento", norm("departamento"))
        flow = flow.withColumn(
            "fecha", F.to_date(F.concat(F.col("periodo"), F.lit("01")), "yyyyMMdd")
        ).withColumn("mes_periodo", F.date_format("fecha", "yyyy-MM"))
        numeric = [c for c in flow.columns if c.startswith("veh_")]
        for col in numeric:
            flow = flow.withColumn(col + "_original", F.col(col)).withColumn(
                col, number(col, "long")
            )
        extra = [
            c
            for c in frames["F03"].columns
            if c.startswith("c") and c[1:].isdigit() or c.startswith("extra_")
        ]
        key = flow.groupBy("replica_id", "codigo_peaje", "periodo").agg(
            F.count("*").alias("filas_clave")
        )
        flow = flow.join(key, ["replica_id", "codigo_peaje", "periodo"], "left")
        no_header = F.lit(False)
        for col in extra:
            no_header = no_header | (F.length(F.trim(F.col(col))) > 0)
        flow = split(
            flow,
            [
                (F.col("fecha").isNull(), "periodo_invalido"),
                (F.col("filas_clave") > 1, "peaje_periodo_ambiguo"),
                (no_header, "datos_sin_encabezado"),
                (F.col("veh_total").isNull() | (F.col("veh_total") < 0), "flujo_invalido"),
                (
                    F.col("veh_total") != F.col("veh_ligeros_total") + F.col("veh_pesados_total"),
                    "total_inconsistente",
                ),
            ],
            "F03",
        )
        assert (
            flow.count() == flow.select("replica_id", "codigo_peaje", "periodo").distinct().count()
        )
        save(flow, "standardized", "flujo_peajes")

        for sid in ["F04", "F07"]:
            d = (
                frames[sid]
                .withColumn("departamento", norm("region"))
                .withColumn("ubigeo", F.lpad(F.trim("ubigeo"), 6, "0"))
                .withColumn("latitud", number("latitud"))
                .withColumn("longitud", number("longitud"))
                .withColumn("fecha_corte", F.to_date("fecha_corte", "yyyyMMdd"))
            )
            d = split(
                d,
                [
                    (F.col("fecha_corte").isNull(), "corte_invalido"),
                    (
                        F.col("latitud").isNull()
                        | F.col("longitud").isNull()
                        | ~F.col("latitud").between(-90, 90)
                        | ~F.col("longitud").between(-180, 180),
                        "coordenada_invalida",
                    ),
                ],
                sid,
            )
            save(
                d, "standardized", "pesaje_2021_contexto" if sid == "F04" else "pesaje_2022_reserva"
            )

        roads = (
            frames["F05"]
            .withColumn("codigo_ruta", F.regexp_replace(norm("codruta"), r"\s+", ""))
            .withColumn("departamento", norm("departamen"))
            .withColumn("fecha_referencia", F.lit("2018-12"))
        )
        roads = split(
            roads,
            [(F.col("codigo_ruta").isNull() | (F.col("codigo_ruta") == ""), "codigo_ruta_vacio")],
            "F05",
        )
        save(roads, "standardized", "red_vial_2018_atributos")
        dim = roads.select("replica_id", "codigo_ruta", "departamento").distinct()
        save(dim, "curated", "dimension_ruta_departamento_2018")
        period = a.filter(F.col("fecha").between("2021-01-01", "2021-09-30"))
        matched = period.join(
            dim.withColumn("referencia_2018_presente", F.lit(True)),
            ["replica_id", "codigo_ruta", "departamento"],
            "left",
        )
        assert matched.count() == period.count(), "El join multiplicó accidentes"
        save(
            matched.filter(F.col("referencia_2018_presente").isNull()),
            "quarantine",
            "rutas_sin_correspondencia_2018_revision",
        )
        # Falta de correspondencia histórica no invalida el registro original.
        route_month = matched.groupBy("replica_id", "codigo_ruta", "departamento", "mes").agg(
            F.count("*").alias("registros_accidentes"),
            F.sum("fallecidos").alias("fallecidos_reportados"),
            F.sum("heridos").alias("heridos_reportados"),
            F.sum(F.col("duplicado_sospechoso").cast("int")).alias(
                "registros_duplicados_sospechosos"
            ),
            F.max(F.coalesce("referencia_2018_presente", F.lit(False))).alias(
                "referencia_2018_presente"
            ),
        )
        save(route_month, "curated", "accidentes_ruta_departamento_mes")
        accidents_month = period.groupBy("replica_id", "departamento", "mes").agg(
            F.count("*").alias("registros_accidentes"),
            F.sum("fallecidos").alias("fallecidos_reportados"),
            F.sum("heridos").alias("heridos_reportados"),
            F.sum(F.col("duplicado_sospechoso").cast("int")).alias(
                "registros_duplicados_sospechosos"
            ),
            F.sum(F.col("fallecidos").isNull().cast("int")).alias(
                "registros_sin_fallecidos_informados"
            ),
        )
        speed_month = (
            speeds["F02"]
            .filter(F.col("fecha").between("2021-01-01", "2021-09-30"))
            .groupBy("replica_id", "departamento", "mes")
            .agg(
                F.count("*").alias("registros_velocidad"),
                F.avg("exceso_kmh").alias("exceso_promedio_kmh"),
            )
        )
        flow_month = (
            flow.filter(F.col("fecha").between("2021-01-01", "2021-09-30"))
            .withColumnRenamed("mes_periodo", "mes_flujo")
            .groupBy("replica_id", "departamento", "mes_flujo")
            .agg(
                F.sum("veh_total").alias("pasos_vehiculares_peajes"),
                F.countDistinct("codigo_peaje").alias("peajes_con_datos"),
            )
            .withColumnRenamed("mes_flujo", "mes")
        )
        for d in [accidents_month, speed_month, flow_month]:
            assert d.count() == d.select("replica_id", "departamento", "mes").distinct().count()
        integrated = (
            accidents_month.join(speed_month, ["replica_id", "departamento", "mes"], "left")
            .join(flow_month, ["replica_id", "departamento", "mes"], "left")
            .withColumn("cobertura_velocidad", F.col("registros_velocidad").isNotNull())
            .withColumn("cobertura_peajes", F.col("pasos_vehiculares_peajes").isNotNull())
            .withColumn("nivel_geografico", F.lit("departamento"))
        )
        assert integrated.count() == accidents_month.count()
        save(integrated, "curated", "integracion_departamento_mes_2021")
        path = save(integrated, "serving", "indicadores_departamento_mes_2021")
        rows = [r.asDict() for r in integrated.orderBy("departamento", "mes").collect()]
        with (out / "indicadores_departamento_mes.csv").open(
            "w", encoding="utf-8-sig", newline=""
        ) as f:
            writer = csv.DictWriter(f, fieldnames=integrated.columns)
            writer.writeheader()
            writer.writerows(rows)
        report["serving_path"] = path
        report["join_rows_before"] = accidents_month.count()
        report["join_rows_after"] = integrated.count()
        baseline_pointer = json.loads(
            Path("/project/config/ultimo_pipeline.json").read_text(encoding="utf-8-sig")
        )
        baseline = json.loads(
            (Path("/project") / baseline_pointer["report_relative_path"]).read_text(
                encoding="utf-8"
            )
        )
        checks = []
        for sid, item in report["sources"].items():
            for field in ["raw_rows", "accepted", "quarantined", "duplicates_extra"]:
                assert item[field] == baseline["sources"][sid][field] * factor, (
                    sid,
                    field,
                    item[field],
                )
                checks.append({"source": sid, "field": field, "passed": True})
        for name, item in report["outputs"].items():
            assert item["rows"] == baseline["outputs"][name]["rows"] * factor, (name, item["rows"])
        serving = spark.read.parquet(path)
        counts = serving.groupBy("replica_id").count().collect()
        assert len(counts) == factor and all(x["count"] == 218 for x in counts)
        report["scale_checks"] = checks
        report["replicas_verified"] = factor
        report["status"] = "success"
    except Exception as exc:
        report["status"] = "failed"
        report["error"] = str(exc)
        raise
    finally:
        report["elapsed_seconds"] = (datetime.now(timezone.utc) - started).total_seconds()
        (out / "resultado_pipeline.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        spark.stop()
        staged_inputs.cleanup()
    print("PIPELINE_OK " + str(out / "resultado_pipeline.json"))


if __name__ == "__main__":
    main()
