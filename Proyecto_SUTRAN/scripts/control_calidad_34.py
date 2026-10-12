"""Comprueba las reglas de calidad sobre las salidas del pipeline."""

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


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
    specs = {
        "F01": (
            "accidentes",
            {
                "fecha": "date",
                "departamento": "string",
                "codigo_ruta": "string",
                "kilometro": "double",
                "fallecidos": "int",
                "heridos": "int",
            },
        ),
        "F02": (
            "velocidad_2019_2021",
            {
                "fecha": "date",
                "departamento": "string",
                "limite_kmh": "double",
                "velocidad_kmh": "double",
                "exceso_kmh": "double",
            },
        ),
        "F03": (
            "flujo_peajes",
            {
                "fecha": "date",
                "departamento": "string",
                "codigo_peaje": "string",
                "periodo": "string",
                "veh_total": "bigint",
                "veh_ligeros_total": "bigint",
                "veh_pesados_total": "bigint",
            },
        ),
        "F04": (
            "pesaje_2021_contexto",
            {
                "fecha_corte": "date",
                "departamento": "string",
                "ubigeo": "string",
                "latitud": "double",
                "longitud": "double",
            },
        ),
        "F05": (
            "red_vial_2018_atributos",
            {"codigo_ruta": "string", "departamento": "string", "fecha_referencia": "string"},
        ),
        "F06": (
            "velocidad_2022_reserva",
            {
                "fecha": "date",
                "departamento": "string",
                "limite_kmh": "double",
                "velocidad_kmh": "double",
                "exceso_kmh": "double",
            },
        ),
        "F07": (
            "pesaje_2022_reserva",
            {
                "fecha_corte": "date",
                "departamento": "string",
                "ubigeo": "string",
                "latitud": "double",
                "longitud": "double",
            },
        ),
    }

    def check(df, source, name, condition, severity="bloqueante"):
        failed = df.filter(F.coalesce(condition, F.lit(False))).count()
        rules.append(
            {
                "tabla": source,
                "regla": name,
                "severidad": severity,
                "incumplimientos": failed,
                "estado": "CUMPLE"
                if not failed
                else "REVISAR"
                if severity == "advertencia"
                else "NO_CUMPLE",
            }
        )

    def literal_rule(source, name, failed):
        rules.append(
            {
                "tabla": source,
                "regla": name,
                "severidad": "bloqueante",
                "incumplimientos": int(failed),
                "estado": "NO_CUMPLE" if failed else "CUMPLE",
            }
        )

    def profile(df, source, critical):
        n = df.count()
        expressions = []
        for i, (col, typ) in enumerate(df.dtypes):
            missing = F.col(col).isNull()
            if typ == "string":
                missing = missing | F.upper(F.trim(F.col(col))).isin(
                    "", "N.I.", "NI", "N/A", "NULL", "NONE", "S/D", "SIN DATO"
                )
            elif typ in ("double", "float"):
                missing = missing | F.isnan(F.col(col))
            expressions.append(F.sum(F.when(missing, 1).otherwise(0)).alias(f"f{i}"))
        counts = df.agg(*expressions).first().asDict()
        for i, (col, typ) in enumerate(df.dtypes):
            missing = int(counts[f"f{i}"] or 0)
            profiles.append(
                {
                    "tabla": source,
                    "campo": col,
                    "tipo": typ,
                    "campo_critico": col in critical,
                    "filas": n,
                    "faltantes": missing,
                    "completitud_pct": round(100 * (n - missing) / n, 4) if n else None,
                }
            )
        return n

    def iqr(df, source, col):
        clean = df.filter(F.col(col).isNotNull() & ~F.isnan(F.col(col)))
        quantiles = clean.approxQuantile(col, [0.25, 0.75], 0.01)
        if len(quantiles) != 2:
            return
        q1, q3 = quantiles
        spread = q3 - q1
        low, high = q1 - 1.5 * spread, q3 + 1.5 * spread
        count = (
            clean.filter((F.col(col) < low) | (F.col(col) > high)).count() if spread > 0 else None
        )
        outliers.append(
            {
                "tabla": source,
                "campo": col,
                "q1": q1,
                "q3": q3,
                "limite_inferior": low,
                "limite_superior": high,
                "filas_fuera_iqr": count,
                "error_relativo_cuantiles": 0.01,
                "decision": "Señalar para revisión; no eliminar automáticamente."
                if spread > 0
                else "IQR cero: criterio no concluyente, no eliminar.",
            }
        )

    try:
        for sid, (name, expected) in specs.items():
            print("AUDITANDO " + sid, flush=True)
            df = spark.read.parquet(pointer["outputs"]["standardized/" + name]["path"]).cache()
            n = profile(df, sid, set(expected))
            actual = dict(df.dtypes)
            mismatch = [col for col, typ in expected.items() if actual.get(col) != typ]
            literal_rule(sid, "Tipos esperados de columnas críticas", len(mismatch))
            literal_rule(
                sid,
                "Filas coinciden con salida del pipeline",
                n != previous["sources"][sid]["accepted"],
            )
            literal_rule(
                sid,
                "Originales = aceptados + rechazados",
                previous["sources"][sid]["raw_rows"] != n + previous["sources"][sid]["quarantined"],
            )
            result["tables"][sid] = {"rows": n, "schema_mismatch": mismatch}
            check(
                df,
                sid,
                "Departamento informado",
                F.col("departamento").isNull() | F.col("departamento").isin("", "N.I.", "NI"),
                "advertencia" if sid == "F05" else "bloqueante",
            )
            check(
                df,
                sid,
                "SHA-256 de procedencia presente",
                F.col("sha256_fuente").isNull() | ~F.col("sha256_fuente").rlike("^[a-f0-9]{64}$"),
            )
            if sid == "F01":
                check(df, sid, "Fecha válida", F.col("fecha").isNull())
                for col in ["kilometro", "fallecidos", "heridos"]:
                    check(df, sid, col + " no negativo", F.col(col) < 0)
                    check(df, sid, col + " informado", F.col(col).isNull(), "advertencia")
                check(
                    df,
                    sid,
                    "Código de ruta informado",
                    F.col("codigo_ruta").isNull() | F.col("codigo_ruta").isin("", "N.I.", "NI"),
                    "advertencia",
                )
                iqr(df, sid, "fallecidos")
                iqr(df, sid, "heridos")
            elif sid in ["F02", "F06"]:
                check(df, sid, "Fecha válida", F.col("fecha").isNull())
                check(
                    df,
                    sid,
                    "Límite positivo y velocidad no negativa",
                    F.col("limite_kmh").isNull()
                    | F.col("velocidad_kmh").isNull()
                    | (F.col("limite_kmh") <= 0)
                    | (F.col("velocidad_kmh") < 0),
                )
                check(
                    df,
                    sid,
                    "Exceso coincide con velocidad menos límite",
                    F.abs(F.col("exceso_kmh") - (F.col("velocidad_kmh") - F.col("limite_kmh")))
                    > 1e-8,
                )
                check(
                    df,
                    sid,
                    "Registro de exceso con velocidad superior al límite",
                    F.col("exceso_kmh") <= 0,
                    "advertencia",
                )
                iqr(df, sid, "velocidad_kmh")
                iqr(df, sid, "exceso_kmh")
            elif sid == "F03":
                check(
                    df,
                    sid,
                    "Período válido y coincide con año/mes",
                    F.col("fecha").isNull()
                    | (F.date_format("fecha", "yyyyMM") != F.col("periodo"))
                    | (F.year("fecha") != F.col("anio").cast("int"))
                    | (F.month("fecha") != F.col("mes").cast("int")),
                )
                check(
                    df,
                    sid,
                    "Total = ligeros + pesados",
                    F.col("veh_total").isNull()
                    | F.col("veh_ligeros_total").isNull()
                    | F.col("veh_pesados_total").isNull()
                    | (
                        F.col("veh_total")
                        != F.col("veh_ligeros_total") + F.col("veh_pesados_total")
                    ),
                )
                numeric = [
                    c for c in df.columns if c.startswith("veh_") and not c.endswith("_original")
                ]
                bad = F.lit(False)
                for col in numeric:
                    bad = bad | F.col(col).isNull() | (F.col(col) < 0)
                check(df, sid, "Todas las medidas de flujo informadas y no negativas", bad)
                literal_rule(
                    sid,
                    "Clave peaje-período única",
                    n - df.select("codigo_peaje", "periodo").distinct().count(),
                )
                iqr(df, sid, "veh_total")
            elif sid in ["F04", "F07"]:
                check(df, sid, "Fecha de corte válida", F.col("fecha_corte").isNull())
                check(
                    df,
                    sid,
                    "Coordenadas dentro de rangos geográficos",
                    F.col("latitud").isNull()
                    | F.col("longitud").isNull()
                    | ~F.col("latitud").between(-90, 90)
                    | ~F.col("longitud").between(-180, 180),
                )
                check(
                    df,
                    sid,
                    "UBIGEO de seis dígitos",
                    F.col("ubigeo").isNull() | ~F.col("ubigeo").rlike("^[0-9]{6}$"),
                )
            elif sid == "F05":
                check(
                    df,
                    sid,
                    "Código de ruta informado",
                    F.col("codigo_ruta").isNull() | (F.col("codigo_ruta") == ""),
                )
            quarantine = spark.read.parquet(pointer["outputs"]["quarantine/" + sid]["path"])
            literal_rule(
                sid,
                "Cantidad en cuarentena coincide con informe",
                quarantine.count() != previous["sources"][sid]["quarantined"],
            )
            for row in (
                quarantine.select(F.explode(F.split("motivos_rechazo", r"\|")).alias("motivo"))
                .groupBy("motivo")
                .count()
                .collect()
            ):
                rejections.append({"fuente": sid, "motivo": row["motivo"], "filas": row["count"]})
            df.unpersist()
        integrated = spark.read.parquet(
            pointer["outputs"]["curated/integracion_departamento_mes_2021"]["path"]
        ).cache()
        n = profile(integrated, "INTEGRACION", {"departamento", "mes", "registros_accidentes"})
        literal_rule(
            "INTEGRACION",
            "Clave departamento-mes única",
            n - integrated.select("departamento", "mes").distinct().count(),
        )
        literal_rule(
            "INTEGRACION",
            "Unión sin multiplicación",
            previous["join_rows_before"] != previous["join_rows_after"],
        )
        check(
            integrated,
            "INTEGRACION",
            "Mes dentro de enero-septiembre 2021",
            F.col("mes").isNull() | ~F.col("mes").between("2021-01", "2021-09"),
        )
        check(
            integrated,
            "INTEGRACION",
            "Conteo de accidentes positivo",
            F.col("registros_accidentes").isNull() | (F.col("registros_accidentes") <= 0),
        )
        check(
            integrated,
            "INTEGRACION",
            "Cobertura velocidad coincide con presencia de dato",
            F.col("cobertura_velocidad") != F.col("registros_velocidad").isNotNull(),
        )
        check(
            integrated,
            "INTEGRACION",
            "Cobertura peajes coincide con presencia de dato",
            F.col("cobertura_peajes") != F.col("pasos_vehiculares_peajes").isNotNull(),
        )
        result["rejected_total"] = sum(s["quarantined"] for s in previous["sources"].values())
        result["rules"] = rules
        result["outliers"] = outliers
        result["rejection_reasons"] = rejections
        result["blocking_failures"] = sum(r["estado"] == "NO_CUMPLE" for r in rules)
        result["warnings"] = sum(r["estado"] == "REVISAR" for r in rules)
        result["status"] = "passed" if not result["blocking_failures"] else "failed"
        result["notes"] = [
            "Completitud: no nulo, no NaN ni marcador explícito de ausencia; cero numérico es válido.",
            "Cobertura de fuentes externas se mide aparte; dato ausente no se sustituye por cero.",
            "IQR aproximado es una señal de revisión, no prueba de error ni criterio de descarte.",
            "Rango geográfico global y UBIGEO de seis dígitos no validan ubicación ni pertenencia administrativa.",
            "Este control no modifica las tablas de 3.3; un fallo bloqueante requiere corregir y reejecutar antes de entrenar.",
            "Motivos de rechazo pueden superponerse: no sumar motivos para calcular registros únicos rechazados.",
        ]
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
