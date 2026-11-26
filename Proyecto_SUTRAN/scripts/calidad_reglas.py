"""Reglas compartidas de calidad para Spark local y AWS Glue."""

from pyspark.sql import functions as F


def inspect_quality(
    spark,
    previous,
    outputs,
    result,
    profiles,
    rules,
    outliers,
    rejections,
    *,
    include_batches=False,
):
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
        status = "CUMPLE"
        if failed:
            status = "REVISAR" if severity == "advertencia" else "NO_CUMPLE"
        rules.append(
            {
                "tabla": source,
                "regla": name,
                "severidad": severity,
                "incumplimientos": failed,
                "estado": status,
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

    def audit_accidents(df, sid):
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

    def audit_speed(df, sid):
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
            F.abs(F.col("exceso_kmh") - (F.col("velocidad_kmh") - F.col("limite_kmh"))) > 1e-8,
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

    def audit_traffic(df, sid, n):
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
            | (F.col("veh_total") != F.col("veh_ligeros_total") + F.col("veh_pesados_total")),
        )
        numeric = [c for c in df.columns if c.startswith("veh_") and not c.endswith("_original")]
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

    def audit_weighing(df, sid):
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

    def audit_roads(df, sid):
        check(
            df,
            sid,
            "Código de ruta informado",
            F.col("codigo_ruta").isNull() | (F.col("codigo_ruta") == ""),
        )

    for sid, (name, expected) in specs.items():
        print("AUDITANDO " + sid, flush=True)
        df = spark.read.parquet(outputs["standardized/" + name]["path"]).cache()
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
        handlers = {
            "F01": audit_accidents,
            "F02": audit_speed,
            "F04": audit_weighing,
            "F05": audit_roads,
            "F06": audit_speed,
            "F07": audit_weighing,
        }
        if sid == "F03":
            audit_traffic(df, sid, n)
        else:
            handlers[sid](df, sid)
        quarantine = spark.read.parquet(outputs["quarantine/" + sid]["path"]).cache()
        try:
            literal_rule(
                sid,
                "Cantidad en cuarentena coincide con informe",
                quarantine.count() != previous["sources"][sid]["quarantined"],
            )
            for row in (
                quarantine.select(
                    F.explode(F.split(F.col("motivos_rechazo"), r"[|]")).alias("motivo")
                )
                .groupBy("motivo")
                .count()
                .collect()
            ):
                rejections.append(
                    {"fuente": sid, "motivo": row.motivo, "filas": row.asDict()["count"]}
                )
        finally:
            quarantine.unpersist()
        df.unpersist()
    integrated = spark.read.parquet(
        outputs["curated/integracion_departamento_mes_2021"]["path"]
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
    if include_batches:
        accidents = spark.read.parquet(outputs["standardized/accidentes"]["path"])
        historical_period = F.col("fecha").between("2021-01-01", "2021-09-30")
        if "lote_s3" in accidents.columns:
            historical_period = historical_period | F.col("lote_s3").isNotNull()
        expected = (
            accidents.filter(historical_period)
            .groupBy("departamento", "mes")
            .agg(F.count("*").alias("registros_accidentes"))
        )
        actual = integrated.select("departamento", "mes", "registros_accidentes")
        literal_rule(
            "INTEGRACION",
            "Conteo por departamento y mes coincide con accidentes estandarizados",
            expected.exceptAll(actual).count() + actual.exceptAll(expected).count(),
        )
        literal_rule(
            "INTEGRACION",
            "Conteo integrado coincide con informe",
            n != previous["join_rows_after"],
        )
    else:
        check(
            integrated,
            "INTEGRACION",
            "Mes dentro de enero-septiembre 2021",
            F.col("mes").isNull() | ~F.col("mes").between("2021-01", "2021-09"),
        )
    result["integrated_rows"] = n
    result["accepted_total"] = sum(s["accepted"] for s in previous["sources"].values())
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
    integrated.unpersist()
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
