"""EDA reproducible de tablas reales validadas; exporta agregados, no microdatos."""

import csv
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.colors import ListedColormap
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

p = Path("/project")


def load(n):
    return json.loads((p / "config" / n).read_text(encoding="utf-8-sig"))


layout, quality, base = (
    load("ultimo_almacenamiento.json"),
    load("ultimo_control_calidad.json"),
    load("ultimo_pipeline.json"),
)
assert layout["status"] == "success" and quality["status"] == "passed"
assert layout["pipeline_run"] == base["run_id"] == quality["run_id_pipeline"]
run = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex[:8]
out = p / "documentation/eda_41" / run
out.mkdir(parents=True, exist_ok=False)
s = SparkSession.builder.appName("SUTRAN-EDA-4.1-datos-reales").getOrCreate()
s.conf.set("spark.sql.session.timeZone", "America/Lima")
paths = {k: v["path"] for k, v in layout["outputs"].items()}
assert all("/benchmark" not in path for path in paths.values())
report = {
    "status": "running",
    "run_id": run,
    "pipeline_run": base["run_id"],
    "layout_id": layout["layout_id"],
    "application_id": s.sparkContext.applicationId,
    "paths": paths,
    "period": ["2021-01-01", "2021-09-30"],
    "unit": "departamento-mes observado en accidentes",
    "synthetic_data_used": False,
}


def records(df):
    return [x.asDict(recursive=True) for x in df.collect()]


def export(name, rows):
    assert rows
    with (out / (name + ".csv")).open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)


def pic(name):
    plt.savefig(out / (name + ".png"), dpi=180, bbox_inches="tight", facecolor="white")
    plt.close()


plt.rcParams.update(
    {
        "font.family": "DejaVu Sans",
        "font.size": 11,
        "axes.spines.top": False,
        "axes.spines.right": False,
    }
)
try:
    frame = s.read.parquet(paths["serving/indicadores_departamento_mes_2021"])
    assert frame.count() == 218 and frame.select("departamento", "mes").distinct().count() == 218
    rows = records(frame.orderBy("departamento", "mes"))
    export("indicadores_reales", rows)
    data = pd.DataFrame(rows)
    accidents = s.read.parquet(paths["standardized/accidentes"]).filter(
        F.col("fecha").between(*report["period"])
    )
    stats = (
        accidents.agg(
            F.count("*").alias("records"),
            F.min("fecha").alias("first_date"),
            F.max("fecha").alias("last_date"),
            F.sum(F.col("fallecidos").isNull().cast("int")).alias("missing_fatalities"),
            F.sum(F.col("heridos").isNull().cast("int")).alias("missing_injuries"),
            F.sum(F.col("duplicado_sospechoso").cast("int")).alias("duplicate_flagged_rows"),
        )
        .first()
        .asDict()
    )
    stats["first_date"] = str(stats["first_date"])
    stats["last_date"] = str(stats["last_date"])
    assert int(data.registros_accidentes.sum()) == stats["records"] == 4138
    dates = records(
        accidents.groupBy("mes")
        .agg(
            F.min("fecha").alias("primera_fecha"),
            F.max("fecha").alias("ultima_fecha"),
            F.countDistinct("fecha").alias("dias_con_registros"),
        )
        .orderBy("mes")
    )
    for x in dates:
        x["primera_fecha"] = str(x["primera_fecha"])
        x["ultima_fecha"] = str(x["ultima_fecha"])
    export("fechas_por_mes", dates)
    dept = (
        data.groupby("departamento", as_index=False)
        .agg(
            registros_accidentes=("registros_accidentes", "sum"),
            fallecidos_reportados=("fallecidos_reportados", lambda x: x.sum(min_count=1)),
            heridos_reportados=("heridos_reportados", lambda x: x.sum(min_count=1)),
            meses_observados=("mes", "nunique"),
        )
        .sort_values(["registros_accidentes", "departamento"], ascending=[False, True])
    )
    export("resumen_departamentos", dept.to_dict("records"))
    top = dept.head(10).sort_values("registros_accidentes")
    fig, ax = plt.subplots(figsize=(9.6, 5.0))
    bars = ax.barh(top.departamento, top.registros_accidentes, color="#245B84")
    ax.bar_label(bars, padding=4, fontsize=10)
    ax.set_xlim(0, top.registros_accidentes.max() * 1.17)
    ax.set_xlabel("Registros de accidentes publicados")
    ax.set_title(
        "Diez departamentos con más registros\nEnero a septiembre de 2021", loc="left", pad=14
    )
    ax.grid(axis="x", alpha=0.15)
    ax.set_axisbelow(True)
    pic("01_concentracion_departamentos")
    monthly = data.groupby("mes", as_index=False).agg(
        registros_accidentes=("registros_accidentes", "sum"),
        fallecidos_reportados=("fallecidos_reportados", lambda x: x.sum(min_count=1)),
        heridos_reportados=("heridos_reportados", lambda x: x.sum(min_count=1)),
        departamentos_observados=("departamento", "nunique"),
    )
    export("resumen_mensual", monthly.to_dict("records"))
    fig, ax = plt.subplots(figsize=(9.6, 4.3))
    bars = ax.bar(monthly.mes.str[-2:], monthly.registros_accidentes, color="#245B84")
    ax.bar_label(bars, padding=4)
    ax.set_ylim(0, monthly.registros_accidentes.max() * 1.2)
    ax.set_xlabel("Mes de 2021")
    ax.set_ylabel("Registros de accidentes")
    ax.set_title(
        "Evolución mensual de los registros\nNo mide una tasa de riesgo", loc="left", pad=14
    )
    ax.grid(axis="y", alpha=0.15)
    ax.set_axisbelow(True)
    pic("02_evolucion_mensual")
    months = [f"2021-{m:02}" for m in range(1, 10)]
    depts = sorted(data.departamento.unique())
    matrix = np.full((len(depts), 9), -1, dtype=int)
    for x in rows:
        matrix[depts.index(x["departamento"]), months.index(x["mes"])] = int(
            x["cobertura_velocidad"]
        ) + 2 * int(x["cobertura_peajes"])
    colors = ["#EEEEEE", "#F1BA62", "#9978B5", "#6AA6CC", "#3B8062"]
    fig, ax = plt.subplots(figsize=(10, 7.1))
    im = ax.imshow(
        matrix,
        cmap=ListedColormap(colors),
        vmin=-1.5,
        vmax=3.5,
        aspect="auto",
        interpolation="nearest",
    )
    ax.set_yticks(range(len(depts)), depts, fontsize=9)
    ax.set_xticks(range(9), [m[-2:] for m in months])
    ax.set_xlabel("Mes de 2021")
    ax.set_title("Disponibilidad de fuentes en departamento y mes", loc="left", pad=14)
    from matplotlib.patches import Patch

    ax.legend(
        handles=[
            Patch(color=colors[i], label=label)
            for i, label in enumerate(
                [
                    "Sin fila de accidentes",
                    "Solo accidentes",
                    "Accidentes y velocidad",
                    "Accidentes y peajes",
                    "Las tres fuentes",
                ]
            )
        ],
        loc="upper center",
        bbox_to_anchor=(0.5, -0.09),
        ncol=2,
        frameon=False,
        fontsize=9,
    )
    pic("03_cobertura_integracion")
    covered = data[data.cobertura_velocidad].copy()
    correlation = float(
        covered.exceso_promedio_kmh.rank().corr(covered.registros_accidentes.rank())
    )
    fig, ax = plt.subplots(figsize=(9.6, 4.7))
    ax.scatter(
        covered.exceso_promedio_kmh,
        covered.registros_accidentes,
        s=42,
        alpha=0.65,
        color="#245B84",
        edgecolors="white",
        linewidth=0.5,
    )
    ax.set_xlabel("Exceso promedio observado sobre el límite en km/h")
    ax.set_ylabel("Registros de accidentes en el mes")
    ax.set_title(
        f"Asociación descriptiva en {len(covered)} unidades con velocidad\nCada punto representa un departamento y un mes",
        loc="left",
        pad=14,
    )
    ax.grid(alpha=0.15)
    ax.set_axisbelow(True)
    pic("04_asociacion_velocidad_accidentes")
    coverage = {
        "units": len(rows),
        "departments": len(depts),
        "grid_cells": len(depts) * 9,
        "absent_accident_cells": int((matrix == -1).sum()),
        "speed": int(data.cobertura_velocidad.sum()),
        "flow": int(data.cobertura_peajes.sum()),
        "both": int((data.cobertura_velocidad & data.cobertura_peajes).sum()),
        "neither": int((~data.cobertura_velocidad & ~data.cobertura_peajes).sum()),
    }
    route = s.read.parquet(paths["curated/accidentes_ruta_departamento_mes"])
    top_routes = records(
        route.groupBy("codigo_ruta", "departamento")
        .agg(
            F.sum("registros_accidentes").alias("registros_accidentes"),
            F.sum("fallecidos_reportados").alias("fallecidos_reportados"),
        )
        .orderBy(F.desc("registros_accidentes"), "departamento", "codigo_ruta")
        .limit(10)
    )
    export("rutas_departamentos_revision", top_routes)
    report.update(
        status="success",
        stats=stats,
        coverage=coverage,
        departments=dept.to_dict("records"),
        months=monthly.to_dict("records"),
        dates_by_month=dates,
        top_routes=top_routes,
        spearman_speed_accidents=correlation,
        speed_units=len(covered),
        top3_share_percent=float(dept.head(3).registros_accidentes.sum() / stats["records"] * 100),
        totals={
            "fatalities_reported": int(data.fallecidos_reportados.sum()),
            "injuries_reported": int(data.heridos_reportados.sum()),
        },
        figures=[
            "01_concentracion_departamentos.png",
            "02_evolucion_mensual.png",
            "03_cobertura_integracion.png",
            "04_asociacion_velocidad_accidentes.png",
        ],
    )
finally:
    (out / "resultado_eda.json").write_text(
        json.dumps(
            report,
            ensure_ascii=False,
            indent=2,
            default=lambda x: x.item() if hasattr(x, "item") else str(x),
        ),
        encoding="utf-8",
    )
    s.stop()
assert report["status"] == "success"
subprocess.run(
    [
        "hdfs",
        "--config",
        "/tmp/hadoop-conf",
        "dfs",
        "-put",
        str(out / "resultado_eda.json"),
        f"hdfs://sutran/sutran/metadata/eda_{run}.json",
    ],
    check=True,
)
(p / "config/ultimo_eda.json").write_text(
    json.dumps(
        {
            "status": "success",
            "run_id": run,
            "pipeline_run": base["run_id"],
            "report_relative_path": (out / "resultado_eda.json").relative_to(p).as_posix(),
        },
        indent=2,
    ),
    encoding="utf-8",
)
print("EDA_OK " + str(out), flush=True)
