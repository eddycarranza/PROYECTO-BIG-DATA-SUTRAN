"""Ampliación descriptiva de EDA; no enlaza accidentes de 2021 con velocidad de 2022."""

import calendar
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
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

p = Path("/project")


def load(n):
    return json.loads((p / "config" / n).read_text(encoding="utf-8-sig"))


layout, quality, eda = (
    load("ultimo_almacenamiento.json"),
    load("ultimo_control_calidad.json"),
    load("ultimo_eda.json"),
)
assert (
    layout["status"] == "success" and quality["status"] == "passed" and eda["status"] == "success"
)
assert layout["pipeline_run"] == quality["run_id_pipeline"] == eda["pipeline_run"]
run = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex[:8]
out = p / "documentation/eda_velocidad_2022" / run
out.mkdir(parents=True, exist_ok=False)
s = SparkSession.builder.appName("SUTRAN-EDA-velocidad-2021-2022").getOrCreate()
s.conf.set("spark.sql.session.timeZone", "America/Lima")
report = {
    "status": "running",
    "run_id": run,
    "pipeline_run": layout["pipeline_run"],
    "eda_run": eda["run_id"],
    "application_id": s.sparkContext.applicationId,
    "synthetic_data_used": False,
    "accidents_2022_used": False,
}


def collect(df):
    return [x.asDict() for x in df.collect()]


def export(n, rows):
    with (out / (n + ".csv")).open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)


def savepic(n):
    plt.savefig(out / (n + ".png"), dpi=180, bbox_inches="tight", facecolor="white")
    plt.close()


try:
    a = s.read.parquet(layout["outputs"]["standardized/velocidad_2019_2021"]["path"])
    b = s.read.parquet(layout["outputs"]["standardized/velocidad_2022_reserva"]["path"])
    assert a.count() == 160018 and b.count() == 103092
    cols = ["fecha", "departamento", "exceso_kmh", "velocidad_kmh", "limite_kmh"]
    data = (
        a.select(*cols)
        .filter(F.year("fecha") == 2021)
        .unionByName(b.select(*cols).filter(F.year("fecha") == 2022))
    )
    assert b.filter(F.year("fecha") != 2022).count() == 0
    data = data.withColumn("anio", F.year("fecha")).withColumn("mes_numero", F.month("fecha"))
    yearly = collect(
        data.groupBy("anio")
        .agg(
            F.count("*").alias("registros"),
            F.min("fecha").alias("primera_fecha"),
            F.max("fecha").alias("ultima_fecha"),
            F.countDistinct("departamento").alias("departamentos"),
            F.avg("exceso_kmh").alias("exceso_promedio_kmh"),
            F.sum((F.col("exceso_kmh") <= 0).cast("int")).alias("registros_exceso_no_positivo"),
        )
        .orderBy("anio")
    )
    monthly = collect(
        data.groupBy("anio", "mes_numero")
        .agg(
            F.count("*").alias("registros"),
            F.min("fecha").alias("primera_fecha"),
            F.max("fecha").alias("ultima_fecha"),
            F.countDistinct("departamento").alias("departamentos"),
            F.countDistinct("fecha").alias("dias_con_registros"),
            F.avg("exceso_kmh").alias("exceso_promedio_kmh"),
        )
        .orderBy("anio", "mes_numero")
    )
    for row in yearly + monthly:
        row["primera_fecha"] = str(row["primera_fecha"])
        row["ultima_fecha"] = str(row["ultima_fecha"])
    export("resumen_anual", yearly)
    export("resumen_mensual", monthly)
    available = {
        year: {x["mes_numero"] for x in monthly if x["anio"] == year} for year in [2021, 2022]
    }
    excluded = {2021: [], 2022: []}
    for row in yearly:
        first = datetime.fromisoformat(row["primera_fecha"])
        last = datetime.fromisoformat(row["ultima_fecha"])
        if first.day != 1:
            excluded[row["anio"]].append(first.month)
        if last.day != calendar.monthrange(last.year, last.month)[1]:
            excluded[row["anio"]].append(last.month)
    common_months = sorted(
        (available[2021] - set(excluded[2021])) & (available[2022] - set(excluded[2022]))
    )
    unit = (
        data.filter(F.col("mes_numero").isin(common_months))
        .groupBy("anio", "departamento", "mes_numero")
        .agg(F.count("*").alias("registros"), F.sum("exceso_kmh").alias("suma_exceso_kmh"))
    )
    left = (
        unit.filter(F.col("anio") == 2021)
        .drop("anio")
        .withColumnRenamed("registros", "registros_2021")
        .withColumnRenamed("suma_exceso_kmh", "suma_exceso_2021")
    )
    right = (
        unit.filter(F.col("anio") == 2022)
        .drop("anio")
        .withColumnRenamed("registros", "registros_2022")
        .withColumnRenamed("suma_exceso_kmh", "suma_exceso_2022")
    )
    pair = left.join(right, ["departamento", "mes_numero"], "inner")
    assert pair.count() > 0
    paired = collect(pair.orderBy("departamento", "mes_numero"))
    export("unidades_comunes", paired)
    means = collect(
        pair.groupBy("mes_numero")
        .agg(
            F.sum("registros_2021").alias("registros_2021"),
            F.sum("registros_2022").alias("registros_2022"),
            (F.sum("suma_exceso_2021") / F.sum("registros_2021")).alias("exceso_promedio_2021"),
            (F.sum("suma_exceso_2022") / F.sum("registros_2022")).alias("exceso_promedio_2022"),
            F.count("*").alias("departamentos_comunes"),
        )
        .orderBy("mes_numero")
    )
    export("comparacion_meses_unidades_comunes", means)
    npairs = len(paired)
    n1 = sum(x["registros_2021"] for x in paired)
    n2 = sum(x["registros_2022"] for x in paired)
    e1 = sum(x["suma_exceso_2021"] for x in paired) / n1
    e2 = sum(x["suma_exceso_2022"] for x in paired) / n2
    plt.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "font.size": 11,
            "axes.spines.top": False,
            "axes.spines.right": False,
        }
    )
    fig, ax = plt.subplots(figsize=(9.6, 4.4))
    for year, color in [(2021, "#245B84"), (2022, "#B66A29")]:
        values = {x["mes_numero"]: x["registros"] for x in monthly if x["anio"] == year}
        ax.plot(
            range(1, 13),
            [values.get(m, np.nan) for m in range(1, 13)],
            marker="o",
            color=color,
            label=str(year),
            linewidth=2,
        )
    ax.set_xticks(range(1, 13), [f"{m:02}" for m in range(1, 13)])
    ax.set_xlabel("Mes del año")
    ax.set_ylabel("Detecciones publicadas")
    ax.set_title(
        "Volumen mensual de detecciones de velocidad\nLos meses sin registros publicados se muestran sin punto",
        loc="left",
        pad=14,
    )
    for row in yearly:
        for month in excluded[row["anio"]]:
            n = next(
                x["registros"]
                for x in monthly
                if x["anio"] == row["anio"] and x["mes_numero"] == month
            )
            ax.annotate(
                str(row["anio"]) + " mes parcial",
                xy=(month, n),
                xytext=(-90, 25),
                textcoords="offset points",
                fontsize=9,
                arrowprops={"arrowstyle": "->", "color": "#555555"},
            )
    ax.legend(frameon=False)
    ax.grid(alpha=0.15)
    ax.set_axisbelow(True)
    savepic("05_velocidad_volumen_2021_2022")
    fig, ax = plt.subplots(figsize=(9.6, 4.4))
    x = np.arange(len(means))
    w = 0.36
    ax.bar(x - w / 2, [i["exceso_promedio_2021"] for i in means], w, label="2021", color="#245B84")
    ax.bar(x + w / 2, [i["exceso_promedio_2022"] for i in means], w, label="2022", color="#B66A29")
    ax.set_xticks(x, [f"{i['mes_numero']:02}" for i in means])
    ax.set_xlabel("Mes con registros en ambos años")
    ax.set_ylabel("Exceso promedio observado en km/h")
    ax.set_title(
        "Exceso de velocidad en departamentos y meses comunes\nPromedios ponderados por número de detecciones",
        loc="left",
        pad=14,
    )
    ax.legend(frameon=False)
    ax.grid(axis="y", alpha=0.15)
    ax.set_axisbelow(True)
    savepic("06_velocidad_exceso_unidades_comunes")
    report.update(
        status="success",
        historical_source_records=160018,
        source_2022_records=103092,
        yearly=yearly,
        monthly=monthly,
        paired_units=npairs,
        paired_departments=len({x["departamento"] for x in paired}),
        paired_months=sorted({x["mes_numero"] for x in paired}),
        excluded_boundary_months=excluded,
        common_calendar_months=common_months,
        comparable={
            "records_2021": n1,
            "records_2022": n2,
            "excess_mean_2021": e1,
            "excess_mean_2022": e2,
            "difference_kmh": e2 - e1,
        },
        monthly_paired=means,
        figures=["05_velocidad_volumen_2021_2022.png", "06_velocidad_exceso_unidades_comunes.png"],
        method="Only department and calendar-month cells observed in both years, excluding global boundary months with partial observed date range; sums divided by records. Does not equalize device locations, limits, hours, operating days or reporting completeness.",
    )
finally:
    (out / "resultado_comparacion.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
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
        str(out / "resultado_comparacion.json"),
        f"hdfs://sutran/sutran/metadata/eda_velocidad_{run}.json",
    ],
    check=True,
)
(p / "config/ultima_comparacion_velocidad.json").write_text(
    json.dumps(
        {
            "status": "success",
            "run_id": run,
            "eda_run": eda["run_id"],
            "report_relative_path": (out / "resultado_comparacion.json").relative_to(p).as_posix(),
        },
        indent=2,
    ),
    encoding="utf-8",
)
print("COMPARACION_OK " + str(out), flush=True)
