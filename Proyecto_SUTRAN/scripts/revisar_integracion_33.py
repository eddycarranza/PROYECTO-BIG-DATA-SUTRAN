"""Revisa formatos, claves y correspondencias de las fuentes originales."""

import csv
import hashlib
import json
import re
import struct
import unicodedata
import zipfile
from collections import Counter
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CONFIG = json.loads((ROOT / "config/sources.json").read_text(encoding="utf-8"))
INPUT = Path(CONFIG["input_dir"])
if not INPUT.is_absolute():
    INPUT = ROOT / INPUT
OUT = ROOT / "documentation/revision_33"


def norm(value):
    text = unicodedata.normalize("NFKD", str(value or "").strip().upper())
    return " ".join("".join(c for c in text if not unicodedata.combining(c)).split())


def route(value):
    return re.sub(r"\s+", "", norm(value))


def csv_rows(path):
    text = path.read_bytes().decode("cp1252")
    rows = list(csv.reader(text.splitlines(), delimiter=";"))
    headers = [norm(h) if h.strip() else f"SIN_NOMBRE_{i + 1}" for i, h in enumerate(rows[0])]
    if len(set(headers)) != len(headers):
        raise ValueError("Encabezados repetidos")
    if any(len(row) != len(headers) for row in rows[1:]):
        raise ValueError(f"Filas de longitud inesperada: {path.name}")
    return headers, [dict(zip(headers, row)) for row in rows[1:]]


def dbf_rows(path):
    with zipfile.ZipFile(path) as z:
        b = z.read(next(n for n in z.namelist() if n.lower().endswith(".dbf")))
    n, head, length = struct.unpack_from("<IHH", b, 4)
    fields = []
    pos = 32
    while b[pos] != 13:
        f = b[pos : pos + 32]
        fields.append((f[:11].split(b"\0")[0].decode("ascii"), f[16]))
        pos += 32
    result = []
    for i in range(n):
        data = b[head + i * length : head + (i + 1) * length]
        if data[:1] == b"*":
            continue
        row, offset = {}, 1
        for name, width in fields:
            raw = data[offset : offset + width]
            try:
                row[name] = raw.decode("utf-8").strip()
            except UnicodeDecodeError:
                row[name] = raw.decode("cp1252").strip()
            offset += width
        result.append(row)
    return result


def main():
    sources = {s["id"]: s for s in CONFIG["sources"]}
    paths = {}
    for s in sources.values():
        path = INPUT / s["original_name"]
        if not path.is_file():
            path = (
                ROOT
                / "data_lake/raw"
                / ("source=" + s["slug"])
                / ("acquired=" + s["acquisition_date"])
                / ("version=" + s["sha256"])
                / s["stored_name"]
            )
        with path.open("rb") as stream:
            digest = hashlib.file_digest(stream, "sha256").hexdigest()
        if digest != s["sha256"]:
            raise ValueError(f"Fuente modificada: {s['id']}")
        paths[s["id"]] = path
    _, accidents = csv_rows(paths["F01"])
    _, speed = csv_rows(paths["F02"])
    flow_headers, flow = csv_rows(paths["F03"])
    _, stations = csv_rows(paths["F04"])
    roads = dbf_rows(paths["F05"])
    road_routes = {route(r["CODRUTA"]) for r in roads}
    road_pairs = {(route(r["CODRUTA"]), norm(r["DEPARTAMEN"])) for r in roads}
    eligible = [r for r in accidents if "20210101" <= r["FECHA"] <= "20210930"]
    hits = [r for r in eligible if route(r["CODIGO_VIA"]) in road_routes]
    pair_hits = [r for r in hits if (route(r["CODIGO_VIA"]), norm(r["DEPARTAMENTO"])) in road_pairs]
    not_found = Counter(
        route(r["CODIGO_VIA"]) for r in eligible if route(r["CODIGO_VIA"]) not in road_routes
    )
    dates_invalid = {}
    for label, data, column in [
        ("accidentes", accidents, "FECHA"),
        ("velocidad", speed, "FECHA_PAPELETA"),
    ]:
        invalid = 0
        for row in data:
            try:
                datetime.strptime(row[column], "%Y%m%d")
            except ValueError:
                invalid += 1
        dates_invalid[label] = invalid
    summary = {
        "estado": "Auditoría de las fuentes originales; consultar el informe del pipeline para sus salidas",
        "fuentes_sha256_verificadas": 7,
        "accidentes_filas_originales": len(accidents),
        "accidentes_duplicados_exactos_excedentes": len(accidents)
        - len(set(tuple(r.values()) for r in accidents)),
        "periodo_candidato": "2021-01-01 a 2021-09-30",
        "accidentes_filas_periodo": len(eligible),
        "accidentes_coinciden_codigo_ruta_2018": len(hits),
        "accidentes_coinciden_codigo_y_departamento_2018": len(pair_hits),
        "sin_correspondencia_codigo": dict(not_found.most_common()),
        "filas_red_vial": len(roads),
        "codigos_red_vial_distintos": len(road_routes),
        "fechas_invalidas": dates_invalid,
        "velocidad_filas_periodo": sum(
            "20210101" <= r["FECHA_PAPELETA"] <= "20210930" for r in speed
        ),
        "carreteras_velocidad": dict(Counter(norm(r["CARRETERA"]) for r in speed)),
        "departamentos_accidentes": dict(Counter(norm(r["DEPARTAMENTO"]) for r in eligible)),
        "peaje_mes_claves_duplicadas_excedentes": len(flow)
        - len({(r["CODIGO_PEAJE"], r["PERIODO"]) for r in flow}),
        "flujo_filas_con_datos_sin_encabezado": sum(
            any(r[h].strip() for h in flow_headers if h.startswith("SIN_NOMBRE_")) for r in flow
        ),
        "flujo_total_inconsistente": sum(
            int(r["VEH_TOTAL"].replace(" ", ""))
            != int(r["VEH_LIGEROS_TOTAL"].replace(" ", ""))
            + int(r["VEH_PESADOS_TOTAL"].replace(" ", ""))
            for r in flow
        ),
        "estaciones_2021": len(stations),
        "advertencias": [
            "Coincidir con un codigo de 2018 no acredita vigencia ni pertenencia del evento en 2021.",
            "Los conteos incluyen duplicados sospechosos: no se afirman accidentes unicos.",
            "No unir eventos individuales entre si por departamento/mes: multiplicaria filas.",
            "F02 no trae codigo de ruta ni kilometro: falta asignacion espacial validada para tramo/mes.",
            "F03 no trae ubicacion ni codigo explicito de ruta: falta catalogo o correspondencia oficial de peajes.",
        ],
    }
    OUT.mkdir(parents=True, exist_ok=True)
    key_counts = Counter((r["CODIGO_PEAJE"], r["PERIODO"]) for r in flow)
    conflict_rows = [r for r in flow if key_counts[(r["CODIGO_PEAJE"], r["PERIODO"])] > 1]
    summary["peaje_mes_grupos_repetidos"] = sum(n > 1 for n in key_counts.values())
    summary["peaje_mes_filas_en_grupos_repetidos"] = len(conflict_rows)
    with (OUT / "peajes_claves_repetidas.csv").open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=flow_headers)
        writer.writeheader()
        writer.writerows(conflict_rows)
    with (OUT / "dim_rutas_referencia_2018.csv").open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["codigo_ruta", "fecha_referencia", "sha256_fuente"])
        writer.writerows(
            (code, "2018-12", sources["F05"]["sha256"]) for code in sorted(road_routes)
        )
    (OUT / "auditoria_integracion.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    with (OUT / "rutas_sin_correspondencia.csv").open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["codigo_normalizado", "filas_accidentes_enero_septiembre_2021"])
        writer.writerows(not_found.most_common())
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
