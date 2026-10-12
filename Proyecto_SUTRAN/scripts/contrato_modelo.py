"""Validación de entradas del modelo; no requiere Spark."""

import csv
import math
import unicodedata
from pathlib import Path

NUMERIC_FEATURES = [
    "accidentes_previos",
    "fallecidos_previos",
    "heridos_previos",
    "exceso_previo_kmh",
    "log_detecciones_previas",
    "log_pasos_peajes_previos",
    "cobertura_velocidad_previa",
    "cobertura_peajes_previa",
    "mes_seno",
    "mes_coseno",
]
NULLABLE_FEATURES = {
    "fallecidos_previos",
    "heridos_previos",
    "exceso_previo_kmh",
    "log_detecciones_previas",
    "log_pasos_peajes_previos",
}


def normalize_department(value):
    text = unicodedata.normalize("NFKD", str(value or "").strip().upper())
    return " ".join("".join(c for c in text if not unicodedata.combining(c)).split())


def read_input(path, max_rows=10000):
    """Lee un CSV de características ya agregadas; rechaza valores ambiguos."""
    rows = []
    with Path(path).open(encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        required = {"departamento", *NUMERIC_FEATURES}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError("Faltan columnas: " + ", ".join(sorted(missing)))
        for index, record in enumerate(reader, start=2):
            if len(rows) >= max_rows:
                raise ValueError(f"El CSV supera el límite de {max_rows} filas.")
            row = {"departamento": normalize_department(record["departamento"])}
            if not row["departamento"]:
                raise ValueError(f"Fila {index}: departamento vacío.")
            for column in NUMERIC_FEATURES:
                raw = (record[column] or "").strip()
                if not raw and column in NULLABLE_FEATURES:
                    row[column] = None
                    continue
                try:
                    number = float(raw)
                except ValueError as exc:
                    raise ValueError(f"Fila {index}: {column} debe ser numérico.") from exc
                if not math.isfinite(number):
                    raise ValueError(f"Fila {index}: {column} no es finito.")
                if column in ("mes_seno", "mes_coseno"):
                    if abs(number) > 1:
                        raise ValueError(f"Fila {index}: {column} debe estar entre -1 y 1.")
                elif number < 0:
                    raise ValueError(f"Fila {index}: {column} no puede ser negativo.")
                if column.startswith("cobertura_") and number not in (0, 1):
                    raise ValueError(f"Fila {index}: {column} debe ser 0 o 1.")
                row[column] = number
            for coverage, measure in (
                ("cobertura_velocidad_previa", "log_detecciones_previas"),
                ("cobertura_peajes_previa", "log_pasos_peajes_previos"),
            ):
                if bool(row[coverage]) != (row[measure] is not None):
                    raise ValueError(f"Fila {index}: {coverage} no coincide con {measure}.")
            rows.append(row)
    if not rows:
        raise ValueError("El archivo no contiene filas de datos.")
    return rows
