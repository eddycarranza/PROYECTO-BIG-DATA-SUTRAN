"""Verifica las métricas publicadas contra las predicciones de la prueba real."""

import csv
import json
import math
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class PublishedMetricsTests(unittest.TestCase):
    def test_report_matches_predictions(self):
        directory = ROOT / "docs/evidencias/modelo"
        report = json.loads((directory / "resultado_modelo.json").read_text(encoding="utf-8"))
        with (directory / "predicciones_prueba.csv").open(encoding="utf-8-sig", newline="") as file:
            rows = list(csv.DictReader(file))
        self.assertEqual(len(rows), report["test_rows"])
        for name, column in [
            ("modelo", "prediccion_modelo"),
            ("referencia", "prediccion_referencia"),
        ]:
            errors = [float(row[column]) - float(row["label"]) for row in rows]
            mae = sum(abs(error) for error in errors) / len(errors)
            rmse = math.sqrt(sum(error**2 for error in errors) / len(errors))
            self.assertAlmostEqual(mae, report["metrics"][name]["mae"], places=10)
            self.assertAlmostEqual(rmse, report["metrics"][name]["rmse"], places=10)


if __name__ == "__main__":
    unittest.main()
