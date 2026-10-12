import csv
import math
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "Proyecto_SUTRAN/scripts"))
from contrato_modelo import NUMERIC_FEATURES, read_input


class InputContractTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "input.csv"
        self.row = dict.fromkeys(NUMERIC_FEATURES, 0)
        self.row.update(departamento="Junín", accidentes_previos=5, mes_coseno=1)
        for column in ("exceso_previo_kmh", "log_detecciones_previas", "log_pasos_peajes_previos"):
            self.row[column] = ""

    def write_input(self, row):
        with self.path.open("w", encoding="utf-8", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=row.keys())
            writer.writeheader()
            writer.writerow(row)

    def test_missing_measures_remain_null_and_department_is_normalized(self):
        self.write_input(self.row)
        result = read_input(self.path)[0]
        self.assertEqual(result["departamento"], "JUNIN")
        self.assertIsNone(result["log_pasos_peajes_previos"])
        self.assertEqual(result["accidentes_previos"], 5)

    def test_missing_required_column_is_rejected(self):
        self.row.pop("accidentes_previos")
        self.write_input(self.row)
        with self.assertRaisesRegex(ValueError, "Faltan columnas"):
            read_input(self.path)

    def test_non_finite_numbers_are_rejected(self):
        self.row["accidentes_previos"] = math.nan
        self.write_input(self.row)
        with self.assertRaisesRegex(ValueError, "no es finito"):
            read_input(self.path)

    def test_coverage_cannot_claim_missing_flow(self):
        self.row["cobertura_peajes_previa"] = 1
        self.write_input(self.row)
        with self.assertRaisesRegex(ValueError, "no coincide"):
            read_input(self.path)

    def test_negative_counts_are_rejected(self):
        self.row["accidentes_previos"] = -1
        self.write_input(self.row)
        with self.assertRaisesRegex(ValueError, "negativo"):
            read_input(self.path)


if __name__ == "__main__":
    unittest.main()
