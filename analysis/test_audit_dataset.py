"""Reject invalid exported records; run with opensafely run test_quality_gate."""
import csv
import tempfile
import unittest
from pathlib import Path
from audit_dataset import audit


def row():
    data = dict(patient_id="1", index_date="2023-01-01", age="70",
                followup_end_date="2024-01-01", admin_end_date="2024-01-01",
                mi_date="", stroke_date="", cvddeath_date="", mace_date="")
    for vaccine in ["covax", "fluvax"]:
        data.update({f"{vaccine}_most_recent_before_index": "",
                     f"{vaccine}_post30_date": "", f"{vaccine}_index_day": "F"})
    for flag in ["index_spell_mi", "index_spell_stroke", "index_primary_hip",
                 "index_hip_unspecified", "index_hip_strict", "reg_2y_at_index", "death_dates_disagree"]:
        data[flag] = "F"
    return data


class TestQualityGate(unittest.TestCase):
    def test_rejects_invalid_exports(self):
        cases = [
            ({"mi_date": "2024-01-02", "mace_date": "2024-01-02"}, False, "out_of_followup_mi"),
            ({"mi_date": "2023-02-01"}, False, "mace_not_earliest_component"),
            ({"covax_post30_date": "2023-01-01"}, False, "invalid_post30_covax"),
            ({}, True, "duplicate_patient_id"),
        ]
        for changes, duplicate, error in cases:
            with self.subTest(check=error), tempfile.TemporaryDirectory() as tmp:
                tmp_path = Path(tmp)
                record = row() | changes
                path = tmp_path / "dataset.csv"
                with path.open("w", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=list(record))
                    writer.writeheader()
                    writer.writerows([record] * (2 if duplicate else 1))
                with self.assertRaisesRegex(ValueError, "Dataset failed"):
                    audit(path, tmp_path / "quality")
                self.assertIn(error, (tmp_path / "quality/validation_errors.csv").read_text())


if __name__ == "__main__":
    result = unittest.main(exit=False).result
    if not result.wasSuccessful():
        raise SystemExit(1)
    Path("output/logs").mkdir(parents=True, exist_ok=True)
    Path("output/logs/test_quality_gate.txt").write_text("PASS: all four quality-gate rejection cases\n")
