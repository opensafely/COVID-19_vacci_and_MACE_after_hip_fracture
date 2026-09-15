"""The quality gate must reject implausible exported records."""
import csv
import pytest
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


@pytest.mark.parametrize("changes,duplicate,error", [
    ({"mi_date":"2024-01-02", "mace_date":"2024-01-02"}, False, "out_of_followup_mi"),
    ({"mi_date":"2023-02-01"}, False, "mace_not_earliest_component"),
    ({"covax_post30_date":"2023-01-01"}, False, "invalid_post30_covax"),
    ({}, True, "duplicate_patient_id"),
])
def test_rejects_invalid_export(tmp_path, changes, duplicate, error):
    record = row() | changes
    path = tmp_path / "dataset.csv"
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(record))
        writer.writeheader()
        writer.writerows([record] * (2 if duplicate else 1))
    with pytest.raises(ValueError, match="Dataset failed"):
        audit(path, tmp_path / "quality")
    assert error in (tmp_path / "quality/validation_errors.csv").read_text()
