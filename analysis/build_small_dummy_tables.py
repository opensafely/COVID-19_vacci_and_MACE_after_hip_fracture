"""Derive a deterministic routine-test subset from the original synthetic tables.

Keeps every event for the selected patients and leaves the original tables intact.
Adds deterministic synthetic recording examples for the second feasibility round.
Run from the repository root: python analysis/build_small_dummy_tables.py
"""
import argparse
import csv
from pathlib import Path
from datetime import date, timedelta


def add_recording_examples(destination):
    def read(name):
        with (destination / name).open(newline="") as f:
            reader = csv.DictReader(f)
            return list(reader), reader.fieldnames

    def write(name, rows, fields):
        with (destination / name).open("w", newline="") as f:
            writer = csv.DictWriter(f, fields, lineterminator="\n")
            writer.writeheader(); writer.writerows(rows)

    def first_code(filename, column="code"):
        with (Path("codelists") / filename).open(newline="") as f:
            return next(csv.DictReader(f))[column]

    admissions, _ = read("apcs.csv")
    indexes = {}
    for row in admissions:
        indexes.setdefault(row["patient_id"], date.fromisoformat(row["admission_date"]))
    events, fields = read("clinical_events.csv")
    fields = fields + ["ctv3_code"]
    mapping = {"266919005": "XE0oh", "8392000": "Ub0oq", "160618006": "137L.",
               "8517006": "Y6628", "160617001": "137K.", "77176002": "137R."}
    for row in events:
        row["ctv3_code"] = mapping.get(row["snomedct_code"], "")
    prescriptions, medication_fields = read("medications.csv")
    egfr_code = first_code("bristol-multimorbidity_chronic-kidney-disease.csv")
    ckd_code = first_code("primis-covid19-vacc-uptake-ckd35.csv")
    medication_codes = [first_code("nhs-drug-refsets-stat_cod.csv"), first_code("nhs-drug-refsets-antihyp_cod.csv"),
        first_code("user-elsie_horne-antiplatelet_dmd.csv", "dmd_id"),
        first_code("user-elsie_horne-anticoagulant_dmd.csv", "dmd_id"), first_code("opensafely-alendronic-acid.csv")]
    for i, (patient, index) in enumerate(indexes.items()):
        def event(offset, snomed="", ctv3="", value=""):
            events.append(dict(patient_id=patient, date=str(index - timedelta(days=offset)),
                               snomedct_code=snomed, ctv3_code=ctv3, numeric_value=value))
        if i % 4 == 0:
            event(180, egfr_code, value=45); event(30, egfr_code, value=42)
        if i % 8 == 0:
            event(60, ckd_code)
        if i % 5 == 0:
            event(120, ctv3="137R."); event(20, ctv3="XE0oh")
        if i % 11 == 0:
            event(20, ctv3="137R.")
        if i % 7 == 0:
            event(1000, "60621009", value=27)
        for j, code in enumerate(medication_codes):
            if i % (j + 3) == 0:
                prescriptions.append(dict(patient_id=patient, date=str(index - timedelta(days=60)), dmd_code=code))
    write("clinical_events.csv", events, fields)
    write("medications.csv", prescriptions, medication_fields)
    addresses, fields = read("addresses.csv")
    fields = fields + ["care_home_is_potential_match"]
    for i, row in enumerate(addresses):
        row["care_home_is_potential_match"] = "" if i % 5 == 0 else ("T" if i % 4 == 0 else "F")
    write("addresses.csv", addresses, fields)


def build(limit=1000):
    if limit < 1:
        raise ValueError("Patient limit must be positive")
    source = Path("dummy-tables")
    destination = source / "small"
    destination.mkdir(exist_ok=True)
    with (source / "patients.csv").open(newline="") as f:
        patients = list(csv.DictReader(f))
    total = len(patients)
    count = min(limit, total)
    chosen = {patients[i * total // count]["patient_id"] for i in range(count)}
    for path in sorted(source.glob("*.csv")):
        with path.open(newline="") as inp, (destination / path.name).open("w", newline="") as out:
            reader = csv.DictReader(inp)
            if "patient_id" not in reader.fieldnames:
                raise ValueError(f"Missing patient_id in {path}")
            writer = csv.DictWriter(out, fieldnames=reader.fieldnames, lineterminator="\n")
            writer.writeheader()
            n = 0
            for row in reader:
                if row["patient_id"] in chosen:
                    writer.writerow(row)
                    n += 1
            print(f"{path.name}: {n} rows")
    add_recording_examples(destination)
    print(f"Wrote {count} synthetic patients and recording examples to {destination}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--patients", type=int, default=1000)
    build(parser.parse_args().patients)
