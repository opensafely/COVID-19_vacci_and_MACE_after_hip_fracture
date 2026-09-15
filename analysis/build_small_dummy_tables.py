"""Derive a deterministic routine-test subset from the original synthetic tables.

Keeps every event for the selected patients and leaves the original tables intact.
Run from the repository root: python analysis/build_small_dummy_tables.py
"""
import argparse
import csv
from pathlib import Path


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
    print(f"Wrote {count} synthetic patients and all their records to {destination}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--patients", type=int, default=1000)
    build(parser.parse_args().patients)
