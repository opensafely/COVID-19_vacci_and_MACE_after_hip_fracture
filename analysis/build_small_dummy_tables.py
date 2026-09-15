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
    counts = []
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
            counts.append(f"- {path.name}: {n} rows")
    (destination / "README.md").write_text(
        "# Small synthetic tables for routine tests\n\n"
        f"Contains {count} of {total} synthetic patients, sampled at evenly spaced "
        "positions in the original patients file. All events belonging to each "
        "selected patient are retained. No real patient data are included.\n\n"
        "Generated with `python analysis/build_small_dummy_tables.py`; "
        "the full source tables remain one directory above. This subset keeps "
        "routine local and CI extraction within modest memory limits. "
        "The separate ehrQL assurance cases test clinical timing boundaries.\n\n"
        + "\n".join(counts) + "\n"
    )
    print(f"Wrote {count} synthetic patients and all their records to {destination}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--patients", type=int, default=1000)
    build(parser.parse_args().patients)
