"""Internal feasibility counts. Contains unsuppressed values: do not release.

Uses the Python standard library so it can run in the OpenSAFELY Python image.
"""
import argparse
import csv
from collections import Counter
from datetime import date
from pathlib import Path


def parsed(value):
    return date.fromisoformat(value) if value else None


def audit(input_path, output_dir):
    counts = Counter({"patients": 0})
    errors = Counter()
    ids = set()
    with Path(input_path).open(newline="") as f:
        reader = csv.DictReader(f)
        required = {"patient_id", "index_date", "followup_end_date", "age",
                    "admin_end_date", "mi_date", "stroke_date", "cvddeath_date", "mace_date"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Missing required columns: {sorted(missing)}")
        for row in reader:
            counts["patients"] += 1
            if row["patient_id"] in ids:
                errors["duplicate_patient_id"] += 1
            ids.add(row["patient_id"])
            index, end = parsed(row["index_date"]), parsed(row["followup_end_date"])
            admin_end = parsed(row["admin_end_date"])
            if not index or not end or not admin_end or end < index or end > admin_end:
                errors["invalid_followup"] += 1
                continue
            counts[f"index_year_{index.year}"] += 1
            counts["zero_followup"] += end == index
            counts["shorter_than_365d"] += (end-index).days < 365
            for field in ["age", "bmi", "smoking_status", "ethnicity6", "imd_quintile"]:
                counts[f"missing_{field}"] += row.get(field, "") in ("", "Missing", "0")
            event_dates = []
            for outcome in ["mi", "stroke", "cvddeath"]:
                event = parsed(row[f"{outcome}_date"])
                if event:
                    event_dates.append(event)
                    counts[f"events_{outcome}"] += 1
                    if not index < event <= end:
                        errors[f"out_of_followup_{outcome}"] += 1
            mace = parsed(row["mace_date"])
            counts["events_mace"] += mace is not None
            if mace != (min(event_dates) if event_dates else None):
                errors["mace_not_earliest_component"] += 1
            for vaccine in ["covax", "fluvax"]:
                prior = parsed(row[f"{vaccine}_most_recent_before_index"])
                if prior and prior >= index:
                    errors[f"nonprior_{vaccine}"] += 1
                counts[f"{vaccine}_prior_record"] += prior is not None
                delta = (index-prior).days if prior else None
                band = ("no_prior_record" if delta is None else
                        "1_90d" if 1 <= delta <= 90 else
                        "91_180d" if 91 <= delta <= 180 else
                        "181_365d" if 181 <= delta <= 365 else
                        "over_365d" if delta > 365 else "invalid")
                counts[f"{vaccine}_prior_{band}"] += 1
                counts[f"year_{index.year}_{vaccine}_prior_{band}"] += 1
                counts[f"{vaccine}_index_day"] += row[f"{vaccine}_index_day"] == "T"
                post = parsed(row[f"{vaccine}_post30_date"])
                counts[f"{vaccine}_post30"] += post is not None
                if post and not (0 < (post-index).days <= 30 and post <= end):
                    errors[f"invalid_post30_{vaccine}"] += 1
            for flag in ["index_spell_mi", "index_spell_stroke", "index_primary_hip",
                         "index_hip_unspecified", "index_hip_strict", "reg_2y_at_index", "death_dates_disagree"]:
                counts[flag] += row[flag] == "T"
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    with (out / "feasibility_counts.csv").open("w", newline="") as f:
        w=csv.writer(f);w.writerow(["measure", "count", "disclosure_status"])
        for name,n in sorted(counts.items()): w.writerow([name,n,"UNSUPPRESSED_INTERNAL_ONLY"])
    with (out / "validation_errors.csv").open("w", newline="") as f:
        w=csv.writer(f);w.writerow(["check", "failures"])
        for name,n in sorted(errors.items()): w.writerow([name,n])
    if errors:
        raise ValueError(f"Dataset failed {sum(errors.values())} checks; inspect internal validation_errors.csv")


if __name__ == "__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--input", required=True)
    p.add_argument("--output-dir", required=True)
    a=p.parse_args()
    audit(a.input, a.output_dir)
