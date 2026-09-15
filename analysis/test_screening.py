"""Screening preserves excluded patients and matches the analytic population."""
from copy import deepcopy
from screening_dataset import dataset
from test_dataset_definition import test_data as analytic_cases

test_data = deepcopy(analytic_cases)
for p in test_data.values():
    included = p.pop("expected_in_population")
    p["expected_in_population"] = bool(p["apcs"])
    p["expected_columns"] = {"included": included}

# Additional source-count columns exist only in the synthetic assurance dataset.
# The production screening extract remains the minimal set of selection flags.
from datetime import date
from cohort_definition import followup_end
from source_activity import source_counts
from test_source_activity import test_data as activity_cases
for name, count in source_counts(date(2023, 6, 1), date(2023, 6, 30), followup_end).items():
    dataset.add_column(f"activity_{name}", count)
for ident, original in activity_cases.items():
    p = deepcopy(original)
    expected_included = p["expected_in_population"]
    p["expected_in_population"] = True
    p["expected_columns"] = {f"activity_{k}": v for k, v in p["expected_columns"].items()}
    p["expected_columns"]["included"] = expected_included
    test_data[100 + ident] = p
