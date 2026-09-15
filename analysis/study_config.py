"""Application dates, retained provisionally until source coverage is confirmed.

These dates are a specification, not evidence of complete data availability.
The effectiveness cohort must additionally be restricted to a period with
concurrent COVID-vaccine eligibility and exposure overlap before fitting models.
"""

from datetime import date

import csv
from pathlib import Path

with Path("analysis/config/study_dates.csv").open() as f:
    configured_dates = {r["parameter"]: date.fromisoformat(r["value"]) for r in csv.DictReader(f)}
STUDY_START = configured_dates["study_start"]
STUDY_END = configured_dates["study_end"]
DATA_END = configured_dates["data_end"]
HISTORY_START = configured_dates["history_start"]
assert HISTORY_START <= STUDY_START <= STUDY_END <= DATA_END
COVID_TARGET = "SARS-2 CORONAVIRUS"
FLU_TARGET = "INFLUENZA"
