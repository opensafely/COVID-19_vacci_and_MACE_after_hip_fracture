"""Application dates, retained provisionally until source coverage is confirmed.

These dates are a specification, not evidence of complete data availability.
The effectiveness cohort must additionally be restricted to a period with
concurrent COVID-vaccine eligibility and exposure overlap before fitting models.
"""

from datetime import date

STUDY_START = date(2019, 1, 1)
STUDY_END = date(2025, 1, 1)
DATA_END = date(2026, 1, 1)
HISTORY_START = date(2017, 1, 1)
COVID_TARGET = "SARS-2 CORONAVIRUS"
FLU_TARGET = "INFLUENZA"
