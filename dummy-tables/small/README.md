# Small synthetic tables for routine tests

Contains 1000 of 12000 synthetic patients, sampled at evenly spaced positions in the original patients file. All events belonging to each selected patient are retained. No real patient data are included.

Generated with `python analysis/build_small_dummy_tables.py`; the full source tables remain one directory above. This subset keeps routine local and CI extraction within modest memory limits. The separate ehrQL assurance cases test clinical timing boundaries.

- addresses.csv: 1000 rows
- apcs.csv: 947 rows
- clinical_events.csv: 2995 rows
- ethnicity_from_sus.csv: 1000 rows
- medications.csv: 0 rows
- ons_deaths.csv: 128 rows
- patients.csv: 1000 rows
- practice_registrations.csv: 1000 rows
- sgss_covid_all_tests.csv: 213 rows
- vaccinations.csv: 4215 rows
