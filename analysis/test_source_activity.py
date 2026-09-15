"""Assurance of monthly counts, duplicate records, censoring and interval edges."""
from datetime import date, timedelta
from ehrql import create_dataset, claim_permissions
from cohort_definition import eligible_population, followup_end
from source_activity import source_counts
from test_dataset_definition import base, vaccine, admission, INDEX

claim_permissions("sgss_covid_all_tests")
dataset = create_dataset()
dataset.define_population(eligible_population)
for name, count in source_counts(date(2023, 6, 1), date(2023, 6, 30), followup_end).items():
    dataset.add_column(name, count)
p = base()
p["expected_columns"] = {"covid_vaccination_records": 3, "covid_vaccination_patients": True,
    "apcs_records": 1, "ons_death_patients": False, "sgss_positive_records": 0}
p["vaccinations"] = [vaccine(date(2023, 5, 31)), vaccine(date(2023, 6, 1)),
    vaccine(date(2023, 6, 30)), vaccine(date(2023, 6, 30)), vaccine(date(2023, 7, 1))]
test_data = {1: p}
p = base(); death = INDEX + timedelta(days=2)
p["patients"]["date_of_death"] = death
p["ons_deaths"] = [{"date": death, "underlying_cause_of_death": "C349"}]
p["vaccinations"] = [vaccine(death), vaccine(death + timedelta(days=1))]
p["expected_columns"] = {"covid_vaccination_records": 1, "ons_death_patients": True}
test_data[2] = p
p = base(); p["patients"]["date_of_birth"] = date(2010, 1, 1)
p["expected_in_population"] = False; p["expected_columns"] = {}; test_data[3] = p
p = base(); p["expected_columns"] = {"gp_medications_records": 0, "gp_medications_patients": False,
    "flu_vaccination_records": 0, "gp_mi_record_patients": False}
test_data[4] = p

# Positive records exercise every table, including distinct patients versus rows.
p = base()
p["clinical_events"] = [
    {"date": date(2023, 6, 1), "snomedct_code": "10273003"},
    {"date": date(2023, 6, 1), "snomedct_code": "10273003"},
    {"date": date(2023, 6, 2), "snomedct_code": "107557061000119108"},
]
p["medications"] = [{"date": date(2023, 6, 3), "dmd_code": "39733211000001101"}]
p["sgss_covid_all_tests"] = [
    {"specimen_taken_date": date(2023, 6, 4), "is_positive": True},
    {"specimen_taken_date": date(2023, 6, 5), "is_positive": False},
]
p["apcs"].append(admission(date(2023, 6, 20), "I210 || I639", ident=2))
p["vaccinations"] = [vaccine(date(2023, 6, 7), "INFLUENZA")]
p["expected_columns"] = {
    "gp_clinical_records": 3, "gp_mi_record_records": 2, "gp_mi_record_patients": True,
    "gp_stroke_record_records": 1, "gp_medications_records": 1, "gp_medications_patients": True,
    "sgss_positive_records": 1, "sgss_positive_patients": True, "hospital_mi_records": 1,
    "hospital_stroke_records": 1, "flu_vaccination_records": 1,
}
test_data[5] = p
