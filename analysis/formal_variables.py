"""Additional extraction for the locked analysis specification.

Registration dates remain patient-level columns. Duplicate starts are coalesced
using the furthest end; an overflow flag prevents silent truncation in R.
"""
from ehrql import days, years, case, when, minimum_of
from ehrql.tables.tpp import practice_registrations, vaccinations, apcs, ons_deaths
from variable_lib import first_event_after_snomed, first_admission_with_diagnosis, first_admission_with_procedure
from study_config import COVID_TARGET, FLU_TARGET
import codelists


def add_formal_variables(dataset, index, admin_end):
    regs = practice_registrations.where(practice_registrations.start_date <= admin_end).where(
        practice_registrations.end_date.is_null() | (practice_registrations.end_date >= index - years(2)))
    previous = "1899-01-01"
    for i in range(1, 13):
        remaining = regs.where(practice_registrations.start_date > previous)
        start = remaining.start_date.minimum_for_patient()
        same = remaining.where(practice_registrations.start_date == start)
        end = case(when(same.where(practice_registrations.end_date.is_null()).exists_for_patient()).then(admin_end),
                   otherwise=minimum_of(same.end_date.maximum_for_patient(), admin_end))
        dataset.add_column(f"reg_chain_{i}_start_date", start)
        dataset.add_column(f"reg_chain_{i}_end_date", end)
        previous = start
    dataset.registration_chain_overflow = regs.where(practice_registrations.start_date > previous).exists_for_patient()
    for prefix, codes, icd in [("acute_mi", codelists.acute_mi_snomed, codelists.acute_mi_icd10),
                                ("ischaemic_stroke", codelists.ischaemic_stroke_snomed, codelists.ischaemic_stroke_icd10)]:
        gp = first_event_after_snomed(codes, index, admin_end)
        hospital = first_admission_with_diagnosis(icd, index, admin_end)
        dataset.add_column(f"{prefix}_gp_date", gp.date)
        dataset.add_column(f"{prefix}_hospital_date", hospital.admission_date)
    dataset.cataract_extraction_date = first_admission_with_procedure(["C71", "C72"], index, admin_end).admission_date
    # Exact death-certificate membership requires four-character codes, not prefixes.
    death_codes = [f"I21{x}" for x in range(10)] + [f"I22{x}" for x in range(10)] + codelists.ischaemic_stroke_icd10
    dataset.mace_specific_death_date = case(when(
        ons_deaths.cause_of_death_is_in(death_codes) & (ons_deaths.date > index) & (ons_deaths.date <= admin_end)
    ).then(ons_deaths.date))
    for prefix, target in [("covax", COVID_TARGET), ("fluvax", FLU_TARGET)]:
        records = vaccinations.where(vaccinations.target_disease == target).where(
            vaccinations.date > index).where(vaccinations.date <= minimum_of(index + days(30), admin_end))
        previous = index
        for i in range(1, 7):
            value = records.where(vaccinations.date > previous).date.minimum_for_patient()
            dataset.add_column(f"{prefix}_post_window_{i}_date", value)
            previous = value
        dataset.add_column(f"{prefix}_post_window_overflow", records.where(vaccinations.date > previous).exists_for_patient())
