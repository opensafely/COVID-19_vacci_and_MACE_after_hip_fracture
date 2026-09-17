"""Shared provisional cohort definition for extraction, flow and source checks."""
from ehrql import days, minimum_of, case, when
from ehrql.tables.tpp import apcs, patients, practice_registrations, ons_deaths
import codelists
from study_config import STUDY_START, STUDY_END, HISTORY_START, DATA_END

hip_evidence = (
    apcs.all_diagnoses.contains_any_of(codelists.hip_fracture_icd10_codes_expanded).when_null_then(False)
    | apcs.all_procedures.contains_any_of(codelists.hip_fracture_opcs4).when_null_then(False)
)
candidates = apcs.where(apcs.admission_date.is_on_or_between(STUDY_START, STUDY_END)).where(hip_evidence)
first_candidate = candidates.sort_by(apcs.admission_date, apcs.apcs_ident).first_for_patient()
non_transport = candidates.except_where(apcs.all_diagnoses.contains(codelists.transport_accident_prefix))
hip_fracture_admissions = non_transport.except_where(apcs.admission_method.is_in(codelists.elective_admission_methods))
first_hf = hip_fracture_admissions.sort_by(apcs.admission_date, apcs.apcs_ident).first_for_patient()
has_prior_hf = apcs.where(hip_evidence).where(
    apcs.admission_date.is_on_or_between(HISTORY_START, first_hf.admission_date - days(1))
).exists_for_patient()
age_at_index = patients.age_on(first_hf.admission_date)
registered_at_index = practice_registrations.exists_for_patient_on(first_hf.admission_date)
current_reg = practice_registrations.for_patient_on(first_hf.admission_date)
alive_at_index = (
    (patients.date_of_death.is_null() | (patients.date_of_death >= first_hf.admission_date))
    & (ons_deaths.date.is_null() | (ons_deaths.date >= first_hf.admission_date))
)
criteria = {
    "has_non_transport_admission": non_transport.exists_for_patient(),
    "has_non_elective_admission": first_hf.admission_date.is_not_null(),
    "no_prior_recorded_hip_fracture": ~has_prior_hf,
    "age_50_or_older": age_at_index >= 50,
    "registered_at_index": registered_at_index,
    "alive_at_index": alive_at_index,
    "known_sex": patients.sex.is_in(["female", "male"]),
}
eligible_population = candidates.exists_for_patient()
for criterion in criteria.values():
    eligible_population = eligible_population & criterion.when_null_then(False)

gp_death_date = case(when(patients.date_of_death <= DATA_END).then(patients.date_of_death))
ons_death_date = case(when(ons_deaths.date <= DATA_END).then(ons_deaths.date))
admin_end_date = minimum_of(first_hf.admission_date + days(365), DATA_END)
preferred_death_date = ons_death_date.when_null_then(gp_death_date)
legacy_followup_end = minimum_of(admin_end_date, current_reg.end_date, gp_death_date, ons_death_date)
# ONS supplies both the death date and the certificate; use GP when ONS is absent.
# Registration censoring remains conservative pending the transition audit.
followup_end = minimum_of(admin_end_date, current_reg.end_date, preferred_death_date)
