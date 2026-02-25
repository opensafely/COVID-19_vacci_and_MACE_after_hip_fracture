######################################
# dataset_definition.py
# COVID-19 vaccination and risk of MACE after hip fracture
#
# Data sources: OpenSAFELY-TPP primary care, APCS (SUS),
#               ONS mortality, SGSS
######################################

from datetime import date
from ehrql import (
    create_dataset,
    case,
    when,
    minimum_of,
    days,
    years,
    months,
    claim_permissions,
)
claim_permissions("sgss_covid_all_tests")
from ehrql.tables.tpp import (
    patients,
    practice_registrations,
    clinical_events,
    medications,
    apcs,
    ons_deaths,
    addresses,
    vaccinations,
    sgss_covid_all_tests,
    ethnicity_from_sus,
)

import codelists
from variable_lib import (
    has_prior_event_snomed,
    last_prior_event_snomed,
    first_event_after_snomed,
    first_admission_with_diagnosis,
    first_admission_with_procedure,
    has_prior_admission_with_diagnosis,
    imd_quintile,
    rural_urban_5,
    get_ethnicity6,
)
from vaccine_history import add_vaccine_history


##########################################################################
# Study dates
##########################################################################

study_start_date = date(2019, 1, 1)     # Earliest hip fracture inclusion date
study_end_date = date(2025, 1, 1)       # Latest hip fracture inclusion date
data_end_date = date(2026, 1, 1)        # End of follow-up / data availability
lookback_start = date(2017, 1, 1)       # 2-year lookback for prior hip fracture


##########################################################################
# Create dataset
##########################################################################

dataset = create_dataset()
dataset.configure_dummy_data(
    population_size=10000,
    timeout=300,
)


##########################################################################
# Identify hip fractures from APCS (SUS)
##########################################################################

# All hip fracture admissions in the study window
# Diagnosis OR procedure for hip fracture
# Exclude: transport accidents, elective admissions
hip_fracture_admissions = (
    apcs
    .where(apcs.admission_date.is_on_or_between(study_start_date, study_end_date))
    .where(
        apcs.all_diagnoses.contains_any_of(codelists.hip_fracture_icd10_codes_expanded)
        | apcs.all_procedures.contains_any_of(codelists.hip_fracture_opcs4)
    )
    .where(
        # Exclude transport accidents (V01-V99)
        ~apcs.all_diagnoses.contains(codelists.transport_accident_prefix)
    )
    .where(
        # Exclude elective admissions (11, 12, 13)
        ~apcs.admission_method.is_in(codelists.elective_admission_methods)
    )
)

# First hip fracture per patient in the study window
first_hf = (
    hip_fracture_admissions
    .sort_by(apcs.admission_date)
    .first_for_patient()
)

# Check for prior hip fracture in 2-year lookback (exclusion if present)
has_prior_hf = (
    apcs
    .where(apcs.admission_date.is_on_or_between(lookback_start, study_start_date - days(1)))
    .where(
        apcs.all_diagnoses.contains_any_of(codelists.hip_fracture_icd10_codes_expanded)
        | apcs.all_procedures.contains_any_of(codelists.hip_fracture_opcs4)
    )
    .exists_for_patient()
)


##########################################################################
# Define study population
##########################################################################

age_at_index = patients.age_on(first_hf.admission_date)

# Must be registered with a TPP GP at the index date
registered_at_index = (
    practice_registrations
    .where(practice_registrations.start_date <= first_hf.admission_date)
    .except_where(practice_registrations.end_date < first_hf.admission_date)
    .exists_for_patient()
)

dataset.define_population(
    first_hf.admission_date.is_not_null()   # Has a hip fracture in study window
    & ~has_prior_hf                          # No prior hip fracture in lookback
    & (age_at_index >= 50)                   # Age >= 50 at index
    & registered_at_index                    # Registered with TPP GP at index
    & patients.sex.is_in(["female", "male"]) # Known sex
)


##########################################################################
# POPULATION VARIABLES
##########################################################################

# Current registration spanning the index date
current_reg = (
    practice_registrations
    .where(practice_registrations.start_date <= first_hf.admission_date)
    .except_where(practice_registrations.end_date < first_hf.admission_date)
    .sort_by(practice_registrations.start_date)
    .last_for_patient()
)

dataset.gp_registration_start = current_reg.start_date
dataset.index_date = first_hf.admission_date
dataset.age = age_at_index
dataset.hf_discharge_date = first_hf.discharge_date
dataset.hf_admission_method = first_hf.admission_method
dataset.hf_primary_diagnosis = first_hf.primary_diagnosis

# Surgery type (from OPCS-4 procedures on the hip fracture spell)
dataset.surgery_type = case(
    when(first_hf.all_procedures.contains("W241")).then("closed_reduction_nail_screw"),
    when(first_hf.all_procedures.contains("W191")).then("open_reduction_pin_plate"),
    when(first_hf.all_procedures.contains("W461")).then("hemiarthroplasty_cemented"),
    when(first_hf.all_procedures.contains("W471")).then("hemiarthroplasty_uncemented"),
    when(first_hf.all_procedures.contains("W481")).then("hemiarthroplasty_nec"),
    otherwise="other_or_none",
)

# Deregistration date (for censoring)
dataset.dereg_date = current_reg.end_date


##########################################################################
# EXPOSURE: COVID-19 VACCINATION HISTORY
##########################################################################

# Extract all COVID-19 vaccine dates and products (up to 6 doses)
add_vaccine_history(
    dataset,
    index_date=first_hf.admission_date,
    target_disease="SARS-2 Coronavirus",
    prefix="covax",
    number_of_vaccines=6,
)

# Binary: had a COVID-19 vaccine within 365 days before index?
dataset.covax_within_365d = (
    vaccinations
    .where(vaccinations.target_disease == "SARS-2 Coronavirus")
    .where(
        vaccinations.date.is_on_or_between(
            first_hf.admission_date - days(365),
            first_hf.admission_date,
        )
    )
    .exists_for_patient()
)

# Most recent COVID vaccine before index (for timing analysis)
dataset.covax_most_recent_before_index = (
    vaccinations
    .where(vaccinations.target_disease == "SARS-2 Coronavirus")
    .where(vaccinations.date <= first_hf.admission_date)
    .sort_by(vaccinations.date)
    .last_for_patient()
    .date
)


##########################################################################
# EXPOSURE: INFLUENZA VACCINATION HISTORY
##########################################################################

# Extract all flu vaccine dates and products (up to 6 doses)
add_vaccine_history(
    dataset,
    index_date=first_hf.admission_date,
    target_disease="INFLUENZA",
    prefix="fluvax",
    number_of_vaccines=6,
)

# Binary: had a flu vaccine within 365 days before index?
dataset.fluvax_within_365d = (
    vaccinations
    .where(vaccinations.target_disease == "INFLUENZA")
    .where(
        vaccinations.date.is_on_or_between(
            first_hf.admission_date - days(365),
            first_hf.admission_date,
        )
    )
    .exists_for_patient()
)

# Most recent flu vaccine before index
dataset.fluvax_most_recent_before_index = (
    vaccinations
    .where(vaccinations.target_disease == "INFLUENZA")
    .where(vaccinations.date <= first_hf.admission_date)
    .sort_by(vaccinations.date)
    .last_for_patient()
    .date
)


##########################################################################
# OUTCOMES
##########################################################################

# Follow-up end date (365 days after index)
# (Defined as a local variable for reuse below)
followup_end = first_hf.admission_date + days(365)

# ----- MI -----
# Hospital (ICD-10, using all_diagnoses for broad capture)
mi_hospital = (
    first_admission_with_diagnosis(
        codelists.mi_icd10_codes_expanded,
        after_date=first_hf.admission_date,
        before_date=followup_end,
    )
)

# Primary care (SNOMED)
mi_gp = first_event_after_snomed(
    codelists.mi_snomed_codes,
    after_date=first_hf.admission_date,
    before_date=followup_end,
)

dataset.mi_date = minimum_of(mi_hospital.admission_date, mi_gp.date)
dataset.mi365 = dataset.mi_date.is_not_null()


# ----- Stroke -----
stroke_hospital = (
    first_admission_with_diagnosis(
        codelists.stroke_icd10_codes_expanded,
        after_date=first_hf.admission_date,
        before_date=followup_end,
    )
)

stroke_gp = first_event_after_snomed(
    codelists.stroke_snomed_codes,
    after_date=first_hf.admission_date,
    before_date=followup_end,
)

dataset.stroke_date = minimum_of(stroke_hospital.admission_date, stroke_gp.date)
dataset.stroke365 = dataset.stroke_date.is_not_null()


# ----- CVD death -----
# Check ONS death certificate for MI or stroke ICD-10 codes in any
# cause_of_death position (underlying + up to 15 mentioned causes)
# Combine MI and stroke ICD-10 codes for CVD death identification
cvd_death_codes = codelists.mi_icd10_codes + codelists.stroke_icd10_codes

has_cvd_death = (
    ons_deaths.cause_of_death_is_in(cvd_death_codes)
    & ons_deaths.date.is_after(first_hf.admission_date)
    & (ons_deaths.date <= followup_end)
)

dataset.cvddeath_date = case(
    when(has_cvd_death).then(ons_deaths.date),
)
dataset.cvddeath365 = has_cvd_death


# ----- Composite MACE (MI, stroke, CVD death - whichever first) -----
dataset.mace_date = minimum_of(
    dataset.mi_date,
    dataset.stroke_date,
    dataset.cvddeath_date,
)
dataset.mace365 = dataset.mace_date.is_not_null()


# ----- All-cause death (competing risk) -----
dataset.all_cause_death_date = ons_deaths.date


# ----- Negative control: Cataract surgery -----
cataract = first_admission_with_procedure(
    codelists.cataract_opcs4,
    after_date=first_hf.admission_date,
    before_date=followup_end,
)
dataset.neg_con_1_date = cataract.admission_date
dataset.neg_con_1 = dataset.neg_con_1_date.is_not_null()


##########################################################################
# CONFOUNDERS / EFFECT MODIFIERS
##########################################################################

# ----- Demographics -----
dataset.sex = patients.sex

# Ethnicity (GP + SUS fallback, 6-category)
dataset.ethnicity6 = get_ethnicity6(
    first_hf.admission_date,
    codelists.ethnicity6_codes,
)
dataset.ethnicity_sus = ethnicity_from_sus.code

# Region (NHS England region from registered practice)
dataset.region = current_reg.practice_nuts1_region_name

# IMD quintile
dataset.imd_quintile = imd_quintile(first_hf.admission_date)

# Rural/urban
dataset.rural_urban = rural_urban_5(first_hf.admission_date)


# ----- Cardiovascular disease history -----

# Composite prior CVD (CHD, stroke/TIA, heart failure, AF)
dataset.prior_cvd = (
    has_prior_event_snomed(codelists.chd_snomed_codes, first_hf.admission_date)
    | has_prior_event_snomed(codelists.stroke_tia_snomed_codes, first_hf.admission_date)
    | has_prior_event_snomed(codelists.heart_failure_snomed_codes, first_hf.admission_date)
    | has_prior_event_snomed(codelists.af_snomed_codes, first_hf.admission_date)
)

# Individual CVD components
dataset.prior_mi = has_prior_event_snomed(
    codelists.mi_snomed_codes, first_hf.admission_date
)
dataset.prior_stroke = has_prior_event_snomed(
    codelists.stroke_snomed_codes, first_hf.admission_date
)
dataset.prior_heart_failure = has_prior_event_snomed(
    codelists.heart_failure_snomed_codes, first_hf.admission_date
)
dataset.prior_af = has_prior_event_snomed(
    codelists.af_snomed_codes, first_hf.admission_date
)


# ----- Other comorbidities -----
dataset.prior_hypertension = has_prior_event_snomed(
    codelists.hypertension_snomed_codes, first_hf.admission_date
)
dataset.prior_diabetes = has_prior_event_snomed(
    codelists.diabetes_snomed_codes, first_hf.admission_date
)
dataset.prior_ckd = has_prior_event_snomed(
    codelists.ckd_snomed_codes, first_hf.admission_date
)
dataset.prior_copd = has_prior_event_snomed(
    codelists.copd_snomed_codes, first_hf.admission_date
)
dataset.prior_cancer = has_prior_event_snomed(
    codelists.cancer_snomed_codes, first_hf.admission_date
)
dataset.prior_dementia = has_prior_event_snomed(
    codelists.dementia_snomed_codes, first_hf.admission_date
)
dataset.prior_depression = has_prior_event_snomed(
    codelists.depression_snomed_codes, first_hf.admission_date
)
dataset.prior_alcohol_problems = has_prior_event_snomed(
    codelists.alcohol_snomed_codes, first_hf.admission_date
)

# Prior fracture at any site (from hospital data)
# Using hip fracture ICD-10 codes broadly - a wider fracture codelist
# would be preferable if available
dataset.prior_frac = has_prior_admission_with_diagnosis(
    codelists.hip_fracture_icd10_codes_expanded,
    before_date=first_hf.admission_date,
)


# ----- BMI -----
bmi_record = (
    clinical_events
    .where(clinical_events.snomedct_code.is_in(codelists.bmi_codes))
    .where(clinical_events.date < first_hf.admission_date)
    .where(clinical_events.date >= first_hf.admission_date - years(2))
    .where(clinical_events.numeric_value.is_not_null())
    .where(
        (clinical_events.numeric_value >= 12.0)
        & (clinical_events.numeric_value <= 80.0)
    )
    .sort_by(clinical_events.date)
    .last_for_patient()
)
dataset.bmi = bmi_record.numeric_value
dataset.bmi_date = bmi_record.date


# ----- Smoking -----
# Most recent smoking code before index from the smoking-clear codelist
# Since the codelist has no category column, we extract the code and
# categorise in downstream R analysis. Alternatively, we define
# separate code lists inline for S/E/N.

# Never smoker codes
smoking_never = [
    "266919005",  # Never smoked tobacco
    "8392000",    # Non-smoker
    "160618006",  # Current non-smoker
    "105540000",  # Non-smoker for religious reasons
    "105539002",  # Non-smoker for personal reasons
    "105541001",  # Non-smoker for medical reasons
    "87739003",   # Tolerant non-smoker
    "360918006",  # Aggressive non-smoker
    "360929005",  # Intolerant non-smoker
    "405746006",  # Current non smoker but past smoking history unknown
    "448755007",  # Tobacco smokefree home
    "505681000000109",  # Non-smoker annual review
]

# Ex-smoker codes
smoking_ex = [
    "8517006",    # Ex-smoker
    "160617001",  # Stopped smoking
    "160620009",  # Ex-pipe smoker
    "160621008",  # Ex-cigar smoker
    "266921000",  # Ex-trivial cigarette smoker (<1/day)
    "266922007",  # Ex-light cigarette smoker (1-9/day)
    "266923002",  # Ex-moderate cigarette smoker (10-19/day)
    "266924008",  # Ex-heavy cigarette smoker (20-39/day)
    "266925009",  # Ex-very heavy cigarette smoker (40+/day)
    "266928006",  # Ex-cigarette smoker amount unknown
    "281018007",  # Ex-cigarette smoker
    "449368009",  # Stopped smoking during pregnancy
    "449369001",  # Stopped smoking before pregnancy
    "492191000000103",  # Ex roll-up cigarette smoker
    "517211000000106",  # Recently stopped smoking
    "505761000000105",  # Ex-smoker annual review
    "228486009",  # Time since stopped smoking
    "160625004",  # Date ceased smoking
    "191889006",  # Tobacco dependence in remission
    "360890004",  # Intolerant ex-smoker
    "360900008",  # Aggressive ex-smoker
    "53896009",   # Tolerant ex-smoker
    "735128000",  # Ex-smoker for less than 1 year
    "48031000119106",  # Ex-smoker for more than 1 year
    "1092031000000108", # Ex-smoker amount unknown
    "1092041000000104", # Ex-very heavy smoker (40+/day)
    "1092071000000105", # Ex-heavy smoker (20-39/day)
    "1092091000000109", # Ex-moderate smoker (10-19/day)
    "1092111000000104", # Ex-light smoker (1-9/day)
    "1092131000000107", # Ex-trivial smoker (<1/day)
]

latest_smoking_code = (
    clinical_events
    .where(clinical_events.snomedct_code.is_in(codelists.smoking_clear_codes))
    .where(clinical_events.date < first_hf.admission_date)
    .sort_by(clinical_events.date)
    .last_for_patient()
    .snomedct_code
)

dataset.smoking_status = case(
    when(latest_smoking_code.is_in(smoking_never)).then("N"),
    when(latest_smoking_code.is_in(smoking_ex)).then("E"),
    when(latest_smoking_code.is_not_null()).then("S"),  # All remaining codes = current smoker
)


# ----- Medications (in year before index) -----
# NOTE: Medication dm+d codelists are placeholders in codelists.py
# Uncomment when codelists are available:
#
# dataset.bone_med = has_prior_medication(
#     codelists.bone_med_codes, first_hf.admission_date, within_days=365
# )
# dataset.cvd_med = (
#     has_prior_medication(codelists.statin_codes, first_hf.admission_date, within_days=365)
#     | has_prior_medication(codelists.antiplatelet_codes, first_hf.admission_date, within_days=365)
#     | has_prior_medication(codelists.anticoagulant_codes, first_hf.admission_date, within_days=365)
# )


# ----- COVID-19 infection (confounder/mediator) -----
# Most recent positive SARS-CoV-2 test up to end of follow-up
dataset.covid_positive_date = (
    sgss_covid_all_tests
    .where(sgss_covid_all_tests.is_positive)
    .where(sgss_covid_all_tests.specimen_taken_date <= followup_end)
    .sort_by(sgss_covid_all_tests.specimen_taken_date)
    .last_for_patient()
    .specimen_taken_date
)

# Most recent positive SARS-CoV-2 test BEFORE index (for confounding)
dataset.covid_positive_before_index = (
    sgss_covid_all_tests
    .where(sgss_covid_all_tests.is_positive)
    .where(sgss_covid_all_tests.specimen_taken_date <= first_hf.admission_date)
    .sort_by(sgss_covid_all_tests.specimen_taken_date)
    .last_for_patient()
    .specimen_taken_date
)
