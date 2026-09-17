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
from vaccine_history import add_vaccine_history, add_index_vaccine_summary
from study_config import STUDY_START, STUDY_END, DATA_END, HISTORY_START, COVID_TARGET, FLU_TARGET


##########################################################################
# Study dates
##########################################################################

study_start_date = STUDY_START
study_end_date = STUDY_END
data_end_date = DATA_END
lookback_start = HISTORY_START


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

from cohort_definition import (
    first_hf, age_at_index, current_reg, eligible_population,
    gp_death_date, ons_death_date, admin_end_date, followup_end,
    preferred_death_date, legacy_followup_end,
)
dataset.define_population(eligible_population)

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

# Keep both death sources within the extraction window. ONS supplies cause.
dataset.gp_death_date = gp_death_date
dataset.ons_death_date = ons_death_date
dataset.death_dates_disagree = (
    gp_death_date.is_not_null() & ons_death_date.is_not_null()
    & (gp_death_date != ons_death_date)
)
dataset.admin_end_date = admin_end_date
dataset.followup_end_date = followup_end
dataset.legacy_followup_end_date = legacy_followup_end
dataset.preferred_death_source = case(when(ons_death_date.is_not_null()).then("ONS"),
    when(gp_death_date.is_not_null()).then("GP fallback"), otherwise="No death record")
dataset.reg_2y_at_index = current_reg.start_date <= first_hf.admission_date - years(2)

# These flags preserve uncertainty about diagnoses within the index spell.
# Admission date does not establish when a MI/stroke began within that spell.
dataset.index_spell_mi = first_hf.all_diagnoses.contains_any_of(codelists.mi_icd10_codes_expanded)
dataset.index_spell_stroke = first_hf.all_diagnoses.contains_any_of(codelists.stroke_icd10_codes_expanded)
dataset.index_hip_strict = first_hf.all_diagnoses.contains_any_of(["S720", "S721", "S722"])
dataset.index_hip_unspecified = first_hf.all_diagnoses.contains("S729")


##########################################################################
# EXPOSURE: COVID-19 VACCINATION HISTORY
##########################################################################

# Extract all COVID-19 vaccine dates and products (up to 6 doses)
add_vaccine_history(
    dataset,
    index_date=first_hf.admission_date,
    target_disease=COVID_TARGET,
    prefix="covax",
    number_of_vaccines=6,
    start_date=lookback_start,
    end_date=followup_end,
)

# Binary: had a COVID-19 vaccine within 365 days before index?
dataset.covax_within_365d = (
    vaccinations
    .where(vaccinations.target_disease == COVID_TARGET)
    .where(
        vaccinations.date.is_on_or_between(
            first_hf.admission_date - days(365),
            first_hf.admission_date - days(1),
        )
    )
    .exists_for_patient()
)

# Most recent COVID vaccine before index (for timing analysis)
dataset.covax_most_recent_before_index = (
    vaccinations
    .where(vaccinations.target_disease == COVID_TARGET)
    .where(vaccinations.date < first_hf.admission_date)
    .where(vaccinations.date >= lookback_start)
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
    target_disease=FLU_TARGET,
    prefix="fluvax",
    number_of_vaccines=6,
    start_date=lookback_start,
    end_date=followup_end,
)

# Binary: had a flu vaccine within 365 days before index?
dataset.fluvax_within_365d = (
    vaccinations
    .where(vaccinations.target_disease == FLU_TARGET)
    .where(
        vaccinations.date.is_on_or_between(
            first_hf.admission_date - days(365),
            first_hf.admission_date - days(1),
        )
    )
    .exists_for_patient()
)

# Most recent flu vaccine before index
dataset.fluvax_most_recent_before_index = (
    vaccinations
    .where(vaccinations.target_disease == FLU_TARGET)
    .where(vaccinations.date < first_hf.admission_date)
    .where(vaccinations.date >= lookback_start)
    .sort_by(vaccinations.date)
    .last_for_patient()
    .date
)


##########################################################################
# OUTCOMES
##########################################################################

# Exposure summaries are independent of the historical six-date display cap.
# Day 0 is recorded separately; a record during a spell is not proof of location.
for target, prefix in [(COVID_TARGET, "covax"), (FLU_TARGET, "fluvax")]:
    add_index_vaccine_summary(dataset, first_hf.admission_date,
                              first_hf.discharge_date, followup_end, target, prefix)

# All outcomes are constrained to observed follow-up, including death and deregistration.

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

dataset.mi_hospital_date = mi_hospital.admission_date
dataset.mi_gp_date = mi_gp.date
dataset.mi_gp_first_code = mi_gp.snomedct_code
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

dataset.stroke_hospital_date = stroke_hospital.admission_date
dataset.stroke_gp_date = stroke_gp.date
dataset.stroke_gp_first_code = stroke_gp.snomedct_code
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


# ----- All-cause death during observed follow-up (competing risk) -----
# Retain source dates above so disagreement can be reviewed before modelling.
first_death_date = preferred_death_date
dataset.all_cause_death_date = case(
    when(first_death_date <= followup_end).then(first_death_date)
)


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

# Preserve the original hip-only history for comparison with the broader field.
dataset.prior_frac = has_prior_admission_with_diagnosis(
    codelists.hip_fracture_icd10_codes_expanded,
    before_date=first_hf.admission_date,
)


# ----- BMI -----
bmi_record = (
    clinical_events
    .where(clinical_events.snomedct_code.is_in(codelists.bmi_codes))
    .where(clinical_events.date < first_hf.admission_date)
    .where(clinical_events.date >= lookback_start)
    .where(clinical_events.date >= first_hf.admission_date - years(2))
    .where(clinical_events.numeric_value.is_not_null())
    .where(
        (clinical_events.numeric_value >= 12.0)
        & (clinical_events.numeric_value <= 80.0)
    )
    .sort_by(clinical_events.date)
    .last_for_patient()
)
dataset.bmi_legacy_2y = bmi_record.numeric_value
dataset.bmi_legacy_2y_date = bmi_record.date


# ----- Smoking -----
# Original SNOMED mapping retained only for the definition comparison.
# The primary review field uses explicit CTV3 categories in review_variables.py.

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
    .where(clinical_events.date >= lookback_start)
    .sort_by(clinical_events.date)
    .last_for_patient()
    .snomedct_code
)

dataset.smoking_legacy_status = case(
    when(latest_smoking_code.is_in(smoking_never)).then("N"),
    when(latest_smoking_code.is_in(smoking_ex)).then("E"),
    when(latest_smoking_code.is_not_null()).then("S"),  # All remaining codes = current smoker
)


# ----- COVID-19 infection (confounder/mediator) -----
# Most recent positive SARS-CoV-2 test up to end of follow-up
dataset.covid_positive_date = (
    sgss_covid_all_tests
    .where(sgss_covid_all_tests.is_positive)
    .where(sgss_covid_all_tests.specimen_taken_date >= lookback_start)
    .where(sgss_covid_all_tests.specimen_taken_date <= followup_end)
    .sort_by(sgss_covid_all_tests.specimen_taken_date)
    .last_for_patient()
    .specimen_taken_date
)

# Most recent positive SARS-CoV-2 test BEFORE index (for confounding)
dataset.covid_positive_before_index = (
    sgss_covid_all_tests
    .where(sgss_covid_all_tests.is_positive)
    .where(sgss_covid_all_tests.specimen_taken_date >= lookback_start)
    .where(sgss_covid_all_tests.specimen_taken_date < first_hf.admission_date)
    .sort_by(sgss_covid_all_tests.specimen_taken_date)
    .last_for_patient()
    .specimen_taken_date
)


# Feasibility diagnostics do not replace the provisional primary definitions.
dataset.registration_records_at_index_n = practice_registrations.spanning(
    first_hf.admission_date, first_hf.admission_date
).count_for_patient()
dataset.practice_go_live_date = current_reg.practice_systmone_go_live_date
dataset.index_hip_diagnosis = first_hf.all_diagnoses.contains_any_of(
    codelists.hip_fracture_icd10_codes_expanded
).when_null_then(False)
dataset.index_hip_procedure = first_hf.all_procedures.contains_any_of(codelists.hip_fracture_opcs4).when_null_then(False)
dataset.index_diagnoses_missing = first_hf.all_diagnoses.is_null()
dataset.index_admission_method_missing = first_hf.admission_method.is_null()
dataset.index_fall_W11_W17 = first_hf.all_diagnoses.contains_any_of([f"W{i}" for i in range(11, 18)]).when_null_then(False)
for name, prefixes in {
    "arterial_ischaemic": ["I630", "I631", "I632", "I633", "I634", "I635", "I638", "I639"],
    "haemorrhagic": ["I60", "I61"],
    "unspecified": ["I64"],
    "venous_infarction": ["I636"],
}.items():
    event = first_admission_with_diagnosis(prefixes, first_hf.admission_date, followup_end)
    dataset.add_column(f"stroke_{name}_hospital_date", event.admission_date)
for name, codes in [("mi", codelists.mi_icd10_codes_expanded), ("stroke", codelists.stroke_icd10_codes_expanded)]:
    event = first_admission_with_diagnosis(codes, first_hf.discharge_date, followup_end)
    dataset.add_column(f"{name}_after_discharge_date", event.admission_date)
    dataset.add_column(f"prior_{name}_hospital", has_prior_admission_with_diagnosis(codes, first_hf.admission_date))
# Certificate codes are available only for a death within observed follow-up,
# including day 0 for diagnostics. R compares chapter I and MI/stroke definitions.
observed_ons_death = ons_deaths.date.is_on_or_between(first_hf.admission_date, followup_end)
for field in ["underlying_cause_of_death"] + [f"cause_of_death_{i:02}" for i in range(1, 16)]:
    dataset.add_column(f"ons_{field}", case(when(observed_ons_death).then(getattr(ons_deaths, field))))
dataset.smoking_last_code = latest_smoking_code
# Generic recording availability, not medication-class ascertainment.
prior_year_clinical = clinical_events.where(clinical_events.date.is_on_or_between(
    first_hf.admission_date - days(365), first_hf.admission_date - days(1)))
prior_year_medications = medications.where(medications.date.is_on_or_between(
    first_hf.admission_date - days(365), first_hf.admission_date - days(1)))
dataset.prior_year_clinical_records_n = prior_year_clinical.count_for_patient()
dataset.prior_year_medication_records_n = prior_year_medications.count_for_patient()

dataset.index_same_day_spells_n = apcs.where(apcs.admission_date == first_hf.admission_date).count_for_patient()

from review_variables import add_review_variables
add_review_variables(dataset, first_hf.admission_date, first_hf, current_reg)
