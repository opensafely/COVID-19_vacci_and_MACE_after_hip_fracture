######################################
# Dataset definition
# COVID-19 vaccination and risk of major adverse cardiac events after hip fracture
#
# This ehrQL dataset definition extracts the study population and variables
# for the analysis of COVID-19 vaccination and MACE risk after hip fracture.
#
# Data sources: OpenSAFELY-TPP primary care, APCS (SUS), ONS mortality, SGSS
######################################

from datetime import date

from ehrql import create_dataset, codelist_from_csv, days, years, months, case, when, minimum_of
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

##########################################################################
# Import codelists
##########################################################################

# --- Hip fracture identification (ICD-10 and OPCS-4) ---
# These are defined inline as simple lists since they are short, well-defined code sets.
# ICD-10 codes for hip fracture diagnosis (without dots, as stored in APCS)
hip_fracture_icd10 = ["S720", "S721", "S722", "S729"]

# OPCS-4 codes for hip fracture procedures (without dots)
hip_fracture_opcs4 = ["W241", "W461", "W471", "W191", "W481"]

# ICD-10 codes for transport accidents (exclusion criterion)
# V01-V99: we use the prefix "V" to match all transport accident codes
transport_accident_prefix = "V"

# --- Outcome codelists (loaded from CSV files in codelists/) ---
# Myocardial infarction ICD-10 codes
mi_icd10_codes = codelist_from_csv(
    "codelists/mi-icd10.csv",
    column="code",
)

# Ischaemic stroke ICD-10 codes
stroke_icd10_codes = codelist_from_csv(
    "codelists/stroke-icd10.csv",
    column="code",
)

# Combined MACE ICD-10 codes (MI + stroke + CVD death codes)
cvd_death_icd10_codes = codelist_from_csv(
    "codelists/cvd-death-icd10.csv",
    column="code",
)

# --- Outcome codelists for primary care (SNOMED) ---
mi_snomed_codes = codelist_from_csv(
    "codelists/mi-snomed.csv",
    column="code",
)

stroke_snomed_codes = codelist_from_csv(
    "codelists/stroke-snomed.csv",
    column="code",
)

# --- Negative control outcome ---
cataract_opcs4_codes = codelist_from_csv(
    "codelists/cataract-opcs4.csv",
    column="code",
)

# --- Confounder / covariate codelists (SNOMED) ---
cvd_snomed_codes = codelist_from_csv(
    "codelists/cvd-snomed.csv",
    column="code",
)

mi_history_snomed_codes = codelist_from_csv(
    "codelists/mi-history-snomed.csv",
    column="code",
)

stroke_history_snomed_codes = codelist_from_csv(
    "codelists/stroke-history-snomed.csv",
    column="code",
)

fracture_snomed_codes = codelist_from_csv(
    "codelists/fracture-snomed.csv",
    column="code",
)

smoking_clear_snomed_codes = codelist_from_csv(
    "codelists/smoking-clear-snomed.csv",
    column="code",
    category_column="category",
)

ethnicity_snomed_codes = codelist_from_csv(
    "codelists/ethnicity-snomed.csv",
    column="code",
    category_column="category",
)

# --- Medication codelists (dm+d) ---
bone_med_codes = codelist_from_csv(
    "codelists/bone-medications-dmd.csv",
    column="code",
)

statin_codes = codelist_from_csv(
    "codelists/statins-dmd.csv",
    column="code",
)

antiplatelet_codes = codelist_from_csv(
    "codelists/antiplatelets-dmd.csv",
    column="code",
)

anticoagulant_codes = codelist_from_csv(
    "codelists/anticoagulants-dmd.csv",
    column="code",
)

# --- Charlson comorbidity codelists (SNOMED) ---
charlson_snomed_codes = codelist_from_csv(
    "codelists/charlson-snomed.csv",
    column="code",
    category_column="category",
)

##########################################################################
# Study dates
##########################################################################

study_start_date = date(2019, 1, 1)     # Earliest hip fracture date
study_end_date = date(2025, 1, 1)       # Latest hip fracture date (1 yr before data end)
data_end_date = date(2026, 1, 1)        # End of follow-up / data availability
lookback_start = date(2017, 1, 1)       # Earliest date for lookback (2 yr before study start)

##########################################################################
# Create dataset
##########################################################################

dataset = create_dataset()
dataset.configure_dummy_data(population_size=10000)

##########################################################################
# Identify hip fractures from APCS
##########################################################################

# Hip fracture admissions: diagnosed OR operated on for hip fracture
# between study_start_date and study_end_date
# Emergency admissions only (admission_method starts with "2" = emergency)
# Exclude transport accidents and age < 50

hip_fracture_admissions = (
    apcs
    .where(
        apcs.admission_date.is_on_or_between(study_start_date, study_end_date)
    )
    .where(
        # Has a hip fracture diagnosis OR procedure
        apcs.all_diagnoses.contains_any_of(hip_fracture_icd10)
        | apcs.all_procedures.contains_any_of(hip_fracture_opcs4)
    )
    .where(
        # Exclude transport accidents (V01-V99)
        ~apcs.all_diagnoses.contains(transport_accident_prefix)
    )
    .where(
        # Emergency admissions only: admission_method starting with "2"
        # (21=A&E, 22=GP, 23=Bed bureau, 24=Consultant clinic, 25=MH crisis,
        #  2A=A&E of another provider, 2B=Transfer, 2C=Baby born in hosp,
        #  2D=Other emergency, 28=Other)
        # Exclude elective: 11, 12, 13
        (apcs.admission_method != "11")
        & (apcs.admission_method != "12")
        & (apcs.admission_method != "13")
    )
)

# Get the FIRST hip fracture per patient in the study window
first_hf = (
    hip_fracture_admissions
    .sort_by(apcs.admission_date)
    .first_for_patient()
)

# Check patient had no prior hip fracture in the 2-year lookback period
prior_hf = (
    apcs
    .where(
        apcs.admission_date.is_on_or_between(lookback_start, study_start_date - days(1))
    )
    .where(
        apcs.all_diagnoses.contains_any_of(hip_fracture_icd10)
        | apcs.all_procedures.contains_any_of(hip_fracture_opcs4)
    )
    .exists_for_patient()
)

##########################################################################
# Define study population
##########################################################################

# Age at index (hip fracture admission date)
age_at_index = patients.age_on(first_hf.admission_date)

# GP registration: patient must be registered with a TPP practice at index
registered_at_index = (
    practice_registrations
    .where(practice_registrations.start_date <= first_hf.admission_date)
    .except_where(practice_registrations.end_date < first_hf.admission_date)
    .exists_for_patient()
)

dataset.define_population(
    # Has a hip fracture admission in study window
    first_hf.admission_date.is_not_null()
    # No prior hip fracture in lookback period
    & ~prior_hf
    # Age >= 50 at index
    & (age_at_index >= 50)
    # Registered with a TPP GP practice at index
    & registered_at_index
)

##########################################################################
# Population variables
##########################################################################

# GP registration start date (most recent registration spanning the index date)
current_registration = (
    practice_registrations
    .where(practice_registrations.start_date <= first_hf.admission_date)
    .except_where(practice_registrations.end_date < first_hf.admission_date)
    .sort_by(practice_registrations.start_date)
    .last_for_patient()
)
dataset.gp_registration_start = current_registration.start_date

# Index date (date of first hip fracture)
dataset.index_date = first_hf.admission_date

# Emergency vs elective flag (should be all emergency given population criteria,
# but included for verification)
dataset.emergency_hf = case(
    when(
        (first_hf.admission_method != "11")
        & (first_hf.admission_method != "12")
        & (first_hf.admission_method != "13")
    ).then(1),
    otherwise=0,
)

# Trauma flag: transport accident codes present in the spell
# (should be 0 for all given exclusion, but kept for verification)
dataset.trauma_flag = case(
    when(first_hf.all_diagnoses.contains(transport_accident_prefix)).then(1),
    otherwise=0,
)

# Age at index
dataset.age = age_at_index

##########################################################################
# Exposure variables: COVID-19 vaccination
##########################################################################

# All COVID-19 vaccinations
covid_vax = vaccinations.where(
    vaccinations.target_disease == "SARS-2 CORONAVIRUS"
)

# COVID-19 vaccination within 365 days before index
dataset.covax = covid_vax.where(
    covid_vax.date.is_on_or_between(
        first_hf.admission_date - days(365),
        first_hf.admission_date
    )
).exists_for_patient()

# All COVID vaccinations sorted by date - extract up to 10 doses
covid_vax_sorted = covid_vax.sort_by(covid_vax.date)

# First COVID vaccine
first_covid_vax = covid_vax_sorted.first_for_patient()
dataset.covax_date_1 = first_covid_vax.date
dataset.covax_type_1 = first_covid_vax.product_name

# Second COVID vaccine
second_covid_vax = (
    covid_vax
    .where(covid_vax.date > first_covid_vax.date)
    .sort_by(covid_vax.date)
    .first_for_patient()
)
dataset.covax_date_2 = second_covid_vax.date
dataset.covax_type_2 = second_covid_vax.product_name

# Third COVID vaccine
third_covid_vax = (
    covid_vax
    .where(covid_vax.date > second_covid_vax.date)
    .sort_by(covid_vax.date)
    .first_for_patient()
)
dataset.covax_date_3 = third_covid_vax.date
dataset.covax_type_3 = third_covid_vax.product_name

# Fourth COVID vaccine
fourth_covid_vax = (
    covid_vax
    .where(covid_vax.date > third_covid_vax.date)
    .sort_by(covid_vax.date)
    .first_for_patient()
)
dataset.covax_date_4 = fourth_covid_vax.date
dataset.covax_type_4 = fourth_covid_vax.product_name

# Fifth COVID vaccine
fifth_covid_vax = (
    covid_vax
    .where(covid_vax.date > fourth_covid_vax.date)
    .sort_by(covid_vax.date)
    .first_for_patient()
)
dataset.covax_date_5 = fifth_covid_vax.date
dataset.covax_type_5 = fifth_covid_vax.product_name

# Sixth COVID vaccine
sixth_covid_vax = (
    covid_vax
    .where(covid_vax.date > fifth_covid_vax.date)
    .sort_by(covid_vax.date)
    .first_for_patient()
)
dataset.covax_date_6 = sixth_covid_vax.date
dataset.covax_type_6 = sixth_covid_vax.product_name

##########################################################################
# Exposure variables: Influenza vaccination
##########################################################################

flu_vax = vaccinations.where(
    vaccinations.target_disease == "Influenza"
)

# Flu vaccination within 365 days before index
dataset.fluvax = flu_vax.where(
    flu_vax.date.is_on_or_between(
        first_hf.admission_date - days(365),
        first_hf.admission_date
    )
).exists_for_patient()

flu_vax_sorted = flu_vax.sort_by(flu_vax.date)

# First flu vaccine
first_flu_vax = flu_vax_sorted.first_for_patient()
dataset.fluvax_date_1 = first_flu_vax.date
dataset.fluvax_type_1 = first_flu_vax.product_name

# Second flu vaccine
second_flu_vax = (
    flu_vax
    .where(flu_vax.date > first_flu_vax.date)
    .sort_by(flu_vax.date)
    .first_for_patient()
)
dataset.fluvax_date_2 = second_flu_vax.date
dataset.fluvax_type_2 = second_flu_vax.product_name

# Third flu vaccine
third_flu_vax = (
    flu_vax
    .where(flu_vax.date > second_flu_vax.date)
    .sort_by(flu_vax.date)
    .first_for_patient()
)
dataset.fluvax_date_3 = third_flu_vax.date
dataset.fluvax_type_3 = third_flu_vax.product_name

# Fourth flu vaccine
fourth_flu_vax = (
    flu_vax
    .where(flu_vax.date > third_flu_vax.date)
    .sort_by(flu_vax.date)
    .first_for_patient()
)
dataset.fluvax_date_4 = fourth_flu_vax.date
dataset.fluvax_type_4 = fourth_flu_vax.product_name

# Fifth flu vaccine
fifth_flu_vax = (
    flu_vax
    .where(flu_vax.date > fourth_flu_vax.date)
    .sort_by(flu_vax.date)
    .first_for_patient()
)
dataset.fluvax_date_5 = fifth_flu_vax.date
dataset.fluvax_type_5 = fifth_flu_vax.product_name

# Sixth flu vaccine
sixth_flu_vax = (
    flu_vax
    .where(flu_vax.date > fifth_flu_vax.date)
    .sort_by(flu_vax.date)
    .first_for_patient()
)
dataset.fluvax_date_6 = sixth_flu_vax.date
dataset.fluvax_type_6 = sixth_flu_vax.product_name

##########################################################################
# Outcome variables
##########################################################################

# --- MI from hospital admissions (ICD-10) ---
mi_hospital = (
    apcs
    .where(apcs.admission_date > first_hf.admission_date)
    .where(apcs.admission_date <= first_hf.admission_date + days(365))
    .where(
        apcs.all_diagnoses.contains_any_of(mi_icd10_codes)
    )
    .sort_by(apcs.admission_date)
    .first_for_patient()
)

# --- MI from primary care (SNOMED) ---
mi_primary_care = (
    clinical_events
    .where(clinical_events.snomedct_code.is_in(mi_snomed_codes))
    .where(clinical_events.date > first_hf.admission_date)
    .where(clinical_events.date <= first_hf.admission_date + days(365))
    .sort_by(clinical_events.date)
    .first_for_patient()
)

# First MI event (earliest across hospital and primary care)
dataset.mi_date = minimum_of(mi_hospital.admission_date, mi_primary_care.date)
dataset.mi365 = dataset.mi_date.is_not_null()

# --- Stroke from hospital admissions (ICD-10) ---
stroke_hospital = (
    apcs
    .where(apcs.admission_date > first_hf.admission_date)
    .where(apcs.admission_date <= first_hf.admission_date + days(365))
    .where(
        apcs.all_diagnoses.contains_any_of(stroke_icd10_codes)
    )
    .sort_by(apcs.admission_date)
    .first_for_patient()
)

# --- Stroke from primary care (SNOMED) ---
stroke_primary_care = (
    clinical_events
    .where(clinical_events.snomedct_code.is_in(stroke_snomed_codes))
    .where(clinical_events.date > first_hf.admission_date)
    .where(clinical_events.date <= first_hf.admission_date + days(365))
    .sort_by(clinical_events.date)
    .first_for_patient()
)

# First stroke event
dataset.stroke_date = minimum_of(stroke_hospital.admission_date, stroke_primary_care.date)
dataset.stroke365 = dataset.stroke_date.is_not_null()

# --- CVD death from ONS mortality ---
# Check cause of death fields for CVD codes
cvd_death = (
    ons_deaths.cause_of_death_is_in(cvd_death_icd10_codes)
    & (ons_deaths.date > first_hf.admission_date)
    & (ons_deaths.date <= first_hf.admission_date + days(365))
)

dataset.cvddeath_date = case(
    when(cvd_death).then(ons_deaths.date),
)
dataset.cvddeath365 = cvd_death

# --- Composite MACE outcome ---
dataset.mace_date = minimum_of(
    dataset.mi_date,
    dataset.stroke_date,
    dataset.cvddeath_date,
)
dataset.mace365 = dataset.mace_date.is_not_null()

# --- All-cause death (competing risk) ---
dataset.all_cause_death_date = ons_deaths.date

# --- Negative control: Cataract surgery ---
cataract_surgery = (
    apcs
    .where(apcs.admission_date > first_hf.admission_date)
    .where(apcs.admission_date <= first_hf.admission_date + days(365))
    .where(
        apcs.all_procedures.contains_any_of(cataract_opcs4_codes)
    )
    .sort_by(apcs.admission_date)
    .first_for_patient()
)
dataset.neg_con_1_date = cataract_surgery.admission_date
dataset.neg_con_1 = dataset.neg_con_1_date.is_not_null()

##########################################################################
# Confounder / modifier / mediator variables
##########################################################################

# --- Demographics ---

# Sex
dataset.sex = patients.sex

# Ethnicity (from primary care, with SUS fallback)
latest_ethnicity_code = (
    clinical_events
    .where(clinical_events.snomedct_code.is_in(ethnicity_snomed_codes))
    .sort_by(clinical_events.date)
    .last_for_patient()
    .snomedct_code
)

dataset.ethnicity = latest_ethnicity_code.to_category(ethnicity_snomed_codes)

# Ethnicity from SUS (fallback)
dataset.ethnicity_sus = ethnicity_from_sus.code

# Region (NHS England region based on registered practice)
dataset.resgor = current_registration.practice_nuts1_region_name

# Index of Multiple Deprivation (quintile)
address_at_index = addresses.for_patient_on(first_hf.admission_date)
dataset.imd5 = address_at_index.imd_quintile

# --- Clinical variables ---

# Type of hip fracture surgery (from the first hip fracture spell)
dataset.surgery_type = case(
    when(first_hf.all_procedures.contains("W241")).then("internal_fixation_nail_screw"),
    when(first_hf.all_procedures.contains("W191")).then("internal_fixation_pin_plate"),
    when(first_hf.all_procedures.contains("W461")).then("hemiarthroplasty_cemented"),
    when(first_hf.all_procedures.contains("W471")).then("hemiarthroplasty_uncemented"),
    when(first_hf.all_procedures.contains("W481")).then("hemiarthroplasty_nec"),
    otherwise="other_or_none",
)

# History of cardiovascular disease (any time before index)
dataset.prior_cvd = (
    clinical_events
    .where(clinical_events.snomedct_code.is_in(cvd_snomed_codes))
    .where(clinical_events.date < first_hf.admission_date)
    .exists_for_patient()
)

# History of MI
dataset.prior_mi = (
    clinical_events
    .where(clinical_events.snomedct_code.is_in(mi_history_snomed_codes))
    .where(clinical_events.date < first_hf.admission_date)
    .exists_for_patient()
)

# History of ischaemic stroke
dataset.prior_stroke = (
    clinical_events
    .where(clinical_events.snomedct_code.is_in(stroke_history_snomed_codes))
    .where(clinical_events.date < first_hf.admission_date)
    .exists_for_patient()
)

# Charlson Comorbidity Index
# Note: Computing a full CCI in ehrQL requires multiple condition-specific
# codelists and weighted scoring. This is a simplified version using a
# single codelist with category weights. The full implementation should be
# done in the downstream analysis script.
# For now, count the number of distinct Charlson comorbidity categories present.
dataset.cci = (
    clinical_events
    .where(clinical_events.snomedct_code.is_in(charlson_snomed_codes))
    .where(clinical_events.date < first_hf.admission_date)
    .snomedct_code
    .to_category(charlson_snomed_codes)
    .count_distinct_for_patient()
)

# History of prior fracture at any site
dataset.prior_frac = (
    clinical_events
    .where(clinical_events.snomedct_code.is_in(fracture_snomed_codes))
    .where(clinical_events.date < first_hf.admission_date)
    .exists_for_patient()
)

# BMI: most recent value before index
bmi_record = (
    clinical_events
    .where(clinical_events.snomedct_code == "60621009")  # SNOMED for BMI
    .where(clinical_events.date < first_hf.admission_date)
    .where(clinical_events.numeric_value.is_not_null())
    .where(
        (clinical_events.numeric_value >= 12.0)
        & (clinical_events.numeric_value <= 80.0)
    )
    .sort_by(clinical_events.date)
    .last_for_patient()
)
dataset.bmi = bmi_record.numeric_value

# Smoking status: most recent before index
dataset.smoking = (
    clinical_events
    .where(clinical_events.snomedct_code.is_in(smoking_clear_snomed_codes))
    .where(clinical_events.date < first_hf.admission_date)
    .sort_by(clinical_events.date)
    .last_for_patient()
    .snomedct_code
    .to_category(smoking_clear_snomed_codes)
)

# Alcohol consumption: latest recorded before index
# This uses the same pattern as smoking - a categorised codelist
# Placeholder: will need a specific alcohol consumption codelist
# For now, use hazardous alcohol use as a binary variable
alcohol_snomed_codes = codelist_from_csv(
    "codelists/alcohol-snomed.csv",
    column="code",
    category_column="category",
)
dataset.alcohol = (
    clinical_events
    .where(clinical_events.snomedct_code.is_in(alcohol_snomed_codes))
    .where(clinical_events.date < first_hf.admission_date)
    .sort_by(clinical_events.date)
    .last_for_patient()
    .snomedct_code
    .to_category(alcohol_snomed_codes)
)

# Use of anti-osteoporosis medication (in the year before index)
dataset.bone_med = (
    medications
    .where(medications.dmd_code.is_in(bone_med_codes))
    .where(
        medications.date.is_on_or_between(
            first_hf.admission_date - days(365),
            first_hf.admission_date,
        )
    )
    .exists_for_patient()
)

# Use of statins / antiplatelets / anticoagulants (in the year before index)
dataset.cvd_med = (
    medications
    .where(
        medications.dmd_code.is_in(statin_codes)
        | medications.dmd_code.is_in(antiplatelet_codes)
        | medications.dmd_code.is_in(anticoagulant_codes)
    )
    .where(
        medications.date.is_on_or_between(
            first_hf.admission_date - days(365),
            first_hf.admission_date,
        )
    )
    .exists_for_patient()
)

# --- Infection dates (confounder / mediator) ---

# Date of positive SARS-CoV-2 test (most recent before index + 365 days)
dataset.cov_date = (
    sgss_covid_all_tests
    .where(sgss_covid_all_tests.is_positive)
    .where(sgss_covid_all_tests.specimen_taken_date <= first_hf.admission_date + days(365))
    .sort_by(sgss_covid_all_tests.specimen_taken_date)
    .last_for_patient()
    .specimen_taken_date
)

# Recorded influenza infection (most recent before index + 365 days)
# Using a broad influenza diagnosis SNOMED code
flu_infection_snomed_codes = codelist_from_csv(
    "codelists/influenza-infection-snomed.csv",
    column="code",
)
dataset.flu_date = (
    clinical_events
    .where(clinical_events.snomedct_code.is_in(flu_infection_snomed_codes))
    .where(clinical_events.date <= first_hf.admission_date + days(365))
    .sort_by(clinical_events.date)
    .last_for_patient()
    .date
)

# --- Hip fracture spell details (for reference) ---
dataset.hf_discharge_date = first_hf.discharge_date
dataset.hf_admission_method = first_hf.admission_method
dataset.hf_primary_diagnosis = first_hf.primary_diagnosis
