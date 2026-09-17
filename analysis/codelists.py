######################################
# codelists.py
# COVID-19 vaccination and risk of MACE after hip fracture
#
# All codelist imports and inline code definitions
######################################

from ehrql import codelist_from_csv

# -----------------------------------------------------------------------
# Helper: expand 3-character ICD-10 codes to include "X" suffix for SUS
# -----------------------------------------------------------------------
def expand_three_char_icd10_codes(codelist):
    return codelist + [f"{code}X" for code in codelist if len(code) == 3]


# =====================================================================
# POPULATION SELECTION
# =====================================================================

# Hip fracture ICD-10 diagnosis codes
hip_fracture_icd10_codes = codelist_from_csv(
    "codelists/user-BillyZhongUOM-hip-fracture-icd-10.csv",
    column="code",
)
# Expand for SUS matching (3-char codes get X suffix)
hip_fracture_icd10_codes_expanded = expand_three_char_icd10_codes(
    hip_fracture_icd10_codes
)

# Hip fracture OPCS-4 procedure codes (inline - short well-defined set)
hip_fracture_opcs4 = [
    "W241",  # Closed reduction intracapsular fracture neck of femur, fixation nail/screw
    "W461",  # Primary prosthetic replacement head of femur using cement
    "W471",  # Primary prosthetic replacement head of femur not using cement
    "W191",  # Primary open reduction fracture neck of femur, fixation pin and plate
    "W481",  # Primary prosthetic replacement head of femur NEC
]

# Transport accident ICD-10 codes (V01-V99) - for exclusion
# We use prefix matching with "V" in all_diagnoses
transport_accident_prefix = "V"

# Elective admission methods - for exclusion
elective_admission_methods = ["11", "12", "13"]


# =====================================================================
# ETHNICITY
# =====================================================================

ethnicity6_codes = codelist_from_csv(
    "codelists/opensafely-ethnicity-snomed-0removed.csv",
    column="code",
    category_column="Grouping_6",
)

ethnicity16_codes = codelist_from_csv(
    "codelists/opensafely-ethnicity-snomed-0removed.csv",
    column="code",
    category_column="Grouping_16",
)


# =====================================================================
# OUTCOMES: ICD-10 (secondary care / death certificates)
# =====================================================================

# Cardiovascular disease - broad ICD-10 codelist for secondary care
# Columns: icd, description, mi, heartfailure
# mi=1 flags MI codes, heartfailure=1 flags HF codes
cvd_icd10_codes = codelist_from_csv(
    "codelists/opensafely-cardiovascular-secondary-care.csv",
    column="icd",
)

# Non-fatal MI - ICD-10
mi_icd10_codes = codelist_from_csv(
    "codelists/user-BillyZhongUOM-non_fatal_mi.csv",
    column="code",
)
mi_icd10_codes_expanded = expand_three_char_icd10_codes(mi_icd10_codes)

# Non-fatal stroke - ICD-10
stroke_icd10_codes = codelist_from_csv(
    "codelists/user-BillyZhongUOM-non_fatal_stroke.csv",
    column="code",
)
stroke_icd10_codes_expanded = expand_three_char_icd10_codes(stroke_icd10_codes)


# =====================================================================
# OUTCOMES: SNOMED (primary care)
# =====================================================================

# MI - SNOMED (NHSD primary care domain refset)
mi_snomed_codes = codelist_from_csv(
    "codelists/nhsd-primary-care-domain-refsets-mi_cod.csv",
    column="code",
)

# Stroke - SNOMED (NHSD primary care domain refset)
stroke_snomed_codes = codelist_from_csv(
    "codelists/nhsd-primary-care-domain-refsets-strk_cod.csv",
    column="code",
)


# =====================================================================
# OUTCOMES: CVD death ICD-10 codes
# Combined MI + stroke ICD-10 codes used for identifying CVD on death
# certificates. Uses the same MI and stroke ICD-10 codelists above.
# =====================================================================
# (No separate codelist needed - we will use mi_icd10_codes and
#  stroke_icd10_codes to check ONS cause_of_death fields)


# =====================================================================
# NEGATIVE CONTROL OUTCOMES
# =====================================================================

# Cataract surgery OPCS-4 codes (inline)
cataract_opcs4 = [
    "C711",  # Extracapsular extraction of lens using phako
    "C712",  # Extracapsular extraction of lens NEC
    "C718",  # Other specified extracapsular extraction of lens
    "C719",  # Unspecified extracapsular extraction of lens
    "C721",  # Intracapsular extraction of lens using cryoprobe
    "C731",  # Incision of capsule of lens
    "C741",  # Prosthesis of lens using posterior chamber intraocular lens
    "C742",  # Prosthesis of lens using anterior chamber intraocular lens
    "C748",  # Other specified prosthesis of lens
    "C749",  # Unspecified prosthesis of lens
    "C751",  # Insertion of prosthetic replacement for lens NEC
]


# =====================================================================
# CONFOUNDERS: HISTORY (SNOMED - primary care)
# =====================================================================

# Coronary heart disease history (Bristol multimorbidity)
chd_snomed_codes = codelist_from_csv(
    "codelists/bristol-multimorbidity_coronary-heart-disease.csv",
    column="code",
)

# Stroke/TIA history (Bristol multimorbidity)
stroke_tia_snomed_codes = codelist_from_csv(
    "codelists/bristol-multimorbidity_stroketransient-ischemic-attack.csv",
    column="code",
)

# Heart failure (Bristol multimorbidity)
heart_failure_snomed_codes = codelist_from_csv(
    "codelists/bristol-multimorbidity_heart-failure.csv",
    column="code",
)

# Atrial fibrillation (Bristol multimorbidity)
af_snomed_codes = codelist_from_csv(
    "codelists/bristol-multimorbidity_atrial-fibrillation.csv",
    column="code",
)

# Hypertension (NHSD domain refset)
hypertension_snomed_codes = codelist_from_csv(
    "codelists/nhsd-primary-care-domain-refsets-hyp_cod.csv",
    column="code",
)

# Diabetes (Bristol multimorbidity)
diabetes_snomed_codes = codelist_from_csv(
    "codelists/bristol-multimorbidity_diabetes.csv",
    column="code",
)

# These are eGFR measurement concepts, NOT CKD diagnoses.
egfr_codes = codelist_from_csv(
    "codelists/bristol-multimorbidity_chronic-kidney-disease.csv",
    column="code",
)

# Stage 3-5 history follows IDAIRE BSI; PRIMIS staging is also compared.
ckd35_codes = codelist_from_csv("codelists/primis-covid19-vacc-uptake-ckd35.csv", column="code")
ckd15_codes = codelist_from_csv("codelists/primis-covid19-vacc-uptake-ckd15.csv", column="code")
ckd_cov_codes = codelist_from_csv("codelists/primis-covid19-vacc-uptake-ckd_cov.csv", column="code")

# COPD (Bristol multimorbidity)
copd_snomed_codes = codelist_from_csv(
    "codelists/bristol-multimorbidity_copd.csv",
    column="code",
)

# Cancer (Bristol multimorbidity)
cancer_snomed_codes = codelist_from_csv(
    "codelists/bristol-multimorbidity_cancer.csv",
    column="code",
)

# Dementia (Bristol multimorbidity)
dementia_snomed_codes = codelist_from_csv(
    "codelists/bristol-multimorbidity_dementia.csv",
    column="code",
)

# Epilepsy (Bristol multimorbidity)
epilepsy_snomed_codes = codelist_from_csv(
    "codelists/bristol-multimorbidity_epilepsy.csv",
    column="code",
)

# Depression/anxiety (Bristol multimorbidity)
depression_snomed_codes = codelist_from_csv(
    "codelists/bristol-multimorbidity_anxietydepression.csv",
    column="code",
)

# Alcohol problems (Bristol multimorbidity)
alcohol_snomed_codes = codelist_from_csv(
    "codelists/bristol-multimorbidity_alcoholproblems.csv",
    column="code",
)

# Connective tissue disorder (Bristol multimorbidity)
ctd_snomed_codes = codelist_from_csv(
    "codelists/bristol-multimorbidity_connective-tissue-disorder.csv",
    column="code",
)

# Psychosis/bipolar (Bristol multimorbidity)
psychosis_snomed_codes = codelist_from_csv(
    "codelists/bristol-multimorbidity_psychosisbipolar-disorder.csv",
    column="code",
)

# Prior fracture at any site
# NOTE: You may need a broader fracture SNOMED codelist.
# For now we can use the hip fracture ICD-10 codes in APCS for prior HF,
# and a SNOMED codelist for other fractures if available.
# Placeholder - replace with a suitable codelist if created.


# =====================================================================
# CONFOUNDERS: BMI (from PRIMIS)
# =====================================================================

bmi_codes = codelist_from_csv(
    "codelists/primis-covid19-vacc-uptake-bmi.csv",
    column="code",
)

bmi_stage_codes = codelist_from_csv(
    "codelists/primis-covid19-vacc-uptake-bmi_stage.csv",
    column="code",
)

sev_obesity_codes = codelist_from_csv(
    "codelists/primis-covid19-vacc-uptake-sev_obesity.csv",
    column="code",
)


# =====================================================================
# CONFOUNDERS: SMOKING
# =====================================================================
# opensafely/smoking-clear-snomed/2020-04-29
# Columns: id, name, active, notes
# No category column in CSV — categorisation (S/E/N) must be done in dataset definition
# using case/when logic based on code descriptions

smoking_clear_codes = codelist_from_csv(
    "codelists/opensafely-smoking-clear-snomed.csv",
    column="id",
)

# Explicit categories used by IDAIRE BSI and post-covid-cvd-methods-redux.
smoking_categorised = codelist_from_csv("codelists/opensafely-smoking-clear.csv",
    column="CTV3Code", category_column="Category")
bmi_value_codes = codelist_from_csv("codelists/nhsd-primary-care-domain-refsets-bmival_cod.csv", column="code")
care_home_codes = codelist_from_csv("codelists/primis-covid19-vacc-uptake-longres.csv", column="code")
fracture_history_icd10 = codelist_from_csv("codelists/bristol-fractures.csv", column="code")

statin_codes = codelist_from_csv("codelists/nhs-drug-refsets-stat_cod.csv", column="code")
antihypertensive_codes = codelist_from_csv("codelists/nhs-drug-refsets-antihyp_cod.csv", column="code")
antiplatelet_codes = codelist_from_csv("codelists/user-elsie_horne-antiplatelet_dmd.csv", column="dmd_id")
anticoagulant_codes = codelist_from_csv("codelists/user-elsie_horne-anticoagulant_dmd.csv", column="dmd_id")
alendronate_codes = codelist_from_csv("codelists/opensafely-alendronic-acid.csv", column="code")
risedronate_codes = codelist_from_csv("codelists/opensafely-risedronate.csv", column="code")
ibandronate_codes = codelist_from_csv("codelists/opensafely-ibandronic-acid.csv", column="code")
zoledronate_codes = codelist_from_csv("codelists/opensafely-zoledronic-acid.csv", column="code")
teriparatide_codes = codelist_from_csv("codelists/opensafely-teriparatide.csv", column="code")
denosumab_codes = codelist_from_csv("codelists/uploaded/user-xixiong-denosumab-for-osteoporosis.csv", column="code")
raloxifene_codes = codelist_from_csv("codelists/uploaded/user-xixiong-raloxifene.csv", column="code")
romosozumab_codes = codelist_from_csv("codelists/uploaded/user-xixiong-romosozumab.csv", column="code")
bone_med_codes = list(set(alendronate_codes + risedronate_codes + ibandronate_codes +
    zoledronate_codes + teriparatide_codes + denosumab_codes + raloxifene_codes + romosozumab_codes))

