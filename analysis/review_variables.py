"""Second feasibility round: explicit measurement, history and recording definitions.

Sources are versioned in codelists.json. No-record flags are not proof of absence.
All baseline clinical and prescription windows end the day before admission.
"""
from ehrql import case, when, days, years
from ehrql.tables.tpp import clinical_events, medications, patients, addresses, apcs, practice_registrations
from ehrql.codes import ICD10Code
from study_config import HISTORY_START, DATA_END
from variable_lib import has_prior_event_snomed, last_prior_event_snomed, has_prior_admission_with_diagnosis
import codelists


def add_review_variables(dataset, index, first_hf, current_reg):
    history = clinical_events.where(clinical_events.date >= HISTORY_START).where(clinical_events.date < index)

    # Separate CKD diagnoses from evidence that renal function was measured.
    dataset.prior_ckd_stage3_5 = has_prior_event_snomed(codelists.ckd35_codes, index)
    dataset.prior_ckd = dataset.prior_ckd_stage3_5
    stage35 = last_prior_event_snomed(codelists.ckd35_codes, index).date
    stage15 = last_prior_event_snomed(codelists.ckd15_codes, index).date
    dataset.ckd_current_primis = (
        has_prior_event_snomed(codelists.ckd_cov_codes, index)
        | (stage35.is_not_null() & (stage15.is_null() | (stage35 >= stage15)))
    )
    egfr = history.where(clinical_events.snomedct_code.is_in(codelists.egfr_codes))
    dataset.egfr_recorded = egfr.exists_for_patient()
    valid_egfr = egfr.where(clinical_events.numeric_value > 0).where(clinical_events.numeric_value <= 200)
    last_egfr = valid_egfr.sort_by(clinical_events.date, clinical_events.numeric_value).last_for_patient()
    dataset.egfr_value = last_egfr.numeric_value
    dataset.egfr_date = last_egfr.date
    # Laboratory corroboration for review, not an automatic diagnostic replacement.
    low = valid_egfr.where(clinical_events.numeric_value < 60)
    low_last = low.sort_by(clinical_events.date).last_for_patient().date
    dataset.egfr_low_repeated_90d = low.where(clinical_events.date <= low_last - days(90)).exists_for_patient()

    # Adult recorded BMI: compare 2y and 5y, capped at the approved 2017 history start.
    bmi_all = history.where(clinical_events.snomedct_code.is_in(list(set(codelists.bmi_codes + codelists.bmi_value_codes))))
    bmi_all = bmi_all.where(patients.age_on(clinical_events.date) >= 18)
    bmi_valid = bmi_all.where(clinical_events.numeric_value >= 10).where(clinical_events.numeric_value <= 100)
    for window in (2, 5):
        record = bmi_valid.where(clinical_events.date >= index - years(window)).sort_by(
            clinical_events.date, clinical_events.numeric_value).last_for_patient()
        dataset.add_column(f"bmi_{window}y", record.numeric_value)
        dataset.add_column(f"bmi_{window}y_date", record.date)
    dataset.bmi = dataset.bmi_5y
    dataset.bmi_date = dataset.bmi_5y_date
    latest_raw = bmi_all.where(clinical_events.numeric_value.is_not_null()).sort_by(
        clinical_events.date, clinical_events.numeric_value).last_for_patient()
    dataset.bmi_latest_out_of_range = ((latest_raw.numeric_value < 10) | (latest_raw.numeric_value > 100))

    clear = history.where(clinical_events.ctv3_code.is_in(codelists.smoking_categorised))
    # Resolve conflicting same-day status deterministically: current > former > never.
    latest_date = clear.date.maximum_for_patient()
    latest = clear.where(clinical_events.date == latest_date)
    current_codes = [code for code, category in codelists.smoking_categorised.items() if category == "S"]
    ex_codes = [code for code, category in codelists.smoking_categorised.items() if category == "E"]
    never_codes = [code for code, category in codelists.smoking_categorised.items() if category == "N"]
    current = latest.where(clinical_events.ctv3_code.is_in(current_codes)).exists_for_patient()
    former = latest.where(clinical_events.ctv3_code.is_in(ex_codes)).exists_for_patient()
    never = latest.where(clinical_events.ctv3_code.is_in(never_codes)).exists_for_patient()
    ever = clear.where(clinical_events.ctv3_code.is_in(current_codes + ex_codes)).exists_for_patient()
    dataset.smoking_status = case(when(current).then("S"), when(former | (never & ever)).then("E"),
                                  when(never).then("N"))
    dataset.smoking_date = latest_date
    dataset.smoking_ever_recorded = ever
    dataset.smoking_same_day_conflict = ((current & former) | (current & never) | (former & never))

    # GP prescriptions do not establish dispensing, adherence or hospital treatment.
    for name, codes in {
        "statin": codelists.statin_codes, "antihypertensive": codelists.antihypertensive_codes,
        "antiplatelet": codelists.antiplatelet_codes, "anticoagulant": codelists.anticoagulant_codes,
        "bone_active": codelists.bone_med_codes,
    }.items():
        records = medications.where(medications.dmd_code.is_in(codes)).where(
            medications.date >= index - days(365)).where(medications.date >= HISTORY_START).where(medications.date < index)
        dataset.add_column(f"rx_{name}_prior365", records.exists_for_patient())

    dataset.care_home_gp_record = has_prior_event_snomed(codelists.care_home_codes, index)
    dataset.care_home_address = addresses.for_patient_on(index).care_home_is_potential_match
    dataset.care_home_evidence = dataset.care_home_gp_record | dataset.care_home_address
    dataset.prior_any_fracture_hospital = has_prior_admission_with_diagnosis(codelists.fracture_history_icd10, index)
    # Retain GP-only histories, and provide the combined ascertainment requested by RP.
    for name, prefixes in {"mi": ["I21", "I22"], "stroke": ["I60", "I61", "I63", "I64"],
                           "heart_failure": ["I50"], "af": ["I48"], "hypertension": ["I10", "I11", "I12", "I13", "I15"]}.items():
        hospital = has_prior_admission_with_diagnosis(prefixes, index)
        dataset.add_column(f"history_{name}_hospital", hospital)
        dataset.add_column(f"history_{name}_combined", getattr(dataset, f"prior_{name}") | hospital)

    # Candidate procedure groups within a diagnosed hip fracture spell; not proof
    # that the procedure treated the fracture, and not new procedure-only eligibility.
    proc = first_hf.all_procedures
    total = proc.contains_any_of(["W37", "W38", "W39", "W93", "W94", "W95"]).when_null_then(False)
    hemi = proc.contains_any_of(["W46", "W47", "W48"]).when_null_then(False)
    fixation = proc.contains_any_of(["W19", "W20", "W22", "W24", "W25"]).when_null_then(False)
    dataset.surgery_multiple_groups = (total & hemi) | (total & fixation) | (hemi & fixation)
    dataset.surgery_group_review = case(
        when(proc.is_null()).then("Missing procedure field"),
        when(dataset.surgery_multiple_groups).then("Multiple procedure groups"),
        when(total).then("Total hip replacement group"), when(hemi).then("Hemiarthroplasty group"),
        when(fixation).then("Fracture fixation group"), otherwise="No matching procedure group")
    # Exact four-character matching misses optional fifth ICD-10 characters.
    hip_prefixes = ["S720", "S721", "S722", "S729"]
    dataset.index_primary_hip_exact = first_hf.primary_diagnosis.is_in(hip_prefixes)
    dataset.index_primary_hip = first_hf.primary_diagnosis.is_in(
        [ICD10Code(code) for prefix in hip_prefixes for code in [prefix, prefix + "0", prefix + "1"]])

    # Audit transitions without claiming a single registration is continuous coverage.
    observable_reg_end = case(when(current_reg.end_date <= DATA_END).then(current_reg.end_date))
    future = practice_registrations.where(practice_registrations.start_date >= observable_reg_end).where(
        practice_registrations.start_date > current_reg.start_date).where(
        practice_registrations.start_date <= observable_reg_end + days(30)).where(
        practice_registrations.start_date <= index + days(365)).where(practice_registrations.start_date <= DATA_END)
    dataset.registration_next_start_date = future.start_date.minimum_for_patient()
    dataset.registration_contiguous_next = future.where(
        practice_registrations.start_date <= observable_reg_end + days(1)).exists_for_patient()
    dataset.registration_overlap_continues = practice_registrations.where(
        practice_registrations.start_date < observable_reg_end).where(
        practice_registrations.end_date.is_null() | (practice_registrations.end_date > observable_reg_end)
    ).exists_for_patient()
    dataset.registration_current_end_date = current_reg.end_date
