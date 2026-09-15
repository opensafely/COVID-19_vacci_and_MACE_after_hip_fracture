"""Monthly source activity in the selected cohort, bounded by observed follow-up."""
from ehrql import case, when, minimum_of
from ehrql.tables.tpp import apcs, clinical_events, medications, vaccinations, ons_deaths, sgss_covid_all_tests
from study_config import COVID_TARGET, FLU_TARGET, HISTORY_START
import codelists


def source_counts(start, end, observation_end):
    end = minimum_of(end, observation_end)
    sources = {
        "apcs": (apcs, apcs.admission_date),
        "gp_clinical": (clinical_events, clinical_events.date),
        "gp_medications": (medications, medications.date),
        "covid_vaccination": (vaccinations.where(vaccinations.target_disease == COVID_TARGET), vaccinations.date),
        "flu_vaccination": (vaccinations.where(vaccinations.target_disease == FLU_TARGET), vaccinations.date),
        "gp_mi_record": (clinical_events.where(clinical_events.snomedct_code.is_in(codelists.mi_snomed_codes)), clinical_events.date),
        "gp_stroke_record": (clinical_events.where(clinical_events.snomedct_code.is_in(codelists.stroke_snomed_codes)), clinical_events.date),
        "hospital_mi": (apcs.where(apcs.all_diagnoses.contains_any_of(codelists.mi_icd10_codes_expanded)), apcs.admission_date),
        "hospital_stroke": (apcs.where(apcs.all_diagnoses.contains_any_of(codelists.stroke_icd10_codes_expanded)), apcs.admission_date),
        "sgss_positive": (sgss_covid_all_tests.where(sgss_covid_all_tests.is_positive), sgss_covid_all_tests.specimen_taken_date),
    }
    result = {}
    for name, (table, date) in sources.items():
        records = table.where(date.is_on_or_between(start, end)).where(date >= HISTORY_START)
        result[f"{name}_records"] = records.count_for_patient()
        result[f"{name}_patients"] = records.exists_for_patient()
    death = ons_deaths.date.is_on_or_between(start, end) & (ons_deaths.date >= HISTORY_START)
    result["ons_death_patients"] = death.when_null_then(False)
    return result
