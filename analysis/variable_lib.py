######################################
# variable_lib.py
# Reusable helper functions for defining variables
######################################

import operator
from functools import reduce

from ehrql import case, when
from ehrql.codes import ICD10Code
from ehrql.tables.tpp import (
    clinical_events,
    apcs,
    addresses,
    medications,
    ethnicity_from_sus,
)


def any_of(conditions):
    """Combine multiple boolean conditions with OR."""
    return reduce(operator.or_, conditions)


# -----------------------------------------------------------------------
# Primary care event helpers
# -----------------------------------------------------------------------

def has_prior_event_snomed(codelist, before_date):
    """Check if patient has any clinical event in codelist before a date."""
    return (
        clinical_events
        .where(clinical_events.snomedct_code.is_in(codelist))
        .where(clinical_events.date < before_date)
        .exists_for_patient()
    )


def last_prior_event_snomed(codelist, before_date):
    """Get the most recent clinical event in codelist before a date."""
    return (
        clinical_events
        .where(clinical_events.snomedct_code.is_in(codelist))
        .where(clinical_events.date < before_date)
        .sort_by(clinical_events.date)
        .last_for_patient()
    )


def first_event_after_snomed(codelist, after_date, before_date):
    """Get the first clinical event in codelist within a date range (exclusive start)."""
    return (
        clinical_events
        .where(clinical_events.snomedct_code.is_in(codelist))
        .where(clinical_events.date > after_date)
        .where(clinical_events.date <= before_date)
        .sort_by(clinical_events.date)
        .first_for_patient()
    )


# -----------------------------------------------------------------------
# Hospital admission (APCS) helpers
# -----------------------------------------------------------------------

def first_admission_with_diagnosis(icd10_codes, after_date, before_date):
    """
    Get first hospital admission where all_diagnoses contains any of 
    the given ICD-10 codes, within a date range (exclusive start).
    """
    return (
        apcs
        .where(apcs.admission_date > after_date)
        .where(apcs.admission_date <= before_date)
        .where(apcs.all_diagnoses.contains_any_of(icd10_codes))
        .sort_by(apcs.admission_date)
        .first_for_patient()
    )


def first_admission_with_procedure(opcs4_codes, after_date, before_date):
    """
    Get first hospital admission where all_procedures contains any of
    the given OPCS-4 codes, within a date range (exclusive start).
    """
    return (
        apcs
        .where(apcs.admission_date > after_date)
        .where(apcs.admission_date <= before_date)
        .where(apcs.all_procedures.contains_any_of(opcs4_codes))
        .sort_by(apcs.admission_date)
        .first_for_patient()
    )


def has_prior_admission_with_diagnosis(icd10_codes, before_date):
    """Check if patient has any hospital admission with diagnosis before a date."""
    return (
        apcs
        .where(apcs.admission_date < before_date)
        .where(apcs.all_diagnoses.contains_any_of(icd10_codes))
        .exists_for_patient()
    )


# -----------------------------------------------------------------------
# Medication helpers
# -----------------------------------------------------------------------

def has_prior_medication(dmd_codelist, before_date, within_days=None):
    """
    Check if patient has any medication in codelist before a date.
    Optionally restrict to within N days before the date.
    """
    meds = medications.where(medications.dmd_code.is_in(dmd_codelist))
    if within_days is not None:
        from ehrql import days
        meds = meds.where(
            medications.date.is_on_or_between(before_date - days(within_days), before_date)
        )
    else:
        meds = meds.where(medications.date < before_date)
    return meds.exists_for_patient()


# -----------------------------------------------------------------------
# Address / geography helpers
# -----------------------------------------------------------------------

def imd_quintile(index_date):
    """Return IMD quintile (1=most deprived, 5=least deprived) at a date."""
    imd_rounded = addresses.for_patient_on(index_date).imd_rounded
    max_imd = 32844
    return case(
        when((imd_rounded >= 0) & (imd_rounded <= int(max_imd * 1 / 5))).then(1),
        when(imd_rounded <= int(max_imd * 2 / 5)).then(2),
        when(imd_rounded <= int(max_imd * 3 / 5)).then(3),
        when(imd_rounded <= int(max_imd * 4 / 5)).then(4),
        when(imd_rounded <= max_imd).then(5),
        otherwise=0,
    )


def rural_urban_5(index_date):
    """Return 5-category rural/urban classification at a date."""
    urban_rural_all = addresses.for_patient_on(index_date).rural_urban_classification
    return case(
        when(urban_rural_all == 1).then("Urban major conurbation"),
        when(urban_rural_all == 2).then("Urban minor conurbation"),
        when(urban_rural_all == 3).then("Urban city and town"),
        when(urban_rural_all == 4).then("Urban city and town"),
        when(urban_rural_all == 5).then("Rural town and fringe"),
        when(urban_rural_all == 6).then("Rural town and fringe"),
        when(urban_rural_all == 7).then("Rural village and dispersed"),
        when(urban_rural_all == 8).then("Rural village and dispersed"),
    )


# -----------------------------------------------------------------------
# Ethnicity helpers
# -----------------------------------------------------------------------

def get_ethnicity6(index_date, ethnicity_codelist):
    """
    Get ethnicity (6-category) from GP records, with SUS fallback.
    """
    latest_ethnicity_code = (
        clinical_events
        .where(clinical_events.snomedct_code.is_in(ethnicity_codelist))
        .where(clinical_events.date.is_on_or_before(index_date))
        .sort_by(clinical_events.date)
        .last_for_patient()
        .snomedct_code.to_category(ethnicity_codelist)
    )

    gp_ethnicity = case(
        when(latest_ethnicity_code == "1").then("White"),
        when(latest_ethnicity_code == "2").then("Mixed"),
        when(latest_ethnicity_code == "3").then("Asian or Asian British"),
        when(latest_ethnicity_code == "4").then("Black or Black British"),
        when(latest_ethnicity_code == "5").then("Chinese or Other Ethnic Groups"),
    )

    sus_ethnicity = case(
        when(ethnicity_from_sus.code.is_in(["A", "B", "C"])).then("White"),
        when(ethnicity_from_sus.code.is_in(["D", "E", "F", "G"])).then("Mixed"),
        when(ethnicity_from_sus.code.is_in(["H", "J", "K", "L"])).then("Asian or Asian British"),
        when(ethnicity_from_sus.code.is_in(["M", "N", "P"])).then("Black or Black British"),
        when(ethnicity_from_sus.code.is_in(["R", "S"])).then("Chinese or Other Ethnic Groups"),
    )

    return case(
        when(gp_ethnicity.is_not_null()).then(gp_ethnicity),
        when(gp_ethnicity.is_null() & sus_ethnicity.is_not_null()).then(sus_ethnicity),
        otherwise="Missing",
    )
