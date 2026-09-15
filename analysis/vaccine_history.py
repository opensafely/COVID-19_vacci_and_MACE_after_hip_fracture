######################################
# vaccine_history.py
# Extract vaccination history (dates and product names)
#
# Vaccination target diseases reference:
# https://reports.opensafely.org/reports/opensafely-tpp-database-reference-values/
# Vaccination product names reference:
# https://reports.opensafely.org/reports/opensafely-tpp-vaccination-names/
######################################

from ehrql import days, months, minimum_of
from ehrql.tables.tpp import vaccinations


def add_vaccine_history(dataset, index_date, target_disease, prefix, number_of_vaccines=6, start_date=None, end_date=None):
    """
    Extract vaccination history for a given target disease.
    
    Adds columns to the dataset:
      - {prefix}_{i}_date: date of ith vaccination
      - {prefix}_{i}_product: product name of ith vaccination
    
    Args:
        dataset: the ehrQL dataset
        index_date: retained for compatibility; use add_index_vaccine_summary
                    for dates relative to the index rather than this display.
        target_disease: string matching vaccinations.target_disease
                        e.g. "SARS-2 CORONAVIRUS", "INFLUENZA"
        prefix: string prefix for column names, e.g. "covax", "fluvax"
        number_of_vaccines: max number of vaccine doses to extract (default 6)
        start_date, end_date: inclusive permitted history bounds.
    """
    type_vaccinations = (
        vaccinations
        .where(vaccinations.target_disease == target_disease)
        .sort_by(vaccinations.date)
    )

    if start_date is not None:
        type_vaccinations = type_vaccinations.where(vaccinations.date >= start_date)
    if end_date is not None:
        type_vaccinations = type_vaccinations.where(vaccinations.date <= end_date)

    # Historical display only; index summaries below do not use this cap.
    # Arbitrary date guaranteed to be before any vaccination events
    previous_vax_date = "1899-01-01"

    for i in range(1, number_of_vaccines + 1):
        current_vax = (
            type_vaccinations
            .where(type_vaccinations.date > previous_vax_date)
            .first_for_patient()
        )
        dataset.add_column(f"{prefix}_{i}_date", current_vax.date)
        dataset.add_column(f"{prefix}_{i}_product", current_vax.product_name)

        previous_vax_date = current_vax.date


def add_index_vaccine_summary(dataset, index_date, discharge_date, followup_end,
                              target_disease, prefix):
    """First observed post-index dates and distinct administration-day counts.

    month1 uses one calendar month; post30 uses 30 days, permitting a sensitivity
    comparison. Both exclude day 0. No inferred administration location is used.
    """
    records = vaccinations.where(vaccinations.target_disease == target_disease)
    post = records.where(vaccinations.date > index_date).where(vaccinations.date <= followup_end)
    first = post.sort_by(vaccinations.date).first_for_patient()
    post30 = post.where(vaccinations.date <= index_date + days(30))
    month1 = post.where(vaccinations.date <= index_date + months(1))
    during = records.where(vaccinations.date >= index_date).where(
        vaccinations.date <= minimum_of(discharge_date, followup_end)
    ).where(discharge_date.is_not_null())
    prior365 = records.where(vaccinations.date >= index_date - days(365)).where(vaccinations.date < index_date)
    dataset.add_column(f"{prefix}_first_post_date", first.date)
    dataset.add_column(f"{prefix}_post30_date", post30.sort_by(vaccinations.date).first_for_patient().date)
    dataset.add_column(f"{prefix}_month1_date", month1.sort_by(vaccinations.date).first_for_patient().date)
    dataset.add_column(f"{prefix}_index_day", records.where(vaccinations.date == index_date).exists_for_patient())
    dataset.add_column(f"{prefix}_during_spell", during.exists_for_patient())
    dataset.add_column(f"{prefix}_prior365_n", prior365.date.count_distinct_for_patient())
    dataset.add_column(f"{prefix}_post30_n", post30.date.count_distinct_for_patient())
