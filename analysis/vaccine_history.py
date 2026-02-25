######################################
# vaccine_history.py
# Extract vaccination history (dates and product names)
#
# Vaccination target diseases reference:
# https://reports.opensafely.org/reports/opensafely-tpp-database-reference-values/
# Vaccination product names reference:
# https://reports.opensafely.org/reports/opensafely-tpp-vaccination-names/
######################################

from ehrql.tables.tpp import vaccinations


def add_vaccine_history(dataset, index_date, target_disease, prefix, number_of_vaccines=6):
    """
    Extract vaccination history for a given target disease.
    
    Adds columns to the dataset:
      - {prefix}_{i}_date: date of ith vaccination
      - {prefix}_{i}_product: product name of ith vaccination
    
    Args:
        dataset: the ehrQL dataset
        index_date: a per-patient date series (e.g. first_hf.admission_date)
                    All vaccinations are extracted (no date restriction here;
                    filtering relative to index is done in the dataset definition)
        target_disease: string matching vaccinations.target_disease
                        e.g. "SARS-2 Coronavirus", "INFLUENZA"
        prefix: string prefix for column names, e.g. "covax", "fluvax"
        number_of_vaccines: max number of vaccine doses to extract (default 6)
    """
    type_vaccinations = (
        vaccinations
        .where(vaccinations.target_disease == target_disease)
        .sort_by(vaccinations.date)
    )

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
