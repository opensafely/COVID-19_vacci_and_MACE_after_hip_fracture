"""Minimal patient-level screening data; excluded patients have no clinical detail."""
from ehrql import create_dataset
from cohort_definition import candidates, first_candidate, first_hf, criteria, eligible_population

dataset = create_dataset()
dataset.define_population(candidates.exists_for_patient())
dataset.candidate_date = first_candidate.admission_date
dataset.index_date = first_hf.admission_date
for name, criterion in criteria.items():
    dataset.add_column(name, criterion.when_null_then(False))
dataset.included = eligible_population
