# COVID-19 vaccination and MACE after hip fracture

[OpenSAFELY project 209](https://www.opensafely.org/project/209/)

Research question: how is the timing of COVID-19 vaccination around hip fracture
associated with subsequent major adverse cardiac events? Influenza vaccination
provides a secondary comparison.

## Current status: R feasibility foundation

The active workflow uses ehrQL for extraction, a Python quality gate, and **R for
preparation, tables and figures**, following the staged IDAIRE BSI structure.
Clinical definitions and the statistical analysis plan are still being developed.
No effectiveness model or disclosure-ready result is supplied by this branch.

| Stage | Entry point | Output |
|---|---|---|
| Configuration | `analysis/study_config.py` | Provisional dates and vaccination targets |
| Extraction | `analysis/dataset_definition.py` | `output/dataset.csv` |
| Internal quality checks | `analysis/audit_dataset.py` | `output/quality/` |
| R preparation | `analysis/prepare_cohort.R` | `output/analytical_cohort.rds` |
| Descriptive tables | `analysis/describe_cohort.R` | `output/tables/` |
| Pre/post-fracture timing figures | `analysis/plot_vaccination_timing.R` | `output/figures/` |
| R preparation tests | `analysis/test_prepare_cohort.R` | `output/logs/test_prepare_cohort.txt` |
| Codelist audit | `analysis/audit_codelists.py` | `codelists/audit/inventory.csv` |

Shared R functions are in `analysis/lib/cohort.R`. The active R dependency for
plotting is ggplot2, supplied by `r:v2`; preparation and tables use base R. Original
Stata scripts are preserved in `analysis/legacy/` for reference.

## Run locally with OpenSAFELY

Start Docker Desktop, then run from the repository root:

```sh
opensafely codelists check
opensafely run run_all
```

This is the required local integration test: it runs the same declared images,
entry points, dependencies and outputs as the project workflow. To rerun selected
stages and their prerequisites after a change:

```sh
opensafely run prepare_cohort
opensafely run describe_cohort plot_vaccination_timing
opensafely run test_preparation
opensafely run test_quality_gate
```

`--dummy-tables` supplies local synthetic records. The secure backend supplies
real tables without editing the workflow; the researcher submits actual data
runs through the OpenSAFELY workspace. Synthetic records are not patient data and
do not estimate clinical feasibility.

The ehrQL assurance file runs before each extraction. It covers eligibility,
vaccine-window boundaries, doses beyond the six-date display, duplicate dates,
deaths, deregistration and same-spell outcome ambiguity. The R test action checks
exposure boundaries, date parsing, exact identifiers, clinical code preservation,
missingness and zero follow-up. Four quality-gate rejection cases also run through
the `test_quality_gate` action. Both test actions are included in `run_all`.

Histograms, counts, logs and tables are **highly sensitive**. They contain
unsuppressed values; release processing must be added before creating publishable
outputs. `table1_feasibility.csv` is a long-format audit table, not a finished
manuscript Table 1. No data are dropped merely because covariates are missing.

## Decisions before effectiveness analysis

- Verify event coverage for every source. Configured dates are provisional;
  import date does not establish complete follow-up. Documented ONS availability
  begins in February 2019.
- Freeze hip fracture, incident MI/stroke, cardiovascular death, trauma, surgery,
  medication and smoking definitions. See [codelist review](codelists/README.md).
- Distinguish pre-fracture vaccination from post-fracture initiation. A future
  vaccination must not be assigned retrospectively at fracture admission.
- Restrict comparisons to contemporaneous, vaccine-eligible populations and
  evaluate overlap before fitting models.
- Review same-spell MACE and transfers: admission date does not establish onset
  within a hospital spell.
- Review continuous registration and GP/ONS death discrepancies. Preparation
  retains missing covariates and zero-follow-up records for audit.
- `all_cause_death_date` is the earliest observed GP/ONS death within follow-up;
  source dates are retained separately. Cause attribution uses ONS.
- The first six distinct vaccination dates are a historical display, not a full
  dose history. Index-relative summaries do not use that cap.

Internal study discussions and correspondence are outside the public repository.
`/doc/`, `/docs/` and `/private/` are ignored, following the IDAIRE BSI convention.

## Transparency

[View on OpenSAFELY](https://jobs.opensafely.org/repo/https%253A%252F%252Fgithub.com%252Fopensafely%252FCOVID-19_vacci_and_MACE_after_hip_fracture)

Details of the purpose and any published outputs from this project can be found at the link above.

The contents of this repository MUST NOT be considered an accurate or valid representation of the study or its purpose. 
This repository may reflect an incomplete or incorrect analysis with no further ongoing work.
The content has ONLY been made public to support the OpenSAFELY [open science and transparency principles](https://www.opensafely.org/about/#contributing-to-best-practice-around-open-science) and to support the sharing of re-usable code for other subsequent users.
No clinical, policy or safety conclusions must be drawn from the contents of this repository.

# About the OpenSAFELY framework

The OpenSAFELY framework is a Trusted Research Environment (TRE) for electronic
health records research in the NHS, with a focus on public accountability and
research quality.

Read more at [OpenSAFELY.org](https://opensafely.org).

# Licences
As standard, research projects have a MIT license. 
