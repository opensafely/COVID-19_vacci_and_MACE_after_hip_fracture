# COVID-19 vaccination and MACE after hip fracture

[OpenSAFELY project 209](https://www.opensafely.org/project/209/)

Research question: how is the timing of COVID-19 vaccination around hip fracture
associated with subsequent major adverse cardiac events? Influenza vaccination
provides a secondary comparison.

## Current status: feasibility foundation

The repository retains the original ehrQL and Stata approach, with explicit
extraction, quality checks and preparation stages. Clinical definitions and the
statistical analysis plan are still being developed. No effectiveness model or
disclosure-ready result is supplied by this branch.

| Stage | Entry point | Output |
|---|---|---|
| Configuration | `analysis/study_config.py` | Provisional dates and vaccination target values |
| Extraction | `analysis/dataset_definition.py` | `output/dataset.csv` |
| Internal quality checks | `analysis/audit_dataset.py` | `output/quality/` |
| Stata preparation | `analysis/derive_covariates.do` | `output/analytical_cohort.dta` |
| Timing histograms | `analysis/histograms.do` | Three pre-fracture timing SVGs |
| Codelist audit | `analysis/audit_codelists.py` | `codelists/audit/inventory.csv` |

The full declared workflow is in `project.yaml`. Histograms, counts and validation
outputs are **highly sensitive** until an explicit disclosure process is added.

## Run and check

With Docker and the relevant OpenSAFELY images available:

```sh
opensafely run audit_dataset
opensafely run generate_histograms
```

The first command runs extraction and its quality gate. The second additionally
requires the Stata image. `--dummy-tables` supplies local synthetic data; the
secure backend supplies real tables without editing the workflow. These fixture
tables contain artificial records, not patient data or estimates of feasibility.

Standalone checks with an installed ehrQL environment:

```sh
ehrql assure analysis/test_dataset_definition.py
python -m pytest analysis/test_audit_dataset.py
python analysis/audit_codelists.py
```

The dataset assurance file is also run before each `generate_dataset` action.
It covers eligibility, vaccine-window boundaries, doses beyond the six-date
display, duplicate administration dates, competing deaths, deregistration and
same-spell outcome ambiguity.

## Decisions before effectiveness analysis

- Verify event coverage for every source. Configured dates are provisional;
  an import date does not establish complete follow-up. In particular, documented
  ONS availability begins in February 2019.
- Freeze hip fracture, incident MI/stroke, cardiovascular death, trauma, surgery,
  medication and smoking definitions. See [codelist review](codelists/README.md).
- Distinguish pre-fracture vaccination from post-fracture initiation. Never assign
  a future vaccination retrospectively at fracture admission.
- Restrict comparisons to contemporaneous, vaccine-eligible populations and
  evaluate overlap before fitting models.
- Review same-spell MACE and transferred admissions: admission date alone does
  not establish onset within a hospital spell.
- Review continuous registration and GP/ONS death discrepancies. The current
  preparation retains missing covariates and zero-follow-up records for audit.
- `all_cause_death_date` is the earliest observed GP/ONS death within follow-up;
  source dates are retained separately. Cause attribution uses ONS.
- The first six distinct vaccination dates are a historical display, not a full
  dose history. Index-relative summaries do not use that cap.

Internal study discussions and correspondence are kept outside the public code
repository. `/doc/`, `/docs/` and `/private/` are ignored, following the existing
IDAIRE BSI workspace convention.

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
