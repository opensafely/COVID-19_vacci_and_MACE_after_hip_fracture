# Codelist status

This is a feasibility repository. A downloaded or referenced codelist has **not**
necessarily been accepted for the final phenotype or analysis.

## Provenance and inventory

- `codelists.json` retains the original OpenCodelists version URLs and download metadata.
- `inventory.csv` records all 43 downloaded files, code counts, code columns,
  references in the analysis, and SHA-256 hashes of the local files.
- Rebuild it from the repository root with `python analysis/audit_codelists.py`.
  The inventory is a structural audit, not a clinical validation.
- Inline definitions (OPCS procedures, transport/elective exclusions and smoking
  categories) are outside that manifest and require separate clinical review.

## Definitions requiring resolution

| Definition | Current extraction | Required review |
|---|---|---|
| Hip fracture | S720/S721/S722/S729 or selected procedures | S729 is unspecified femur; decide diagnosis positions, procedure-only evidence, anatomical specificity and high-energy trauma exclusions. |
| MI | ICD-10 I21/I22 plus GP domain refset | GP list includes old MI and history codes; recording date cannot establish incident onset. ICD labels do not establish non-fatal status. |
| Stroke | I60/I61/I63/I64 plus GP domain refset | Resolve ischaemic versus all stroke; review I636 venous infarction and GP history codes. |
| Cardiovascular death | Any MI/stroke mention on ONS certificate | This is narrower than all cardiovascular death. Resolve code scope and underlying-cause sensitivity before modelling. |
| Prior CVD | Current GP histories | Add/reconcile permitted hospital history; do not reuse acute-outcome and history lists indiscriminately. |
| Prior fracture | Hip-only history placeholder | Replace with a reviewed wider fracture history definition; current cohort selection largely makes this flag redundant. |
| Smoking | SNOMED list plus legacy inline categorisation | Some non-smoker/home-status concepts do not establish lifetime never smoking. Review the classified approach in reusable-variables/IDAIRE BSI. |
| Surgery | Five legacy OPCS codes | Incomplete procedure groups, hierarchy and procedure-only cohort evidence need clinical review. |
| Medications | Placeholders only | Add versioned dm+d lists and baseline windows for osteoporosis and cardiovascular treatments. |
| Negative controls | Legacy cataract procedures only | Validate procedure meaning, ascertainment and the causal rationale; upper-limb fracture is not implemented. |

## Inactive candidates

`candidates/` contains three **drafts not imported by the dataset definition**:

1. Strict hip diagnoses: S720–S722, omitting S729.
2. Arterial ischaemic stroke: I630–I635, I638, I639; omits venous I636.
3. Ischaemic-or-unspecified stroke: the same list plus I64.

Each is a subset of an existing versioned list; see `candidates/provenance.json`.
They are not published OpenCodelists versions or an approved primary definition.
The existing all-stroke list remains the provisional extraction definition.
GP acute ischaemic stroke still needs a separate reviewed SNOMED definition.

**Prefix matching matters:** APCS `all_diagnoses.contains_any_of` matches code
prefixes. Adding parent `I63` to candidate 2 would re-include `I636`. Decide how
standalone I63/I63X records should be handled separately. ONS matching uses its
own cause-of-death API; test every source's matching behaviour before adoption.

References: [ehrQL TPP schema](https://docs.opensafely.org/ehrql/reference/schemas/tpp/),
[current MI refset](https://www.opencodelists.org/codelist/nhsd-primary-care-domain-refsets/mi_cod/20250912/),
[current stroke refset](https://www.opencodelists.org/codelist/nhsd-primary-care-domain-refsets/strk_cod/20250912/).
