source("analysis/lib/feasibility.R")
source("analysis/lib/disclosure.R")
opts <- read_options(list(input = "output/analytical_cohort.rds", screening = "output/screening.csv",
                         output = "output/review", codes = "output/feasibility/gp_record_codes.csv"))
dates <- read_study_dates()
d <- add_feasibility_variables(readRDS(opts$input))
s <- read_screening(opts$screening)

# Raw aggregates stay in memory; only the protected tables are review outputs.
tables <- list(
  cohort_flow = screening_flow(s, d),
  definition_diagnostics = flag_summary(d),
  outcome_sources = outcome_source_summary(d),
  outcome_timing = outcome_timing(d),
  followup = followup_summary(d),
  vaccination_overlap = vaccination_overlap(d, dates),
  post_vaccination = post_vaccination_feasibility(d),
  missingness_by_year = missingness_by_year(d)
)
months <- format(seq(dates$study_start, dates$study_end, by = "month"), "%Y-%m")
tables$index_monthly <- as.data.frame(table(month = factor(d$index_month, levels = months)))
names(tables$index_monthly)[2] <- "n"
tables$readiness <- data.frame(
  check = c("cohort_not_empty", "screening_reconciled", "monthly_source_activity",
            "clinical_definitions", "medication_classes", "frailty_care_home", "broader_prior_fracture",
            "high_energy_trauma", "source_complete_dates", "effectiveness_models"),
  status = c(if (nrow(d)) "PASS" else "REVIEW_EMPTY_COHORT", "PASS", "NOT_RUN",
             "REVIEW_REQUIRED", "NOT_IMPLEMENTED", "NOT_IMPLEMENTED", "NOT_IMPLEMENTED",
             "REVIEW_REQUIRED", "REVIEW_REQUIRED", "NOT_RUN")
)
write_internal_csv(gp_code_summary(d), opts$codes)
write_review(review_tables(tables), opts$output)
message("Saved disclosure-controlled feasibility tables and report.")
