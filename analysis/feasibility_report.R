source("analysis/lib/feasibility.R")
opts <- read_options(list(input = "output/analytical_cohort.rds", screening = "output/screening.csv",
                         activity = "output/source_activity.csv", output = "output/feasibility"))
dates <- read_study_dates()
d <- add_feasibility_variables(readRDS(opts$input))
s <- read_screening(opts$screening)
write_table <- function(x, name) write_internal_csv(x, file.path(opts$output, paste0(name, ".csv")))
write_table(screening_flow(s, d), "cohort_flow")
write_table(flag_summary(d), "definition_diagnostics")
write_table(outcome_source_summary(d), "outcome_sources")
write_table(outcome_timing(d), "outcome_timing")
write_table(followup_summary(d), "followup")
write_table(vaccination_overlap(d, dates), "vaccination_overlap")
write_table(post_vaccination_feasibility(d), "post_vaccination")
write_table(missingness_by_year(d), "missingness_by_year")
write_table(gp_code_summary(d), "gp_record_codes")
activity <- source_activity_summary(opts$activity, dates, d)
write_table(activity, "source_activity_monthly")
months <- format(seq(dates$study_start, dates$study_end, by = "month"), "%Y-%m")
index <- as.data.frame(table(month = factor(d$index_month, levels = months)))
names(index)[2] <- "n"; write_table(index, "index_monthly")
status <- data.frame(check = c("cohort_not_empty", "screening_reconciled", "monthly_activity_rows_present",
  "clinical_definitions", "medication_classes", "frailty_care_home", "broader_prior_fracture", "high_energy_trauma", "source_complete_dates", "effectiveness_models"),
  status = c(if (nrow(d)) "PASS" else "REVIEW_EMPTY_COHORT", "PASS",
  if (all(activity$row_status != "missing_with_nonzero_denominator")) "PASS" else "REVIEW_MISSING_INTERVALS",
  "REVIEW_REQUIRED", "NOT_IMPLEMENTED", "NOT_IMPLEMENTED", "NOT_IMPLEMENTED", "REVIEW_REQUIRED", "REVIEW_REQUIRED", "NOT_RUN"))
write_table(status, "readiness")
suppressPackageStartupMessages(library(ggplot2))
plot_data <- activity[activity$measure_unit == "patients", ]
plot_data$month <- as.Date(plot_data$interval_start)
plot_data$value <- plot_data$numerator
p <- ggplot(plot_data, aes(month, value)) + geom_line(colour = "#286D8C", na.rm = TRUE) +
  facet_wrap(~source, ncol = 3, scales = "free_y") +
  scale_y_continuous(limits = c(0, NA), breaks = function(lim) unique(pmax(0, round(pretty(lim, n = 3))))) + theme_minimal(base_size = 11) +
  labs(title = "Recorded source activity in the selected cohort", x = NULL, y = "Patients with a record",
       caption = "Internal unsuppressed feasibility output. No inference of complete source coverage.\nCounts depend on cohort selection and individual follow-up; the final interval may be partial.") +
  theme(panel.grid.minor = element_blank(), axis.text.x = element_text(angle = 45, hjust = 1),
        plot.caption = element_text(hjust = 0))
ggsave(file.path(opts$output, "source_activity.png"), p, width = 12, height = 11, dpi = 160, bg = "white")
message("Feasibility outputs written. Review readiness.csv before choosing final definitions or dates.")
