source("analysis/lib/feasibility.R")
dir.create("output/logs", recursive = TRUE, showWarnings = FALSE)
sink("output/logs/test_feasibility.txt", split = TRUE)
expect_error <- function(expr) {
  result <- tryCatch({force(expr); FALSE}, error = function(e) TRUE)
  stopifnot(result)
}
d <- add_feasibility_variables(readRDS("output/analytical_cohort.rds"))
s <- read_screening("output/screening.csv")
f <- screening_flow(s, d)
stopifnot(tail(f$n_remaining, 1) == nrow(d), all(f$n_before - f$n_excluded == f$n_remaining))
if (nrow(d)) {
  broken <- d; broken$patient_id[1] <- "not-in-screening"
  expect_error(screening_flow(s, broken))
}
dates <- read_study_dates()
x <- source_activity_summary("output/source_activity.csv", dates, d)
stopifnot(nrow(x) == length(seq(dates$history_start, dates$data_end, by = "month")) * 21L)
stopifnot(all(x$numerator >= 0, na.rm = TRUE), all(x$denominator >= 0, na.rm = TRUE))
# Missing months with a nonzero denominator remain unknown; known empty months are zero.
raw_activity <- read.csv("output/source_activity.csv", stringsAsFactors = FALSE)
check_file <- tempfile(fileext = ".csv")
if (nrow(raw_activity)) {
  write.csv(raw_activity[-1, ], check_file, row.names = FALSE)
  missing_row <- source_activity_summary(check_file, dates, d)
  stopifnot(any(missing_row$row_status == "missing_with_nonzero_denominator"),
            all(is.na(missing_row$numerator[missing_row$row_status == "missing_with_nonzero_denominator"])))
  changed <- raw_activity; changed$denominator[1] <- changed$denominator[1] + 1
  write.csv(changed, check_file, row.names = FALSE)
  expect_error(source_activity_summary(check_file, dates, d))
}
write.csv(raw_activity[FALSE, ], check_file, row.names = FALSE)
empty_activity <- source_activity_summary(check_file, dates, d[FALSE, ])
stopifnot(all(empty_activity$row_status == "no_observable_cohort"), all(empty_activity$numerator == 0))
unlink(check_file)
# Patient counts cannot exceed the per-interval cohort denominator; record counts can.
patients <- x[x$measure_unit == "patients" & x$extraction_row_present, ]
stopifnot(all(patients$numerator <= patients$denominator))
for (v in c("covax", "fluvax")) {
  o <- vaccination_overlap(d, dates)
  stopifnot(sum(o$n[o$vaccine == v]) == nrow(d))
}
p <- post_vaccination_feasibility(d)
stopifnot(all(p$vaccinated_by_day + p$no_post_record_observed_through_day +
               p$no_post_record_observation_ended_earlier == p$denominator))
stopifnot(sum(followup_summary(d)$n) == nrow(d))
t <- outcome_timing(d)
stopifnot(all(tapply(t$n, t$outcome, sum) == nrow(d)))
# Explicit toy timeline checks competing observation, vaccine timing and MACE order.
if (nrow(d)) {
  toy <- d[rep(1, 4), ]; toy$index_date <- as.Date("2023-01-01") + rep(0, 4)
  toy$followup_days <- c(30, 5, 30, 30)
  toy$all_cause_death_date <- as.Date(c(NA, "2023-01-06", NA, NA))
  toy$mace_date <- as.Date(c(NA, NA, "2023-01-05", "2023-01-11"))
  for (v in c("covax", "fluvax")) {
    toy[[paste0(v, "_first_post_date")]] <- as.Date(c(NA, NA, "2023-01-11", "2023-01-11"))
    toy[[paste0(v, "_index_day")]] <- rep(FALSE, 4)
  }
  a <- subset(post_vaccination_feasibility(toy), vaccine == "covax" & day == 30)
  stopifnot(a$vaccinated_by_day == 2, a$no_post_record_observed_through_day == 1,
            a$no_post_record_observation_ended_earlier == 1, a$provisional_mace_before_post_vaccination == 1,
            a$provisional_mace_same_day_as_post_vaccination == 1,
            a$alive_registered_event_free_without_post_record == 1)
}
# Empty cohorts must produce interpretable zero-denominator tables, not crash.
e <- d[FALSE, ]
stopifnot(all(flag_summary(e)$denominator == 0), all(is.na(flag_summary(e)$percent)),
          all(post_vaccination_feasibility(e)$denominator == 0), sum(gp_code_summary(e)$n) == 0,
          all(missingness_by_year(e)$denominator == 0), sum(outcome_timing(e)$n) == 0)
empty_file <- tempfile(fileext = ".rds")
empty_output <- tempfile()
saveRDS(e, empty_file)
for (script in c("analysis/describe_cohort.R", "analysis/plot_vaccination_timing.R")) {
  result <- system2("Rscript", c(script, "--input", empty_file, "--output", empty_output))
  stopifnot(result == 0)
}
unlink(c(empty_file, empty_output), recursive = TRUE)
cat("PASS: cohort reconciliation, interval counts, denominators, timeline boundaries and empty cohort handling\n")
sink()
