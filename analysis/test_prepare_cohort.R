# Run from the repository root with opensafely run test_preparation.
source("analysis/lib/cohort.R")
assert_error <- function(expr, pattern) {
  result <- tryCatch(force(expr), error = function(e) conditionMessage(e))
  stopifnot(is.character(result), length(result) == 1L, grepl(pattern, result))
}
expected <- c("No prior record", "1-90 days", "1-90 days", "91-180 days", "91-180 days",
              "181-365 days", "181-365 days", ">365 days")
stopifnot(identical(as.character(vaccination_category(c(NA, 1, 90, 91, 180, 181, 365, 366))), expected))
assert_error(vaccination_category(0), "strictly before")
assert_error(parse_iso_date("2023-02-30", "event"), "Invalid ISO date")
assert_error(parse_flag("maybe", "flag"), "Invalid boolean")

fixture <- data.frame(
  patient_id = c("9007199254740993", "9007199254740995"),
  index_date = c("2023-01-01", "2023-01-01"),
  followup_end_date = c("2024-01-01", "2023-01-01"),
  admin_end_date = c("2024-01-01", "2024-01-01"), age = c("50", "90"),
  sex = c("female", "male"), bmi = c(NA, "25"), imd_quintile = c("0", "5"),
  ethnicity6 = c(NA, "White"), ethnicity_sus = c("F", "T"), smoking_status = c(NA, "E"),
  mi_date = c(NA, NA), stroke_date = c(NA, NA), cvddeath_date = c(NA, NA),
  mace_date = c(NA, NA), all_cause_death_date = c(NA, "2023-01-01"),
  covax_most_recent_before_index = c(NA, "2022-12-31"),
  fluvax_most_recent_before_index = c(NA, NA), prior_cvd = c("F", "T"),
  stringsAsFactors = FALSE
)
path <- tempfile(fileext = ".csv")
write.csv(fixture, path, row.names = FALSE, na = "")
prepared <- prepare_cohort(path)
stopifnot(identical(prepared$patient_id, fixture$patient_id),
          identical(prepared$ethnicity_sus, c("F", "T")),
          identical(prepared$prior_cvd, c(FALSE, TRUE)),
          is.na(prepared$bmi[1]), is.na(prepared$imd[1]),
          is.na(prepared$anyvax_most_recent_date[1]),
          identical(as.character(prepared$age_group), c("50-59", "90+")),
          identical(prepared$followup_days, c(365, 0)), nrow(prepared) == 2)
fixture$patient_id[2] <- fixture$patient_id[1]
write.csv(fixture, path, row.names = FALSE, na = "")
assert_error(prepare_cohort(path), "Invalid patient identifiers")
unlink(path)
cat("R preparation tests passed: exposure boundaries, invalid inputs, identifiers, clinical codes, missingness and zero follow-up.\n")
dir.create("output/logs", recursive = TRUE, showWarnings = FALSE)
writeLines("PASS: R preparation edge cases and preservation checks", "output/logs/test_prepare_cohort.txt")
