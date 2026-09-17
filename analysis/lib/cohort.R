# Shared preparation for project 209. Clinical definitions remain provisional.
# Base R keeps preparation portable across the OpenSAFELY R environment.

exposure_levels <- c("No prior record", ">365 days", "181-365 days",
                     "91-180 days", "1-90 days")

parse_iso_date <- function(x, name) {
  result <- as.Date(x, format = "%Y-%m-%d")
  invalid <- !is.na(x) & (is.na(result) | format(result, "%Y-%m-%d") != x)
  if (any(invalid)) stop("Invalid ISO date in ", name)
  result
}

parse_number <- function(x, name) {
  result <- suppressWarnings(as.numeric(x))
  if (any(!is.na(x) & (is.na(result) | !is.finite(result)))) {
    stop("Invalid numeric value in ", name)
  }
  result
}

parse_flag <- function(x, name) {
  if (any(!is.na(x) & !x %in% c("T", "F", "1", "0"))) {
    stop("Invalid boolean value in ", name)
  }
  ifelse(is.na(x), NA, x %in% c("T", "1"))
}

vaccination_category <- function(days_before) {
  if (any(!is.na(days_before) & days_before < 1)) {
    stop("Pre-index vaccination must be strictly before index")
  }
  result <- rep("No prior record", length(days_before))
  result[which(days_before > 365)] <- ">365 days"
  result[which(days_before >= 181 & days_before <= 365)] <- "181-365 days"
  result[which(days_before >= 91 & days_before <= 180)] <- "91-180 days"
  result[which(days_before >= 1 & days_before <= 90)] <- "1-90 days"
  factor(result, levels = exposure_levels)
}

prepare_cohort <- function(path) {
  d <- read.csv(path, colClasses = "character", na.strings = "",
                check.names = FALSE, stringsAsFactors = FALSE)
  required <- c("patient_id", "index_date", "followup_end_date", "admin_end_date",
                "age", "sex", "bmi", "imd_quintile", "ethnicity6", "smoking_status",
                "mi_date", "stroke_date", "cvddeath_date", "mace_date",
                "all_cause_death_date", "covax_most_recent_before_index",
                "fluvax_most_recent_before_index", "covax_post30_date", "fluvax_post30_date")
  missing <- setdiff(required, names(d))
  if (length(missing)) stop("Missing columns: ", paste(missing, collapse = ", "))
  if (anyDuplicated(names(d))) stop("Duplicate column names")
  if (anyNA(d$patient_id) || anyDuplicated(d$patient_id)) stop("Invalid patient identifiers")

  date_columns <- unique(c(grep("_date$", names(d), value = TRUE),
                           intersect(c("gp_registration_start", "covax_most_recent_before_index",
                                       "fluvax_most_recent_before_index", "covid_positive_before_index"), names(d))))
  for (name in date_columns) d[[name]] <- parse_iso_date(d[[name]], name)
  numeric_columns <- intersect(c("age", "bmi", "bmi_2y", "bmi_5y", "bmi_legacy_2y", "egfr_value", "imd_quintile", "covax_prior365_n",
                                 "fluvax_prior365_n", "covax_post30_n", "fluvax_post30_n"), names(d))
  numeric_columns <- union(numeric_columns, grep("_n$", names(d), value = TRUE))
  for (name in numeric_columns) d[[name]] <- parse_number(d[[name]], name)
  flag_columns <- grep(paste0("^prior_|365$|_within_365d$|^index_spell_|^index_hip_|",
                             "^index_primary_hip$|^reg_2y_at_index$|_index_day$|",
                             "_during_spell$|^neg_con_1$|^death_dates_disagree$"), names(d), value = TRUE)
  flag_columns <- union(flag_columns, intersect(c("index_diagnoses_missing", "index_admission_method_missing", "index_fall_W11_W17"), names(d)))
  flag_columns <- union(flag_columns, grep("^history_|^care_home_|^ckd_current_|^egfr_(recorded|low_)|^smoking_(ever_recorded|same_day_conflict)$|^bmi_latest_out_of_range$|^surgery_multiple_groups$|^registration_(contiguous_next|overlap_continues)$|^index_primary_hip_exact$", names(d), value = TRUE))
  flag_columns <- setdiff(flag_columns, numeric_columns)
  for (name in flag_columns) d[[name]] <- parse_flag(d[[name]], name)

  if (anyNA(d$index_date) || anyNA(d$followup_end_date) || anyNA(d$admin_end_date)) {
    stop("Missing observation dates")
  }
  if (any(d$followup_end_date < d$index_date | d$followup_end_date > d$admin_end_date)) {
    stop("Invalid follow-up interval")
  }
  if (anyNA(d$age) || any(d$age < 50)) stop("Invalid age for this cohort")
  if (anyNA(d$sex) || any(!d$sex %in% c("female", "male"))) stop("Unexpected sex in extracted cohort")
  for (outcome in c("mi", "stroke", "cvddeath", "mace")) {
    event <- d[[paste0(outcome, "_date")]]
    if (any(!is.na(event) & (event <= d$index_date | event > d$followup_end_date))) {
      stop("Outcome outside follow-up: ", outcome)
    }
  }
  first_component <- pmin(as.numeric(d$mi_date), as.numeric(d$stroke_date),
                         as.numeric(d$cvddeath_date), na.rm = TRUE)
  first_component[!is.finite(first_component)] <- NA_real_
  if (!identical(unname(as.numeric(d$mace_date)), unname(first_component))) {
    stop("MACE must equal the earliest component date")
  }
  for (vaccine in c("covax", "fluvax")) {
    post <- d[[paste0(vaccine, "_post30_date")]]
    if (any(!is.na(post) & (post <= d$index_date | post > d$index_date + 30 |
                           post > d$followup_end_date))) {
      stop("Post-fracture vaccination outside the observed 30-day window: ", vaccine)
    }
  }

  d$death_date <- d$all_cause_death_date
  d$covax_most_recent_date <- d$covax_most_recent_before_index
  d$fluvax_most_recent_date <- d$fluvax_most_recent_before_index
  most_recent <- pmax(as.numeric(d$covax_most_recent_date),
                     as.numeric(d$fluvax_most_recent_date), na.rm = TRUE)
  most_recent[!is.finite(most_recent)] <- NA_real_
  d$anyvax_most_recent_date <- as.Date(most_recent, origin = "1970-01-01")
  for (vaccine in c("covax", "fluvax", "anyvax")) {
    delta <- as.numeric(d$index_date - d[[paste0(vaccine, "_most_recent_date")]])
    d[[paste0("days_since_last_", vaccine)]] <- delta
    d[[paste0(vaccine, "_cat")]] <- vaccination_category(delta)
  }
  d$age_group <- cut(d$age, c(50, 60, 70, 80, 90, Inf), right = FALSE,
                     labels = c("50-59", "60-69", "70-79", "80-89", "90+"))
  d$sex <- factor(d$sex, levels = c("female", "male"))
  d$imd_quintile[d$imd_quintile == 0 & !is.na(d$imd_quintile)] <- NA_real_
  d$imd <- factor(d$imd_quintile, levels = 1:5,
                  labels = c("1 most deprived", "2", "3", "4", "5 least deprived"))
  d$ethnicity <- d$ethnicity6
  d$ethnicity[is.na(d$ethnicity) | d$ethnicity == "Missing"] <- "Missing"
  d$smoking <- factor(d$smoking_status, levels = c("N", "E", "S"),
                      labels = c("Never smoker recorded", "Former smoker recorded", "Current smoker recorded"))
  d$followup_days <- as.numeric(d$followup_end_date - d$index_date)
  d$positive_followup <- d$followup_days > 0
  d$observed_30_days <- d$followup_days >= 30
  d$index_year <- as.integer(format(d$index_date, "%Y"))
  d$index_month <- format(d$index_date, "%Y-%m")
  attr(d, "phenotype_status") <- "PROVISIONAL_FEASIBILITY_ONLY"
  d
}

read_options <- function(defaults) {
  args <- commandArgs(trailingOnly = TRUE)
  if (length(args) %% 2 != 0) stop("Supply arguments as --name value pairs")
  if (length(args)) for (i in seq(1, length(args), by = 2)) {
    key <- sub("^--", "", args[i])
    if (!startsWith(args[i], "--") || !key %in% names(defaults)) stop("Unknown option: ", args[i])
    defaults[[key]] <- args[i + 1]
  }
  defaults
}

write_internal_csv <- function(x, path) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  x$disclosure_status <- rep("UNSUPPRESSED_INTERNAL_ONLY", nrow(x))
  write.csv(x, path, row.names = FALSE, na = "")
}
