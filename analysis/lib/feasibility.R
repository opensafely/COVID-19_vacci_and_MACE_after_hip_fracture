source("analysis/lib/cohort.R")

read_study_dates <- function() {
  x <- read.csv("analysis/config/study_dates.csv", stringsAsFactors = FALSE)
  setNames(as.list(as.Date(x$value)), x$parameter)
}

safe_percent <- function(n, denominator) ifelse(denominator > 0, 100 * n / denominator, NA_real_)
missing_value <- function(x) is.na(x) | as.character(x) %in% c("", "Missing")
flag_true <- function(x) x %in% TRUE

read_screening <- function(path) {
  x <- read.csv(path, colClasses = "character", na.strings = "", check.names = FALSE)
  if (anyNA(x$patient_id) || anyDuplicated(x$patient_id)) stop("Invalid screening identifiers")
  for (name in c("candidate_date", "index_date")) x[[name]] <- parse_iso_date(x[[name]], name)
  for (name in setdiff(names(x), c("patient_id", "candidate_date", "index_date"))) x[[name]] <- parse_flag(x[[name]], name)
  x
}

screening_flow <- function(s, d) {
  criteria <- c("has_non_transport_admission", "has_non_elective_admission", "no_prior_recorded_hip_fracture",
                "age_50_or_older", "registered_at_index", "alive_at_index", "known_sex")
  keep <- rep(TRUE, nrow(s)); rows <- list()
  add <- function(step, criterion, before, after) data.frame(step = step, criterion = criterion,
      n_before = before, n_excluded = before - after, n_remaining = after)
  rows[[1]] <- add(0, "Any diagnosis/procedure candidate in study window", nrow(s), nrow(s))
  for (i in seq_along(criteria)) {
    before <- sum(keep); keep <- keep & flag_true(s[[criteria[i]]])
    rows[[i + 1L]] <- add(i, criteria[i], before, sum(keep))
  }
  if (!identical(keep, flag_true(s$included))) stop("Screening criteria do not reconcile")
  selected <- s[keep, ]
  if (!setequal(selected$patient_id, d$patient_id)) stop("Screening and analytic patient sets differ")
  if (!identical(as.numeric(selected$index_date[match(d$patient_id, selected$patient_id)]),
                 as.numeric(d$index_date))) stop("Screening and analytic index dates differ")
  do.call(rbind, rows)
}

add_feasibility_variables <- function(d) {
  # No record is not equivalent to no vaccination; categories retain that wording.
  d$index_procedure_only <- flag_true(d$index_hip_procedure) & !flag_true(d$index_hip_diagnosis)
  d$index_quarter <- paste0(format(d$index_date, "%Y"), "-Q", (as.integer(format(d$index_date, "%m")) - 1L) %/% 3L + 1L)
  d$discharge_missing <- is.na(d$hf_discharge_date)
  d$discharge_before_index <- !is.na(d$hf_discharge_date) & d$hf_discharge_date < d$index_date
  d$overlapping_registrations <- d$registration_records_at_index_n > 1
  d$registration_start_unknown <- !is.na(d$gp_registration_start) & d$gp_registration_start < as.Date("1901-01-01")
  d$index_before_practice_go_live <- !is.na(d$practice_go_live_date) & d$index_date < d$practice_go_live_date
  d$ons_january_2019 <- d$index_date < as.Date("2019-02-01")
  d$index_multiple_spells <- d$index_same_day_spells_n > 1
  d$surgery_other_or_none <- d$surgery_type == "other_or_none"
  d$reg_1y_at_index <- !is.na(d$gp_registration_start) & as.numeric(d$index_date - d$gp_registration_start) >= 365
  d$zero_followup <- d$followup_days == 0
  for (event in c("mi", "stroke")) {
    date <- d[[paste0(event, "_hospital_date")]]
    d[[paste0(event, "_hospital_overlaps_index_spell")]] <- !is.na(date) & !is.na(d$hf_discharge_date) & date <= d$hf_discharge_date
    d[[paste0(event, "_hospital_within_one_day_discharge")]] <- !is.na(date) & !is.na(d$hf_discharge_date) &
      date > d$hf_discharge_date & date <= d$hf_discharge_date + 1
  }
  causes <- grep("^ons_(underlying_cause_of_death|cause_of_death_[0-9]+)$", names(d), value = TRUE)
  d$ons_observed_death <- !is.na(d$ons_death_date) & d$ons_death_date >= d$index_date & d$ons_death_date <= d$followup_end_date
  d$ons_cause_missing <- d$ons_observed_death & Reduce(`&`, lapply(d[causes], missing_value))
  d$ons_chapter_i_any <- Reduce(`|`, lapply(d[causes], function(x) !is.na(x) & grepl("^I[0-9]{2}", x)))
  d$ons_chapter_i_underlying <- !is.na(d$ons_underlying_cause_of_death) & grepl("^I[0-9]{2}", d$ons_underlying_cause_of_death)
  d$ons_chapter_i_outside_legacy <- d$ons_chapter_i_any & is.na(d$cvddeath_date) & d$ons_death_date > d$index_date
  d$ons_death_on_index_date <- d$ons_observed_death & d$ons_death_date == d$index_date
  d$no_baseline_clinical_records <- d$prior_year_clinical_records_n == 0
  d$no_baseline_medication_records <- d$prior_year_medication_records_n == 0
  d
}

flag_summary <- function(d) {
  flags <- c("index_hip_strict", "index_hip_unspecified", "index_primary_hip", "index_primary_hip_exact", "index_procedure_only",
             "diagnosed_lower_trauma_candidate", "registration_contiguous_next", "surgery_multiple_groups",
             "index_diagnoses_missing", "index_admission_method_missing", "index_fall_W11_W17",
             "index_spell_mi", "index_spell_stroke", "mi_hospital_overlaps_index_spell",
             "stroke_hospital_overlaps_index_spell", "mi_hospital_within_one_day_discharge",
             "stroke_hospital_within_one_day_discharge", "discharge_missing", "discharge_before_index",
             "overlapping_registrations", "registration_start_unknown", "index_before_practice_go_live",
             "reg_1y_at_index", "reg_2y_at_index", "index_multiple_spells", "surgery_other_or_none", "death_dates_disagree", "ons_observed_death", "ons_cause_missing",
             "ons_death_on_index_date", "ons_chapter_i_any", "ons_chapter_i_underlying", "ons_chapter_i_outside_legacy",
             "ons_january_2019", "zero_followup", "no_baseline_clinical_records", "no_baseline_medication_records")
  strata <- c("All", as.character(sort(unique(d$index_year))))
  do.call(rbind, lapply(strata, function(year) {
    x <- if (year == "All") d else d[d$index_year == as.integer(year), ]
    do.call(rbind, lapply(flags, function(name) data.frame(index_year = year, diagnostic = name,
      n = sum(flag_true(x[[name]])), denominator = nrow(x), n_unknown = sum(is.na(x[[name]])),
      percent = safe_percent(sum(flag_true(x[[name]])), nrow(x)))))
  }))
}

outcome_source_summary <- function(d) {
  rows <- list()
  for (event in c("mi", "stroke")) {
    gp <- !is.na(d[[paste0(event, "_gp_date")]])
    hospital <- !is.na(d[[paste0(event, "_hospital_date")]])
    tab <- as.data.frame(table(gp_record = factor(gp, levels = c(FALSE, TRUE)),
                                hospital_record = factor(hospital, levels = c(FALSE, TRUE))))
    names(tab)[3] <- "n"; tab$outcome <- event; tab$denominator <- nrow(d)
    rows[[event]] <- tab
  }
  do.call(rbind, rows)
}

outcome_timing <- function(d) {
  fields <- c("mi_date", "mi_hospital_date", "mi_gp_date", "stroke_date", "stroke_hospital_date", "stroke_gp_date",
              "stroke_arterial_ischaemic_hospital_date", "stroke_haemorrhagic_hospital_date",
              "stroke_unspecified_hospital_date", "stroke_venous_infarction_hospital_date",
              "mi_after_discharge_date", "stroke_after_discharge_date", "cvddeath_date", "mace_date",
              "mace_hospital_date", "mace_hospital_ischaemic_date", "all_cause_death_date")
  do.call(rbind, lapply(fields, function(name) {
    delta <- as.numeric(d[[name]] - d$index_date)
    band <- cut(delta, c(-1, 0, 7, 30, 90, 365), labels = c("Day 0", "Days 1-7", "Days 8-30", "Days 31-90", "Days 91-365"))
    values <- ifelse(is.na(delta), "No observed record", as.character(band))
    tab <- as.data.frame(table(window = factor(values, levels = c(levels(band), "No observed record"))))
    names(tab)[2] <- "n"; tab$outcome <- name; tab$denominator <- nrow(d)
    tab
  }))
}

followup_summary <- function(d) {
  # Mutually exclusive endpoint labels give priority to death, then deregistration.
  reason <- rep("Administrative end", nrow(d))
  reason[which(!is.na(d$dereg_date) & d$dereg_date == d$followup_end_date)] <- "Registration ended"
  reason[which(!is.na(d$all_cause_death_date) & d$all_cause_death_date == d$followup_end_date)] <- "Death"
  days <- cut(d$followup_days, c(-1, 0, 6, 29, 89, 364, 365),
              labels = c("0", "1-6", "7-29", "30-89", "90-364", "365"))
  tab <- as.data.frame(table(end_reason = factor(reason, levels = c("Administrative end", "Registration ended", "Death")),
                              followup_days = days))
  names(tab)[3] <- "n"; tab$denominator <- nrow(d); tab
}

vaccination_overlap <- function(d, dates) {
  quarters <- unique(paste0(format(seq(dates$study_start, dates$study_end, by = "month"), "%Y"), "-Q",
      (as.integer(format(seq(dates$study_start, dates$study_end, by = "month"), "%m")) - 1) %/% 3 + 1))
  rows <- list()
  for (vaccine in c("covax", "fluvax")) {
    tab <- as.data.frame(table(quarter = factor(d$index_quarter, levels = quarters), age_group = d$age_group,
                              sex = d$sex, group = d[[paste0(vaccine, "_cat")]]))
    names(tab)[5] <- "n"; tab$vaccine <- vaccine
    key <- interaction(tab$quarter, tab$age_group, tab$sex, drop = TRUE)
    tab$stratum_n <- ave(tab$n, key, FUN = sum)
    tab$percent <- safe_percent(tab$n, tab$stratum_n)
    rows[[vaccine]] <- tab
  }
  do.call(rbind, rows)
}

post_vaccination_feasibility <- function(d) {
  rows <- list()
  for (vaccine in c("covax", "fluvax")) for (day in c(1, 7, 14, 30)) {
    post <- d[[paste0(vaccine, "_first_post_date")]]
    vaccinated <- !is.na(post) & post <= d$index_date + day
    observable <- d$followup_days >= day
    still_alive <- is.na(d$all_cause_death_date) | d$all_cause_death_date > d$index_date + day
    event_free <- is.na(d$mace_date) | d$mace_date > d$index_date + day
    rows[[length(rows) + 1L]] <- data.frame(vaccine = vaccine, day = day, denominator = nrow(d),
      vaccinated_by_day = sum(vaccinated), no_post_record_observed_through_day = sum(!vaccinated & observable),
      no_post_record_observation_ended_earlier = sum(!vaccinated & !observable),
      alive_registered_event_free_at_day = sum(observable & still_alive & event_free),
      alive_registered_event_free_without_post_record = sum(observable & still_alive & event_free & !vaccinated),
      provisional_mace_before_post_vaccination = sum(vaccinated & !is.na(d$mace_date) & d$mace_date < post),
      provisional_mace_same_day_as_post_vaccination = sum(vaccinated & !is.na(d$mace_date) & d$mace_date == post),
      index_day_record = sum(flag_true(d[[paste0(vaccine, "_index_day")]])))
  }
  do.call(rbind, rows)
}

missingness_by_year <- function(d) {
  fields <- c("bmi", "smoking_status", "smoking_legacy_status",
              "egfr_value", "care_home_address", "care_home_evidence", "ethnicity6", "imd_quintile", "region", "surgery_type",
              "hf_discharge_date", "hf_admission_method", "practice_go_live_date", "gp_registration_start")
  do.call(rbind, lapply(c("All", as.character(sort(unique(d$index_year)))), function(year) {
    x <- if (year == "All") d else d[d$index_year == as.integer(year), ]
    do.call(rbind, lapply(fields, function(name) data.frame(index_year = year, variable = name,
        denominator = nrow(x), n_missing = sum(missing_value(x[[name]])),
        percent_missing = safe_percent(sum(missing_value(x[[name]])), nrow(x)))))
  }))
}

gp_code_summary <- function(d) {
  inputs <- list(
    mi = c("mi_gp_first_code", "codelists/nhsd-primary-care-domain-refsets-mi_cod.csv", "code"),
    stroke = c("stroke_gp_first_code", "codelists/nhsd-primary-care-domain-refsets-strk_cod.csv", "code"),
    smoking = c("smoking_last_code", "codelists/opensafely-smoking-clear-snomed.csv", "id"))
  do.call(rbind, lapply(names(inputs), function(name) {
    spec <- inputs[[name]]
    dict <- read.csv(spec[2], colClasses = "character", check.names = FALSE)
    labels <- intersect(c("term", "description", "name"), names(dict))
    if (!length(labels)) stop("No codelist description column")
    code <- d[[spec[1]]]; code[is.na(code)] <- "No record"
    tab <- as.data.frame(table(code = factor(code, levels = unique(c("No record", code)))), stringsAsFactors = FALSE); names(tab)[2] <- "n"
    tab$domain <- name; tab$description <- dict[[labels[1]]][match(tab$code, dict[[spec[3]]])]
    tab$denominator <- nrow(d); tab
  }))
}
