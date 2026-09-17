# Definition comparisons for researcher review, not final analysis populations.
date_min <- function(...) {
  x <- pmin(..., na.rm = TRUE)
  x[!is.finite(as.numeric(x))] <- as.Date(NA)
  x
}

add_round2_variables <- function(d) {
  # Retain the RP's existing MI/stroke certificate component in both hospital
  # alternatives. These are sensitivity candidates, not validated MACE phenotypes.
  d$mace_hospital_date <- date_min(d$mi_hospital_date, d$stroke_hospital_date, d$cvddeath_date)
  d$mace_hospital_ischaemic_date <- date_min(d$mi_hospital_date, d$stroke_arterial_ischaemic_hospital_date, d$cvddeath_date)
  d$bmi_group <- cut(d$bmi, c(-Inf, 18.5, 25, 30, Inf), right = FALSE,
                     labels = c("<18.5", "18.5-<25", "25-<30", "30+"))
  d$diagnosed_lower_trauma_candidate <- flag_true(d$index_hip_strict) & !flag_true(d$index_fall_W11_W17)
  d$history_cvd_combined <- flag_true(d$prior_cvd) | flag_true(d$history_mi_hospital) |
    flag_true(d$history_stroke_hospital) | flag_true(d$history_heart_failure_hospital) | flag_true(d$history_af_hospital)
  d
}

count_categories <- function(values, levels, domain, definition) {
  values <- as.character(values); values[is.na(values)] <- "Missing"
  tab <- as.data.frame(table(category = factor(values, levels = unique(c(levels, "Missing")))))
  names(tab)[2] <- "n"
  tab$domain <- domain; tab$definition <- definition; tab$denominator <- length(values)
  tab[c("domain", "definition", "category", "n", "denominator")]
}

variable_definition_review <- function(d) {
  rows <- list()
  add <- function(x) rows[[length(rows) + 1L]] <<- x
  for (name in c("egfr_recorded", "prior_ckd_stage3_5", "ckd_current_primis", "egfr_low_repeated_90d")) {
    add(count_categories(ifelse(d[[name]], "Recorded", "Not recorded"), c("Recorded", "Not recorded"), "CKD", name))
  }
  cross <- paste0("eGFR record=", flag_true(d$egfr_recorded), "; CKD35 diagnosis=", flag_true(d$prior_ckd_stage3_5))
  add(count_categories(cross, sort(unique(cross)), "CKD", "Measurement versus diagnosis"))
  for (name in c("bmi_legacy_2y", "bmi_2y", "bmi_5y")) {
    age <- as.numeric(d$index_date - d[[paste0(name, "_date")]])
    band <- cut(age, c(0, 365, 730, 1095, Inf), labels = c("1-365 days", "366-730 days", "731-1095 days", "1096+ days"))
    add(count_categories(band, levels(band), "BMI", name))
  }
  add(count_categories(d$bmi_latest_out_of_range, c("FALSE", "TRUE"), "BMI", "Latest recorded numeric value outside 10-100"))
  smoking_label <- function(x) ifelse(is.na(x), "Missing", x)
  cross <- paste0("Legacy=", smoking_label(d$smoking_legacy_status), "; explicit=", smoking_label(d$smoking_status))
  add(count_categories(cross, as.vector(outer(c("N", "E", "S", "Missing"), c("N", "E", "S", "Missing"),
    function(x, y) paste0("Legacy=", x, "; explicit=", y))), "Smoking", "Legacy SNOMED versus categorised CTV3"))
  add(count_categories(d$smoking_same_day_conflict, c("FALSE", "TRUE"), "Smoking", "Conflicting explicit statuses on latest day"))
  for (name in c("mi", "stroke", "heart_failure", "af", "hypertension")) {
    cross <- paste0("GP=", flag_true(d[[paste0("prior_", name)]]), "; hospital=", flag_true(d[[paste0("history_", name, "_hospital")]]))
    add(count_categories(cross, sort(unique(cross)), "Prior disease", name))
  }
  add(count_categories(d$surgery_group_review, c("Total hip replacement group", "Hemiarthroplasty group",
    "Fracture fixation group", "Multiple procedure groups", "No matching procedure group", "Missing procedure field"), "Surgery", "Index spell OPCS groups"))
  do.call(rbind, rows)
}

observation_review <- function(d) {
  rows <- list()
  add <- function(values, levels, definition) rows[[length(rows) + 1L]] <<-
    count_categories(values, levels, "Observation", definition)
  gap <- as.numeric(d$ons_death_date - d$gp_death_date)
  band <- cut(gap, c(-Inf, -31, -8, -1, 0, 7, 30, Inf),
    labels = c("ONS 31+ days earlier", "ONS 8-30 days earlier", "ONS 1-7 days earlier", "Same date",
               "ONS 1-7 days later", "ONS 8-30 days later", "ONS 31+ days later"))
  status <- as.character(band)
  status[is.na(d$ons_death_date) & is.na(d$gp_death_date)] <- "Neither source"
  status[is.na(d$ons_death_date) & !is.na(d$gp_death_date)] <- "GP only"
  status[!is.na(d$ons_death_date) & is.na(d$gp_death_date)] <- "ONS only"
  add(status, c(levels(band), "Neither source", "GP only", "ONS only"), "Death sources through data end, not restricted to year 1")
  extension <- as.numeric(d$followup_end_date - d$legacy_followup_end_date)
  band <- cut(extension, c(-1, 0, 7, 30, Inf), labels = c("Unchanged", "1-7 days longer", "8-30 days longer", "31+ days longer"))
  add(band, levels(band), "ONS preference effect on observed follow-up")
  for (name in c("mace_date", "cvddeath_date", "all_cause_death_date")) {
    value <- ifelse(is.na(d[[name]]), "No observed event", ifelse(d[[name]] > d$legacy_followup_end_date,
       "Event after legacy endpoint", "Event on or before legacy endpoint"))
    add(value, c("No observed event", "Event after legacy endpoint", "Event on or before legacy endpoint"), name)
  }
  # Only diagnose transitions after the selected index registration ends.
  censored <- !is.na(d$dereg_date) & d$dereg_date == d$followup_end_date &
    d$dereg_date < d$admin_end_date & (is.na(d$all_cause_death_date) | d$all_cause_death_date > d$dereg_date)
  transition <- rep("Not censored at registration end", nrow(d))
  transition[censored] <- "Censored; no next registration within 30 days"
  transition[censored & !is.na(d$registration_next_start_date)] <- "Censored; next registration in 2-30 days"
  transition[censored & flag_true(d$registration_contiguous_next)] <- "Censored; next registration within 1 day"
  transition[censored & flag_true(d$registration_overlap_continues)] <- "Censored; overlapping registration continues"
  add(transition, c("Not censored at registration end", "Censored; no next registration within 30 days",
    "Censored; next registration in 2-30 days", "Censored; next registration within 1 day",
    "Censored; overlapping registration continues"), "Registration transition audit; censoring unchanged")
  do.call(rbind, rows)
}

exposure_event_review <- function(d) {
  rows <- list()
  for (vaccine in c("covax", "fluvax")) for (year in 2021:2024) for (group in exposure_levels) {
    x <- d[d$index_year == year & d[[paste0(vaccine, "_cat")]] == group, ]
    rows[[length(rows) + 1L]] <- data.frame(vaccine = vaccine, index_year = year, group = group,
      denominator = nrow(x), n_mace_provisional = sum(!is.na(x$mace_date)),
      n_mace_hospital = sum(!is.na(x$mace_hospital_date)),
      n_mace_hospital_ischaemic = sum(!is.na(x$mace_hospital_ischaemic_date)),
      n_death = sum(!is.na(x$all_cause_death_date)))
  }
  do.call(rbind, rows)
}

baseline_exposure_review <- function(d) {
  # A descriptive common-period comparison, not selection of the final estimand.
  rows <- list()
  for (vaccine in c("covax", "fluvax")) for (group in exposure_levels) {
    x <- d[d$index_year %in% 2022:2024 & d[[paste0(vaccine, "_cat")]] == group, ]
    for (name in c("age_group", "sex", "bmi_group", "smoking", "prior_ckd_stage3_5", "history_cvd_combined",
      "history_hypertension_combined", "care_home_evidence", "rx_statin_prior365", "rx_antihypertensive_prior365",
      "rx_antiplatelet_prior365", "rx_anticoagulant_prior365", "rx_bone_active_prior365")) {
      values <- x[[name]]
      levels <- if (is.logical(values)) c("FALSE", "TRUE") else levels(d[[name]])
      row <- count_categories(values, levels, "Baseline 2022-2024", name)
      row$vaccine <- vaccine; row$group <- group
      rows[[length(rows) + 1L]] <- row
    }
  }
  do.call(rbind, rows)
}

post_vaccine_sequence_review <- function(d) {
  rows <- list()
  for (vaccine in c("covax", "fluvax")) {
    post <- d[[paste0(vaccine, "_post30_date")]]
    vaccinated <- !is.na(post)
    for (outcome in c("mace_date", "mace_hospital_ischaemic_date")) {
      event <- d[[outcome]]
      status <- rep("No post record; observation ended before day 30", nrow(d))
      status[!vaccinated & d$followup_days >= 30] <- "No post record; observed through day 30"
      status[vaccinated & is.na(event)] <- "Vaccinated; no later or earlier observed MACE"
      status[which(vaccinated & event < post)] <- "MACE before vaccination"
      status[which(vaccinated & event == post)] <- "MACE on vaccination day"
      status[which(vaccinated & event > post)] <- "MACE after vaccination"
      row <- count_categories(status, c("No post record; observation ended before day 30", "No post record; observed through day 30",
        "Vaccinated; no later or earlier observed MACE", "MACE before vaccination", "MACE on vaccination day", "MACE after vaccination"),
        outcome, "First days 1-30 vaccine and first observed MACE sequence")
      row$vaccine <- vaccine; rows[[length(rows) + 1L]] <- row
      alive <- is.na(d$all_cause_death_date) | d$all_cause_death_date > d$index_date + 30
      event_free <- is.na(event) | event > d$index_date + 30
      eligible <- alive & event_free & d$followup_days > 30
      status <- ifelse(eligible, ifelse(vaccinated, "Eligible; post vaccine by day 30", "Eligible; no post vaccine by day 30"),
                       "Ineligible: death, MACE or observation ending by day 30")
      row <- count_categories(status, c("Eligible; post vaccine by day 30", "Eligible; no post vaccine by day 30",
        "Ineligible: death, MACE or observation ending by day 30"), outcome, "Day 30 landmark feasibility only")
      row$vaccine <- vaccine; rows[[length(rows) + 1L]] <- row
    }
  }
  do.call(rbind, rows)
}
