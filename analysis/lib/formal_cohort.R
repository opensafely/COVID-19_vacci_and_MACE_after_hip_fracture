source("analysis/lib/feasibility.R")
source("analysis/lib/round2.R")
source("analysis/lib/disclosure.R")
library(survival)

formal_config <- function() {
  cfg <- jsonlite::fromJSON("analysis/config/formal_analysis.json")
  cfg$run_mode <- if(identical(Sys.getenv("OPENSAFELY_BACKEND"),"expectations")) "synthetic_workflow" else "full_analysis"
  # The documented local backend contains invented records, so only repeated-fit
  # execution is checked there. Real-data MICE convergence must be assessed in TPP.
  # Never reduce the real-data settings based on patient count or elapsed time.
  if(cfg$run_mode=="synthetic_workflow") {cfg$imputations <- 3L;cfg$imputation_iterations <- 3L}
  cfg
}
formal_cohort <- function(d) {
  d <- add_round2_variables(add_feasibility_variables(d))
  d$formal_eligible <- flag_true(d$index_hip_strict) & !flag_true(d$index_fall_W11_W17) &
    d$continuous_registration_prior_days >= 365 & d$positive_followup &
    d$index_date <= as.Date("2024-12-31")
  d$event_mi <- date_min(d$acute_mi_gp_date, d$acute_mi_hospital_date)
  d$event_stroke <- date_min(d$ischaemic_stroke_gp_date, d$ischaemic_stroke_hospital_date)
  d$event_cvdeath <- d$mace_specific_death_date
  d$event_mace <- date_min(d$event_mi, d$event_stroke, d$event_cvdeath)
  d$event_death <- d$all_cause_death_date
  d$event_cataract <- d$cataract_extraction_date
  d$event_hospital_mace <- date_min(d$acute_mi_hospital_date, d$ischaemic_stroke_hospital_date, d$event_cvdeath)
  for (v in c("covax", "fluvax")) {
    category <- as.character(d[[paste0(v, "_cat")]])
    category[category %in% c("No prior record", ">365 days")] <- "No record within 365 days"
    d[[paste0(v, "_pre")]] <- factor(category, levels = formal_config()$pre_groups)
  }
  d
}

# Every model uses time since admission and an explicit landmark start.
endpoint <- function(d, outcome, start = 0, horizon = 365) {
  event <- as.numeric(d[[paste0("event_", outcome)]] - d$index_date)
  death <- as.numeric(d$all_cause_death_date - d$index_date)
  end <- pmin(d$followup_days, horizon)
  type <- rep(0L, nrow(d))
  hit <- !is.na(event) & event > start & event <= end
  type[hit] <- 1L
  end[hit] <- event[hit]
  # Same-day outcome and death are classified as the outcome, including fatal MI/stroke.
  competing <- outcome != "death" & !is.na(death) & death > start & death <= end & !hit
  type[competing] <- 2L; end[competing] <- death[competing]
  if (start > 0) end[!is.na(event) & event <= start] <- start
  list(time = end, status = type)
}

analysis_frame <- function(d, vaccine, design, early = FALSE) {
  cfg <- formal_config()
  from <- if (early) as.Date("2021-01-01") else as.Date(cfg$primary_start)
  d <- d[d$formal_eligible & d$index_date >= from & d$index_date <= as.Date(cfg$primary_end), ]
  if (design == "post") {
    # Exclude day-0 vaccination; no within-day ordering is available.
    keep <- d$followup_days > 30 & (is.na(d$event_mace) | d$event_mace > d$index_date + 30) &
      !flag_true(d[[paste0(vaccine, "_index_day")]])
    d <- d[keep, ]
  }
  x <- data.frame(patient_id = d$patient_id, age = d$age, sex = d$sex, bmi = d$bmi,
    smoking = factor(d$smoking_status, levels = c("N", "E", "S")),
    ethnicity = factor(replace(d$ethnicity6, d$ethnicity6 %in% c("Missing", ""), NA)),
    imd = factor(d$imd_quintile, levels = 1:5), region = factor(d$region),
    history_cvd = as.integer(d$history_cvd_combined), hypertension = as.integer(d$history_hypertension_combined),
    diabetes = as.integer(d$prior_diabetes), ckd = as.integer(d$prior_ckd_stage3_5),
    copd = as.integer(d$prior_copd), cancer = as.integer(d$prior_cancer), dementia = as.integer(d$prior_dementia),
    prior_fracture = as.integer(d$prior_any_fracture_hospital), care_home = as.integer(d$care_home_evidence),
    statin = as.integer(d$rx_statin_prior365), antihypertensive = as.integer(d$rx_antihypertensive_prior365),
    antiplatelet = as.integer(d$rx_antiplatelet_prior365), anticoagulant = as.integer(d$rx_anticoagulant_prior365),
    bone_active = as.integer(d$rx_bone_active_prior365), prior_covid = as.integer(!is.na(d$covid_positive_before_index)),
    other_vaccine = as.integer(d[[paste0(ifelse(vaccine == "covax", "fluvax", "covax"), "_within_365d")]]),
    surgery = factor(d$surgery_group_review), quarter = factor(d$index_quarter),
    age_band = factor(ifelse(d$age < 80, "50-79", "80+")),
    registration_2y = d$continuous_registration_prior_days >= 730,
    index_cvd = flag_true(d$index_spell_mi) | flag_true(d$index_spell_stroke),
    followup = d$followup_days,
    post_day = as.numeric(d[[paste0(vaccine, "_post30_date")]] - d$index_date),
    pre_separate = d[[paste0(vaccine, "_cat")]],
    baseline_vaccine = d[[paste0(vaccine, "_pre")]],
    stringsAsFactors = FALSE)
  x$exposure <- if (design == "pre") d[[paste0(vaccine, "_pre")]] else
    factor(ifelse(is.na(d[[paste0(vaccine, "_post30_date")]]), cfg$post_groups[1], cfg$post_groups[2]), levels = cfg$post_groups)
  x$start <- ifelse(design == "post", 30, 0)
  for (outcome in c(cfg$outcomes, "hospital_mace")) {
    e <- endpoint(d, outcome, ifelse(design == "post", 30, 0))
    x[[paste0("time_", outcome)]] <- e$time
    x[[paste0("status_", outcome)]] <- e$status
  }
  x
}

formal_flow <- function(d, s) {
  flow <- screening_flow(s, d)
  keep <- rep(TRUE, nrow(d))
  checks <- list("S72.0-S72.2 diagnosis on index spell" = flag_true(d$index_hip_strict),
    "No recorded fall from height W11-W17" = !flag_true(d$index_fall_W11_W17),
    "At least 365 days continuous registration before index" = d$continuous_registration_prior_days >= 365,
    "Positive follow-up after admission day" = d$positive_followup,
    "Complete entry years through 2024" = d$index_date <= as.Date("2024-12-31"),
    "Primary comparison period 2022-2024" = d$index_date >= as.Date("2022-01-01"))
  for (name in names(checks)) {
    before <- sum(keep); keep <- keep & checks[[name]]
    flow <- rbind(flow, data.frame(step = nrow(flow), criterion = name,
      n_before = before, n_excluded = before-sum(keep), n_remaining = sum(keep)))
  }
  hidden <- small_count(flow$n_remaining)
  gap <- head(flow$n_remaining, -1) - tail(flow$n_remaining, -1)
  pos <- which(small_count(gap)); hidden[unique(c(pos, pos + 1))] <- TRUE
  data.frame(step = flow$step, criterion = flow$criterion,
    n_remaining_rounded5 = display_count(flow$n_remaining, hidden))
}
