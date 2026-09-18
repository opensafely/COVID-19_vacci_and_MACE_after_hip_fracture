# Merge observed registration intervals; do not bridge an uncovered calendar day.
reconcile_registration <- function(d) {
  if (any(d$registration_chain_overflow %in% c("T", "1", TRUE)))
    stop("Registration extraction capacity exceeded; extend extraction before analysis")
  starts <- as.matrix(data.frame(lapply(d[paste0("reg_chain_", 1:12, "_start_date")], as.numeric)))
  ends <- as.matrix(data.frame(lapply(d[paste0("reg_chain_", 1:12, "_end_date")], as.numeric)))
  index <- as.numeric(d$index_date)
  left <- right <- rep(NA_real_, nrow(d))
  for (i in seq_len(nrow(d))) {
    valid <- !is.na(starts[i, ]) & !is.na(ends[i, ]) & ends[i, ] >= starts[i, ]
    a <- starts[i, valid]; b <- ends[i, valid]
    if (!length(a)) next
    l <- a[1]; r <- b[1]
    for (j in seq_along(a)[-1]) {
      if (a[j] <= r + 1) r <- max(r, b[j]) else {
        if (l <= index[i] && r >= index[i]) { left[i] <- l; right[i] <- r; break }
        l <- a[j]; r <- b[j]
      }
    }
    if (is.na(right[i]) && l <= index[i] && r >= index[i]) { left[i] <- l; right[i] <- r }
  }
  if (anyNA(right)) stop("No continuous registration interval covering an included index date")
  d$continuous_registration_start_date <- as.Date(left, origin = "1970-01-01")
  d$continuous_registration_end_date <- as.Date(right, origin = "1970-01-01")
  d$followup_end_date <- pmin(d$followup_end_date, d$continuous_registration_end_date)
  d$dereg_date <- d$continuous_registration_end_date
  d$reg_2y_at_index <- left <= as.numeric(as.Date(format(d$index_date, "%Y-%m-%d"))) - 730
  d$continuous_registration_prior_days <- index - left
  outcome_dates <- grep("^(mi|stroke|acute_mi|ischaemic_stroke|mace|cvddeath|mace_specific_death|all_cause_death|neg_con_1|cataract_extraction).*_date$", names(d), value = TRUE)
  d$registration_added_mace <- !is.na(d$mace_date) & d$mace_date > d$single_registration_followup_end_date & d$mace_date <= d$followup_end_date
  for (name in outcome_dates) d[[name]][which(d[[name]] > d$followup_end_date)] <- as.Date(NA)
  for (prefix in c("covax", "fluvax")) {
    dates <- grep(paste0("^", prefix, "_.*_date$"), names(d), value = TRUE)
    for (name in dates) d[[name]][which(d[[name]] > d$followup_end_date)] <- as.Date(NA)
    if (any(d[[paste0(prefix, "_post_window_overflow")]] %in% c("T", "1", TRUE)))
      stop("Post-vaccination extraction capacity exceeded")
    d[[paste0(prefix, "_post30_n")]] <- rowSums(!is.na(d[paste0(prefix, "_post_window_", 1:6, "_date")]))
    # In a truncated observation window, a spell vaccination beyond its end is not observed.
    if (paste0(prefix, "_during_spell") %in% names(d)) {
      first <- d[[paste0(prefix, "_first_post_date")]]
      d[[paste0(prefix, "_during_spell")]] <- d[[paste0(prefix, "_index_day")]] |
        (!is.na(first) & !is.na(d$hf_discharge_date) & first <= d$hf_discharge_date)
    }
  }
  for (outcome in c("mi", "stroke", "cvddeath", "mace")) d[[paste0(outcome, "365")]] <- !is.na(d[[paste0(outcome, "_date")]])
  d$neg_con_1 <- !is.na(d$neg_con_1_date)
  outside <- !is.na(d$ons_death_date) & d$ons_death_date > d$followup_end_date
  for (name in grep("^ons_(underlying_cause|cause_of_death)", names(d), value = TRUE)) d[[name]][outside] <- NA_character_
  d
}
