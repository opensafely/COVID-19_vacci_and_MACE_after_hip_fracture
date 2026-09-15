# Unsuppressed feasibility summaries, not final manuscript tables.
source("analysis/lib/cohort.R")
opts <- read_options(list(input = "output/analytical_cohort.rds", output = "output/tables"))
d <- readRDS(opts$input)

summarise_baseline <- function(x, exposure, group) {
  rows <- list()
  add <- function(variable, level = "", n = 0L, mean = NA_real_, sd = NA_real_,
                  median = NA_real_, p25 = NA_real_, p75 = NA_real_, n_missing = 0L) {
    rows[[length(rows) + 1L]] <<- data.frame(
      exposure = exposure, group = group, variable = variable, level = level,
      n = n, denominator = nrow(x), percent = if (nrow(x)) 100 * n / nrow(x) else NA_real_,
      mean = mean, sd = sd, median = median, p25 = p25, p75 = p75, n_missing = n_missing
    )
  }
  add("participants", n = nrow(x))
  for (name in c("age", "bmi")) {
    values <- x[[name]][!is.na(x[[name]])]
    q <- if (length(values)) quantile(values, c(.25, .5, .75), names = FALSE) else rep(NA_real_, 3)
    add(name, n = length(values), mean = if (length(values)) mean(values) else NA_real_,
        sd = if (length(values) > 1) sd(values) else NA_real_, median = q[2], p25 = q[1],
        p75 = q[3], n_missing = sum(is.na(x[[name]])))
  }
  categorical <- intersect(c("age_group", "sex", "ethnicity", "imd", "smoking", "prior_cvd",
                              "prior_mi", "prior_stroke", "prior_heart_failure", "prior_af",
                              "prior_hypertension", "prior_diabetes", "prior_ckd", "prior_copd",
                              "prior_cancer", "prior_dementia", "reg_2y_at_index"), names(x))
  for (name in categorical) {
    values <- x[[name]]
    values <- if (is.logical(values)) ifelse(values, "Yes", "No") else as.character(values)
    values[is.na(values)] <- "Missing"
    counts <- table(values)
    for (level in names(counts)) add(name, level, as.integer(counts[[level]]),
                                     n_missing = sum(values == "Missing"))
  }
  do.call(rbind, rows)
}

tables <- list(summarise_baseline(d, "overall", "All patients"))
for (exposure in c("covax", "fluvax")) for (group in exposure_levels) {
  rows <- d[[paste0(exposure, "_cat")]] == group
  tables[[length(tables) + 1L]] <- summarise_baseline(d[rows, , drop = FALSE], exposure, group)
}
write_internal_csv(do.call(rbind, tables), file.path(opts$output, "table1_feasibility.csv"))

distributions <- list()
for (vaccine in c("covax", "fluvax")) {
  tab <- as.data.frame(table(index_year = d$index_year, group = d[[paste0(vaccine, "_cat")]]))
  names(tab)[3] <- "n"
  tab$vaccine <- vaccine
  distributions[[length(distributions) + 1L]] <- tab
}
write_internal_csv(do.call(rbind, distributions), file.path(opts$output, "vaccination_by_year.csv"))

missing_fields <- intersect(c("bmi", "smoking_status", "ethnicity6", "imd_quintile", "region"), names(d))
missingness <- do.call(rbind, lapply(missing_fields, function(name) {
  values <- d[[name]]
  n_missing <- sum(is.na(values) | (!is.na(values) & as.character(values) == "Missing"))
  data.frame(variable = name, n = nrow(d), n_missing = n_missing,
             percent_missing = if (nrow(d)) 100 * n_missing / nrow(d) else NA_real_)
}))
write_internal_csv(missingness, file.path(opts$output, "missingness.csv"))

post <- do.call(rbind, lapply(c("covax", "fluvax"), function(vaccine) {
  data.frame(vaccine = vaccine, n = nrow(d),
    index_day = sum(d[[paste0(vaccine, "_index_day")]], na.rm = TRUE),
    post_30_days = sum(!is.na(d[[paste0(vaccine, "_post30_date")]])),
    post_calendar_month = sum(!is.na(d[[paste0(vaccine, "_month1_date")]])),
    date_overlaps_spell = sum(d[[paste0(vaccine, "_during_spell")]], na.rm = TRUE),
    observed_at_least_30_days = sum(d$observed_30_days))
}))
write_internal_csv(post, file.path(opts$output, "post_fracture_vaccination.csv"))
message("Saved internal feasibility tables: ", opts$output)
