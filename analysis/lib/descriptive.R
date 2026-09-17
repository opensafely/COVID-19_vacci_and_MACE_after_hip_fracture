baseline_summary <- function(d) {
  d$bmi_group <- cut(d$bmi, c(-Inf, 18.5, 25, 30, Inf), right = FALSE,
                     labels = c("<18.5", "18.5-<25", "25-<30", "30+"))
  fields <- c("age_group", "sex", "ethnicity", "imd", "bmi_group", "smoking", "prior_cvd",
              "prior_mi", "prior_stroke", "prior_heart_failure", "prior_af", "prior_hypertension",
              "prior_diabetes", "prior_ckd", "prior_copd", "prior_cancer", "prior_dementia", "reg_2y_at_index")
  do.call(rbind, lapply(fields, function(name) {
    values <- d[[name]]
    if (is.null(values)) stop("Missing baseline variable: ", name)
    categories <- if (is.logical(values)) c("No", "Yes") else if (is.factor(values)) levels(values) else sort(unique(values[!is.na(values)]))
    values <- if (is.logical(values)) ifelse(values, "Yes", "No") else as.character(values)
    values[is.na(values)] <- "Missing"
    tab <- as.data.frame(table(level = factor(values, levels = unique(c(categories, "Missing")))))
    names(tab)[2] <- "n"
    tab$variable <- name
    tab$denominator <- nrow(d)
    tab
  }))
}

plot_vaccination_summaries <- function(before, after, directory) {
  suppressPackageStartupMessages(library(ggplot2))
  labels <- c(covax = "COVID-19", fluvax = "Influenza")
  caption <- paste("Counts are rounded to 5; * denotes a redacted cell, not zero.",
                   "Provisional definitions. Absence of a record does not establish non-vaccination.", sep = "\n")
  study_theme <- theme_minimal(base_size = 11) + theme(
    panel.grid.minor = element_blank(), strip.text = element_text(face = "bold"),
    plot.title = element_text(face = "bold"), plot.caption = element_text(hjust = 0))

  before$value <- suppressWarnings(as.numeric(before$n_rounded5))
  before$label <- ifelse(before$n_rounded5 == "[REDACTED]", "*", before$n_rounded5)
  before$vaccine <- factor(before$vaccine, levels = names(labels), labels = labels)
  before$group <- factor(before$group, levels = exposure_levels)
  p <- ggplot(before, aes(quarter, group, fill = value)) +
    geom_tile(colour = "white") + geom_text(aes(label = label), size = 2.7) +
    facet_wrap(~vaccine, ncol = 1) +
    scale_fill_gradient(low = "#edf5fa", high = "#347fa5", na.value = "#dddddd", name = "Patients\n(rounded)") +
    labs(title = "Vaccination before hip fracture, by admission quarter",
         x = "Admission quarter (2025 Q1 contains 1 January only)",
         y = "Most recent recorded vaccination before admission", caption = caption) +
    study_theme + theme(axis.text.x = element_text(angle = 45, hjust = 1), panel.grid = element_blank())
  ggsave(file.path(directory, "vaccination_before_fracture.png"), p, width = 12, height = 8, dpi = 160, bg = "white")

  after$value <- suppressWarnings(as.numeric(after$n_rounded5))
  after$label <- ifelse(after$n_rounded5 == "[REDACTED]", "*", after$n_rounded5)
  # Hidden cells have no bar; the star preserves the distinction from observed zero.
  after$bar_height <- ifelse(is.na(after$value), 0, after$value)
  after$vaccine <- factor(after$vaccine, levels = names(labels), labels = labels)
  after$category <- factor(after$category,
    levels = c("vaccinated_by_day", "no_post_record_observed_through_day", "no_post_record_observation_ended_earlier"),
    labels = c("Recorded vaccination\nwithin 30 days", "No post-fracture record;\nobserved through day 30", "No post-fracture record;\nobservation ended earlier"))
  q <- ggplot(after, aes(category, bar_height)) + geom_col(fill = "#278578", width = .65) +
    geom_text(aes(label = label), vjust = -.4, size = 3.3) + facet_wrap(~vaccine, ncol = 1) +
    scale_y_continuous(expand = expansion(mult = c(0, .15))) +
    labs(title = "Vaccination during days 1-30 after hip fracture", x = NULL,
         y = "Patients (rounded)", caption = caption) + study_theme
  ggsave(file.path(directory, "vaccination_after_fracture.png"), q, width = 10, height = 8, dpi = 160, bg = "white")
}
