# Aggregate outputs for researcher review in Level 4; not automatic public release.
# https://docs.opensafely.org/outputs/sdc/
# Suppress positive counts <=7, protect complements/partitions, then round to 5.
# Public release still requires review of the complete set of outputs in Airlock.

check_counts <- function(x) {
  if (!is.numeric(x) || any(!is.na(x) & (!is.finite(x) | x < 0 | x != floor(x)))) {
    stop("Invalid aggregate count")
  }
  invisible(TRUE)
}

small_count <- function(x) !is.na(x) & x > 0 & x <= 7

display_count <- function(x, suppress = rep(FALSE, length(x))) {
  check_counts(x)
  result <- as.character(5 * floor(x / 5 + 0.5))
  result[is.na(x)] <- "[NOT_AVAILABLE]"
  result[!is.na(x) & (suppress | small_count(x))] <- "[REDACTED]"
  result
}

# Return only declared labels/counts. Never copy raw percentages or other fields.
protect_counts <- function(x, labels, counts, partition = NULL, denominator = NULL) {
  stopifnot(all(c(labels, counts, partition, denominator) %in% names(x)))
  for (name in counts) check_counts(x[[name]])
  mask <- lapply(x[counts], small_count)
  if (!is.null(denominator)) {
    stopifnot(denominator %in% counts)
    total <- x[[denominator]]
    for (name in setdiff(counts, denominator)) {
      if (any(x[[name]] > total, na.rm = TRUE)) stop("Count exceeds denominator")
      # Also suppress a count revealing a small complementary group.
      mask[[name]] <- mask[[name]] | small_count(total - x[[name]]) | small_count(total)
    }
  }
  if (!is.null(partition)) {
    stopifnot("n" %in% counts)
    groups <- if (length(partition)) interaction(x[partition], drop = TRUE, lex.order = TRUE) else rep(1, nrow(x))
    for (rows in split(seq_len(nrow(x)), groups)) {
      hidden <- rows[mask$n[rows]]
      # A second positive cell prevents recovery of a single hidden cell from a total.
      if (length(hidden) == 1L) {
        choices <- rows[!mask$n[rows] & !is.na(x$n[rows]) & x$n[rows] > 0]
        if (length(choices)) mask$n[choices[which.min(x$n[choices])]] <- TRUE
        else if (!is.null(denominator)) mask[[denominator]][rows] <- TRUE
      }
    }
  }
  out <- x[labels]
  for (name in counts) out[[paste0(name, "_rounded5")]] <- display_count(x[[name]], mask[[name]])
  out
}

review_tables <- function(tables) {
  read <- function(name) {
    if (is.null(tables[[name]])) stop("Missing aggregate table: ", name)
    tables[[name]]
  }
  out <- list()
  readiness <- read("readiness")
  allowed_checks <- c("cohort_not_empty", "screening_reconciled", "monthly_source_activity",
    "clinical_definitions", "medication_classes", "frailty_care_home", "broader_prior_fracture",
    "high_energy_trauma", "source_complete_dates", "effectiveness_models")
  allowed_status <- c("PASS", "REVIEW_EMPTY_COHORT", "REVIEW_REQUIRED",
                      "NOT_IMPLEMENTED", "NOT_RUN")
  if (anyNA(readiness[c("check", "status")]) || any(!readiness$check %in% allowed_checks) ||
      any(!readiness$status %in% allowed_status)) stop("Unexpected readiness value")
  out$readiness <- readiness[c("check", "status")]

  flow <- read("cohort_flow")
  check_counts(flow$n_remaining)
  # Neighbouring survivor counts would otherwise reveal small exclusions.
  hide <- small_count(flow$n_remaining)
  if (nrow(flow) > 1) {
    gap <- head(flow$n_remaining, -1) - tail(flow$n_remaining, -1)
    check_counts(gap)
    positions <- which(small_count(gap))
    hide[unique(c(positions, positions + 1L))] <- TRUE
  }
  out$cohort_flow <- flow[c("step", "criterion")]
  out$cohort_flow$n_remaining_rounded5 <- display_count(flow$n_remaining, hide)

  out$index_monthly <- protect_counts(read("index_monthly"), "month", "n", partition = character())
  # Overall diagnostics avoid overlapping overall/year-specific margins in this review.
  diagnostic <- read("definition_diagnostics")
  diagnostic <- diagnostic[diagnostic$index_year == "All", ]
  out$definition_diagnostics <- protect_counts(diagnostic, "diagnostic", c("n", "denominator", "n_unknown"),
    denominator = "denominator")
  out$outcome_sources <- protect_counts(read("outcome_sources"), c("outcome", "gp_record", "hospital_record"),
    c("n", "denominator"), partition = "outcome", denominator = "denominator")
  out$outcome_timing <- protect_counts(read("outcome_timing"), c("outcome", "window"),
    c("n", "denominator"), partition = "outcome", denominator = "denominator")
  out$followup <- protect_counts(read("followup"), c("end_reason", "followup_days"),
    c("n", "denominator"), partition = character(), denominator = "denominator")
  out$vaccination_overlap <- protect_vaccination_overlap(read("vaccination_overlap"))
  out$post_vaccination <- protect_post_vaccination(read("post_vaccination"))
  missing <- read("missingness_by_year")
  missing <- missing[missing$index_year != "All", ]
  out$missingness_by_year <- protect_counts(missing, c("index_year", "variable"),
    c("n_missing", "denominator"), denominator = "denominator")
  out
}

protect_vaccination_overlap <- function(vaccination) {
  # Both analysis actions use exactly the same protected quarter-by-vaccine cells.
  # Age/sex detail stays internal until the comparison window is chosen.
  vaccination <- aggregate(n ~ quarter + vaccine + group, data = vaccination, FUN = sum)
  key <- interaction(vaccination$quarter, vaccination$vaccine, drop = TRUE)
  vaccination$denominator <- ave(vaccination$n, key, FUN = sum)
  protect_counts(vaccination, c("quarter", "vaccine", "group"),
    c("n", "denominator"), partition = c("quarter", "vaccine"), denominator = "denominator")
}

protect_post_vaccination <- function(post) {
  # Only the prespecified day-30 three-way partition is exported: no cumulative
  # day-1/7/14 margins from which sparse intervening vaccination counts can be inferred.
  post <- post[post$day == 30, ]
  categories <- c("vaccinated_by_day", "no_post_record_observed_through_day",
                  "no_post_record_observation_ended_earlier")
  post <- do.call(rbind, lapply(categories, function(name) data.frame(vaccine = post$vaccine,
    day = post$day, category = name, n = post[[name]], denominator = post$denominator)))
  protect_counts(post, c("vaccine", "day", "category"), c("n", "denominator"),
    partition = "vaccine", denominator = "denominator")
}

escape_html <- function(x) {
  x <- gsub("&", "&amp;", as.character(x), fixed = TRUE)
  x <- gsub("<", "&lt;", x, fixed = TRUE)
  gsub(">", "&gt;", x, fixed = TRUE)
}

write_review <- function(tables, directory, title = "Hip fracture vaccination: feasibility review",
                         figures = character()) {
  dir.create(directory, recursive = TRUE, showWarnings = FALSE)
  for (name in names(tables)) {
    write.csv(tables[[name]], file.path(directory, paste0(name, ".csv")), row.names = FALSE, na = "[NOT_AVAILABLE]")
  }
  sections <- vapply(names(tables), function(name) {
    x <- tables[[name]]
    header <- paste0("<th>", escape_html(names(x)), "</th>", collapse = "")
    body <- apply(x, 1, function(row) paste0("<tr><td>", paste(escape_html(row), collapse = "</td><td>"), "</td></tr>"))
    paste0("<h2>", escape_html(gsub("_", " ", name)), "</h2><table><thead><tr>", header,
      "</tr></thead><tbody>", paste(body, collapse = "\n"), "</tbody></table>")
  }, character(1))
  figure_html <- vapply(figures, function(name) {
    # Filenames are supplied by the analysis script, never from patient data.
    if (!grepl("^[a-z_]+[.]png$", name)) stop("Unexpected report figure filename")
    paste0('<img src="', name, '" alt="', escape_html(sub(".png", "", name, fixed = TRUE)),
           '" style="max-width:100%;height:auto">')
  }, character(1))
  html <- c(paste0('<!doctype html><html lang="en"><head><meta charset="utf-8"><title>', escape_html(title), '</title>'),
    '<style>body{font-family:Arial,sans-serif;max-width:1200px;margin:40px auto;padding:0 24px;color:#172c3a}table{border-collapse:collapse;font-size:13px;margin-bottom:32px}th,td{border:1px solid #ccd7dd;padding:7px;text-align:left}th{background:#edf3f6}h2{margin-top:36px}</style></head><body>',
    paste0('<h1>', escape_html(title), '</h1>'),
    '<p>Provisional definitions. Review within the OpenSAFELY secure environment. This report is not approved for release.</p>',
    '<p>Positive counts of 7 or fewer and selected related cells are [REDACTED]. Remaining counts are rounded to the nearest 5; rounded cells may not sum to rounded denominators. [NOT_AVAILABLE] means unavailable, not zero. Raw percentages are omitted.</p>',
    '<p>Clinical definitions remain provisional. Entry ends on 1 January 2025, so the final entry month and quarter are partial. Follow-up is capped at 1 January 2026. Monthly source activity was not run; source completeness remains unverified. No prior vaccine record does not establish non-vaccination.</p>',
    '<p>Outcome dates and definitions remain provisional, including GP history codes, same-admission events and the current MI/stroke death definition. The post-vaccination table uses the 30-day window and distinguishes early observation endings. Detailed GP codes, age/sex vaccine cross-tabulations and individual data remain internal. No effectiveness model has been fitted.</p>',
    figure_html, sections, '</body></html>')
  writeLines(html, file.path(directory, "report.html"))
}
