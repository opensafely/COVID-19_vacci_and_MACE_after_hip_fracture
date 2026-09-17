source("analysis/lib/feasibility.R")
source("analysis/lib/disclosure.R")
source("analysis/lib/descriptive.R")
opts <- read_options(list(input = "output/analytical_cohort.rds", output = "output/descriptive"))
d <- add_feasibility_variables(readRDS(opts$input))
dates <- read_study_dates()

tables <- list(
  table1 = protect_counts(baseline_summary(d), c("variable", "level"), c("n", "denominator"),
                         partition = "variable", denominator = "denominator"),
  vaccination_overlap = protect_vaccination_overlap(vaccination_overlap(d, dates)),
  post_vaccination = protect_post_vaccination(post_vaccination_feasibility(d))
)
dir.create(opts$output, recursive = TRUE, showWarnings = FALSE)
# Plot exactly the released table cells, without access to raw patient counts.
plot_vaccination_summaries(tables$vaccination_overlap, tables$post_vaccination, opts$output)
write_review(tables, opts$output, title = "Hip fracture vaccination: descriptive analysis",
             figures = c("vaccination_before_fracture.png", "vaccination_after_fracture.png"))
message("Saved disclosure-controlled baseline table, vaccination figures and report.")
