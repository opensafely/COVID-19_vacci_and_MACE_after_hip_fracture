source("analysis/lib/cohort.R")
opts <- read_options(list(input = "output/dataset.csv",
                          output = "output/analytical_cohort.rds",
                          log = "output/logs/prepare_cohort.txt"))
d <- prepare_cohort(opts$input)
dir.create(dirname(opts$output), recursive = TRUE, showWarnings = FALSE)
saveRDS(d, opts$output, version = 3)
dir.create(dirname(opts$log), recursive = TRUE, showWarnings = FALSE)
writeLines(c("Project 209: shared cohort preparation for analysis review",
             paste("Rows:", nrow(d)), paste("Columns:", ncol(d)),
             "Preparation validated identifiers, observation dates, outcomes and vaccination windows.",
             "Missing covariates and zero-follow-up records are retained.",
             "Regression is performed by the downstream formal-analysis actions.", capture.output(sessionInfo())), opts$log)
message("Saved prepared R cohort: ", opts$output)
