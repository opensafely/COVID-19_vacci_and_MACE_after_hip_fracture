source("analysis/lib/cohort.R")
suppressPackageStartupMessages(library(ggplot2))
opts <- read_options(list(input = "output/analytical_cohort.rds", output = "output/figures"))
d <- readRDS(opts$input)
dir.create(opts$output, recursive = TRUE, showWarnings = FALSE)
labels <- c(covax = "COVID-19", fluvax = "Influenza", anyvax = "Either vaccine")
theme_study <- theme_minimal(base_size = 12) + theme(
  panel.grid.minor = element_blank(), panel.grid.major.x = element_blank(),
  strip.text = element_text(face = "bold", hjust = 0),
  plot.title = element_text(face = "bold"), plot.caption = element_text(hjust = 0, colour = "#555555")
)
caption <- "Internal unsuppressed feasibility output. Clinical definitions are provisional.\nDay 0 is reported separately; absence of a record does not establish non-vaccination."
before <- do.call(rbind, lapply(names(labels), function(vaccine) {
  data.frame(vaccine = rep(labels[[vaccine]], nrow(d)), days = d[[paste0("days_since_last_", vaccine)]])
}))
before <- before[!is.na(before$days) & before$days >= 1 & before$days <= 365, ]
before$vaccine <- factor(before$vaccine, levels = unname(labels))
p <- if (nrow(before)) { ggplot(before, aes(days)) +
  geom_histogram(binwidth = 14, boundary = 0, fill = "#2E6D91", colour = "white", linewidth = .2) +
  facet_wrap(~vaccine, ncol = 1, scales = "free_y", drop = FALSE) +
  scale_x_continuous(breaks = c(0, 90, 180, 270, 365)) + coord_cartesian(xlim = c(0, 365)) +
  labs(title = "Vaccination in the year before hip fracture", x = "Days before fracture admission",
       y = "Patients", caption = caption) + theme_study
} else ggplot() + annotate("text", x = 0, y = 0, label = "No qualifying vaccination records") + theme_void()
ggsave(file.path(opts$output, "vaccination_before_fracture.png"), p, width = 9, height = 9, dpi = 180, bg = "white")

after <- do.call(rbind, lapply(c("covax", "fluvax"), function(vaccine) {
  data.frame(vaccine = rep(labels[[vaccine]], nrow(d)),
             days = as.numeric(d[[paste0(vaccine, "_post30_date")]] - d$index_date))
}))
after <- after[!is.na(after$days), ]
after$vaccine <- factor(after$vaccine, levels = unname(labels[c("covax", "fluvax")]))
q <- if (nrow(after)) { ggplot(after, aes(days)) +
  geom_histogram(binwidth = 1, boundary = .5, fill = "#278578", colour = "white", linewidth = .2) +
  facet_wrap(~vaccine, ncol = 1, scales = "free_y", drop = FALSE) +
  scale_x_continuous(breaks = c(1, 7, 14, 21, 30)) + coord_cartesian(xlim = c(.5, 30.5)) +
  labs(title = "First vaccination within 30 days after hip fracture",
       x = "Days after fracture admission", y = "Patients", caption = caption) + theme_study
} else ggplot() + annotate("text", x = 0, y = 0, label = "No qualifying vaccination records") + theme_void()
ggsave(file.path(opts$output, "vaccination_after_fracture.png"), q, width = 9, height = 7, dpi = 180, bg = "white")
message("Saved internal timing plots: ", opts$output)
