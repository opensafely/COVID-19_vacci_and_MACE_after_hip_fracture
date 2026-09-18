source("analysis/lib/formal_cohort.R")
source("analysis/lib/formal_models.R")
source("analysis/lib/formal_outputs.R")
opts <- read_options(list(input="output/analytical_cohort.rds",vaccine="covax",design="pre",output="output/manuscript/pre_covax"))
stopifnot(opts$vaccine %in% c("covax","fluvax"),opts$design %in% c("pre","post"))
cfg <- formal_config()
d <- formal_cohort(readRDS(opts$input))
x <- analysis_frame(d,opts$vaccine,opts$design)
if(!nrow(x)) stop("No patients eligible for this prespecified analysis")
message("Preparing ",opts$design," ",opts$vaccine," analysis")
mi <- formal_impute(x,cfg)
results <- list()
add <- function(a) results[[length(results)+1L]] <<- a
for(outcome in cfg$outcomes) {
  message("Fitting prespecified ",outcome," models")
  add(run_cox(list(x),outcome,cfg,"age_sex_calendar",adjusted=FALSE))
  add(run_cox(mi$datasets,outcome,cfg,"primary_multiple_imputation"))
  add(run_cox(list(x),outcome,cfg,"complete_case"))
}
add(run_finegray(x,cfg))
add(run_cox(mi$datasets,"hospital_mace",cfg,"hospital_only_multiple_imputation"))
add(run_cox(mi$datasets,"mace",cfg,"exclude_index_cvd",subset_rule=function(z)!z$index_cvd))
add(run_cox(mi$datasets,"mace",cfg,"two_year_registration",subset_rule=function(z)z$registration_2y))
if(opts$design=="pre") add(run_cox(mi$datasets,"mace",cfg,"surgery_adjusted",surgery=TRUE))
# Period-specific HRs are prespecified, not selected using the PH test's p-value.
for(k in seq_len(length(cfg$period_breaks)-1)) {
  lo <- cfg$period_breaks[k];hi <- cfg$period_breaks[k+1]
  if(opts$design=="post" && hi<=30) next
  add(run_cox(mi$datasets,"mace",cfg,paste0("period_",lo+1,"_",hi),lower=lo,upper=hi))
}
# Effect modification is exploratory, on a fixed complete-case cohort. This avoids
# representing additive-imputation models as interaction-compatible imputations.
complete_fields <- unique(c(cfg$main_adjusters,"exposure","baseline_vaccine","quarter"))
cc <- x[complete.cases(x[complete_fields]),]
for(group in cfg$subgroups) {
  values <- unique(as.character(cc[[group]])); values <- values[!is.na(values)]
  if(length(values)<2) next
  add(run_cox(list(cc),"mace",cfg,paste0("interaction_",group),interaction=group))
  for(value in values) {
    local({g <- group;v <- value
      add(run_cox(list(cc),"mace",cfg,paste0("subgroup_",g,"_",v),subset_rule=function(z)as.character(z[[g]])==v))
    })
  }
}
if(opts$design=="pre") {
  separate <- x; separate$exposure <- separate$pre_separate
  add(run_cox(list(separate),"mace",cfg,"separate_no_prior_record_complete_case"))
  earlier <- analysis_frame(d,opts$vaccine,"pre",early=TRUE)
  add(run_cox(list(earlier),"mace",cfg,"include_2021_complete_case"))
  # This supplements the landmark analysis; time-dependent confounding remains.
  add(run_cox(list(x),"mace",cfg,"post_vaccination_time_varying_complete_case",tv=TRUE))
}
tables <- formal_table1(x)
tables$missingness <- formal_missingness(x,cfg)
cc_comparison <- x
cc_comparison$exposure <- factor(ifelse(complete.cases(x[complete_fields]),"Complete covariates","One or more missing covariates"))
cc_tables <- formal_table1(cc_comparison)
tables$complete_case_categorical <- cc_tables$categorical
tables$complete_case_continuous <- cc_tables$continuous
tables$events_person_time <- formal_events(x,cfg)
tables$cumulative_incidence <- formal_cif(x,cfg)
tables$models <- protect_model_results(results)
tables$model_diagnostics <- protect_model_diagnostics(results)
tables$imputation_methods <- imputation_diagnostics(mi,x)
tables$imputation_trace <- imputation_trace(mi,x)
tables$analysis_metadata <- data.frame(
 parameter=c("run_mode","imputations","imputation_iterations","specification","vaccine","design","start","end","reference","bmi","imputation_logged_events","clinical_definition_review","source_completeness"),
 value=c(cfg$run_mode,as.character(cfg$imputations),as.character(cfg$imputation_iterations),cfg$specification,opts$vaccine,opts$design,cfg$primary_start,cfg$primary_end,levels(x$exposure)[1],cfg$bmi_definition,
         as.character(mi$events),cfg$clinical_status,cfg$source_completeness_status))
dir.create(opts$output,recursive=TRUE,showWarnings=FALSE)
for(name in names(tables)) {
  if(nrow(tables[[name]])>5000) stop("CSV exceeds release row limit: ",name)
  write.csv(tables[[name]],file.path(opts$output,paste0(name,".csv")),row.names=FALSE,na="[NOT_AVAILABLE]")
}
# Fits and imputed individual records are deliberately not emitted as review outputs.
message("Saved aggregate manuscript CSVs; real-data diagnostics and Airlock review are still required")
