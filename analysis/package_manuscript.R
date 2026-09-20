# Collect the prespecified CSVs; this creates an output-review bundle, not a release.
source("analysis/lib/cohort.R")
opts <- read_options(list(input="output/manuscript",output="output/release"))
dir.create(opts$output,recursive=TRUE,showWarnings=FALSE)
read_table <- function(path) read.csv(path,stringsAsFactors=FALSE,check.names=FALSE,colClasses="character",na.strings="[NOT_AVAILABLE]")
combine <- function(name) {
  parts <- list()
  for(design in c("pre","post")) for(vaccine in c("covax","fluvax")) {
    path <- file.path(opts$input,paste0(design,"_",vaccine),paste0(name,".csv"))
    if(!file.exists(path)) stop("Missing planned output: ",path)
    x <- read_table(path);x$design <- rep(design,nrow(x));x$vaccine <- rep(vaccine,nrow(x));parts[[length(parts)+1]] <- x
  }
  all_names <- unique(unlist(lapply(parts,names)))
  parts <- lapply(parts,function(x){for(name in setdiff(all_names,names(x)))x[[name]]<-rep(NA_character_,nrow(x));x[all_names]})
  do.call(rbind,parts)
}
outputs <- list(
 figure1_cohort_flow=read_table(file.path(opts$input,"overview/cohort_flow.csv")),
 figure1_landmark_flow=read_table(file.path(opts$input,"overview/landmark_flow.csv")),
 table1_categorical=combine("categorical"),table1_continuous=combine("continuous"),
 table2_events_person_time=combine("events_person_time"),
 figure2_vaccination_calendar=read_table(file.path(opts$input,"overview/vaccination_calendar.csv")),
 figure3_cumulative_incidence=combine("cumulative_incidence"),
 all_models=combine("models"),model_diagnostics=combine("model_diagnostics"),
 supplementary_missingness=combine("missingness"),
 supplementary_complete_case_categorical=combine("complete_case_categorical"),
 supplementary_complete_case_continuous=combine("complete_case_continuous"),
 imputation_methods=combine("imputation_methods"),
 imputation_trace=combine("imputation_trace"),
 imputation_chain_trace=combine("imputation_chain_trace"),
 imputation_logged_events=combine("imputation_logged_events"),
 imputation_predictors=combine("imputation_predictors"),
 imputation_distributions=combine("imputation_distributions"),
 supplementary_registration_change=read_table(file.path(opts$input,"overview/registration_change.csv")),
 variable_definitions=read_table(file.path(opts$input,"overview/definitions.csv")),analysis_metadata=combine("analysis_metadata"))
primary <- outputs$all_models$model %in% c("primary_multiple_imputation","age_sex_calendar")
outputs$table3_primary_models <- outputs$all_models[primary,]
outputs$supplementary_models <- outputs$all_models[!primary,]
outputs$all_models <- NULL
# Link every released effect estimate to population, event counts and model df.
for(name in c("table3_primary_models","supplementary_models")) {
 d <- outputs$model_diagnostics
 keep <- intersect(c("design","vaccine","model","outcome","n_rounded5","events_rounded5","parameters"),names(d))
 outputs[[name]] <- merge(outputs[[name]],d[keep],by=c("design","vaccine","model","outcome"),all.x=TRUE,sort=FALSE)
}
manifest <- list()
for(name in names(outputs)) {
 x <- outputs[[name]]
 if(nrow(x)>5000)stop("Review file exceeds 5000 rows: ",name)
 path <- file.path(opts$output,paste0(name,".csv"))
 write.csv(x,path,row.names=FALSE,na="[NOT_AVAILABLE]")
 if(file.info(path)$size>16*1024^2)stop("Review file exceeds size limit")
 support <- grepl("^imputation_",name) || name %in% c("model_diagnostics","analysis_metadata","variable_definitions")
 manifest[[length(manifest)+1]] <- data.frame(file=paste0(name,".csv"),role=if(support)"supporting_review" else "manuscript_output",
   rows=nrow(x),status="REQUIRES_RESEARCHER_REVIEW_AND_AIRLOCK_APPROVAL",
   controls="Counts <=7 and selected complements suppressed; counts rounded5; rates derived from rounded cells; no patient-level outputs")
}
write.csv(do.call(rbind,manifest),file.path(opts$output,"output_manifest.csv"),row.names=FALSE)
message("Prepared manuscript CSV bundle. No files have been released.")
