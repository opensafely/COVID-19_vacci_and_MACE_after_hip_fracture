source("analysis/lib/formal_cohort.R")
opts <- read_options(list(input="output/analytical_cohort.rds",screening="output/screening.csv",output="output/manuscript/overview"))
d <- formal_cohort(readRDS(opts$input));s <- read_screening(opts$screening)
tables <- list(cohort_flow=formal_flow(d,s))
# Quantify the registration correction without exporting patient-level intervals.
change <- as.numeric(d$followup_end_date-d$single_registration_followup_end_date)
categories <- cut(change,c(-Inf,-1,0,7,30,90,Inf),labels=c("Shorter","Unchanged","1-7 days longer","8-30 days longer","31-90 days longer","91+ days longer"))
rows <- count_categories(categories,levels(categories),"Registration","Merged overlapping or adjacent observed intervals")
tables$registration_change <- protect_counts(rows,c("domain","definition","category"),c("n","denominator"),partition="definition",denominator="denominator")
x <- d[d$formal_eligible,]
# Descriptive COVID-vaccine rollout includes pre-rollout years, never used as a control group.
rows <- list()
for(v in c("covax","fluvax")) for(q in sort(unique(x$index_quarter))) {
  z <- x[x$index_quarter==q,];a <- table(z[[paste0(v,"_pre")]])
  rows[[length(rows)+1]] <- data.frame(vaccine=v,quarter=q,group=names(a),n=as.numeric(a),denominator=nrow(z))
}
tables$vaccination_calendar <- protect_counts(do.call(rbind,rows),c("vaccine","quarter","group"),c("n","denominator"),partition=c("vaccine","quarter"),denominator="denominator")
# Landmark selection is explicit in the manuscript flow, by vaccine.
rows <- list()
base <- d[d$formal_eligible & d$index_date>=as.Date(formal_config()$primary_start),]
for(v in c("covax","fluvax")) {
  keep <- rep(TRUE,nrow(base))
  stages <- list("Primary pre-fracture cohort"=rep(TRUE,nrow(base)),
    "Alive and observed beyond day30"=base$followup_days>30,
    "No MACE through day30"=is.na(base$event_mace) | base$event_mace>base$index_date+30,
    "No index-day vaccination of this vaccine"=!flag_true(base[[paste0(v,"_index_day")]]))
  counts <- numeric()
  for(label in names(stages)) {keep <- keep & stages[[label]];counts <- c(counts,sum(keep))}
  hidden <- small_count(counts)
  pos <- which(small_count(head(counts,-1)-tail(counts,-1)))
  hidden[unique(c(pos,pos+1))] <- TRUE
  rows[[length(rows)+1]] <- data.frame(vaccine=v,step=seq_along(counts),criterion=names(stages),n_remaining_rounded5=display_count(counts,hidden))
}
tables$landmark_flow <- do.call(rbind,rows)
# The final admission month/quarter is deliberately omitted from main comparisons.
tables$definitions <- data.frame(variable=c("population","primary_period","pre_exposure","post_exposure","followup","MI","stroke","CV_death","MACE","cataract_negative_control","CKD","BMI","registration","missing_data","subgroups"),
 definition=c("First candidate hip fracture admission; S72.0-S72.2 confirmed; exclude V transport codes, elective admissions, W11-W17; age >=50; >=365-day registration",
 "2022-01-01 to 2024-12-31 for both vaccines","Latest pre-admission vaccination: none within365,181-365,91-180,1-90 days",
 "Day30 landmark: alive, MACE-free, observable beyond30; exclude index-day vaccination; compare first record during days1-30",
 "From day1 (pre) or31 (post) to365, death, end of continuous observed registration, or administrative end",
 "Acute/recurrent MI hospital I21-I22 or specific GP event codes; history/sequela codes excluded",
 "Arterial cerebral infarction hospital I630-I635/I638/I639 or specific GP ischaemic event codes",
 "ONS death with MI/ischaemic-stroke ICD10 mentioned in any cause position; not all chapter-I deaths",
 "First MI, ischaemic stroke, or specified MI/stroke-related death; index-spell onset cannot be inferred",
 "Hospital lens extraction C71/C72; exploratory negative-control proxy, not validated cataract diagnosis",
 "Recorded stage3-5 diagnostic history; no creatinine-derived variable added",
 "One primary definition: latest valid direct BMI record in prior5 calendar years,age>=18,value10-100",
 "Merge overlaps/adjacent calendar days within available TPP registration records; never bridge an uncovered day",
 "Real-data plan:30 chained-equation imputations,20 iterations; BMI PMM; categorical appropriate methods; outcome/hazard auxiliaries; complete-case comparison",
 "Exploratory complete-case MACE interactions and within-group estimates, with prespecified sparse-model rules"))
# A dedicated report avoids carrying feasibility-stage wording into the formal bundle.
dir.create(opts$output,recursive=TRUE,showWarnings=FALSE)
for(name in names(tables)) write.csv(tables[[name]],file.path(opts$output,paste0(name,".csv")),row.names=FALSE,na="[NOT_AVAILABLE]")
sections <- vapply(names(tables),function(name) {
  x <- tables[[name]]
  header <- paste0("<th>",escape_html(names(x)),"</th>",collapse="")
  body <- apply(x,1,function(row)paste0("<tr><td>",paste(escape_html(row),collapse="</td><td>"),"</td></tr>"))
  paste0("<h2>",escape_html(name),"</h2><table><tr>",header,"</tr>",paste(body,collapse=""),"</table>")
},character(1))
writeLines(c('<!doctype html><html lang="en"><meta charset="utf-8"><title>Manuscript cohort and definitions</title>',
  '<style>body{font-family:Arial;max-width:1200px;margin:32px auto}table{border-collapse:collapse}th,td{border:1px solid #ccc;padding:6px;text-align:left}</style>',
  '<h1>Manuscript cohort and definitions</h1><p>First formal analysis batch. Researcher review and Airlock approval are required before release. Specific GP clinical-event subsets and source completeness require confirmation.</p>',
  '<p>Counts <=7 and selected related cells are suppressed; other counts rounded to5. Separate model CSVs include unestimated models and diagnostics. Cumulative incidence is unadjusted, while Cox hazard ratios adjust for listed covariates. These observational associations do not by themselves establish vaccine effects.</p>',
  sections,'</html>'),file.path(opts$output,"report.html"))
