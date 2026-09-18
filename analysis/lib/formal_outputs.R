# Publication CSVs contain aggregate summaries and selected exposure coefficients.
formal_table1 <- function(x) {
  categorical <- c("sex", "age_band", "smoking", "ethnicity", "imd", "history_cvd", "hypertension",
    "diabetes", "ckd", "copd", "cancer", "dementia", "prior_fracture", "care_home",
    "statin", "antihypertensive", "antiplatelet", "anticoagulant", "bone_active", "prior_covid", "other_vaccine", "surgery")
  x$bmi_group <- cut(x$bmi, c(-Inf,18.5,25,30,Inf), right=FALSE,
    labels=c("<18.5","18.5-<25","25-<30","30+"))
  rows <- list(); numeric_rows <- list()
  for (group in levels(x$exposure)) {
    z <- x[x$exposure == group, ]
    for (name in c(categorical, "bmi_group")) {
      values <- as.character(z[[name]]); values[is.na(values)] <- "Missing"
      categories <- if (is.factor(x[[name]])) levels(x[[name]]) else sort(unique(as.character(x[[name]][!is.na(x[[name]])])))
      counts <- table(factor(values, levels=unique(c(categories,"Missing"))))
      rows[[length(rows)+1]] <- data.frame(group=group, variable=name, level=names(counts), n=as.numeric(counts), denominator=nrow(z))
    }
    for (name in c("age","bmi")) {
      a <- z[[name]][!is.na(z[[name]])]; enough <- length(a) >= 32
      numeric_rows[[length(numeric_rows)+1]] <- data.frame(group=group, variable=name,
        n_observed_rounded5=display_count(length(a),small_count(sum(is.na(z[[name]])))), n_missing_rounded5=display_count(sum(is.na(z[[name]])),small_count(length(a))),
        mean=if(enough) round(mean(a),1) else NA, sd=if(enough) round(sd(a),1) else NA,
        median=if(enough) round(median(a),1) else NA,
        q25=if(enough) round(as.numeric(quantile(a,.25)),1) else NA,
        q75=if(enough) round(as.numeric(quantile(a,.75)),1) else NA)
    }
  }
  list(categorical=protect_counts(do.call(rbind,rows), c("group","variable","level"),c("n","denominator"),partition=c("group","variable"),denominator="denominator"),
       continuous=do.call(rbind,numeric_rows))
}

formal_missingness <- function(x,cfg) {
  rows <- list()
  for (group in levels(x$exposure)) for(name in cfg$main_adjusters) {
    z <- x[x$exposure==group,]
    rows[[length(rows)+1]] <- data.frame(group=group,variable=name,n=sum(is.na(z[[name]])),denominator=nrow(z))
  }
  protect_counts(do.call(rbind,rows),c("group","variable"),c("n","denominator"),denominator="denominator")
}

formal_events <- function(x,cfg) {
  rows <- list()
  for (group in levels(x$exposure)) for(outcome in cfg$outcomes) {
    z <- x[x$exposure==group,]
    time <- z[[paste0("time_",outcome)]]; status <- z[[paste0("status_",outcome)]]
    for(k in seq_len(length(cfg$period_breaks)-1)) {
      lo <- cfg$period_breaks[k]; hi <- cfg$period_breaks[k+1]
      if(nrow(z) && hi <= min(z$start)) next
      at <- time > pmax(lo,z$start)
      py <- sum(pmax(0,pmin(time,hi)-pmax(lo,z$start)))/365.25
      rows[[length(rows)+1]] <- data.frame(group=group,outcome=outcome,day_start=lo+1,day_end=hi,
        denominator=sum(at),n_events=sum(at & status==1 & time<=hi),
        n_competing=sum(at & status==2 & time<=hi),person_years=py)
    }
  }
  raw <- do.call(rbind,rows)
  out <- protect_counts(raw,c("group","outcome","day_start","day_end"),c("denominator","n_events","n_competing"),denominator="denominator")
  hide <- small_count(raw$denominator) | small_count(raw$n_events) | small_count(raw$n_competing)
  out$person_years_rounded5 <- ifelse(hide,"[REDACTED]",as.character(5*floor(raw$person_years/5+.5)))
  e <- suppressWarnings(as.numeric(out$n_events_rounded5)); py <- suppressWarnings(as.numeric(out$person_years_rounded5))
  out$rate_per_1000_person_years <- ifelse(!is.na(e) & !is.na(py) & py>0, round(1000*e/py,2), NA_real_)
  out
}

formal_cif <- function(x,cfg) {
  rows <- list()
  for(group in levels(x$exposure)) {
    z <- x[x$exposure==group,]; if(!nrow(z)) next
    time <- z$time_mace; status <- z$status_mace
    grid <- cfg$risk_times[cfg$risk_times > min(z$start)]
    previous <- min(z$start)
    sparse <- nrow(z)<32
    for(day in grid) {
      counts <- c(sum(time>previous & time<=day & status==1),sum(time>previous & time<=day & status==2),sum(time>previous & time<=day & status==0))
      sparse <- sparse | any(small_count(counts)) | small_count(sum(time>=day))
      previous <- day
    }
    fit <- if(!sparse) tryCatch(survfit(Surv(time-min(z$start),factor(status,levels=0:2,labels=c("censored","MACE","death")))~1,data=z),error=function(e)NULL) else NULL
    for(day in grid) {
      a <- if(!is.null(fit)) tryCatch(summary(fit,times=day-min(z$start),extend=TRUE),error=function(e)NULL) else NULL
      index <- if(is.null(a)) NA else match("MACE",colnames(a$pstate))
      good <- !is.null(a) && !is.na(index) && sum(time>=day)>=8
      rows[[length(rows)+1]] <- data.frame(group=group,day=day,adjustment="Unadjusted",
        n_rounded5=display_count(nrow(z),sparse),at_risk_rounded5=display_count(sum(time>=day),sparse),
        mace_cumulative_rounded5=display_count(sum(status==1 & time<=day),sparse),
        competing_cumulative_rounded5=display_count(sum(status==2 & time<=day),sparse),
        cumulative_incidence=if(good) round(a$pstate[1,index],4) else NA_real_,
        lower=if(good) round(a$lower[1,index],4) else NA_real_,upper=if(good) round(a$upper[1,index],4) else NA_real_,
        status=if(good)"ESTIMATED_AALEN_JOHANSEN" else "NOT_RELEASED_SPARSE_OR_UNESTIMABLE")
    }
  }
  if(!length(rows)) return(data.frame(group=character(),day=numeric(),status=character()))
  do.call(rbind,rows)
}

protect_model_results <- function(results) {
  rows <- lapply(results, function(a) a$coefficients)
  rows <- rows[vapply(rows,nrow,integer(1))>0]
  if(!length(rows)) return(data.frame(model=character(),outcome=character(),term=character(),hr=numeric(),lower=numeric(),upper=numeric(),p_value=numeric()))
  x <- do.call(rbind,rows)
  for(name in intersect(c("log_hr","se","hr","lower","upper","p_value","pooling_df","fraction_missing_information","mcse_log_hr"),names(x))) x[[name]] <- signif(x[[name]],5)
  x
}
protect_model_diagnostics <- function(results) {
  x <- do.call(rbind,lapply(results,function(a)a$diagnostic))
  x$n_rounded5 <- display_count(x$n); x$events_rounded5 <- display_count(x$events, small_count(x$n-x$events))
  x$n <- x$events <- NULL
  x
}

imputation_diagnostics <- function(mi,x) {
  rows <- list()
  for(name in names(mi$methods)) {
    if(!name %in% names(x)) next
    count <- sum(is.na(x[[name]]))
    rows[[length(rows)+1]] <- data.frame(variable=name,method=ifelse(mi$methods[name]=="","observed",mi$methods[name]),
      n_missing_rounded5=display_count(count,small_count(nrow(x)-count)),n_observed_rounded5=display_count(nrow(x)-count,small_count(count)),
      imputations=length(mi$datasets),status=if(count && mi$methods[name]=="")"REVIEW_UNIMPUTED" else "COMPLETED")
  }
  do.call(rbind,rows)
}

imputation_trace <- function(mi,x) {
  if(is.null(mi$imp)) return(data.frame(variable=character(),iteration=integer(),mean_across_chains=numeric(),sd_across_chains=numeric()))
  a <- mi$imp$chainMean; rows <- list()
  for(name in dimnames(a)[[1]]) {
    if(!name %in% names(x) || sum(is.na(x[[name]]))<8) next
    for(i in seq_len(dim(a)[2])) rows[[length(rows)+1]] <- data.frame(variable=name,iteration=i,
      mean_across_chains=round(mean(a[name,i,],na.rm=TRUE),3),sd_across_chains=round(sd(a[name,i,],na.rm=TRUE),3))
  }
  if(!length(rows)) return(data.frame(variable=character(),iteration=integer(),mean_across_chains=numeric(),sd_across_chains=numeric()))
  do.call(rbind,rows)
}
