# Regression and multiple imputation; all patient-level objects stay highly sensitive.
# No model is silently simplified after convergence failure.
suppressPackageStartupMessages(library(mice))

formal_impute <- function(x, cfg) {
  is_post <- all(x$start == 30)
  # Pre-fracture exposure already IS baseline_vaccine. In landmark analyses,
  # post_vaccination already IS exposure. Include each definition only once.
  names_to_use <- unique(c("exposure", cfg$main_adjusters,
    if (is_post) "baseline_vaccine", "quarter", "surgery"))
  z <- x[names_to_use]
  for (outcome in cfg$outcomes) {
    time <- x[[paste0("time_", outcome)]] - x$start
    status <- x[[paste0("status_", outcome)]]
    fit <- survfit(Surv(time, status == 1) ~ 1)
    at <- findInterval(time, fit$time)
    z[[paste0("event_", outcome)]] <- as.integer(status == 1)
    if (outcome != "death") z[[paste0("competing_", outcome)]] <- as.integer(status == 2)
    z[[paste0("hazard_", outcome)]] <- c(0, fit$cumhaz)[at + 1]
  }
  # Numerical auxiliaries preserve follow-up information in imputation.
  z$followup <- x$followup
  if (!is_post) z$post_vaccination <- as.integer(!is.na(x$post_day))
  methods <- mice::make.method(z)
  for (name in names(z)) {
    if (!anyNA(z[[name]])) methods[name] <- ""
    else if (all(is.na(z[[name]]))) stop("An imputation variable is entirely missing: ", name)
    else if (name == "bmi") methods[name] <- "pmm"
    else if (is.factor(z[[name]])) methods[name] <- if (nlevels(droplevels(z[[name]])) == 2) "logreg" else "polyreg"
    else methods[name] <- "pmm"
  }
  if (!any(methods != "")) return(list(datasets = list(x), imp = NULL, methods = methods, events = 0L))
  predictors <- mice::make.predictorMatrix(z)
  # Keep incomplete targets active: global remove.collinear can disable their
  # imputation. MICE's within-model dependency handling remains enabled, and its
  # actual predictor changes are retained in the diagnostic outputs.
  imp <- mice(z, m = cfg$imputations, maxit = cfg$imputation_iterations, method = methods,
    predictorMatrix = predictors, seed = cfg$seed, printFlag = TRUE,
    remove.constant = TRUE, remove.collinear = FALSE, donors = 5, nnet.MaxNWts = 20000)
  changed <- names(methods)[methods != ""]
  if (any(imp$method[changed] == "")) stop("MICE disabled a required imputation target")
  result <- lapply(seq_len(cfg$imputations), function(i) {
    out <- x; values <- mice::complete(imp, i)
    for (name in changed) out[[name]] <- values[[name]]
    remaining <- cfg$main_adjusters[vapply(out[cfg$main_adjusters], anyNA, logical(1))]
    if (length(remaining)) stop("Imputation left missing values in: ", paste(remaining, collapse=", "))
    out
  })
  list(datasets = result, imp = imp, methods = imp$method,
       events = if (is.null(imp$loggedEvents)) 0L else nrow(imp$loggedEvents))
}

model_formula <- function(x, cfg, adjusted = TRUE, surgery = FALSE, interaction = NULL) {
  adjusters <- if (adjusted) cfg$main_adjusters else c("age", "sex")
  if (nlevels(droplevels(x$baseline_vaccine)) > 1 && nlevels(droplevels(x$exposure)) == 2)
    adjusters <- unique(c(adjusters, "baseline_vaccine"))
  if (surgery || (adjusted && nrow(x) && all(x$start == 30))) adjusters <- unique(c(adjusters, "surgery"))
  # Structural constants cannot be estimated; this rule does not use p-values.
  adjusters <- adjusters[vapply(x[adjusters], function(a) length(unique(a[!is.na(a)])) > 1, logical(1))]
  rhs <- c("exposure", vapply(adjusters, function(a) {
    if(!a %in% c("age","bmi") || length(unique(x[[a]][!is.na(x[[a]])]))<4 ||
       (a=="age" && isTRUE(cfg$linear_age))) return(a)
    if(a=="age") "splines::ns(age, knots=c(65,80), Boundary.knots=c(50,110))" else
      "splines::ns(bmi, knots=c(22,30), Boundary.knots=c(10,100))"
  }, character(1)), "strata(quarter)")
  if (!is.null(interaction)) rhs <- c(rhs, paste0("exposure:", interaction), if (!interaction %in% adjusters) interaction)
  as.formula(paste("Surv(tstart, tstop, event) ~", paste(rhs, collapse = " + ")))
}

regression_data <- function(x, outcome, lower = NULL, upper = 365, tv = FALSE) {
  x$tstart <- if (is.null(lower)) x$start else pmax(x$start, lower)
  x$tstop <- pmin(x[[paste0("time_", outcome)]], upper)
  x$event <- as.integer(x[[paste0("status_", outcome)]] == 1 & x[[paste0("time_", outcome)]] <= upper)
  x <- x[x$tstop > x$tstart, ]
  if (!tv) return(x)
  # Vaccination at day d changes exposure AFTER that day. Same-day events remain
  # unexposed because ordering is unknown. Receipt only during days 1-30 qualifies.
  v <- x$post_day
  switch <- !is.na(v) & v > x$tstart & v < x$tstop
  pre <- x; pre$exposure <- 0L
  pre$tstop[switch] <- v[switch]; pre$event[switch] <- 0L
  post <- x[switch, ]; post$tstart <- v[switch]; post$exposure <- 1L
  # For entry after day 0, prior qualifying vaccination has already switched status.
  pre$exposure[!is.na(v) & v <= x$tstart] <- 1L
  out <- rbind(pre, post)
  out$exposure <- factor(out$exposure, levels=0:1, labels=c("No qualifying record yet", "Recorded days1-30"))
  out
}

fit_cox_safe <- function(x, formula, cfg) {
  frame <- tryCatch(model.frame(formula, data = x, na.action = na.pass), error = function(e) NULL)
  if (is.null(frame)) return(list(status = "DESIGN_NOT_ESTIMABLE"))
  complete <- complete.cases(frame)
  reference <- levels(x$exposure)[1]
  x <- droplevels(x[complete, ])
  if(!length(reference) || !any(as.character(x$exposure)==reference))
    return(list(status="MISSING_REFERENCE_GROUP",n=length(unique(x$patient_id)),events=sum(x$event)))
  if (!nrow(x) || length(unique(x$exposure)) < 2) return(list(status = "INSUFFICIENT_EXPOSURE_GROUPS"))
  # Quarterly strata have separate baseline hazards, not fitted coefficients.
  # Count only regression terms for the unchanged events-per-parameter rule.
  regression_terms <- terms(formula)
  strata_terms <- grep("^strata\\([^()]*\\)$", attr(regression_terms, "term.labels"))
  if (length(strata_terms)) regression_terms <- drop.terms(regression_terms, strata_terms, keep.response = TRUE)
  design <- tryCatch(model.matrix(regression_terms, data = x), error = function(e) NULL)
  if (is.null(design)) return(list(status = "DESIGN_NOT_ESTIMABLE"))
  parameters <- max(1L, ncol(design) - 1L)
  event_n <- sum(x$event)
  group_events <- tapply(x$event, x$exposure, sum)
  if (any(group_events < cfg$minimum_group_events)) return(list(status = "INSUFFICIENT_GROUP_EVENTS", n = length(unique(x$patient_id)), events = event_n, df = parameters))
  if (event_n < cfg$minimum_events_per_parameter * parameters) return(list(status = "INSUFFICIENT_EVENTS_FOR_MODEL", n = length(unique(x$patient_id)), events = event_n, df = parameters))
  warnings <- character()
  fit <- tryCatch(withCallingHandlers(coxph(formula, data = x, ties = "efron", x = TRUE, y = TRUE,
      model = TRUE, singular.ok = FALSE, control = coxph.control(iter.max = 50)),
      warning = function(w) { warnings <<- c(warnings, conditionMessage(w)); invokeRestart("muffleWarning") }),
    error = function(e) { message("Cox fit error: ", conditionMessage(e)); NULL })
  if (length(warnings)) message("Cox fit warning: ", paste(unique(warnings), collapse="; "))
  if (is.null(fit)) return(list(status = "FIT_FAILED", n = length(unique(x$patient_id)), events = event_n, df = parameters))
  if (length(warnings) || any(!is.finite(coef(fit))) || any(!is.finite(vcov(fit))) || any(diag(vcov(fit)) <= 0))
    return(list(status = "CONVERGENCE_REVIEW", n = length(unique(x$patient_id)), events = event_n, df = parameters))
  list(status = "OK", fit = fit, n = length(unique(x$patient_id)), events = event_n, df = length(coef(fit)))
}

pool_coefficients <- function(fits) {
  terms <- names(coef(fits[[1]]))
  if (any(vapply(fits, function(f) !identical(names(coef(f)), terms), logical(1)))) stop("Inconsistent coefficient sets across imputations")
  q <- do.call(cbind, lapply(fits, coef))
  u <- do.call(cbind, lapply(fits, function(f) diag(vcov(f))))
  m <- ncol(q); estimate <- rowMeans(q); within <- rowMeans(u)
  between <- if (m > 1) apply(q, 1, var) else rep(0, nrow(q))
  total <- within + (1 + 1/m) * between
  lambda <- (1 + 1/m) * between / total
  old_df <- ifelse(lambda > 0, (m - 1) / lambda^2, Inf)
  df_complete <- max(1, fits[[1]]$nevent - length(terms))
  obs_df <- (df_complete + 1)/(df_complete + 3) * df_complete * (1 - lambda)
  df <- if (m > 1) 1/(1/old_df + 1/obs_df) else rep(Inf, length(terms))
  se <- sqrt(total); critical <- qt(.975, df)
  data.frame(term = terms, log_hr = estimate, se = se, hr = exp(estimate),
    lower = exp(estimate - critical*se), upper = exp(estimate + critical*se),
    p_value = 2*pt(-abs(estimate/se), df), imputations = m, pooling_df = df,
    fraction_missing_information = lambda + (1-lambda) * 2/(df+3),
    mcse_log_hr = sqrt(between/m), stringsAsFactors = FALSE)
}

run_cox <- function(datasets, outcome, cfg, model_id, adjusted = TRUE, surgery = FALSE,
                    lower = NULL, upper = 365, subset_rule = NULL, tv = FALSE, interaction = NULL) {
  # Within a prespecified age band, use linear age: all-cohort spline knots
  # outside that band's support otherwise create a rank-deficient design.
  if(grepl("^subgroup_age_band_",model_id)) cfg$linear_age <- TRUE
  fits <- list(); first <- NULL; first_n <- NA_integer_
  for (i in seq_along(datasets)) {
    x <- datasets[[i]]
    if (!is.null(subset_rule)) x <- x[subset_rule(x), ]
    x <- regression_data(x, outcome, lower, upper, tv)
    if (is.null(first)) first <- x
    f <- tryCatch(model_formula(x, cfg, adjusted, surgery, interaction), error = function(e) NULL)
    fit <- if (is.null(f)) list(status = "DESIGN_NOT_ESTIMABLE") else fit_cox_safe(x, f, cfg)
    if (fit$status != "OK") return(list(coefficients = data.frame(), diagnostic = data.frame(
      model = model_id, outcome = outcome, status = fit$status, n = if (is.null(fit$n)) nrow(x) else fit$n,
      events = if (is.null(fit$events)) sum(x$event) else fit$events, parameters = if (is.null(fit$df)) NA else fit$df,
      imputations = length(datasets), ph_global_p = NA_real_, ph_exposure_p = NA_real_, interaction_p = NA_real_)))
    # Only the first fit is retained for PH diagnostics. Subsequent fits need
    # coefficients and variance for Rubin pooling, not 30 patient-level matrices.
    fits[[i]] <- if(i == 1 || !is.null(interaction)) fit$fit else
      structure(list(coefficients=coef(fit$fit), var=vcov(fit$fit), nevent=fit$fit$nevent), class="coxph")
    if (i == 1) first_n <- fit$n
  }
  pooled <- pool_coefficients(fits)
  pooled <- pooled[grepl("^exposure", pooled$term), ]
  pooled$model <- model_id; pooled$outcome <- outcome
  pooled$effect_measure <- ifelse(grepl(":",pooled$term), "ratio_of_hazard_ratios", "cause_specific_hazard_ratio")
  ph <- tryCatch(cox.zph(fits[[1]], transform = "km")$table, error = function(e) NULL)
  ph_global <- if (is.null(ph)) NA_real_ else ph["GLOBAL", "p"]
  ph_exp <- if (is.null(ph) || !"exposure" %in% rownames(ph)) NA_real_ else ph["exposure", "p"]
  interaction_p <- NA_real_
  if (!is.null(interaction)) {
    # D1 pools the joint interaction test; it does not select subgroups by significance.
    reduced <- lapply(datasets, function(z) {
      z <- regression_data(z, outcome, lower, upper, tv)
      co <- model_formula(z, cfg, adjusted, surgery, NULL)
      if (!interaction %in% cfg$main_adjusters) co <- update(co, paste(". ~ . +", interaction))
      coxph(co, data = z, ties = "efron")
    })
    if(length(fits)==1L) {
      # Complete-case interactions use the nested-model likelihood-ratio test.
      extra <- length(coef(fits[[1]]))-length(coef(reduced[[1]]))
      if(extra>0 && fits[[1]]$n==reduced[[1]]$n)
        interaction_p <- pchisq(max(0,2*(fits[[1]]$loglik[2]-reduced[[1]]$loglik[2])),df=extra,lower.tail=FALSE)
    } else {
      test <- tryCatch(mice::D1(mice::as.mira(fits), mice::as.mira(reduced)), error = function(e) NULL)
      if (!is.null(test)) interaction_p <- as.numeric(test$result[1, "p.value"])
    }
  }
  list(coefficients = pooled, diagnostic = data.frame(model = model_id, outcome = outcome,
    status = "OK", n = first_n, events = fits[[1]]$nevent,
    parameters = length(coef(fits[[1]])), imputations = length(fits),
    ph_global_p = ph_global, ph_exposure_p = ph_exp, interaction_p = interaction_p))
}

run_finegray <- function(x, cfg, model_id="fine_gray_complete_case") {
  fields <- unique(c(cfg$main_adjusters,"exposure","baseline_vaccine","quarter"))
  reference <- levels(x$exposure)[1]
  x <- droplevels(x[complete.cases(x[fields]),])
  x$tstart <- x$start; x$tstop <- x$time_mace; x$event <- as.integer(x$status_mace==1)
  f <- model_formula(x,cfg)
  # Fine-Gray uses quarter indicators; cause-specific Cox uses quarter strata.
  rhs <- attr(terms(f),"term.labels")
  rhs <- sub("strata\\(quarter\\)","quarter",rhs)
  mat <- tryCatch(model.matrix(as.formula(paste("~",paste(rhs,collapse="+"))),x)[,-1,drop=FALSE],error=function(e)NULL)
  p <- if(is.null(mat)) NA_integer_ else ncol(mat)
  status <- "OK"; fit <- NULL
  ge <- tapply(x$status_mace==1,x$exposure,sum)
  if(!reference %in% as.character(x$exposure) || is.null(mat) || length(ge)<2 || any(ge<cfg$minimum_group_events) || sum(x$status_mace==1)<cfg$minimum_events_per_parameter*p)
    status <- "INSUFFICIENT_EVENTS_FOR_MODEL"
  else {
    fit <- tryCatch(suppressWarnings(cmprsk::crr(ftime=x$time_mace-x$start,fstatus=x$status_mace,
      cov1=mat,cengroup=x$exposure,failcode=1,cencode=0,maxiter=50)),error=function(e)NULL)
    if(is.null(fit) || !isTRUE(fit$converged)) status <- "CONVERGENCE_REVIEW"
  }
  co <- data.frame()
  if(status=="OK") {
    b <- fit$coef; se <- sqrt(diag(fit$var)); keep <- grepl("^exposure",names(b))
    if(any(!is.finite(b)) || any(!is.finite(se)) || any(se<=0)) status <- "CONVERGENCE_REVIEW"
    else co <- data.frame(term=names(b)[keep],log_hr=b[keep],se=se[keep],hr=exp(b[keep]),
      lower=exp(b[keep]-1.96*se[keep]),upper=exp(b[keep]+1.96*se[keep]),p_value=2*pnorm(-abs(b[keep]/se[keep])),
      imputations=1,pooling_df=Inf,fraction_missing_information=NA_real_,mcse_log_hr=NA_real_,
      model=model_id,outcome="mace",effect_measure="subdistribution_hazard_ratio",row.names=NULL)
  }
  list(coefficients=co,diagnostic=data.frame(model=model_id,outcome="mace",status=status,n=nrow(x),
    events=sum(x$status_mace==1),parameters=p,imputations=1,ph_global_p=NA_real_,ph_exposure_p=NA_real_,interaction_p=NA_real_))
}
