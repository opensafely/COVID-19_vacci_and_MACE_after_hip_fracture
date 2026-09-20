# Aggregate diagnostics; the full mids object is saved separately as highly sensitive.
imputation_logged_events <- function(mi) {
  empty <- data.frame(variable=character(),method=character(),phase=character(),
    event_type=character(),predictors=character(),log_entries=integer(),
    chains_affected=integer(),first_iteration=integer(),last_iteration=integer())
  if (is.null(mi$imp) || is.null(mi$imp$loggedEvents)) return(empty)
  z <- mi$imp$data
  allowed <- unique(c(names(z), "(Intercept)", unlist(lapply(names(z), function(name) {
    if (is.factor(z[[name]])) paste0(name, levels(z[[name]])) else character()
  }))))
  events <- mi$imp$loggedEvents
  events$event_type <- events$predictors <- rep("", nrow(events))
  for (i in seq_len(nrow(events))) {
    detail <- as.character(events$out[i])
    tokens <- strsplit(detail, ", ", fixed=TRUE)[[1]]
    type <- if (events$meth[i] %in% c("constant","collinear")) as.character(events$meth[i]) else
      if (grepl("ridge penalty",detail,fixed=TRUE)) "ridge_fallback" else
      if (grepl("df set",detail,fixed=TRUE)) "residual_df_adjustment" else
      if (grepl("All predictors",detail,fixed=TRUE)) "no_usable_predictors" else
      if (length(tokens) && all(tokens %in% allowed)) "predictors_removed" else "other_review_saved_object"
    events$event_type[i] <- type
    # Do not copy arbitrary warning text or embedded unprotected counts.
    events$predictors[i] <- if (length(tokens) && all(tokens %in% allowed)) paste(tokens,collapse="; ") else ""
  }
  events$phase <- ifelse(events$it == 0,"initialisation","iteration")
  events$dep[is.na(events$dep) | events$dep == ""] <- "setup"
  keys <- c("dep","meth","phase","event_type","predictors")
  groups <- split(seq_len(nrow(events)), do.call(paste,c(events[keys],sep="\r")))
  rows <- lapply(groups,function(index) {
    e <- events[index,]
    data.frame(variable=as.character(e$dep[1]),method=as.character(e$meth[1]),phase=e$phase[1],
      event_type=e$event_type[1],predictors=e$predictors[1],log_entries=nrow(e),
      chains_affected=length(unique(e$im[e$im>0])),first_iteration=min(e$it),last_iteration=max(e$it))
  })
  do.call(rbind,rows)
}

imputation_predictors <- function(mi) {
  empty <- data.frame(variable=character(),predictor=character(),used_at_initialisation=integer())
  if (is.null(mi$imp)) return(empty)
  targets <- names(mi$imp$method)[mi$imp$method != ""]
  if (!length(targets)) return(empty)
  p <- mi$imp$predictorMatrix[targets,,drop=FALSE]
  data.frame(variable=rep(rownames(p),each=ncol(p)),predictor=rep(colnames(p),nrow(p)),
    used_at_initialisation=as.integer(t(p)),row.names=NULL)
}

imputation_chain_trace <- function(mi,x) {
  empty <- data.frame(variable=character(),iteration=integer())
  if (is.null(mi$imp)) return(empty)
  means <- mi$imp$chainMean; variances <- mi$imp$chainVar; rows <- list()
  for (name in intersect(dimnames(means)[[1]],names(x))) {
    if (sum(is.na(x[[name]])) < 32) next
    for (i in seq_len(dim(means)[2])) {
      row <- data.frame(variable=name,iteration=i)
      for (chain in seq_len(dim(means)[3])) {
        row[[sprintf("mean_chain_%02d",chain)]] <- round(means[name,i,chain],3)
        row[[sprintf("sd_chain_%02d",chain)]] <- round(sqrt(variances[name,i,chain]),3)
      }
      rows[[length(rows)+1]] <- row
    }
  }
  if (!length(rows)) return(empty)
  do.call(rbind,rows)
}

imputation_distributions <- function(mi,x) {
  empty <- data.frame(variable=character(),source=character(),imputation=integer(),
    category=character(),n_rounded5=character(),denominator_rounded5=character())
  targets <- names(mi$methods)[mi$methods != "" & names(mi$methods) %in% names(x)]
  rows <- list()
  for (name in targets) {
    missing <- is.na(x[[name]])
    classify <- if (name == "bmi") function(v) cut(v,c(-Inf,18.5,25,30,Inf),right=FALSE,
      labels=c("<18.5","18.5-<25","25-<30","30+")) else function(v) as.character(v)
    categories <- if (name == "bmi") levels(classify(x[[name]])) else
      if (is.factor(x[[name]])) levels(x[[name]]) else sort(unique(as.character(x[[name]][!missing])))
    for (i in 0:length(mi$datasets)) {
      values <- if (i == 0) x[[name]][!missing] else mi$datasets[[i]][[name]][missing]
      counts <- table(factor(classify(values),levels=categories))
      rows[[length(rows)+1]] <- data.frame(variable=name,source=if(i==0)"observed" else "imputed_missing",
        imputation=i,category=names(counts),n=as.numeric(counts),denominator=length(values))
    }
  }
  if (!length(rows)) return(empty)
  protect_counts(do.call(rbind,rows),c("variable","source","imputation","category"),
    c("n","denominator"),partition=c("variable","source","imputation"),denominator="denominator")
}

plot_imputation_trace <- function(trace,path,label) {
  variables <- unique(trace$variable)
  png(path,width=1800,height=max(500,300*length(variables)),res=150)
  on.exit(dev.off())
  if (!length(variables)) {
    plot.new();text(.5,.5,"No imputation trace eligible for aggregate review");return(invisible(NULL))
  }
  par(mfrow=c(length(variables),2),mar=c(3.1,4,2.2,1),oma=c(0,0,2,0))
  for (name in variables) for (stat in c("mean","sd")) {
    d <- trace[trace$variable==name,]
    y <- as.matrix(d[grep(paste0("^",stat,"_chain_"),names(d))])
    if (!any(is.finite(y))) {plot.new();title(paste(name,stat,"not available"));next}
    matplot(d$iteration,y,type="l",lty=1,col=adjustcolor(rainbow(ncol(y)),alpha.f=.6),
      xlab="Iteration",ylab=if(stat=="mean")"Imputed mean" else "Imputed SD",
      main=paste(name,if(stat=="mean")"chain means" else "within-chain SD"))
  }
  mtext(paste(label,"- missing values only; one line per imputation"),outer=TRUE,cex=.9)
}
