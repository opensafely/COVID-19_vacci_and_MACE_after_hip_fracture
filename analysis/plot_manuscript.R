# Run locally against officially released CSVs; no individual-level input is used.
source("analysis/lib/cohort.R")
library(ggplot2)
opts <- read_options(list(input="released",output="manuscript-figures"))
dir.create(opts$output,recursive=TRUE,showWarnings=FALSE)
read_released <- function(name) read.csv(file.path(opts$input,name),na.strings=c("[NOT_AVAILABLE]","[REDACTED]"),stringsAsFactors=FALSE)
m <- read_released("table3_primary_models.csv")
m <- m[m$model=="primary_multiple_imputation" & m$outcome=="mace",]
if(nrow(m)) {
 p <- ggplot(m,aes(x=hr,y=term,xmin=lower,xmax=upper))+geom_vline(xintercept=1,linetype=2,colour="grey50")+
  geom_pointrange(orientation="y",colour="#246b77")+scale_x_log10()+facet_grid(design~vaccine,scales="free_y")+
  labs(x="Adjusted cause-specific hazard ratio (95% CI)",y=NULL)+theme_minimal(base_size=11)
 ggsave(file.path(opts$output,"mace_forest.png"),p,width=10,height=7,dpi=300)
}
c <- read_released("figure3_cumulative_incidence.csv")
if(nrow(c)) {
 p <- ggplot(c,aes(day,cumulative_incidence,colour=group,group=group))+geom_line()+geom_point()+
  facet_grid(design~vaccine)+labs(x="Days after hip-fracture admission",y="Unadjusted cumulative incidence of MACE",colour="Vaccination record")+
  theme_minimal(base_size=11)+theme(legend.position="bottom")
 ggsave(file.path(opts$output,"mace_cumulative_incidence.png"),p,width=11,height=7,dpi=300)
}
