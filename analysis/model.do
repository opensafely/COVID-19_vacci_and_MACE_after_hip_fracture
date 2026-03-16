
***** Stata script to create a histgoram for time since *****
***** last COVID, flu, or either vaccine before index date **


*Directory
cd "output"

*Dataset
import delimited dataset.csv, clear

*Histogram
histogram covax_most_recent_before_index

*Save histogram
graph export Figure1.png, replace

** Runs but needs > 0 observations