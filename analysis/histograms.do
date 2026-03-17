
***************************************************************
* Do file name:   histograms.do
* Project:        COVID-19_vacci_and_MACE_after_hip_fracture
* Date:     	  17/03/2026
* Author:         James Webster
* Description:    Figure 2: Histogram of days since most recent 
*                 vaccine (COVID and/or flu)
***************************************************************


***** Packages
*ssc install table1_mc - this wont run


***** Directory
cd "output"


***** Dataset
import delimited dataset.csv, clear


***** Convert string dates to Stata dates
gen index_date_stata = date(index_date, "YMD")
gen covax_most_recent_date = date(covax_most_recent_before_index, "YMD")
gen fluvax_most_recent_date = date(fluvax_most_recent_before_index, "YMD")
egen anyvax_most_recent_date = rowmax(covax_most_recent_date fluvax_most_recent_date)
foreach var of varlist index_date_stata covax_most_recent_date fluvax_most_recent_date anyvax_most_recent_date {
    format `var' %td
}


***** Calculate n days since most recent vaccination before index by type
gen days_since_last_covax = index_date_stata - covax_most_recent_date
gen days_since_last_fluvax = index_date_stata - fluvax_most_recent_date
gen days_since_last_anyvax = index_date_stata - anyvax_most_recent_date


***** Histograms
foreach var of varlist days_since_last_anyvax days_since_last_covax days_since_last_fluvax {
    histogram `var'
    graph export `var'.svg
}


*************************************************************
***** END ***************************************************
*************************************************************