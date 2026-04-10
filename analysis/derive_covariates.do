********************************************************************************
*
*	DO-FILE: 
*
*	PROGRAMMED BY:	James Webster, adapted from Emily Herrett 
*	('001_cr_define_covariates_cohorts.do' from 'covid_collateral_hf_update')
*
*
********************************************************************************
*
*	PURPOSE: takes raw cohort extracted using EHRQL, checks exclusions, 
*			 and defines covariates
*
********************************************************************************



********************************************************************************
***** LOAD DATA AND SET LOG ****************************************************
********************************************************************************


clear
capture log close
cd "output"
import delimited dataset.csv, clear
log using define_covariates.log, replace



********************************************************************************
***** CONVERT STRING DATES TO STATA DATES **************************************
********************************************************************************


*Index, deregistration, covid vax, flu vax, or either vax	

gen index_date_stata = date(index_date, "YMD")
drop index_date
rename index_date_stata index_date

gen dereg_date_stata = date(dereg_date, "YMD")
drop dereg_date
rename dereg_date_stata dereg_date

gen death_date = date(all_cause_death_date, "YMD")
drop all_cause_death_date

gen covax_most_recent_date = date(covax_most_recent_before_index, "YMD")
gen fluvax_most_recent_date = date(fluvax_most_recent_before_index, "YMD")
egen anyvax_most_recent_date = rowmax(covax_most_recent_date fluvax_most_recent_date)

foreach var of varlist ///
	index_date ///
	dereg_date ///
	death_date ///
	covax_most_recent_date ///
	fluvax_most_recent_date ///
	anyvax_most_recent_date ///
	{
		format `var' %td
}



********************************************************************************
***** CATEGORICAL VARIABLES FOR VACCINE TIMING *********************************
********************************************************************************


*Variable structure
	// this should be based on the observed distribution in the histograms
	// for now:
		// > 12 months
		// 6-12 months
		// 3-6 months
		// < 3 months


*Covid vaccine groups		
gen covax_cat = 0
replace covax_cat = 1 if ///
	covax_most_recent_date < index_date - 365
replace covax_cat = 2 if ///
	covax_most_recent_date > (index_date - 365) & ///
	covax_most_recent_date < (index_date - (30*6))
replace covax_cat = 3 if ///
	covax_most_recent_date > (index_date - (30*6)) & ///
	covax_most_recent_date < (index_date - (30*3))
replace covax_cat = 4 if ///
	covax_most_recent_date > (index_date - (30*3)) & ///
	covax_most_recent_date < index_date
label define exposurelab 1 "> 12 months" 2 "6-12 months" 3 "3-6 months" 4 "< 3 months"
label values covax_cat exposurelab


*Flu vaccine groups		
gen fluvax_cat = 0
replace fluvax_cat = 1 if ///
	fluvax_most_recent_date < index_date - 365
replace fluvax_cat = 2 if ///
	fluvax_most_recent_date > (index_date - 365) & ///
	fluvax_most_recent_date < (index_date - (30*6))
replace fluvax_cat = 3 if ///
	fluvax_most_recent_date > (index_date - (30*6)) & ///
	fluvax_most_recent_date < (index_date - (30*3))
replace fluvax_cat = 4 if ///
	fluvax_most_recent_date > (index_date - (30*3)) & ///
	fluvax_most_recent_date < index_date
label values fluvax_cat exposurelab


*Either vaccine groups
gen anyvax_cat = 0
replace anyvax_cat = 1 if ///
	anyvax_most_recent_date < index_date - 365
replace anyvax_cat = 2 if ///
	anyvax_most_recent_date > (index_date - 365) & ///
	anyvax_most_recent_date < (index_date - (30*6))
replace anyvax_cat = 3 if ///
	anyvax_most_recent_date > (index_date - (30*6)) & ///
	anyvax_most_recent_date < (index_date - (30*3))
replace anyvax_cat = 4 if ///
	anyvax_most_recent_date > (index_date - (30*3)) & ///
	anyvax_most_recent_date < index_date
label values anyvax_cat exposurelab



********************************************************************************
***** RECODE STRINGS TO NUMERIC VARIABLES **************************************
********************************************************************************


***** Change F and T to 0 and 1

foreach var of varlist _all {
	capture replace `var'="1" if `var'=="T"
	capture replace `var'="0" if `var'=="F"
}

ds, has(type string)  // Get all string variables
foreach var of varlist `r(varlist)' {

	*Check if the variable contains only "0" and "1"
	quietly {
		generate byte temp_var = inlist(`var', "0", "1")
		replace temp_var = . if missing(`var')
		count if temp_var == 0
	}
	// If the count is 0, meaning variable contains only "0" and "1", convert it
    if r(N) == 0 {
        // Convert string "0" and "1" to numeric 
		// (Stata will treat them as 0 and 1 automatically)
        generate byte num_`var' = real(`var')
        drop `var'  // Drop the original string variable
        rename num_`var' `var'  // Rename new numeric variable back to OG name
    }
    drop temp_var  // Clean up temporary variable
	}
	di "STARTING COUNT FROM IMPORT:"
	count



********************************************************************************
***** RECODE SEX ***************************************************************
********************************************************************************


assert inlist(sex, "male", "female")
gen male = (sex=="male")
drop sex
label define sexLab 1 "male" 0 "female"
label values male sexLab
label var male "sex = 0 F, 1 M"



********************************************************************************
***** RECODE IMD ***************************************************************
********************************************************************************


tab imd, m 
drop if imd==.
drop if imd==0
label define imd 1 "1 (most deprived)" 2 "2" 3 "3" 4 "4" 5 "5 (least deprived)" 
label values imd imd
tab imd



********************************************************************************
***** RECODE SMOKING ***********************************************************
********************************************************************************


label define smoke 1 "Never" 2 "Former" 3 "Current" 
gen smoke = 1  if smoking_status=="N"
replace smoke = 2  if smoking_status=="E"
replace smoke = 3  if smoking_status=="S"
replace smoke = . if smoking_status=="M"
label values smoke smoke
drop smoking_status
gen smokmiss=0
replace smokmiss=1 if smoke==.



********************************************************************************
***** ETHNICITY ****************************************************************
********************************************************************************


tab ethnicity6
gen ethnicity = 0
replace ethnicity = 1 if ethnicity6 == "White"
replace ethnicity = 2 if ethnicity6 == "Mixed"
replace ethnicity = 3 if ethnicity6 == "Asian or Asian British"
replace ethnicity = 4 if ethnicity6 == "Black"
replace ethnicity = 5 if ethnicity6 == "Other"
replace ethnicity = 6 if ethnicity6 == ""

label define ethnicity_lab 	1 "White"  								///
							2 "Mixed" 								///
							3 "Asian or Asian British"				///
							4 "Black"  								///
							5 "Other"								///
							6 "Unknown"
label values ethnicity ethnicity_lab
tab ethnicity

	
	
********************************************************************************
***** REGION *******************************************************************
********************************************************************************
	

tab region
rename region region_string
gen region_9 = 1 if region_string=="East Midlands"
replace region_9 = 2 if region_string=="East"
replace region_9 = 3 if region_string=="London"
replace region_9 = 4 if region_string=="North East"
replace region_9 = 5 if region_string=="North West"
replace region_9 = 6 if region_string=="South East"
replace region_9 = 7 if region_string=="South West"
replace region_9 = 8 if region_string=="West Midlands"
replace region_9 = 9 if region_string=="Yorkshire and The Humber"

label define region_9 	1 "East Midlands" 					///
							2 "East of England"   				///
							3 "London" 							///
							4 "North East" 						///
							5 "North West" 						///
							6 "South East" 						///
							7 "South West"						///
							8 "West Midlands" 					///
							9 "Yorkshire and The Humber"
label values region_9 region_9
label var region_9 "Region of England (9 regions)"
tab region_9



********************************************************************************
***** AGE **********************************************************************
********************************************************************************


* Check there are no missing ages
assert age < .
assert age >= 50

gen agegrp = .
replace agegrp = 1 if age >= 60 & age < 65
replace agegrp = 2 if age >= 65 & age < 70
replace agegrp = 3 if age >= 70 & age < 75
replace agegrp = 4 if age >= 75 & age < 80
replace agegrp = 5 if age >= 80 & age < 85
replace agegrp = 6 if age >= 85 & age < 90
replace agegrp = 7 if age >= 90
label define agelab 1 "60-64" 2 "65-69" 3 "70-74" 4 "75-79" 5 "80-84" 6 "85-89" 7 "90+"
label values agegrp agelab

drop if age<50



********************************************************************************
***** SBP **********************************************************************
********************************************************************************

			// 	requires ehrQL coding first

*	sum sbp, d
*	*set plausible values for SBP
*	replace sbp=. if sbp>300 & sbp!=.
*	replace sbp=. if sbp<20
*	gen sbpmiss=0
*	replace sbpmiss=1 if sbp==.



********************************************************************************
***** BMI **********************************************************************
********************************************************************************


sum bmi, d
	*set plausible values for BMI
	replace bmi=. if bmi<10
	replace bmi=. if bmi>70
gen bmimiss=0
replace bmimiss=1 if bmi==.



********************************************************************************
***** eGFR *********************************************************************
********************************************************************************


		// requires ehrQL coding first


*	* Set implausible creatinine values to missing (Note: zero changed to missing)
*	replace creatinine = . if !inrange(creatinine, 20, 3000) 
*			
*	* Divide by 88.4 (to convert umol/l to mg/dl)
*	gen SCr_adj = creatinine/88.4
*
*	gen min=.
*	replace min = SCr_adj/0.7 if male==0
*	replace min = SCr_adj/0.9 if male==1
*	replace min = min^-0.329  if male==0
*	replace min = min^-0.411  if male==1
*	replace min = 1 if min<1
*
*	gen max=.
*	replace max=SCr_adj/0.7 if male==0
*	replace max=SCr_adj/0.9 if male==1
*	replace max=max^-1.209
*	replace max=1 if max>1
*
*	gen egfr=min*max*141
*	replace egfr=egfr*(0.993^age)
*	replace egfr=egfr*1.018 if male==0
*	label var egfr "egfr calculated using CKD-EPI formula with no eth"	



********************************************************************************
***** CHECK ALL INCLUSION AND EXCLUSION CRITERIA ARE MET ***********************
********************************************************************************

		
*check that all patients have a hip fracture
drop if hf_primary_diagnosis == ""
count 
		
*check that all patients are aged 18+ at study start
drop if age<50
count 
		
*drop patients who ended follow up before study start
drop if dereg_date < index_date
count
		
*drop patients who died before study start
drop if death_date < index_date
count

*drop if follow up ended before study start
	/////////////
		
*drop patients who aren't male or female
tab male, nol
drop if male!=0 & male!=1
count 
		
*drop if IMD is missing
tab imd, m
tab imd, nol
drop if imd>5
count



********************************************************************************
***** SAVE DATASET FOR USE IN ANALYSIS FILES ***********************************
********************************************************************************

save analytical_cohort.dta, replace
log close