********************************************************************************
* Project 209 feasibility preparation, based on James Webster's original script.
* Categories are descriptive and provisional pending the statistical analysis plan.
********************************************************************************
version 16
clear all
set more off
capture log close
log using output/define_covariates.log, text replace
import delimited output/dataset.csv, clear varnames(1) stringcols(_all)
isid patient_id

* Dates retain their ehrQL names. Import as strings even when entirely missing.
ds
foreach var of varlist `r(varlist)' {
    if regexm("`var'", "_date$") | inlist("`var'", "gp_registration_start", "covax_most_recent_before_index", "fluvax_most_recent_before_index", "covid_positive_before_index") {
        tempvar parsed_date
        gen double `parsed_date' = daily(`var', "YMD")
        assert !missing(`parsed_date') if !missing(`var')
        drop `var'
        rename `parsed_date' `var'
        format `var' %td
    }
}
* Convert only known boolean fields. F/T can be valid codes in other columns.
* Keep identifiers, ethnicity source codes and clinical code strings intact.
destring age imd_quintile bmi covax_prior365_n fluvax_prior365_n covax_post30_n fluvax_post30_n, replace
ds prior_* *365 *_within_365d index_spell_* index_primary_hip index_hip_* reg_2y_at_index *_index_day *_during_spell neg_con_1 death_dates_disagree
foreach var of varlist `r(varlist)' {
    assert inlist(`var', "", "T", "F", "1", "0")
    quietly replace `var' = "1" if `var' == "T"
    quietly replace `var' = "0" if `var' == "F"
    destring `var', replace
}
gen double death_date = all_cause_death_date
gen double covax_most_recent_date = covax_most_recent_before_index
gen double fluvax_most_recent_date = fluvax_most_recent_before_index
egen double anyvax_most_recent_date = rowmax(covax_most_recent_date fluvax_most_recent_date)
format death_date *_most_recent_date %td

* Disjoint day ranges include all boundaries; same-day records remain separate.
label define exposurelab 0 "No prior record since 2017" 1 ">365 days" 2 "181-365 days" 3 "91-180 days" 4 "1-90 days"
foreach vaccine in covax fluvax anyvax {
    gen double days_since_last_`vaccine' = index_date - `vaccine'_most_recent_date
    assert days_since_last_`vaccine' >= 1 if !missing(days_since_last_`vaccine')
    gen byte `vaccine'_cat = 0
    replace `vaccine'_cat = 1 if days_since_last_`vaccine' > 365 & !missing(days_since_last_`vaccine')
    replace `vaccine'_cat = 2 if inrange(days_since_last_`vaccine', 181, 365)
    replace `vaccine'_cat = 3 if inrange(days_since_last_`vaccine', 91, 180)
    replace `vaccine'_cat = 4 if inrange(days_since_last_`vaccine', 1, 90)
    label values `vaccine'_cat exposurelab
}
assert inlist(sex, "male", "female")
gen byte male = (sex == "male")
label define sexlab 0 "Female" 1 "Male"
label values male sexlab
assert age >= 50 & age < .
gen byte agegrp = 1 if age < 60
replace agegrp = 2 if inrange(age, 60, 69)
replace agegrp = 3 if inrange(age, 70, 79)
replace agegrp = 4 if inrange(age, 80, 89)
replace agegrp = 5 if age >= 90 & age < .
label define agelab 1 "50-59" 2 "60-69" 3 "70-79" 4 "80-89" 5 "90+"
label values agegrp agelab

* Preparation preserves missingness; model-specific exclusions must be counted.
gen byte imd = imd_quintile
replace imd = . if imd == 0
label define imdlab 1 "1 most deprived" 2 "2" 3 "3" 4 "4" 5 "5 least deprived"
label values imd imdlab
gen byte ethnicity = 6
replace ethnicity = 1 if ethnicity6 == "White"
replace ethnicity = 2 if ethnicity6 == "Mixed"
replace ethnicity = 3 if ethnicity6 == "Asian or Asian British"
replace ethnicity = 4 if ethnicity6 == "Black or Black British"
replace ethnicity = 5 if ethnicity6 == "Chinese or Other Ethnic Groups"
label define ethnicitylab 1 "White" 2 "Mixed" 3 "Asian" 4 "Black" 5 "Chinese or other" 6 "Missing"
label values ethnicity ethnicitylab
gen byte smoke = .
replace smoke = 1 if smoking_status == "N"
replace smoke = 2 if smoking_status == "E"
replace smoke = 3 if smoking_status == "S"
label define smokelab 1 "Never" 2 "Former" 3 "Current"
label values smoke smokelab
gen byte smokmiss = missing(smoke)
gen byte bmimiss = missing(bmi)
assert followup_end_date >= index_date
assert followup_end_date <= admin_end_date
foreach outcome in mi stroke cvddeath mace {
    assert `outcome'_date > index_date if !missing(`outcome'_date)
    assert `outcome'_date <= followup_end_date if !missing(`outcome'_date)
}
gen double followup_days = followup_end_date - index_date
* No automatic exclusion for absent IMD, primary diagnosis, or zero follow-up.
save output/analytical_cohort.dta, replace
log close
