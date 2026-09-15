"""Synthetic assurance cases for cohort selection, timing, and censoring."""
from datetime import date, timedelta
from dataset_definition import dataset

D = date.fromisoformat
INDEX = D("2023-06-15")

def admission(day=INDEX, codes="S720", method="21", ident=1):
    return {"apcs_ident": ident, "admission_date": day,
            "discharge_date": day + timedelta(days=10),
            "all_diagnoses": codes, "primary_diagnosis": codes.split(" || ")[0],
            "all_procedures": "", "admission_method": method}

def base():
    return {
        "patients": {"date_of_birth": D("1950-01-01"), "sex": "female"},
        "apcs": [admission()],
        "practice_registrations": [{"start_date": D("2010-01-01"),
                                    "end_date": None, "practice_pseudo_id": 1}],
        "ons_deaths": [], "clinical_events": [], "vaccinations": [],
        "addresses": [], "ethnicity_from_sus": [], "sgss_covid_all_tests": [],
        "expected_in_population": True,
        "expected_columns": {"index_date": INDEX, "age": 73, "mace365": False},
    }

def vaccine(day, target="SARS-2 CORONAVIRUS"):
    return {"date": day, "target_disease": target, "product_name": "Synthetic vaccine"}

test_data = {}
# Exact endpoints of the baseline year and the separate same-day category.
for i, offset in enumerate([1, 90, 91, 180, 181, 365, 366], 1):
    p = base(); day = INDEX - timedelta(days=offset)
    p["vaccinations"] = [vaccine(day)]
    p["expected_columns"].update(covax_most_recent_before_index=day,
        covax_within_365d=(offset <= 365), covax_prior365_n=int(offset <= 365))
    test_data[i] = p

p=base(); p["vaccinations"]=[vaccine(INDEX)]
p["expected_columns"].update(covax_within_365d=False, covax_index_day=True,
    covax_most_recent_before_index=None, covax_post30_date=None)
test_data[8]=p

# An eighth historical dose must not hide vaccination in the post-fracture month.
p=base(); p["vaccinations"]=[vaccine(D(f"{y}-03-01")) for y in range(2017,2024)]
p["vaccinations"] += [vaccine(INDEX+timedelta(days=30)), vaccine(INDEX+timedelta(days=30))]
p["expected_columns"].update(covax_post30_date=INDEX+timedelta(days=30),covax_post30_n=1)
test_data[9]=p
p=base(); p["vaccinations"]=[vaccine(INDEX+timedelta(days=31))]
p["expected_columns"].update(covax_post30_date=None,covax_first_post_date=INDEX+timedelta(days=31))
test_data[10]=p

# Death and deregistration bound non-fatal outcomes and vaccination.
for ident, source in [(11,"ons"),(12,"dereg"),(13,"gp")]:
    p=base(); end=INDEX+timedelta(days=5)
    if source=="ons": p["ons_deaths"]=[{"date":end,"underlying_cause_of_death":"C349"}]
    elif source=="gp": p["patients"]["date_of_death"]=end
    else: p["practice_registrations"][0]["end_date"]=end
    p["apcs"].append(admission(INDEX+timedelta(days=8),"I210",ident=2))
    p["vaccinations"]=[vaccine(INDEX+timedelta(days=8))]
    p["expected_columns"].update(followup_end_date=end,mi365=False,covax_post30_date=None,
        all_cause_death_date=end if source != "dereg" else None)
test_data[ident]=p

# Cardiovascular death exactly on the censor date remains an event.
p=base(); end=INDEX+timedelta(days=5)
p["ons_deaths"]=[{"date":end,"underlying_cause_of_death":"I210","cause_of_death_01":"I210"}]
p["expected_columns"].update(cvddeath_date=end,mace_date=end,mace365=True)
test_data[14]=p

# Earlier in-window traumatic fracture: later fracture cannot become first recorded.
p=base(); p["apcs"].append(admission(D("2021-06-01"),"S720 || V010",ident=2))
p["expected_in_population"]=False; test_data[15]=p
for ident, dob, included in [(16,"1973-06-01",True),(17,"1973-07-01",False)]:
    p=base();p["patients"]["date_of_birth"]=D(dob)
    p["expected_in_population"]=included;p["expected_columns"]={"age":50}
    test_data[ident]=p
p=base();p["practice_registrations"]=[];p["expected_in_population"]=False;test_data[18]=p
p=base();p["patients"]["date_of_death"]=INDEX-timedelta(days=1);p["expected_in_population"]=False;test_data[19]=p

# Same-spell diagnosis: flag uncertainty, do not invent an onset date.
p=base();p["apcs"]=[admission(codes="S720 || I210")]
p["expected_columns"].update(index_spell_mi=True,mi_date=None);test_data[20]=p
for ident, offset in [(21,365),(22,366)]:
    p=base();day=INDEX+timedelta(days=offset)
    p["apcs"].append(admission(day,"I210",ident=2))
    p["expected_columns"].update(mi_date=day if offset==365 else None,mace365=(offset==365))
    test_data[ident]=p
p=base();p["vaccinations"]=[vaccine(D("2016-12-01"),"INFLUENZA")]
p["expected_columns"].update(fluvax_most_recent_before_index=None);test_data[23]=p

# Calendar month can include day 31; the two windows are deliberately distinct.
p=base(); july=D("2023-07-15"); aug=D("2023-08-15")
p["apcs"]=[admission(july)];p["vaccinations"]=[vaccine(aug)]
p["expected_columns"].update(index_date=july,covax_post30_date=None,covax_month1_date=aug)
test_data[24]=p

# Death after administrative follow-up is not an observed competing event.
p=base(); p["ons_deaths"]=[{"date":D("2025-01-01"),"underlying_cause_of_death":"C349"}]
p["expected_columns"].update(all_cause_death_date=None)
test_data[25]=p

# The extraction window must not expose later death dates.
p=base();p["patients"]["date_of_death"]=D("2026-02-01")
p["expected_columns"].update(gp_death_date=None,all_cause_death_date=None)
test_data[26]=p

# Conflicting death dates are retained and flagged; the first ends follow-up.
p=base();end=INDEX+timedelta(days=5)
p["patients"]["date_of_death"]=end
p["ons_deaths"]=[{"date":end+timedelta(days=1),"underlying_cause_of_death":"C349"}]
p["expected_columns"].update(death_dates_disagree=True,all_cause_death_date=end,followup_end_date=end)
test_data[27]=p
