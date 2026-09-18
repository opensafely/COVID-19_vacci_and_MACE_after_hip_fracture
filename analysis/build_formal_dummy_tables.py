"""Generate reproducible synthetic input for the normal OpenSAFELY workflow.

These invented patients deliberately have frequent outcomes and balanced exposures
so local opensafely run exercises estimation, imputation and competing-risk code.
They are not representative clinical data and must never be used as study results.
The original dummy-tables remain intact. This writes only dummy-tables/small.
"""
import csv
import random
from datetime import date, timedelta
from pathlib import Path


def build(n=4000):
    rng = random.Random(2092026)
    root = Path("dummy-tables")
    destination = root / "small"
    destination.mkdir(exist_ok=True)
    fields = {p.name: next(csv.reader(p.open())) for p in root.glob("*.csv")}
    fields["clinical_events.csv"] += ["ctv3_code"]
    fields["addresses.csv"] += ["care_home_is_potential_match"]
    rows = {name: [] for name in fields}
    def add(table, **values):
        rows[table + ".csv"].append(values)
    def code(filename, column="code"):
        return next(csv.DictReader((Path("codelists")/filename).open()))[column]
    illnesses = ["bristol-multimorbidity_coronary-heart-disease.csv",
        "nhsd-primary-care-domain-refsets-hyp_cod.csv", "bristol-multimorbidity_diabetes.csv",
        "primis-covid19-vacc-uptake-ckd35.csv", "bristol-multimorbidity_copd.csv",
        "bristol-multimorbidity_cancer.csv", "bristol-multimorbidity_dementia.csv"]
    medications = [("nhs-drug-refsets-stat_cod.csv","code"),
        ("nhs-drug-refsets-antihyp_cod.csv","code"),
        ("user-elsie_horne-antiplatelet_dmd.csv","dmd_id"),
        ("user-elsie_horne-anticoagulant_dmd.csv","dmd_id"),
        ("opensafely-alendronic-acid.csv","code")]
    illness_codes = [code(f) for f in illnesses]
    medicine_codes = [code(f,c) for f,c in medications]
    ami = code("project-acute-mi-snomed.csv")
    stroke = code("project-ischaemic-stroke-snomed.csv")
    egfr = code("bristol-multimorbidity_chronic-kidney-disease.csv")
    regions = ["North East", "North West", "Yorkshire and The Humber", "East Midlands",
        "West Midlands", "East", "London", "South East", "South West"]
    for patient in range(1,n+1):
        index = date(2022,1,1) + timedelta(days=rng.randrange(1096))
        def day(offset): return str(index+timedelta(days=offset))
        def event(offset, snomed="", ctv3="", value=""):
            add("clinical_events",patient_id=patient,date=day(offset),snomedct_code=snomed,
                ctv3_code=ctv3,numeric_value=value)
        def admission(offset, diagnosis, procedure="", method="21"):
            add("apcs", patient_id=patient, apcs_ident=len(rows["apcs.csv"])+1,
                admission_date=day(offset),discharge_date=day(offset+5),admission_method=method,
                primary_diagnosis=diagnosis,secondary_diagnosis="",all_diagnoses=diagnosis+",",
                all_procedures=procedure+"," if procedure else "",spell_core=1)
        age = rng.randint(51,96)
        death = rng.randint(65,365) if rng.random()<.5 else None
        add("patients", patient_id=patient,date_of_birth=f"{index.year-age}-01-01",
            sex=rng.choice(["female","male"]),date_of_death=day(death) if death else "")
        if death:
            cause = "I219" if rng.random()<.75 else "J449"
            add("ons_deaths",patient_id=patient,date=day(death),underlying_cause_of_death=cause,cause_of_death_01=cause)
        reg = dict(patient_id=patient,practice_pseudo_id=rng.randint(1,200),
            practice_nuts1_region_name=rng.choice(regions),practice_systmone_go_live_date="2010-01-01")
        start = -rng.randint(800,1800)
        # Coverage examples: adjacency, overlap, real gap, duplicate start, and
        # two adjacent prior registrations supplying the required baseline year.
        pattern = patient % 20
        if pattern in (0,1,2,3):
            end = rng.randint(45,240)
            add("practice_registrations",**reg,start_date=day(start),end_date=day(end))
            add("practice_registrations",**reg,start_date=day(end + {0:1,1:-10,2:10,3:1}[pattern]),end_date="")
            if pattern==3:
                add("practice_registrations",**reg,start_date=day(start),end_date=day(end+15))
        elif pattern==4:
            add("practice_registrations",**reg,start_date=day(start),end_date=day(-101))
            add("practice_registrations",**reg,start_date=day(-100),end_date="")
        else:
            add("practice_registrations",**reg,start_date=day(start),end_date="")
        add("addresses",patient_id=patient,address_id=patient,start_date="2000-01-01",end_date="",
            imd_rounded=rng.randint(1,32800),rural_urban_classification=rng.randint(1,8),
            care_home_is_potential_match=rng.choices(["T","F",""],[.25,.65,.1])[0])
        add("ethnicity_from_sus",patient_id=patient,code=rng.choices(["A","C","H","M","R",""],[.35,.12,.15,.12,.16,.1])[0])
        admission(0,rng.choice(["S720","S721","S722"]),rng.choice(["W371","W461","W241"]))
        if rng.random()<.25: admission(-rng.randint(60,700),"S525")
        for c in illness_codes:
            if rng.random()<.3: event(-rng.randint(10,700),c)
        for c in medicine_codes:
            if rng.random()<.35: add("medications",patient_id=patient,date=day(-rng.randint(1,365)),dmd_code=c)
        if rng.random()<.9: event(-rng.randint(5,1700),"60621009",value=round(rng.uniform(16,39),1))
        if rng.random()<.9: event(-rng.randint(10,600),ctv3=rng.choice(["XE0oh","137K.","137R."]))
        event(-150,egfr,value=rng.randint(30,110))
        if rng.random()<.25: add("sgss_covid_all_tests",patient_id=patient,specimen_taken_date=day(-rng.randint(10,500)),is_positive="T")
        for target in ["SARS-2 CORONAVIRUS","INFLUENZA"]:
            timing = rng.choice([None,450,270,135,45])
            if timing: add("vaccinations",patient_id=patient,date=day(-timing),target_disease=target,product_name="Synthetic vaccine")
            if rng.random()<.5: add("vaccinations",patient_id=patient,date=day(rng.randint(1,30)),target_disease=target,product_name="Synthetic vaccine")
        for c,icd in [(ami,"I219"),(stroke,"I639")]:
            if rng.random()<.5:
                t=rng.randint(31,365)
                event(t,c)
                if rng.random()<.9: admission(t,icd)
        if rng.random()<.5: admission(rng.randint(31,365),"H259","C711","11")
    for name, data in rows.items():
        with (destination/name).open("w",newline="") as f:
            writer=csv.DictWriter(f,fields[name],lineterminator="\n")
            writer.writeheader();writer.writerows(data)
        print(name,len(data),"synthetic rows")

if __name__ == "__main__":
    build()
