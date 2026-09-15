"""Internal counts only: activity in this cohort does not prove source completeness."""
from ehrql import create_measures, INTERVAL, claim_permissions
from datetime import date, timedelta
from cohort_definition import eligible_population, followup_end
from source_activity import source_counts
from study_config import HISTORY_START, DATA_END

claim_permissions("sgss_covid_all_tests")
intervals = []
start = HISTORY_START
while start <= DATA_END:
    next_month = date(start.year + (start.month == 12), start.month % 12 + 1, 1)
    intervals.append((start, min(next_month - timedelta(days=1), DATA_END)))
    start = next_month

measures = create_measures()
# Every output is highly_sensitive; exact counts are needed for internal reconciliation.
measures.configure_disclosure_control(enabled=False)
measures.define_defaults(
    denominator=eligible_population & (followup_end >= INTERVAL.start_date),
    intervals=intervals,
)
for name, count in source_counts(INTERVAL.start_date, INTERVAL.end_date, followup_end).items():
    measures.define_measure(name, numerator=count)
