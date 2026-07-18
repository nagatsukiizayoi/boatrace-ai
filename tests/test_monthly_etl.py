import datetime as dt
from boatrace_ai.pipelines.monthly_etl import build_summary_path,month_dates,run_monthly_etl

def make_quality(date_value):
    return {"race_date":date_value.isoformat(),"record_count":6,"race_count":1,"venue_count":1,"special_finish_count":0,"status":"SUCCESS"}

def test_month_dates_leap_year():
    dates=month_dates(2024,2)
    assert len(dates)==29
    assert dates[0]==dt.date(2024,2,1)
    assert dates[-1]==dt.date(2024,2,29)

def test_month_dates_partial_range():
    dates=month_dates(2026,6,start_day=28,end_day=30)
    assert [value.day for value in dates]==[28,29,30]

def test_build_summary_path(tmp_path):
    path=build_summary_path(2026,6,tmp_path)
    assert path==tmp_path/"system"/"monthly_runs"/"202606"/"monthly_etl_summary.json"

def test_monthly_success_and_skip(tmp_path):
    def fake_runner(race_date,data_root,overwrite_outputs=False,overwrite_archives=False):
        return {"quality":make_quality(race_date),"skipped":race_date.day==2}
    summary=run_monthly_etl(2026,6,tmp_path,start_day=1,end_day=3,wait_seconds=0,runner=fake_runner)
    assert summary["status"]=="SUCCESS"
    assert summary["days_total"]==3
    assert summary["days_success"]==2
    assert summary["days_skipped"]==1
    assert summary["days_failed"]==0
    assert summary["record_count"]==18
    assert summary["race_count"]==3

def test_monthly_continue_after_failure(tmp_path):
    def failing_runner(race_date,data_root,overwrite_outputs=False,overwrite_archives=False):
        if race_date.day==2:
            raise RuntimeError("test failure")
        return {"quality":make_quality(race_date),"skipped":False}
    summary=run_monthly_etl(2026,6,tmp_path,start_day=1,end_day=3,wait_seconds=0,runner=failing_runner,continue_on_error=True)
    assert summary["status"]=="PARTIAL"
    assert summary["days_success"]==2
    assert summary["days_failed"]==1
    assert summary["details"][1]["error_type"]=="RuntimeError"
