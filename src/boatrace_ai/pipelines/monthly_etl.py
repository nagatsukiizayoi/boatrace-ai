# Monthly BOAT RACE historical ETL pipeline.
from __future__ import annotations
import calendar
import datetime as dt
import json
import time
from pathlib import Path
from boatrace_ai.pipelines.daily_etl import run_daily_etl

class MonthlyETLError(RuntimeError):
    pass

def month_dates(year,month,start_day=1,end_day=None):
    year=int(year)
    month=int(month)
    if month<1 or month>12:
        raise ValueError("monthは1から12で指定してください")
    last_day=calendar.monthrange(year,month)[1]
    end_day=last_day if end_day is None else int(end_day)
    start_day=int(start_day)
    if start_day<1 or end_day>last_day or start_day>end_day:
        raise ValueError("日付範囲が不正です: {}-{}-{}..{}".format(year,month,start_day,end_day))
    return [dt.date(year,month,day) for day in range(start_day,end_day+1)]

def build_summary_path(year,month,data_root):
    return Path(data_root)/"system"/"monthly_runs"/"{:04d}{:02d}".format(int(year),int(month))/"monthly_etl_summary.json"

def atomic_json(value,path):
    destination=Path(path)
    destination.parent.mkdir(parents=True,exist_ok=True)
    temporary=destination.with_name(destination.name+".tmp")
    temporary.write_text(json.dumps(value,ensure_ascii=False,indent=2),encoding="utf-8")
    temporary.replace(destination)

def run_monthly_etl(year,month,data_root,start_day=1,end_day=None,wait_seconds=2.0,overwrite_outputs=False,overwrite_archives=False,continue_on_error=True,summary_path=None,runner=None,sleep_fn=None):
    dates=month_dates(year,month,start_day=start_day,end_day=end_day)
    root=Path(data_root)
    runner=run_daily_etl if runner is None else runner
    sleep_fn=time.sleep if sleep_fn is None else sleep_fn
    destination=Path(summary_path) if summary_path is not None else build_summary_path(year,month,root)
    started_at=dt.datetime.now(dt.timezone.utc)
    details=[]
    record_count=0
    race_count=0
    venue_day_count=0
    days_success=0
    days_skipped=0
    days_failed=0
    for index,race_date in enumerate(dates):
        day_started=dt.datetime.now(dt.timezone.utc)
        try:
            result=runner(race_date,root,overwrite_outputs=overwrite_outputs,overwrite_archives=overwrite_archives)
            quality=result.get("quality",{})
            skipped=bool(result.get("skipped",False))
            day_status="SKIPPED" if skipped else "SUCCESS"
            if skipped:
                days_skipped+=1
            else:
                days_success+=1
            day_records=int(quality.get("record_count",0))
            day_races=int(quality.get("race_count",0))
            day_venues=int(quality.get("venue_count",0))
            record_count+=day_records
            race_count+=day_races
            venue_day_count+=day_venues
            details.append({"race_date":race_date.isoformat(),"status":day_status,"record_count":day_records,"race_count":day_races,"venue_count":day_venues,"special_finish_count":int(quality.get("special_finish_count",0)),"quality_status":quality.get("status"),"started_at":day_started.isoformat(),"finished_at":dt.datetime.now(dt.timezone.utc).isoformat()})
        except Exception as exc:
            days_failed+=1
            details.append({"race_date":race_date.isoformat(),"status":"FAILED","error_type":type(exc).__name__,"error_message":str(exc)[:1000],"started_at":day_started.isoformat(),"finished_at":dt.datetime.now(dt.timezone.utc).isoformat()})
            if not continue_on_error:
                break
        if index<len(dates)-1 and float(wait_seconds)>0:
            sleep_fn(float(wait_seconds))
    finished_at=dt.datetime.now(dt.timezone.utc)
    covered_days=days_success+days_skipped
    if days_failed==0 and len(details)==len(dates):
        status="SUCCESS"
    elif covered_days>0:
        status="PARTIAL"
    else:
        status="FAILED"
    summary={"target_month":"{:04d}-{:02d}".format(int(year),int(month)),"status":status,"days_total":len(dates),"days_processed":len(details),"days_success":days_success,"days_skipped":days_skipped,"days_failed":days_failed,"record_count":record_count,"race_count":race_count,"venue_day_count":venue_day_count,"start_day":int(start_day),"end_day":dates[-1].day,"wait_seconds":float(wait_seconds),"overwrite_outputs":bool(overwrite_outputs),"overwrite_archives":bool(overwrite_archives),"started_at":started_at.isoformat(),"finished_at":finished_at.isoformat(),"details":details}
    atomic_json(summary,destination)
    summary["summary_path"]=str(destination)
    return summary
