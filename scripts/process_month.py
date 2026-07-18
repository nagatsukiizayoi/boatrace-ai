# Command-line entry point for monthly historical ETL.
from __future__ import annotations
import argparse
import json
from boatrace_ai.pipelines.monthly_etl import run_monthly_etl

def build_parser():
    parser=argparse.ArgumentParser(description="Process one month of official BOAT RACE data")
    parser.add_argument("--month",required=True,help="Target month in YYYY-MM format")
    parser.add_argument("--data-root",required=True,help="Private Google Drive data root")
    parser.add_argument("--start-day",type=int,default=1)
    parser.add_argument("--end-day",type=int)
    parser.add_argument("--wait-seconds",type=float,default=2.0)
    parser.add_argument("--overwrite-outputs",action="store_true")
    parser.add_argument("--overwrite-archives",action="store_true")
    parser.add_argument("--stop-on-error",action="store_true")
    return parser

def main(argv=None):
    args=build_parser().parse_args(argv)
    year_text,month_text=args.month.split("-",1)
    summary=run_monthly_etl(year=int(year_text),month=int(month_text),data_root=args.data_root,start_day=args.start_day,end_day=args.end_day,wait_seconds=args.wait_seconds,overwrite_outputs=args.overwrite_outputs,overwrite_archives=args.overwrite_archives,continue_on_error=not args.stop_on_error)
    public_summary={key:value for key,value in summary.items() if key!="details"}
    print(json.dumps(public_summary,ensure_ascii=False,indent=2))
    return 0 if summary["days_failed"]==0 else 1

if __name__=="__main__":
    raise SystemExit(main())
