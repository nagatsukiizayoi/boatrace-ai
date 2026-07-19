from __future__ import annotations
import pandas as pd
import pytest
from boatrace_ai.parsers.program import parse_program_text
from boatrace_ai.parsers.result import parse_result_text
import boatrace_ai.pipelines.daily_etl as daily_etl

def test_program_splits_equipment_62_from_100_percent(): text="01BBGN\n1R TEST H1800m\n1 4926吉川貴仁32三重52A1 7.79 64.76 9.86100.00 67 41.18 62100.00             12"; frame=parse_program_text(text,race_date="2026-06-06",source_file="B260606.TXT",validate_structure=False); assert len(frame)==1; assert int(frame.loc[0,"boat_no_equipment"])==62; assert float(frame.loc[0,"boat_place2_rate_pct"])==100.00

def test_result_accepts_1200m_and_finish_00(): text="03KBGN\n8R TEST H1200m\n  00  5 4568 井 芹    大 志 25   61  6.80   5    0.15      .  ."; frame=parse_result_text(text,race_date="2026-06-06",source_file="K260606.TXT",validate_structure=False); assert len(frame)==1; row=frame.iloc[0]; assert int(row["race_no"])==8; assert row["finish_raw"]=="00"; assert int(row["boat_no"])==5; assert row["racer_name_result"]=="井芹大志"; assert pd.isna(row["finish_position"])

def test_ordered_name_abbreviation(): program=pd.Series(["大豆生蒼","大豆生蒼"]); result=pd.Series(["大豆生田蒼","大生豆田蒼"]); matches=daily_etl.names_are_prefixes(program,result); assert matches.tolist()==[True,False]

def make_daily_frames(result_boats): program=pd.DataFrame([{"race_date":"2026-06-27","venue_code":"04","race_no":1,"boat_no":boat_no,"racer_id":str(5000+boat_no),"racer_name":"選手名{}".format(boat_no),"motor_no":10+boat_no,"boat_no_equipment":20+boat_no} for boat_no in range(1,7)]); result=program[["race_date","venue_code","race_no","boat_no","racer_id"]].copy(); result["racer_name_result"]=program["racer_name"]; result["motor_no_result"]=program["motor_no"]; result["boat_no_equipment_result"]=program["boat_no_equipment"]; result["finish_raw"]=result["boat_no"].map(lambda value:"{:02d}".format(value)); result["finish_position"]=result["boat_no"]; result=result.loc[result["boat_no"].isin(result_boats)].reset_index(drop=True); return program,result

def test_complete_result_absence_is_cancelled(monkeypatch,tmp_path): program,result=make_daily_frames([]); monkeypatch.setattr(daily_etl,"parse_program_file",lambda *args,**kwargs:program.copy()); monkeypatch.setattr(daily_etl,"parse_result_file",lambda *args,**kwargs:result.copy()); outcome=daily_etl.process_daily_files(tmp_path/"B260627.TXT",tmp_path/"K260627.TXT","2026-06-27",tmp_path); quality=outcome["quality"]; assert quality["status"]=="SUCCESS"; assert quality["left_only"]==6; assert quality["right_only"]==0; assert quality["partial_result_missing"]==0; assert quality["cancelled_entry_count"]==6; assert quality["cancelled_race_count"]==1; assert quality["finish_status_counts"]["CANCELLED"]==6; assert quality["cancelled_races"]==[{"race_date":"2026-06-27","venue_code":"04","race_no":1}]; merged=pd.read_parquet(outcome["paths"]["merged"]); assert len(merged)==6; assert not merged["result_available"].any(); assert merged["race_cancelled"].all()

def test_partial_result_absence_is_error(monkeypatch,tmp_path): program,result=make_daily_frames([1,2,3,4,5]); monkeypatch.setattr(daily_etl,"parse_program_file",lambda *args,**kwargs:program.copy()); monkeypatch.setattr(daily_etl,"parse_result_file",lambda *args,**kwargs:result.copy()); captured=pytest.raises(daily_etl.DailyETLError,daily_etl.process_daily_files,tmp_path/"B260627.TXT",tmp_path/"K260627.TXT","2026-06-27",tmp_path); assert "partial_result_missing=1" in str(captured.value)
