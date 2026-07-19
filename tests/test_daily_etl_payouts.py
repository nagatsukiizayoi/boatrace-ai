import pandas as pd
import pytest

import boatrace_ai.pipelines.daily_etl as daily_etl


BET_TYPES=["WIN","PLACE","EXACTA","QUINELLA","QUINELLA_PLACE","TRIFECTA","TRIO"]


def make_daily_frames():
    program=pd.DataFrame([{"race_date":"2026-06-30","venue_code":"01","race_no":1,"boat_no":boat_no,"racer_id":str(5000+boat_no),"racer_name":"選手名{}".format(boat_no),"motor_no":10+boat_no,"boat_no_equipment":20+boat_no} for boat_no in range(1,7)])
    result=program[["race_date","venue_code","race_no","boat_no","racer_id"]].copy()
    result["racer_name_result"]=program["racer_name"]
    result["motor_no_result"]=program["motor_no"]
    result["boat_no_equipment_result"]=program["boat_no_equipment"]
    result["finish_raw"]=result["boat_no"].map(lambda value:"{:02d}".format(value))
    result["finish_position"]=result["boat_no"]
    return program,result


def make_payouts(include_trio=True):
    combinations={"WIN":"1","PLACE":"1","EXACTA":"1-2","QUINELLA":"1-2","QUINELLA_PLACE":"1-2","TRIFECTA":"1-2-3","TRIO":"1-2-3"}
    rows=[]
    for bet_type in BET_TYPES:
        if bet_type=="TRIO" and not include_trio:
            continue
        rows.append({"race_date":"2026-06-30","venue_code":"01","race_no":1,"bet_type":bet_type,"selection_no":1,"combination":combinations[bet_type],"payout_yen":100,"popularity":pd.NA if bet_type in {"WIN","PLACE"} else 1,"payout_status":"NORMAL","source_kind":"DETAIL"})
    return pd.DataFrame(rows)


def install_mocks(monkeypatch,program,result,payouts):
    monkeypatch.setattr(daily_etl,"parse_program_file",lambda *args,**kwargs:program.copy())
    monkeypatch.setattr(daily_etl,"parse_result_file",lambda *args,**kwargs:result.copy())
    monkeypatch.setattr(daily_etl,"parse_payout_file",lambda *args,**kwargs:payouts.copy(),raising=False)


def test_daily_etl_writes_payout_and_quality(monkeypatch,tmp_path):
    program,result=make_daily_frames()
    payouts=make_payouts()
    install_mocks(monkeypatch,program,result,payouts)
    outcome=daily_etl.process_daily_files(tmp_path/"B260630.TXT",tmp_path/"K260630.TXT","2026-06-30",tmp_path)
    assert outcome["paths"]["payout"].exists()
    saved=pd.read_parquet(outcome["paths"]["payout"])
    assert len(saved)==7
    quality=outcome["quality"]
    assert quality["payout_record_count"]==7
    assert quality["payout_group_count"]==7
    assert quality["payout_expected_group_count"]==7
    assert quality["payout_missing_group_count"]==0
    assert quality["payout_unexpected_group_count"]==0
    assert quality["payout_duplicate_keys"]==0
    assert quality["payout_invalid_combinations"]==0
    assert quality["payout_invalid_amounts"]==0
    assert quality["payout_invalid_popularity"]==0
    assert quality["payout_summary_mismatch"]==0


def test_daily_etl_rejects_missing_payout_group(monkeypatch,tmp_path):
    program,result=make_daily_frames()
    payouts=make_payouts(include_trio=False)
    install_mocks(monkeypatch,program,result,payouts)
    with pytest.raises(daily_etl.DailyETLError) as captured:
        daily_etl.process_daily_files(tmp_path/"B260630.TXT",tmp_path/"K260630.TXT","2026-06-30",tmp_path)
    assert "payout_missing_group_count" in str(captured.value)
