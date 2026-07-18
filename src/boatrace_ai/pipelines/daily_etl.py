# Daily BOAT RACE ETL pipeline.
from __future__ import annotations
import datetime as dt
import json
from pathlib import Path
import pandas as pd
from boatrace_ai.ingestion.daily_archives import download_and_extract_daily
from boatrace_ai.parsers.program import parse_program_file
from boatrace_ai.parsers.result import parse_result_file

JOIN_KEYS=["race_date","venue_code","race_no","boat_no","racer_id"]
RACE_KEYS=["race_date","venue_code","race_no"]

class DailyETLError(RuntimeError):
    pass

def normalize_date(value):
    if isinstance(value,dt.datetime):
        return value.date()
    if isinstance(value,dt.date):
        return value
    return dt.date.fromisoformat(str(value))

def build_output_paths(race_date,data_root):
    date_value=normalize_date(race_date)
    parts=(date_value.strftime("%Y"),date_value.strftime("%m"),date_value.strftime("%d"))
    root=Path(data_root)
    return {"program":root/"curated"/"entries"/parts[0]/parts[1]/parts[2]/"program_entries.parquet","result":root/"curated"/"results"/parts[0]/parts[1]/parts[2]/"race_results.parquet","merged":root/"curated"/"races"/parts[0]/parts[1]/parts[2]/"program_result_merged.parquet","quality":root/"curated"/"races"/parts[0]/parts[1]/parts[2]/"quality.json"}

def select_text_file(files,prefix):
    candidates=[Path(path) for path in files if Path(path).suffix.upper()==".TXT" and Path(path).name.upper().startswith(prefix.upper())]
    if len(candidates)!=1:
        raise DailyETLError("{}で始まるTXTが1件ではありません: {}".format(prefix,[str(path) for path in candidates]))
    return candidates[0]

def names_are_prefixes(program_names,result_names):
    return pd.Series([str(result).startswith(str(program)) or str(program).startswith(str(result)) for program,result in zip(program_names,result_names)],index=program_names.index,dtype="bool")

def atomic_parquet(frame,path):
    destination=Path(path)
    destination.parent.mkdir(parents=True,exist_ok=True)
    temporary=destination.with_name(destination.name+".tmp")
    temporary.unlink(missing_ok=True)
    frame.to_parquet(temporary,index=False,engine="pyarrow")
    temporary.replace(destination)

def atomic_json(value,path):
    destination=Path(path)
    destination.parent.mkdir(parents=True,exist_ok=True)
    temporary=destination.with_name(destination.name+".tmp")
    temporary.write_text(json.dumps(value,ensure_ascii=False,indent=2),encoding="utf-8")
    temporary.replace(destination)

def process_daily_files(program_file,result_file,race_date,data_root,overwrite=False):
    date_value=normalize_date(race_date)
    race_date_iso=date_value.isoformat()
    paths=build_output_paths(date_value,data_root)
    if not overwrite and all(path.exists() for path in paths.values()):
        quality=json.loads(paths["quality"].read_text(encoding="utf-8"))
        if quality.get("status")=="SUCCESS":
            return {"paths":paths,"quality":quality,"skipped":True}
    program_df=parse_program_file(program_file,race_date=race_date_iso)
    result_df=parse_result_file(result_file,race_date=race_date_iso)
    program_duplicates=int(program_df.duplicated(JOIN_KEYS).sum())
    result_duplicates=int(result_df.duplicated(JOIN_KEYS).sum())
    if program_duplicates or result_duplicates:
        raise DailyETLError("結合キー重複: program={} result={}".format(program_duplicates,result_duplicates))
    merged=program_df.merge(result_df,on=JOIN_KEYS,how="outer",indicator=True,validate="one_to_one")
    merge_counts={str(key):int(value) for key,value in merged["_merge"].value_counts().items()}
    left_only=int((merged["_merge"]=="left_only").sum())
    right_only=int((merged["_merge"]=="right_only").sum())
    if left_only or right_only:
        raise DailyETLError("結合不一致: left_only={} right_only={}".format(left_only,right_only))
    program_names=merged["racer_name"].fillna("").astype(str)
    result_names=merged["racer_name_result"].fillna("").astype(str)
    exact_names=program_names==result_names
    prefix_names=names_are_prefixes(program_names,result_names)
    name_exact_mismatch=int((~exact_names).sum())
    name_expected_abbreviation=int(((~exact_names)&prefix_names).sum())
    name_unexplained_mismatch=int(((~exact_names)&(~prefix_names)).sum())
    motor_mismatch=int((merged["motor_no"]!=merged["motor_no_result"]).sum())
    boat_mismatch=int((merged["boat_no_equipment"]!=merged["boat_no_equipment_result"]).sum())
    merged["racer_name_program"]=merged["racer_name"]
    merged["racer_name_canonical"]=merged["racer_name_result"].where(result_names.str.len()>0,merged["racer_name"])
    race_sizes=merged.groupby(RACE_KEYS).size()
    invalid_race_size=int((race_sizes!=6).sum())
    invalid_boat_sets=int(merged.groupby(RACE_KEYS)["boat_no"].apply(lambda values:set(map(int,values))!=set(range(1,7))).sum())
    duplicate_keys=int(merged.duplicated(JOIN_KEYS).sum())
    finish_status_counts={str(key):int(value) for key,value in merged["finish_raw"].value_counts(dropna=False).items()}
    quality={"race_date":race_date_iso,"record_count":int(len(merged)),"program_record_count":int(len(program_df)),"result_record_count":int(len(result_df)),"venue_count":int(merged["venue_code"].nunique()),"race_count":int(merged[RACE_KEYS].drop_duplicates().shape[0]),"duplicate_keys":duplicate_keys,"program_duplicate_keys":program_duplicates,"result_duplicate_keys":result_duplicates,"merge_counts":merge_counts,"left_only":left_only,"right_only":right_only,"invalid_race_size":invalid_race_size,"invalid_boat_sets":invalid_boat_sets,"name_exact_mismatch":name_exact_mismatch,"name_expected_abbreviation":name_expected_abbreviation,"name_unexplained_mismatch":name_unexplained_mismatch,"motor_mismatch":motor_mismatch,"boat_equipment_mismatch":boat_mismatch,"special_finish_count":int(merged["finish_position"].isna().sum()),"finish_status_counts":finish_status_counts,"created_at":dt.datetime.now(dt.timezone.utc).isoformat(),"status":"SUCCESS"}
    required_zero=["duplicate_keys","program_duplicate_keys","result_duplicate_keys","left_only","right_only","invalid_race_size","invalid_boat_sets","name_unexplained_mismatch","motor_mismatch","boat_equipment_mismatch"]
    failures={key:quality[key] for key in required_zero if quality[key]!=0}
    if failures:
        quality["status"]="FAILED"
        raise DailyETLError("品質検査失敗: {}".format(failures))
    merged_output=merged.drop(columns="_merge")
    atomic_parquet(program_df,paths["program"])
    atomic_parquet(result_df,paths["result"])
    atomic_parquet(merged_output,paths["merged"])
    atomic_json(quality,paths["quality"])
    return {"paths":paths,"quality":quality,"skipped":False}

def run_daily_etl(race_date,data_root,overwrite_outputs=False,overwrite_archives=False):
    date_value=normalize_date(race_date)
    archives=download_and_extract_daily(date_value,Path(data_root),overwrite=overwrite_archives)
    short_date=date_value.strftime("%y%m%d")
    program_file=select_text_file(archives["program"]["files"],"B"+short_date)
    result_file=select_text_file(archives["result"]["files"],"K"+short_date)
    return process_daily_files(program_file,result_file,date_value,data_root,overwrite=overwrite_outputs)
