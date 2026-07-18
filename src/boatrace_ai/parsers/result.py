# Parser for official BOAT RACE result text files.
from __future__ import annotations
import datetime as dt
import re
from pathlib import Path
import pandas as pd

FW_TRANS=str.maketrans("０１２３４５６７８９Ｒｒ＃","0123456789Rr#")
RESULT_PATTERN=re.compile(r"^\s*(?P<finish_raw>\S+)\s+(?P<boat_no>[1-6])\s+(?P<racer_id>\d{4})\s+(?P<racer_name_result>.+?)\s+(?P<motor_no_result>\d{1,2})\s+(?P<boat_no_equipment_result>\d{1,2})\s+(?P<exhibition_time>\d\.\d{2})\s+(?P<course>[1-6])\s+(?P<start_timing_raw>\S+)\s+(?P<race_time_raw>.+?)\s*$")
VENUE_PATTERN=re.compile(r"^\s*(?P<venue_code>\d{2})KBGN\s*$",re.IGNORECASE)
RACE_PATTERN=re.compile(r"^\s*(?P<race_no>\d{1,2})R\s+.*H1800m",re.IGNORECASE)
CANDIDATE_PATTERN=re.compile(r"^\s*(?:0[1-6]|[1-6]|F|L\d?|K\d?|S\d?)\s+[1-6]\s+\d{4}\s+",re.IGNORECASE)
KEY_COLUMNS=["race_date","venue_code","race_no","boat_no","racer_id"]
OUTPUT_COLUMNS=["race_date","venue_code","race_no","finish_raw","boat_no","racer_id","racer_name_result","motor_no_result","boat_no_equipment_result","exhibition_time","course","start_timing_raw","race_time_raw","finish_position","source_file_result"]

class ResultParseError(ValueError):
    pass

def normalize_line(line):
    return line.translate(FW_TRANS).replace("\u3000"," ")

def normalize_name(value):
    return re.sub(r"\s+","",str(value)).strip()

def normalize_date(value):
    if isinstance(value,dt.datetime):
        return value.date().isoformat()
    if isinstance(value,dt.date):
        return value.isoformat()
    return dt.date.fromisoformat(str(value)).isoformat()

def infer_date(source_file):
    match=re.search(r"K(\d{2})(\d{2})(\d{2})",source_file,re.IGNORECASE)
    if match is None:
        raise ResultParseError("race_dateを推定できません: {}".format(source_file))
    year,month,day=map(int,match.groups())
    return dt.date(2000+year,month,day).isoformat()

def parse_result_text(text,race_date=None,source_file="result.txt",strict=True,validate_structure=True):
    resolved_date=normalize_date(race_date) if race_date is not None else infer_date(source_file)
    current_venue=None
    current_race=None
    rows=[]
    unmatched=[]
    for line_number,original_line in enumerate(text.splitlines(),start=1):
        line=normalize_line(original_line)
        venue_match=VENUE_PATTERN.search(line)
        if venue_match is not None:
            venue_number=int(venue_match.group("venue_code"))
            if 1<=venue_number<=24:
                current_venue="{:02d}".format(venue_number)
                current_race=None
        race_match=RACE_PATTERN.search(line)
        if race_match is not None:
            race_number=int(race_match.group("race_no"))
            if 1<=race_number<=12:
                current_race=race_number
        if CANDIDATE_PATTERN.match(line) is None:
            continue
        result_match=RESULT_PATTERN.match(line)
        if result_match is None or current_venue is None or current_race is None:
            unmatched.append((line_number,original_line))
            continue
        row=result_match.groupdict()
        row["race_date"]=resolved_date
        row["venue_code"]=current_venue
        row["race_no"]=current_race
        row["source_file_result"]=source_file
        rows.append(row)
    if strict and unmatched:
        examples=" | ".join("{}: {}".format(n,line) for n,line in unmatched[:10])
        raise ResultParseError("未解析行が{}件あります: {}".format(len(unmatched),examples))
    frame=pd.DataFrame(rows)
    if frame.empty:
        raise ResultParseError("競走成績レコードを抽出できませんでした")
    frame["racer_name_result"]=frame["racer_name_result"].astype("string").map(normalize_name)
    frame["finish_raw"]=frame["finish_raw"].astype("string").str.strip().str.upper()
    frame["racer_id"]=frame["racer_id"].astype("string").str.strip().str.zfill(4)
    for column in ["race_no","boat_no","motor_no_result","boat_no_equipment_result","course"]:
        frame[column]=pd.to_numeric(frame[column],errors="raise").astype("int64")
    frame["exhibition_time"]=pd.to_numeric(frame["exhibition_time"],errors="raise").astype("float64")
    positions=[int(value) if re.fullmatch(r"0?[1-6]",str(value)) else pd.NA for value in frame["finish_raw"]]
    frame["finish_position"]=pd.Series(positions,index=frame.index,dtype="Int64")
    frame=frame[OUTPUT_COLUMNS].reset_index(drop=True)
    duplicate_count=int(frame.duplicated(KEY_COLUMNS).sum())
    if duplicate_count:
        raise ResultParseError("競走成績キーの重複が{}件あります".format(duplicate_count))
    if validate_structure:
        race_keys=["race_date","venue_code","race_no"]
        invalid_sizes=int((frame.groupby(race_keys).size()!=6).sum())
        invalid_boats=int(frame.groupby(race_keys)["boat_no"].apply(lambda values:set(map(int,values))!=set(range(1,7))).sum())
        if invalid_sizes:
            raise ResultParseError("6艇ではないレースが{}件あります".format(invalid_sizes))
        if invalid_boats:
            raise ResultParseError("艇番構成が不正なレースが{}件あります".format(invalid_boats))
    return frame

def parse_result_file(path,race_date=None,encoding="cp932",strict=True,validate_structure=True):
    file_path=Path(path)
    text=file_path.read_bytes().decode(encoding,errors="strict")
    return parse_result_text(text,race_date=race_date,source_file=file_path.name,strict=strict,validate_structure=validate_structure)
