# Parser for official BOAT RACE program text files.
from __future__ import annotations
import datetime as dt
import re
from pathlib import Path
import pandas as pd

FW_TRANS=str.maketrans("０１２３４５６７８９Ｒｒ＃","0123456789Rr#")
PROGRAM_PATTERN=re.compile(r"^(?P<boat_no>[1-6])\s(?P<racer_id>\d{4})(?P<racer_name>.{4})(?P<age>\d{2})(?P<branch>.{2})(?P<weight_kg>\d{2})(?P<class>[AB][12])\s*(?P<national_win_rate>\d\.\d{2})\s*(?P<national_place2_rate_pct>\d{1,3}\.\d{2})\s*(?P<local_win_rate>\d{1,2}\.\d{2})\s*(?P<local_place2_rate_pct>\d{1,3}\.\d{2})\s*(?P<motor_no>\d{1,2})\s*(?P<motor_place2_rate_pct>\d{1,3}\.\d{2})\s*(?P<boat_no_equipment>(?:1\d{2}|\d{1,2}))\s*(?P<boat_place2_rate_pct>(?:100\.00|\d{1,2}\.\d{2}))(?P<series_results_raw>.*)$")
VENUE_PATTERN=re.compile(r"^\s*(?P<venue_code>\d{2})BBGN\s*$",re.IGNORECASE)
RACE_PATTERN=re.compile(r"^\s*(?P<race_no>\d{1,2})R\s+",re.IGNORECASE)
CANDIDATE_PATTERN=re.compile(r"^[1-6]\s\d{4}")
KEY_COLUMNS=["race_date","venue_code","race_no","boat_no","racer_id"]
OUTPUT_COLUMNS=["race_date","venue_code","race_no","boat_no","racer_id","racer_name","age","branch","weight_kg","class","national_win_rate","national_place2_rate_pct","local_win_rate","local_place2_rate_pct","motor_no","motor_place2_rate_pct","boat_no_equipment","boat_place2_rate_pct","series_results_raw","source_file"]

class ProgramParseError(ValueError):
    pass

def normalize_line(line):
    return line.translate(FW_TRANS).replace("\u3000"," ")

def normalize_name(value):
    return re.sub(r"\s+","",str(value)).strip()

def normalize_text(value):
    return re.sub(r"\s+","",str(value)).strip()

def normalize_date(value):
    if isinstance(value,dt.datetime):
        return value.date().isoformat()
    if isinstance(value,dt.date):
        return value.isoformat()
    return dt.date.fromisoformat(str(value)).isoformat()

def infer_date(source_file):
    match=re.search(r"B(\d{2})(\d{2})(\d{2})",source_file,re.IGNORECASE)
    if match is None:
        raise ProgramParseError("race_dateを推定できません: {}".format(source_file))
    year,month,day=map(int,match.groups())
    return dt.date(2000+year,month,day).isoformat()

def parse_program_text(text,race_date=None,source_file="program.txt",strict=True,validate_structure=True):
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
        program_match=PROGRAM_PATTERN.match(line)
        if program_match is None or current_venue is None or current_race is None:
            unmatched.append((line_number,original_line))
            continue
        row=program_match.groupdict()
        row["race_date"]=resolved_date
        row["venue_code"]=current_venue
        row["race_no"]=current_race
        row["source_file"]=source_file
        rows.append(row)
    if strict and unmatched:
        examples=" | ".join("{}: {}".format(number,line) for number,line in unmatched[:10])
        raise ProgramParseError("番組表の未解析行が{}件あります: {}".format(len(unmatched),examples))
    frame=pd.DataFrame(rows)
    if frame.empty:
        raise ProgramParseError("番組表レコードを抽出できませんでした")
    frame["racer_id"]=frame["racer_id"].astype("string").str.strip().str.zfill(4)
    frame["racer_name"]=frame["racer_name"].astype("string").map(normalize_name)
    frame["branch"]=frame["branch"].astype("string").map(normalize_text)
    frame["class"]=frame["class"].astype("string").str.strip()
    frame["series_results_raw"]=frame["series_results_raw"].astype("string").str.strip()
    for column in ["race_no","boat_no","age","weight_kg","motor_no","boat_no_equipment"]:
        frame[column]=pd.to_numeric(frame[column],errors="raise").astype("int64")
    for column in ["national_win_rate","national_place2_rate_pct","local_win_rate","local_place2_rate_pct","motor_place2_rate_pct","boat_place2_rate_pct"]:
        frame[column]=pd.to_numeric(frame[column],errors="raise").astype("float64")
    frame=frame[OUTPUT_COLUMNS].reset_index(drop=True)
    duplicate_count=int(frame.duplicated(KEY_COLUMNS).sum())
    if duplicate_count:
        raise ProgramParseError("番組表キーの重複が{}件あります".format(duplicate_count))
    if validate_structure:
        race_keys=["race_date","venue_code","race_no"]
        invalid_sizes=int((frame.groupby(race_keys).size()!=6).sum())
        invalid_boats=int(frame.groupby(race_keys)["boat_no"].apply(lambda values:set(map(int,values))!=set(range(1,7))).sum())
        if invalid_sizes:
            raise ProgramParseError("6艇ではないレースが{}件あります".format(invalid_sizes))
        if invalid_boats:
            raise ProgramParseError("艇番構成が不正なレースが{}件あります".format(invalid_boats))
    return frame

def parse_program_file(path,race_date=None,encoding="cp932",strict=True,validate_structure=True):
    file_path=Path(path)
    text=file_path.read_bytes().decode(encoding,errors="strict")
    return parse_program_text(text,race_date=race_date,source_file=file_path.name,strict=strict,validate_structure=validate_structure)
