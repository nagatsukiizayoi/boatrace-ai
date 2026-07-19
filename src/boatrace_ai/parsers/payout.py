"""Parser for payout records in official BOAT RACE result text files."""

from __future__ import annotations

import datetime as dt
import re
import unicodedata
from pathlib import Path

import pandas as pd


BET_TYPE_MAP = {"単勝":"WIN","複勝":"PLACE","2連単":"EXACTA","2連複":"QUINELLA","拡連複":"QUINELLA_PLACE","3連単":"TRIFECTA","3連複":"TRIO"}
BET_TYPE_ORDER = ["WIN","PLACE","EXACTA","QUINELLA","QUINELLA_PLACE","TRIFECTA","TRIO"]
BET_TYPE_RAW = {value:key for key,value in BET_TYPE_MAP.items()}
ORDERED_BET_TYPES = {"EXACTA","TRIFECTA"}

VENUE_PATTERN = re.compile(r"^\s*(?P<venue_code>\d{2})KBGN\s*$",re.IGNORECASE)
RACE_PATTERN = re.compile(r"^\s*(?P<race_no>\d{1,2})R\s+.*H\d{4}m",re.IGNORECASE)
CANCELLED_SUMMARY_PATTERN = re.compile(r"^\s*(?P<race_no>\d{1,2})R\s+中\s*止\s*$")
BET_PATTERN = re.compile(r"^\s*(?P<bet_type>単勝|複勝|2連単|2連複|拡連複|3連単|3連複)\s+(?P<payload>.+?)\s*$")
COMBINATION_PATTERN = re.compile(r"(?P<combination>[1-6](?:-[1-6]){1,2})\s+(?P<payout>\d+)\s+人気\s+(?P<popularity>\d+)")
SIMPLE_PATTERN = re.compile(r"(?P<combination>[1-6])\s+(?P<payout>\d+)")
CONTINUATION_PATTERN = re.compile(r"^\s*(?P<combination>[1-6](?:-[1-6]){1,2})\s+(?P<payout>\d+)\s+人気\s+(?P<popularity>\d+)\s*$")

OUTPUT_COLUMNS = ["race_date","venue_code","race_no","bet_type","bet_type_raw","selection_no","combination","combination_raw","payout_yen","popularity","payout_status","is_ordered","source_kind","source_line_no","source_file"]
PRIMARY_KEY = ["race_date","venue_code","race_no","bet_type","selection_no","payout_status"]


class PayoutParseError(ValueError):
    """Raised when payout records cannot be parsed safely."""


def normalize_line(line):
    return unicodedata.normalize("NFKC",line).replace("\u3000"," ")


def normalize_date(value):
    if isinstance(value,dt.datetime):
        return value.date().isoformat()
    if isinstance(value,dt.date):
        return value.isoformat()
    return dt.date.fromisoformat(str(value)).isoformat()


def infer_date(source_file):
    match=re.search(r"K(\d{2})(\d{2})(\d{2})",source_file,re.IGNORECASE)
    if match is None:
        raise PayoutParseError("race_dateを推定できません: {}".format(source_file))
    year,month,day=map(int,match.groups())
    return dt.date(2000+year,month,day).isoformat()


def make_row(race_date,venue_code,race_no,bet_type,bet_type_raw,combination,payout_yen,popularity,payout_status,source_kind,source_line_no,source_file):
    return {"race_date":race_date,"venue_code":venue_code,"race_no":race_no,"bet_type":bet_type,"bet_type_raw":bet_type_raw,"combination":combination,"combination_raw":combination,"payout_yen":payout_yen,"popularity":popularity,"payout_status":payout_status,"is_ordered":bet_type in ORDERED_BET_TYPES,"source_kind":source_kind,"source_line_no":source_line_no,"source_file":source_file}


def parse_payout_text(text,race_date=None,source_file="result.txt",strict=True):
    resolved_date=normalize_date(race_date) if race_date is not None else infer_date(source_file)
    current_venue=None
    current_race=None
    current_bet=None
    in_summary=False
    rows=[]
    cancelled_races={}
    not_established_races=set()
    unmatched=[]
    for line_number,original_line in enumerate(text.splitlines(),start=1):
        line=normalize_line(original_line)
        venue_match=VENUE_PATTERN.search(line)
        if venue_match is not None:
            venue_number=int(venue_match.group("venue_code"))
            if 1<=venue_number<=24:
                current_venue="{:02d}".format(venue_number)
                current_race=None
                current_bet=None
                in_summary=False
            continue
        if "[払戻金]" in line:
            in_summary=True
            current_race=None
            current_bet=None
            continue
        race_match=RACE_PATTERN.search(line)
        if race_match is not None:
            race_number=int(race_match.group("race_no"))
            if 1<=race_number<=12:
                current_race=race_number
                current_bet=None
                in_summary=False
            continue
        if in_summary:
            cancelled_match=CANCELLED_SUMMARY_PATTERN.match(line)
            if cancelled_match is not None and current_venue is not None:
                race_number=int(cancelled_match.group("race_no"))
                if 1<=race_number<=12:
                    cancelled_races[(current_venue,race_number)]=line_number
            continue
        if current_venue is None or current_race is None:
            continue
        if "レース不成立" in line:
            race_key=(current_venue,current_race)
            if race_key not in not_established_races:
                for bet_type in BET_TYPE_ORDER:
                    rows.append(make_row(resolved_date,current_venue,current_race,bet_type,BET_TYPE_RAW[bet_type],pd.NA,pd.NA,pd.NA,"NOT_ESTABLISHED","DETAIL",line_number,source_file))
                not_established_races.add(race_key)
            current_bet=None
            continue
        bet_match=BET_PATTERN.match(line)
        if bet_match is not None:
            bet_type_raw=bet_match.group("bet_type")
            bet_type=BET_TYPE_MAP[bet_type_raw]
            payload=bet_match.group("payload").strip()
            current_bet=bet_type
            if payload=="不成立":
                rows.append(make_row(resolved_date,current_venue,current_race,bet_type,bet_type_raw,pd.NA,pd.NA,pd.NA,"NOT_ESTABLISHED","DETAIL",line_number,source_file))
                continue
            if payload.startswith("特払い"):
                amount_match=re.search(r"特払い\s+(\d+)",payload)
                if amount_match is None:
                    unmatched.append((line_number,original_line))
                    continue
                rows.append(make_row(resolved_date,current_venue,current_race,bet_type,bet_type_raw,pd.NA,int(amount_match.group(1)),pd.NA,"SPECIAL_PAYOUT","DETAIL",line_number,source_file))
                continue
            if "返還" in payload:
                rows.append(make_row(resolved_date,current_venue,current_race,bet_type,bet_type_raw,pd.NA,pd.NA,pd.NA,"REFUND","DETAIL",line_number,source_file))
                continue
            pattern=SIMPLE_PATTERN if bet_type in {"WIN","PLACE"} else COMBINATION_PATTERN
            matches=list(pattern.finditer(payload))
            if not matches:
                unmatched.append((line_number,original_line))
                continue
            for match in matches:
                combination=match.group("combination")
                payout_yen=int(match.group("payout"))
                popularity=pd.NA if bet_type in {"WIN","PLACE"} else int(match.group("popularity"))
                rows.append(make_row(resolved_date,current_venue,current_race,bet_type,bet_type_raw,combination,payout_yen,popularity,"NORMAL","DETAIL",line_number,source_file))
            continue
        continuation_match=CONTINUATION_PATTERN.match(line)
        if continuation_match is not None and current_bet not in {None,"WIN","PLACE"}:
            combination=continuation_match.group("combination")
            payout_yen=int(continuation_match.group("payout"))
            popularity=int(continuation_match.group("popularity"))
            rows.append(make_row(resolved_date,current_venue,current_race,current_bet,BET_TYPE_RAW[current_bet],combination,payout_yen,popularity,"NORMAL","DETAIL",line_number,source_file))
            continue
        if not line.strip():
            current_bet=None
    for (venue_code,race_no),line_number in sorted(cancelled_races.items()):
        for bet_type in BET_TYPE_ORDER:
            rows.append(make_row(resolved_date,venue_code,race_no,bet_type,BET_TYPE_RAW[bet_type],pd.NA,pd.NA,pd.NA,"CANCELLED","SUMMARY",line_number,source_file))
    if strict and unmatched:
        examples=" | ".join("{}: {}".format(number,line) for number,line in unmatched[:10])
        raise PayoutParseError("未解析払戻行が{}件あります: {}".format(len(unmatched),examples))
    frame=pd.DataFrame(rows)
    if frame.empty:
        raise PayoutParseError("払戻レコードを抽出できませんでした")
    group_keys=["race_date","venue_code","race_no","bet_type","payout_status"]
    frame["selection_no"]=frame.groupby(group_keys,sort=False).cumcount().add(1).astype("int64")
    frame["race_date"]=frame["race_date"].astype("string")
    frame["venue_code"]=frame["venue_code"].astype("string").str.zfill(2)
    frame["race_no"]=pd.to_numeric(frame["race_no"],errors="raise").astype("int64")
    frame["bet_type"]=frame["bet_type"].astype("string")
    frame["bet_type_raw"]=frame["bet_type_raw"].astype("string")
    frame["combination"]=frame["combination"].astype("string")
    frame["combination_raw"]=frame["combination_raw"].astype("string")
    frame["payout_yen"]=pd.to_numeric(frame["payout_yen"],errors="coerce").astype("Int64")
    frame["popularity"]=pd.to_numeric(frame["popularity"],errors="coerce").astype("Int64")
    frame["payout_status"]=frame["payout_status"].astype("string")
    frame["is_ordered"]=frame["is_ordered"].astype("boolean")
    frame["source_kind"]=frame["source_kind"].astype("string")
    frame["source_line_no"]=pd.to_numeric(frame["source_line_no"],errors="raise").astype("int64")
    frame["source_file"]=frame["source_file"].astype("string")
    frame=frame[OUTPUT_COLUMNS].reset_index(drop=True)
    duplicate_count=int(frame.duplicated(PRIMARY_KEY).sum())
    if duplicate_count:
        raise PayoutParseError("払戻主キーの重複が{}件あります".format(duplicate_count))
    return frame


def parse_payout_file(path,race_date=None,encoding="cp932",strict=True):
    file_path=Path(path)
    text=file_path.read_bytes().decode(encoding,errors="strict")
    return parse_payout_text(text,race_date=race_date,source_file=file_path.name,strict=strict)
