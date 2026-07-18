from __future__ import annotations

import pandas as pd

from boatrace_ai.parsers.program import parse_program_text
from boatrace_ai.parsers.result import parse_result_text


def test_program_accepts_three_digit_boat_equipment_number():
    text="\n".join(["01BBGN","1R TEST H1800m","1 3703鳥飼 眞52福岡50A2 6.10 46.28 5.49 36.49 53 28.26167 28.89 123 645      6"])
    frame=parse_program_text(text,race_date="2026-06-29",source_file="B260629.TXT",validate_structure=False)
    assert len(frame)==1
    assert int(frame.loc[0,"motor_no"])==53
    assert int(frame.loc[0,"boat_no_equipment"])==167
    assert frame.loc[0,"racer_name"]=="鳥飼眞"


def test_result_accepts_three_digit_boat_equipment_and_k0():
    text="\n".join(["01KBGN","1R TEST H1800m","  01  1 3703 鳥 飼    眞 53  167  6.80   1    0.15     1.49.9","  K0  4 4474 渡 辺    崇 37   74 K .         K .        .  . "])
    frame=parse_result_text(text,race_date="2026-06-29",source_file="K260629.TXT",validate_structure=False)
    assert len(frame)==2
    normal=frame.loc[frame["finish_raw"].eq("01")].iloc[0]
    special=frame.loc[frame["finish_raw"].eq("K0")].iloc[0]
    assert int(normal["boat_no_equipment_result"])==167
    assert float(normal["exhibition_time"])==6.80
    assert int(normal["course"])==1
    assert special["racer_name_result"]=="渡辺崇"
    assert int(special["motor_no_result"])==37
    assert int(special["boat_no_equipment_result"])==74
    assert pd.isna(special["exhibition_time"])
    assert pd.isna(special["course"])
    assert pd.isna(special["finish_position"])
    assert special["start_timing_raw"]=="K"
