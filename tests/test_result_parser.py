import pandas as pd
from boatrace_ai.parsers.result import parse_result_file,parse_result_text

RESULT_TEXT="\n".join(["24KBGN"," 1R  一般  H1800m","  01  1 3434 松　尾　　宣　邦 15   63  6.90   1    0.20     1.52.0","   F  2 4027 松　江　　秀　徳 79   68  6.92   2    F.01      .  .","  S0  3 5312 川　辺　　郭　人 66   61  6.89   3    0.10      .  .","  S1  4 3772 一　柳　　和　孝 81   44  6.94   4    0.11      .  ."])

def test_result_parser_special_finishes():
    frame=parse_result_text(RESULT_TEXT,race_date="2026-06-30",source_file="K260630.TXT",validate_structure=False)
    assert len(frame)==4
    assert frame.iloc[0]["finish_position"]==1
    special=frame.loc[frame["finish_position"].isna(),"finish_raw"].tolist()
    assert special==["F","S0","S1"]
    assert frame.iloc[0]["racer_name_result"]=="松尾宣邦"

def test_result_file_cp932(tmp_path):
    path=tmp_path/"K260630.TXT"
    path.write_bytes(RESULT_TEXT.encode("cp932"))
    frame=parse_result_file(path,validate_structure=False)
    assert len(frame)==4
    assert frame.iloc[0]["race_date"]=="2026-06-30"
    assert frame.iloc[0]["source_file_result"]=="K260630.TXT"
