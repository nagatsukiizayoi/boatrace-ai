from boatrace_ai.parsers.program import parse_program_file,parse_program_text

PROGRAM_ROW="1 4926吉川貴仁32三重52A1 7.79 64.76 9.86100.00 67 41.18 73 36.36             12"
PROGRAM_TEXT="24BBGN\n 1R  一般  Ｈ1800ｍ\n"+PROGRAM_ROW

def test_program_parser_handles_joined_percentages():
    frame=parse_program_text(PROGRAM_TEXT,race_date="2026-06-30",source_file="B260630.TXT",validate_structure=False)
    assert len(frame)==1
    row=frame.iloc[0]
    assert row["racer_id"]=="4926"
    assert row["racer_name"]=="吉川貴仁"
    assert row["local_win_rate"]==9.86
    assert row["local_place2_rate_pct"]==100.00
    assert row["motor_no"]==67
    assert row["boat_no_equipment"]==73

def test_program_file_cp932(tmp_path):
    path=tmp_path/"B260630.TXT"
    path.write_bytes(PROGRAM_TEXT.encode("cp932"))
    frame=parse_program_file(path,validate_structure=False)
    assert frame.iloc[0]["race_date"]=="2026-06-30"
    assert frame.iloc[0]["source_file"]=="B260630.TXT"
