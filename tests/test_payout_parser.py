from pathlib import Path

from boatrace_ai.parsers.payout import parse_payout_file,parse_payout_text


def test_normal_and_multiple_winner_payouts():
    text="\n".join([
        "24KBGN",
        "1R 予選 H1800m",
        " 単勝 1 120",
        " 複勝 1 100 2 210",
        " 2連単 1-2 200 人気 2",
        "        1-5 400 人気 4",
        " 2連複 1-2 180 人気 2",
        " 拡連複 1-2 200 人気 3",
        "        1-5 310 人気 6",
        "        2-5 760 人気 12",
        " 3連単 1-2-5 1090 人気 11",
        "        1-5-2 1750 人気 15",
        " 3連複 1-2-5 1310 人気 7",
        "        1-3-5 1500 人気 8",
    ])
    frame=parse_payout_text(text,race_date="2026-06-11",source_file="K260611.TXT")
    assert len(frame)==13
    assert frame["bet_type"].nunique()==7
    assert set(frame["payout_status"])=={"NORMAL"}
    assert frame.loc[frame["bet_type"].eq("EXACTA"),"combination"].tolist()==["1-2","1-5"]
    assert frame.loc[frame["bet_type"].eq("TRIFECTA"),"combination"].tolist()==["1-2-5","1-5-2"]
    assert frame.loc[frame["bet_type"].eq("QUINELLA_PLACE"),"selection_no"].tolist()==[1,2,3]


def test_special_payout_and_not_established():
    text="\n".join([
        "01KBGN",
        "1R 一般 H1800m",
        " 単勝 特払い 70",
        "2R 一般 H1800m",
        " 3連複 不成立",
    ])
    frame=parse_payout_text(text,race_date="2026-06-06",source_file="K260606.TXT")
    assert len(frame)==2
    special=frame.loc[frame["payout_status"].eq("SPECIAL_PAYOUT")].iloc[0]
    unsettled=frame.loc[frame["payout_status"].eq("NOT_ESTABLISHED")].iloc[0]
    assert special["bet_type"]=="WIN"
    assert int(special["payout_yen"])==70
    assert unsettled["bet_type"]=="TRIO"
    assert unsettled["combination"] is not None or bool(frame.loc[frame["payout_status"].eq("NOT_ESTABLISHED"),"combination"].isna().all())


def test_full_race_not_established_generates_all_bet_types():
    text="\n".join([
        "03KBGN",
        "8R 一般 H1200m",
        " レース不成立",
    ])
    frame=parse_payout_text(text,race_date="2026-06-06",source_file="K260606.TXT")
    assert len(frame)==7
    assert frame["bet_type"].nunique()==7
    assert set(frame["payout_status"])=={"NOT_ESTABLISHED"}
    assert frame["combination"].isna().all()


def test_cancelled_summary_generates_all_bet_types():
    text="\n".join([
        "04KBGN",
        "[払戻金] 3連単 3連複 2連単 2連複",
        "1R 中　止",
    ])
    frame=parse_payout_text(text,race_date="2026-06-27",source_file="K260627.TXT")
    assert len(frame)==7
    assert frame["bet_type"].nunique()==7
    assert set(frame["payout_status"])=={"CANCELLED"}
    assert set(frame["source_kind"])=={"SUMMARY"}
    assert frame["combination"].isna().all()


def test_payout_file_cp932(tmp_path):
    path=Path(tmp_path)/"K260601.TXT"
    text="\n".join([
        "24KBGN",
        "1R 予選 H1800m",
        " 単勝 6 5920",
    ])
    path.write_bytes(text.encode("cp932"))
    frame=parse_payout_file(path)
    assert len(frame)==1
    assert frame.iloc[0]["race_date"]=="2026-06-01"
    assert frame.iloc[0]["venue_code"]=="24"
    assert frame.iloc[0]["bet_type"]=="WIN"
    assert int(frame.iloc[0]["payout_yen"])==5920
    assert frame.iloc[0]["source_file"]=="K260601.TXT"

def test_venue_unavailable_generates_all_races_cancelled(): text="\n".join(["13KBGN","ボートレース尼崎","データは、この場の全レース終了後に登録されます。","13KEND"]); frame=parse_payout_text(text,race_date="2026-05-16",source_file="K260516.TXT"); assert len(frame)==84; assert set(frame["venue_code"])=={"13"}; assert set(frame["race_no"])==set(range(1,13)); assert frame["bet_type"].nunique()==7; assert set(frame["payout_status"])=={"CANCELLED"}; assert set(frame["source_kind"])=={"SUMMARY"}; assert frame["combination"].isna().all()
