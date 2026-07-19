import pandas as pd
from boatrace_ai.pipelines.daily_etl import build_output_paths,names_are_prefixes

def test_build_output_paths(tmp_path):
    paths=build_output_paths("2026-06-30",tmp_path)
    assert paths["program"]==tmp_path/"curated"/"entries"/"2026"/"06"/"30"/"program_entries.parquet"
    assert paths["result"]==tmp_path/"curated"/"results"/"2026"/"06"/"30"/"race_results.parquet"
    assert paths["payout"]==tmp_path/"curated"/"payouts"/"2026"/"06"/"30"/"race_payouts.parquet"
    assert paths["merged"]==tmp_path/"curated"/"races"/"2026"/"06"/"30"/"program_result_merged.parquet"
    assert paths["quality"]==tmp_path/"curated"/"races"/"2026"/"06"/"30"/"quality.json"

def test_name_prefix_matching():
    program=pd.Series(["海老澤泰","松尾宣邦","不一致名"])
    result=pd.Series(["海老澤泰行","松尾宣邦","別の選手"])
    matches=names_are_prefixes(program,result)
    assert matches.tolist()==[True,True,False]
