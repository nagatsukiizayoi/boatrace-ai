from __future__ import annotations

import math

import pytest

from boatrace_ai.pipelines.pre_night_features import (
    EXCLUDED_COLUMNS,
    MODEL_FEATURE_COLUMNS,
    OUTPUT_COLUMNS,
    PROGRAM_INPUT_COLUMNS,
    ProgramOnlyFeatureContractError,
    build_pre_night_features,
    build_program_only_features,
    schema_contract,
)


def make_race(
    *,
    race_date: str = "2025-01-02",
    venue_code: str = "01",
    race_no: int = 1,
    source_file: str = "program-20250102-01.txt",
):
    rows = []

    for boat_no in range(1, 7):
        rows.append(
            {
                "race_date": race_date,
                "venue_code": venue_code,
                "race_no": race_no,
                "boat_no": boat_no,
                "age": 20 + boat_no,
                "boat_place2_rate_pct": 30.0 + boat_no,
                "branch": "東京",
                "class": "A1",
                "local_place2_rate_pct": 40.0 + boat_no,
                "local_win_rate": 5.0 + boat_no / 10,
                "motor_place2_rate_pct": 35.0 + boat_no,
                "national_place2_rate_pct": 45.0 + boat_no,
                "national_win_rate": 6.0 + boat_no / 10,
                "weight_kg": 50.0 + boat_no / 10,
                "boat_no_equipment": 10 + boat_no,
                "motor_no": 20 + boat_no,
                "racer_id": str(4000 + boat_no),
                "racer_name": f"選手{boat_no}",
                "source_file": source_file,
                "series_results_raw": "1 2 3",
            }
        )

    return rows


def test_schema_contract_v1():
    contract = schema_contract()

    assert contract["schema_name"] == "pre-night-program-only-features"
    assert contract["schema_version"] == "1.0.0"
    assert len(contract["primary_key_columns"]) == 4
    assert len(contract["model_feature_columns"]) == 10
    assert len(contract["metadata_columns"]) == 5
    assert len(contract["excluded_columns"]) == 1
    assert len(contract["output_columns"]) == 19
    assert len(contract["program_input_columns"]) == 20


def test_builds_fixed_order_19_column_output():
    result = build_program_only_features(make_race())

    assert len(result) == 6
    assert list(result[0]) == list(OUTPUT_COLUMNS)
    assert len(result[0]) == 19
    assert "series_results_raw" not in result[0]
    assert EXCLUDED_COLUMNS == ("series_results_raw",)
    assert len(MODEL_FEATURE_COLUMNS) == 10
    assert len(PROGRAM_INPUT_COLUMNS) == 20


def test_output_is_sorted_deterministically():
    rows = list(reversed(make_race()))

    result1 = build_program_only_features(rows)
    result2 = build_program_only_features(list(reversed(rows)))

    assert result1 == result2
    assert [row["boat_no"] for row in result1] == [1, 2, 3, 4, 5, 6]


def test_alias_has_same_result():
    rows = make_race()

    assert build_pre_night_features(rows) == build_program_only_features(rows)


def test_rejects_duplicate_primary_key():
    rows = make_race()
    rows[-1] = dict(rows[0])

    with pytest.raises(
        ProgramOnlyFeatureContractError,
        match="duplicate primary key",
    ):
        build_program_only_features(rows)


def test_rejects_incomplete_six_boat_race():
    rows = make_race()[:-1]

    with pytest.raises(
        ProgramOnlyFeatureContractError,
        match="exactly boats 1..6",
    ):
        build_program_only_features(rows)


def test_rejects_mixed_source_file_in_one_race():
    rows = make_race()
    rows[-1]["source_file"] = "different-file.txt"

    with pytest.raises(
        ProgramOnlyFeatureContractError,
        match="mixed source_file",
    ):
        build_program_only_features(rows)


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("national_win_rate", math.nan),
        ("local_win_rate", math.inf),
        ("motor_place2_rate_pct", -1.0),
        ("boat_place2_rate_pct", 101.0),
        ("weight_kg", None),
    ],
)
def test_rejects_missing_nan_inf_and_out_of_range(column, value):
    rows = make_race()
    rows[0][column] = value

    with pytest.raises(ProgramOnlyFeatureContractError):
        build_program_only_features(rows)


def test_rejects_forbidden_result_column():
    rows = make_race()
    rows[0]["result"] = "1"

    with pytest.raises(
        ProgramOnlyFeatureContractError,
        match="forbidden result/post-race columns",
    ):
        build_program_only_features(rows)


def test_rejects_unexpected_column():
    rows = make_race()
    rows[0]["unknown_future_column"] = "unexpected"

    with pytest.raises(
        ProgramOnlyFeatureContractError,
        match="unexpected",
    ):
        build_program_only_features(rows)


def test_rejects_missing_contract_column():
    rows = make_race()
    del rows[0]["age"]

    with pytest.raises(
        ProgramOnlyFeatureContractError,
        match="missing",
    ):
        build_program_only_features(rows)


def test_multiple_complete_races_are_supported_and_sorted():
    race2 = make_race(
        race_date="2025-01-02",
        venue_code="02",
        race_no=2,
        source_file="program-20250102-02.txt",
    )
    race1 = make_race(
        race_date="2025-01-02",
        venue_code="01",
        race_no=1,
        source_file="program-20250102-01.txt",
    )

    result = build_program_only_features(race2 + race1)

    assert len(result) == 12
    assert result[0]["venue_code"] == "01"
    assert result[0]["race_no"] == 1
    assert result[0]["boat_no"] == 1
    assert result[-1]["venue_code"] == "02"
    assert result[-1]["race_no"] == 2
    assert result[-1]["boat_no"] == 6
