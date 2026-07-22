from __future__ import annotations

import pandas as pd
import pytest

from boatrace_ai.audits.period import (
    PeriodAuditError,
    _blank_cell_count,
    assert_period_audit,
    audit_period,
)


BET_TYPES = [
    "WIN",
    "PLACE",
    "EXACTA",
    "QUINELLA",
    "QUINELLA_PLACE",
    "TRIFECTA",
    "TRIO",
]


def make_month(
    race_date: str = "2026-01-01",
    *,
    venue_code: str = "01",
    race_no: int = 1,
):
    program = pd.DataFrame({
        "race_date": [race_date] * 6,
        "venue_code": [venue_code] * 6,
        "race_no": [race_no] * 6,
        "boat_no": [1, 2, 3, 4, 5, 6],
    })

    result = program.copy()
    merged = program.copy()

    payout = pd.DataFrame({
        "race_date": [race_date] * 7,
        "venue_code": [venue_code] * 7,
        "race_no": [race_no] * 7,
        "bet_type": BET_TYPES,
        "combination": pd.Series(
            [pd.NA] * 7,
            dtype="string",
        ),
        "payout_yen": pd.Series(
            [pd.NA] * 7,
            dtype="Int64",
        ),
        "payout_status": pd.Series(
            ["CANCELLED"] * 7,
            dtype="string",
        ),
    })

    return {
        "program": program,
        "result": result,
        "merged": merged,
        "payout": payout,
    }


def failed_check_names(report):
    return {
        item["check"]
        for item in report["failed_checks"]
    }


def test_audit_period_accepts_valid_month():
    report = audit_period({
        "2026-01": make_month(),
    })

    assert report["status"] == "SUCCESS"
    assert report["classification"] == "PERIOD_AUDIT_SUCCESS"
    assert report["month_count"] == 1
    assert report["failed_checks"] == []
    assert report["cross_period_metrics"][
        "program_race_count"
    ] == 1
    assert report["cross_period_metrics"][
        "payout_race_count"
    ] == 1


def test_assert_period_audit_returns_success_report():
    report = assert_period_audit({
        "2026-01": make_month(),
    })

    assert report["status"] == "SUCCESS"


def test_empty_period_fails():
    report = audit_period({})

    assert report["status"] == "FAILED"
    assert "months_non_empty_mapping" in failed_check_names(
        report
    )


def test_invalid_month_label_fails():
    report = audit_period({
        "202601": make_month(),
    })

    assert report["status"] == "FAILED"
    assert "month_label_format" in failed_check_names(
        report
    )


def test_missing_frame_fails():
    month = make_month()
    del month["result"]

    report = audit_period({
        "2026-01": month,
    })

    assert report["status"] == "FAILED"
    assert "required_frames" in failed_check_names(report)


def test_program_duplicate_is_detected():
    month = make_month()
    month["program"] = pd.concat(
        [
            month["program"],
            month["program"].iloc[[0]],
        ],
        ignore_index=True,
    )

    report = audit_period({
        "2026-01": month,
    })

    assert report["status"] == "FAILED"
    assert (
        "program_duplicate_rows"
        in failed_check_names(report)
    )


def test_placeholder_duplicate_is_detected():
    month = make_month()
    month["payout"] = pd.concat(
        [
            month["payout"],
            month["payout"].iloc[[0]],
        ],
        ignore_index=True,
    )

    report = audit_period({
        "2026-01": month,
    })

    assert report["status"] == "FAILED"
    assert (
        "placeholder_duplicate_rows"
        in failed_check_names(report)
    )


def test_program_payout_race_mismatch_is_detected():
    month = make_month()
    month["payout"]["race_no"] = 2

    report = audit_period({
        "2026-01": month,
    })

    checks = failed_check_names(report)

    assert report["status"] == "FAILED"
    assert "program_only_races" in checks
    assert "payout_only_races" in checks


def test_missing_bet_type_group_is_detected():
    month = make_month()
    month["payout"] = month["payout"].iloc[:-1].copy()

    report = audit_period({
        "2026-01": month,
    })

    checks = failed_check_names(report)

    assert report["status"] == "FAILED"
    assert "bet_type_set" in checks
    assert "races_without_exactly_7_bet_types" in checks


def test_blank_combination_is_detected():
    month = make_month()
    month["payout"].loc[0, "combination"] = "   "

    report = audit_period({
        "2026-01": month,
    })

    assert report["status"] == "FAILED"
    assert (
        "blank_combination_rows"
        in failed_check_names(report)
    )


def test_blank_cell_count_handles_multiple_rows():
    frame = pd.DataFrame({
        "value": pd.Series(
            ["", "   ", "ok", pd.NA],
            dtype="string",
        ),
    })

    assert _blank_cell_count(frame, ["value"]) == 2


def test_nullable_string_and_integer_dtypes_are_supported():
    month = make_month()

    month["program"]["race_date"] = (
        month["program"]["race_date"].astype("string")
    )
    month["program"]["venue_code"] = (
        month["program"]["venue_code"].astype("string")
    )
    month["program"]["race_no"] = (
        month["program"]["race_no"].astype("Int64")
    )
    month["program"]["boat_no"] = (
        month["program"]["boat_no"].astype("Int64")
    )

    month["result"] = month["program"].copy()
    month["merged"] = month["program"].copy()

    report = audit_period({
        "2026-01": month,
    })

    assert report["status"] == "SUCCESS"


def test_cross_month_duplicate_is_detected():
    first = make_month("2026-01-01")
    second = make_month("2026-01-01")

    report = audit_period({
        "2026-01": first,
        "2026-02": second,
    })

    checks = failed_check_names(report)

    assert report["status"] == "FAILED"
    assert "program_duplicate_rows" in checks
    assert "placeholder_duplicate_rows" in checks


def test_assert_period_audit_raises_with_report():
    month = make_month()
    month["payout"] = month["payout"].iloc[:-1].copy()

    with pytest.raises(PeriodAuditError) as exc_info:
        assert_period_audit({
            "2026-01": month,
        })

    assert exc_info.value.report["status"] == "FAILED"
