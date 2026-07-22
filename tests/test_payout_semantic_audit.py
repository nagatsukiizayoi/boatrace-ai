import pandas as pd
import pytest

from boatrace_ai.audits.payout_semantics import (
    PayoutSemanticAuditError,
    assert_payout_semantics,
    audit_payout_semantics,
)


def normal_row(**overrides):
    row = {
        "race_date": "2023-05-01",
        "venue_code": "01",
        "race_no": 1,
        "bet_type": "WIN",
        "combination": "1",
        "payout_yen": 250,
        "payout_status": "NORMAL",
    }
    row.update(overrides)
    return row


def placeholder_row(**overrides):
    row = {
        "race_date": "2023-05-01",
        "venue_code": "01",
        "race_no": 2,
        "bet_type": "WIN",
        "combination": None,
        "payout_yen": None,
        "payout_status": "CANCELLED",
    }
    row.update(overrides)
    return row


def program_for(*race_numbers):
    return pd.DataFrame(
        [
            {
                "race_date": "2023-05-01",
                "venue_code": "01",
                "race_no": race_no,
            }
            for race_no in race_numbers
        ]
    )


def test_accepts_normal_and_all_placeholder_statuses():
    payout = pd.DataFrame(
        [
            normal_row(race_no=1),
            placeholder_row(
                race_no=2,
                payout_status="CANCELLED",
            ),
            placeholder_row(
                race_no=3,
                payout_status="NOT_ESTABLISHED",
            ),
            placeholder_row(
                race_no=4,
                payout_status="SPECIAL_PAYOUT",
                payout_yen=70,
            ),
        ]
    )

    report = audit_payout_semantics(
        payout,
        program=program_for(1, 2, 3, 4),
    )

    assert report["status"] == "SUCCESS"
    assert report["failed_checks"] == []
    assert report["metrics"]["normal_payout_rows"] == 1
    assert report["metrics"]["placeholder_rows"] == 3
    assert report["metrics"]["normal_duplicate_rows"] == 0
    assert report["metrics"]["placeholder_duplicate_rows"] == 0


def test_accepts_multiple_normal_combinations_for_same_bet_type():
    payout = pd.DataFrame(
        [
            normal_row(combination="1-2", payout_yen=500),
            normal_row(combination="2-1", payout_yen=800),
        ]
    )

    report = audit_payout_semantics(
        payout,
        program=program_for(1),
    )

    assert report["status"] == "SUCCESS"
    assert report["metrics"]["normal_payout_rows"] == 2


def test_rejects_duplicate_normal_full_key():
    row = normal_row()
    payout = pd.DataFrame([row, row.copy()])

    report = audit_payout_semantics(payout)

    assert report["status"] == "FAILED"
    assert "normal_full_keys_unique" in report["failed_checks"]
    assert report["metrics"]["normal_duplicate_rows"] == 2
    assert report["metrics"]["normal_duplicate_groups"] == 1


def test_rejects_duplicate_placeholder_key():
    row = placeholder_row()
    payout = pd.DataFrame([row, row.copy()])

    report = audit_payout_semantics(payout)

    assert report["status"] == "FAILED"
    assert "placeholder_keys_unique" in report["failed_checks"]
    assert report["metrics"]["placeholder_duplicate_rows"] == 2


@pytest.mark.parametrize(
    ("status", "payout_yen", "failed_check"),
    [
        (
            "UNKNOWN",
            None,
            "placeholder_statuses_allowed",
        ),
        (
            "CANCELLED",
            100,
            "cancelled_not_established_payout_is_null",
        ),
        (
            "NOT_ESTABLISHED",
            100,
            "cancelled_not_established_payout_is_null",
        ),
        (
            "SPECIAL_PAYOUT",
            None,
            "special_payout_is_70_yen",
        ),
        (
            "SPECIAL_PAYOUT",
            80,
            "special_payout_is_70_yen",
        ),
    ],
)
def test_rejects_invalid_placeholder_semantics(
    status,
    payout_yen,
    failed_check,
):
    payout = pd.DataFrame(
        [
            placeholder_row(
                payout_status=status,
                payout_yen=payout_yen,
            )
        ]
    )

    report = audit_payout_semantics(payout)

    assert report["status"] == "FAILED"
    assert failed_check in report["failed_checks"]


def test_rejects_blank_combination():
    payout = pd.DataFrame(
        [
            normal_row(combination="   "),
        ]
    )

    report = audit_payout_semantics(payout)

    assert report["status"] == "FAILED"
    assert (
        "combination_has_no_blank_strings"
        in report["failed_checks"]
    )


def test_rejects_base_key_null():
    payout = pd.DataFrame(
        [
            normal_row(venue_code=None),
        ]
    )

    report = audit_payout_semantics(payout)

    assert report["status"] == "FAILED"
    assert "base_race_keys_non_null" in report["failed_checks"]


def test_rejects_program_payout_race_set_difference():
    payout = pd.DataFrame(
        [
            normal_row(race_no=1),
        ]
    )

    report = audit_payout_semantics(
        payout,
        program=program_for(1, 2),
    )

    assert report["status"] == "FAILED"
    assert (
        "payout_covers_all_program_races"
        in report["failed_checks"]
    )
    assert report["metrics"]["program_only_race_count"] == 1


def test_assert_helper_raises_with_report():
    payout = pd.DataFrame(
        [
            placeholder_row(
                payout_status="SPECIAL_PAYOUT",
                payout_yen=999,
            )
        ]
    )

    with pytest.raises(PayoutSemanticAuditError) as captured:
        assert_payout_semantics(payout)

    assert captured.value.report["status"] == "FAILED"
    assert (
        "special_payout_is_70_yen"
        in captured.value.report["failed_checks"]
    )


def test_audit_does_not_modify_input_frames():
    payout = pd.DataFrame(
        [
            normal_row(),
            placeholder_row(),
        ]
    )
    program = program_for(1, 2)

    payout_before = payout.copy(deep=True)
    program_before = program.copy(deep=True)

    assert_payout_semantics(payout, program)

    pd.testing.assert_frame_equal(payout, payout_before)
    pd.testing.assert_frame_equal(program, program_before)
