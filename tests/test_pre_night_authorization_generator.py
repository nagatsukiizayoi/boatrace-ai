import hashlib
import json

import pytest

from boatrace_ai.authorization.pre_night_generator import (
    AuthorizationContractError,
    generate_pre_night_authorization_artifacts,
    validate_deadlines,
)


DEADLINES = [
    {"race": 1, "deadline": "10:47"},
    {"race": 2, "deadline": "11:16"},
    {"race": 3, "deadline": "11:45"},
    {"race": 4, "deadline": "12:14"},
    {"race": 5, "deadline": "12:44"},
    {"race": 6, "deadline": "13:14"},
    {"race": 7, "deadline": "13:45"},
    {"race": 8, "deadline": "14:16"},
    {"race": 9, "deadline": "14:48"},
    {"race": 10, "deadline": "15:21"},
    {"race": 11, "deadline": "15:55"},
    {"race": 12, "deadline": "16:30"},
]


def _write_json(path, value):
    path.write_text(
        json.dumps(
            value,
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def _sha256(path):
    return hashlib.sha256(
        path.read_bytes()
    ).hexdigest()


def _source_files(tmp_path):
    visual = tmp_path / "visual.json"
    contract = tmp_path / "contract.json"

    _write_json(
        visual,
        {
            "classification": (
                "PRE_NIGHT_DEADLINE_VISUAL_REVIEW_CONFIRMED"
            ),
            "race_date": "2026-08-17",
            "venue_code": "02",
            "live_execution_performed": False,
        },
    )

    _write_json(
        contract,
        {
            "classification": (
                "PRE_NIGHT_AUTHORIZATION_GENERATOR_NOT_FOUND"
            ),
            "errors": [],
            "live_execution_performed": False,
        },
    )

    return visual, contract


def _generate(tmp_path, **overrides):
    visual, contract = _source_files(tmp_path)

    arguments = {
        "output_root": tmp_path / "output",
        "race_date": "2026-08-17",
        "venue_code": "02",
        "venue_name": "戸田",
        "reviewer": "長月十六夜",
        "confirmation_phrase": (
            "CONFIRM DEADLINES 2026-08-17 "
            "VENUE 02 1R-12R MATCH"
        ),
        "deadlines": DEADLINES,
        "visual_review_path": visual,
        "expected_visual_review_sha256": (
            _sha256(visual)
        ),
        "contract_review_path": contract,
        "expected_contract_review_sha256": (
            _sha256(contract)
        ),
        "test_state": {
            "focused": "PASSED",
            "full_suite": "PASSED",
        },
    }
    arguments.update(overrides)

    return generate_pre_night_authorization_artifacts(
        **arguments
    )


def test_validate_deadlines_accepts_expected_schedule():
    assert validate_deadlines(DEADLINES) == DEADLINES


def test_validate_deadlines_rejects_missing_race():
    with pytest.raises(
        AuthorizationContractError,
        match="exactly 12",
    ):
        validate_deadlines(DEADLINES[:-1])


def test_validate_deadlines_rejects_wrong_order():
    invalid = list(DEADLINES)
    invalid[0], invalid[1] = invalid[1], invalid[0]

    with pytest.raises(
        AuthorizationContractError,
        match="1R-12R order",
    ):
        validate_deadlines(invalid)


def test_validate_deadlines_rejects_bad_interval():
    invalid = [
        dict(item)
        for item in DEADLINES
    ]
    invalid[1]["deadline"] = "10:48"

    with pytest.raises(
        AuthorizationContractError,
        match="between 5 and 90",
    ):
        validate_deadlines(invalid)


def test_generator_writes_exact_downstream_contract(
    tmp_path,
):
    generated = _generate(tmp_path)

    authorization = json.loads(
        generated.authorization_path.read_text(
            encoding="utf-8"
        )
    )
    test_state = json.loads(
        generated.test_state_path.read_text(
            encoding="utf-8"
        )
    )
    receipt = json.loads(
        generated.receipt_path.read_text(
            encoding="utf-8"
        )
    )

    assert authorization == {"approved": True}

    assert test_state == {
        "focused": "PASSED",
        "full_suite": "PASSED",
    }

    assert receipt["race_date"] == "2026-08-17"
    assert receipt["venue_code"] == "02"
    assert receipt["reviewer"] == "長月十六夜"
    assert receipt["deadlines"] == DEADLINES
    assert receipt["human_approval_recorded"] is True
    assert receipt["authorization_created"] is True
    assert receipt["live_execution_performed"] is False


def test_generator_rejects_wrong_confirmation_phrase(
    tmp_path,
):
    with pytest.raises(
        AuthorizationContractError,
        match="confirmation phrase",
    ):
        _generate(
            tmp_path,
            confirmation_phrase="CONFIRM",
        )


def test_generator_rejects_visual_hash_mismatch(
    tmp_path,
):
    with pytest.raises(
        AuthorizationContractError,
        match="visual review SHA-256 mismatch",
    ):
        _generate(
            tmp_path,
            expected_visual_review_sha256="0" * 64,
        )


def test_generator_rejects_unapproved_test_state(
    tmp_path,
):
    with pytest.raises(
        AuthorizationContractError,
        match="test_state must exactly equal",
    ):
        _generate(
            tmp_path,
            test_state={
                "focused": "PASSED",
                "full_suite": "FAILED",
            },
        )


def test_generator_refuses_overwrite(tmp_path):
    _generate(tmp_path)

    with pytest.raises(
        FileExistsError,
        match="refusing to overwrite",
    ):
        _generate(tmp_path)
