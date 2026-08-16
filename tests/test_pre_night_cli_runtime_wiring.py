import json

import pytest

from boatrace_ai.cli import pre_night


RACE_DATE = "2026-07-30"
VENUE_CODE = "01"
RUN_ID = "pre-night-20260729T120000Z-cli"


@pytest.fixture
def approved_root(tmp_path, monkeypatch):
    root = tmp_path / "approved"
    root.mkdir()

    monkeypatch.setattr(
        pre_night,
        "APPROVED_DATA_ROOTS",
        (root,),
    )

    return root


def write_json(tmp_path, name, payload):
    path = tmp_path / name
    path.write_text(
        json.dumps(payload),
        encoding="utf-8",
    )
    return path


def test_legacy_mode_preserves_existing_runner(
    approved_root,
    monkeypatch,
    capsys,
):
    calls = []

    def legacy_runner(race_date, data_root, **kwargs):
        calls.append(
            ("legacy", race_date, data_root, kwargs)
        )
        return {"status": "DRY_RUN"}

    def bound_runner(*args, **kwargs):
        pytest.fail(
            "bound runner must not be called "
            "in backward-compatible mode"
        )

    monkeypatch.setattr(
        pre_night,
        "run_pre_night_daily",
        legacy_runner,
    )
    monkeypatch.setattr(
        pre_night,
        "run_pre_night_bound_daily",
        bound_runner,
    )

    return_code = pre_night.main(
        [
            "--race-date",
            RACE_DATE,
            "--data-root",
            str(approved_root),
        ]
    )

    assert return_code == 0
    assert len(calls) == 1
    assert calls[0][0] == "legacy"
    assert calls[0][1] == RACE_DATE
    assert calls[0][3]["dry_run"] is True

    output = json.loads(capsys.readouterr().out)
    assert output["runtime_mode"] == "LEGACY"
    assert "venue_code" not in output
    assert "run_id" not in output


def test_venue_bound_dry_run_dispatches_bound_runner(
    approved_root,
    monkeypatch,
    capsys,
):
    observed = {}

    def legacy_runner(*args, **kwargs):
        pytest.fail(
            "legacy runner must not be called "
            "in venue-bound mode"
        )

    def bound_runner(race_date, data_root, **kwargs):
        observed["race_date"] = race_date
        observed["data_root"] = data_root
        observed.update(kwargs)
        return {"status": "DRY_RUN"}

    monkeypatch.setattr(
        pre_night,
        "run_pre_night_daily",
        legacy_runner,
    )
    monkeypatch.setattr(
        pre_night,
        "run_pre_night_bound_daily",
        bound_runner,
    )

    return_code = pre_night.main(
        [
            "--race-date",
            RACE_DATE,
            "--data-root",
            str(approved_root),
            "--venue-code",
            VENUE_CODE,
            "--run-id",
            RUN_ID,
            "--dry-run",
        ]
    )

    assert return_code == 0
    assert observed["race_date"] == RACE_DATE
    assert observed["venue_code"] == VENUE_CODE
    assert observed["run_id"] == RUN_ID
    assert observed["dry_run"] is True
    assert observed["authorization_state"] is None
    assert observed["test_state"] is None

    output = json.loads(capsys.readouterr().out)
    assert output["runtime_mode"] == "VENUE_BOUND"
    assert output["venue_code"] == VENUE_CODE
    assert output["run_id"] == RUN_ID


def test_venue_bound_live_loads_all_contract_inputs(
    approved_root,
    tmp_path,
    monkeypatch,
    capsys,
):
    evidence_path = write_json(
        tmp_path,
        "deadline.json",
        {
            "venue_code": VENUE_CODE,
            "source": "contract-test",
        },
    )
    authorization_path = write_json(
        tmp_path,
        "authorization.json",
        {
            "approved": True,
            "approver": "contract-test",
        },
    )
    test_state_path = write_json(
        tmp_path,
        "test-state.json",
        {
            "focused": "PASSED",
            "full_suite": "PASSED",
        },
    )

    observed = {}

    def bound_runner(race_date, data_root, **kwargs):
        observed["race_date"] = race_date
        observed["data_root"] = data_root
        observed.update(kwargs)
        return {"status": "SUCCESS"}

    monkeypatch.setattr(
        pre_night,
        "run_pre_night_bound_daily",
        bound_runner,
    )

    return_code = pre_night.main(
        [
            "--race-date",
            RACE_DATE,
            "--data-root",
            str(approved_root),
            "--venue-code",
            VENUE_CODE,
            "--run-id",
            RUN_ID,
            "--live",
            "--deadline-evidence",
            str(evidence_path),
            "--authorization-state",
            str(authorization_path),
            "--test-state",
            str(test_state_path),
        ]
    )

    assert return_code == 0
    assert observed["dry_run"] is False
    assert observed["venue_code"] == VENUE_CODE
    assert observed["run_id"] == RUN_ID
    assert observed["deadline_evidence"][
        "venue_code"
    ] == VENUE_CODE
    assert observed["authorization_state"][
        "approved"
    ] is True
    assert observed["test_state"][
        "full_suite"
    ] == "PASSED"

    output = json.loads(capsys.readouterr().out)
    assert output["ok"] is True
    assert output["runtime_mode"] == "VENUE_BOUND"


def test_rejects_venue_code_without_run_id(
    approved_root,
):
    with pytest.raises(SystemExit) as exc_info:
        pre_night.main(
            [
                "--race-date",
                RACE_DATE,
                "--data-root",
                str(approved_root),
                "--venue-code",
                VENUE_CODE,
            ]
        )

    assert exc_info.value.code == 2


def test_rejects_run_id_without_venue_code(
    approved_root,
):
    with pytest.raises(SystemExit) as exc_info:
        pre_night.main(
            [
                "--race-date",
                RACE_DATE,
                "--data-root",
                str(approved_root),
                "--run-id",
                RUN_ID,
            ]
        )

    assert exc_info.value.code == 2


def test_rejects_invalid_venue_code(
    approved_root,
):
    with pytest.raises(SystemExit) as exc_info:
        pre_night.main(
            [
                "--race-date",
                RACE_DATE,
                "--data-root",
                str(approved_root),
                "--venue-code",
                "25",
                "--run-id",
                RUN_ID,
            ]
        )

    assert exc_info.value.code == 2


def test_rejects_invalid_run_id(
    approved_root,
):
    with pytest.raises(SystemExit) as exc_info:
        pre_night.main(
            [
                "--race-date",
                RACE_DATE,
                "--data-root",
                str(approved_root),
                "--venue-code",
                VENUE_CODE,
                "--run-id",
                "invalid run id",
            ]
        )

    assert exc_info.value.code == 2


def test_bound_live_requires_authorization_state(
    approved_root,
    tmp_path,
):
    evidence_path = write_json(
        tmp_path,
        "deadline.json",
        {"venue_code": VENUE_CODE},
    )
    test_state_path = write_json(
        tmp_path,
        "test-state.json",
        {"focused": "PASSED"},
    )

    with pytest.raises(SystemExit) as exc_info:
        pre_night.main(
            [
                "--race-date",
                RACE_DATE,
                "--data-root",
                str(approved_root),
                "--venue-code",
                VENUE_CODE,
                "--run-id",
                RUN_ID,
                "--live",
                "--deadline-evidence",
                str(evidence_path),
                "--test-state",
                str(test_state_path),
            ]
        )

    assert exc_info.value.code == 2


def test_bound_live_requires_test_state(
    approved_root,
    tmp_path,
):
    evidence_path = write_json(
        tmp_path,
        "deadline.json",
        {"venue_code": VENUE_CODE},
    )
    authorization_path = write_json(
        tmp_path,
        "authorization.json",
        {"approved": True},
    )

    with pytest.raises(SystemExit) as exc_info:
        pre_night.main(
            [
                "--race-date",
                RACE_DATE,
                "--data-root",
                str(approved_root),
                "--venue-code",
                VENUE_CODE,
                "--run-id",
                RUN_ID,
                "--live",
                "--deadline-evidence",
                str(evidence_path),
                "--authorization-state",
                str(authorization_path),
            ]
        )

    assert exc_info.value.code == 2


def test_malformed_authorization_json_fails_closed(
    approved_root,
    tmp_path,
    capsys,
):
    evidence_path = write_json(
        tmp_path,
        "deadline.json",
        {"venue_code": VENUE_CODE},
    )
    authorization_path = (
        tmp_path / "authorization.json"
    )
    authorization_path.write_text(
        "{invalid",
        encoding="utf-8",
    )
    test_state_path = write_json(
        tmp_path,
        "test-state.json",
        {"focused": "PASSED"},
    )

    return_code = pre_night.main(
        [
            "--race-date",
            RACE_DATE,
            "--data-root",
            str(approved_root),
            "--venue-code",
            VENUE_CODE,
            "--run-id",
            RUN_ID,
            "--live",
            "--deadline-evidence",
            str(evidence_path),
            "--authorization-state",
            str(authorization_path),
            "--test-state",
            str(test_state_path),
        ]
    )

    assert return_code == 1

    error = json.loads(capsys.readouterr().err)
    assert error["ok"] is False
    assert error["runtime_mode"] == "VENUE_BOUND"


def test_non_object_test_state_fails_closed(
    approved_root,
    tmp_path,
    capsys,
):
    test_state_path = write_json(
        tmp_path,
        "test-state.json",
        ["not", "an", "object"],
    )

    return_code = pre_night.main(
        [
            "--race-date",
            RACE_DATE,
            "--data-root",
            str(approved_root),
            "--venue-code",
            VENUE_CODE,
            "--run-id",
            RUN_ID,
            "--test-state",
            str(test_state_path),
        ]
    )

    assert return_code == 1

    error = json.loads(capsys.readouterr().err)
    assert error["error_type"] == "ValueError"
    assert "JSON root must be an object" in error["error"]


def test_rejects_bound_state_options_in_legacy_mode(
    approved_root,
    tmp_path,
):
    authorization_path = write_json(
        tmp_path,
        "authorization.json",
        {"approved": True},
    )

    with pytest.raises(SystemExit) as exc_info:
        pre_night.main(
            [
                "--race-date",
                RACE_DATE,
                "--data-root",
                str(approved_root),
                "--authorization-state",
                str(authorization_path),
            ]
        )

    assert exc_info.value.code == 2
