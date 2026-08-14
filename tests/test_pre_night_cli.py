import json
from pathlib import Path

import pytest

from boatrace_ai.cli import pre_night


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


def test_default_mode_is_dry_run(approved_root, monkeypatch, capsys):
    calls = []

    def fake_run(race_date, data_root, **kwargs):
        calls.append((race_date, data_root, kwargs))
        return {"status": "dry-run-ok"}

    monkeypatch.setattr(
        pre_night,
        "run_pre_night_daily",
        fake_run,
    )

    return_code = pre_night.main(
        [
            "--race-date",
            "2026-07-01",
            "--data-root",
            str(approved_root),
        ]
    )

    assert return_code == 0
    assert calls[0][0] == "2026-07-01"
    assert calls[0][2]["dry_run"] is True
    assert calls[0][2]["overwrite"] is False

    output = json.loads(capsys.readouterr().out)
    assert output["ok"] is True
    assert output["dry_run"] is True


def test_explicit_dry_run(approved_root, monkeypatch):
    observed = {}

    def fake_run(race_date, data_root, **kwargs):
        observed.update(kwargs)
        return {}

    monkeypatch.setattr(
        pre_night,
        "run_pre_night_daily",
        fake_run,
    )

    assert pre_night.main(
        [
            "--race-date",
            "2026-07-02",
            "--data-root",
            str(approved_root),
            "--dry-run",
        ]
    ) == 0

    assert observed["dry_run"] is True


def test_live_requires_deadline_evidence(approved_root):
    with pytest.raises(SystemExit) as exc_info:
        pre_night.main(
            [
                "--race-date",
                "2026-07-01",
                "--data-root",
                str(approved_root),
                "--live",
            ]
        )

    assert exc_info.value.code == 2


def test_live_passes_deadline_evidence(
    approved_root,
    tmp_path,
    monkeypatch,
):
    evidence_path = tmp_path / "deadline-evidence.json"
    evidence_path.write_text(
        json.dumps(
            {
                "source": "contract-test",
                "collected_at": "2026-07-01T08:00:00+09:00",
            }
        ),
        encoding="utf-8",
    )

    observed = {}

    def fake_run(race_date, data_root, **kwargs):
        observed.update(kwargs)
        return {"status": "live-contract-ok"}

    monkeypatch.setattr(
        pre_night,
        "run_pre_night_daily",
        fake_run,
    )

    return_code = pre_night.main(
        [
            "--race-date",
            "2026-07-01",
            "--data-root",
            str(approved_root),
            "--live",
            "--deadline-evidence",
            str(evidence_path),
        ]
    )

    assert return_code == 0
    assert observed["dry_run"] is False
    assert observed["deadline_evidence"]["source"] == "contract-test"


def test_pre_cutoff_date_is_rejected(approved_root):
    with pytest.raises(SystemExit) as exc_info:
        pre_night.main(
            [
                "--race-date",
                "2026-06-30",
                "--data-root",
                str(approved_root),
            ]
        )

    assert exc_info.value.code == 2


def test_invalid_date_is_rejected(approved_root):
    with pytest.raises(SystemExit) as exc_info:
        pre_night.main(
            [
                "--race-date",
                "not-a-date",
                "--data-root",
                str(approved_root),
            ]
        )

    assert exc_info.value.code == 2


def test_unapproved_data_root_is_rejected(
    approved_root,
    tmp_path,
):
    outside = tmp_path / "outside"
    outside.mkdir()

    with pytest.raises(SystemExit) as exc_info:
        pre_night.main(
            [
                "--race-date",
                "2026-07-01",
                "--data-root",
                str(outside),
            ]
        )

    assert exc_info.value.code == 2


def test_forbidden_file_name_is_rejected(
    approved_root,
):
    forbidden = approved_root / "holdout.parquet"

    with pytest.raises(SystemExit) as exc_info:
        pre_night.main(
            [
                "--race-date",
                "2026-07-01",
                "--data-root",
                str(forbidden),
            ]
        )

    assert exc_info.value.code == 2


def test_overwrite_is_rejected_in_dry_run(approved_root):
    with pytest.raises(SystemExit) as exc_info:
        pre_night.main(
            [
                "--race-date",
                "2026-07-01",
                "--data-root",
                str(approved_root),
                "--overwrite",
            ]
        )

    assert exc_info.value.code == 2


def test_malformed_deadline_evidence_fails_closed(
    approved_root,
    tmp_path,
    capsys,
):
    evidence_path = tmp_path / "invalid.json"
    evidence_path.write_text("{invalid", encoding="utf-8")

    return_code = pre_night.main(
        [
            "--race-date",
            "2026-07-01",
            "--data-root",
            str(approved_root),
            "--live",
            "--deadline-evidence",
            str(evidence_path),
        ]
    )

    assert return_code == 1
    error_output = json.loads(capsys.readouterr().err)
    assert error_output["ok"] is False
