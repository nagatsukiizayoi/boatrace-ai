import hashlib
from pathlib import Path

import pandas as pd
import pytest

from boatrace_ai.pipelines import pre_night_bound_daily as bound


RACE_DATE = "2026-07-30"
VENUE_CODE = "01"
RUN_ID = "pre-night-20260729T120000Z-runtime"


def _write_daily_artifacts(root):
    source_archive = root / "legacy/source.zip"
    output_parquet = root / "legacy/program.parquet"

    source_archive.parent.mkdir(parents=True)
    source_archive.write_bytes(b"exact-program-source")

    frame = pd.DataFrame([
        {
            "race_date": RACE_DATE,
            "venue_code": VENUE_CODE,
            "race_no": race_no,
            "boat_no": boat_no,
        }
        for race_no in range(1, 13)
        for boat_no in range(1, 7)
    ])
    frame.to_parquet(output_parquet, index=False)

    return source_archive, output_parquet


def _daily_result(root):
    source_archive, output_parquet = _write_daily_artifacts(
        root
    )

    return {
        "status": "SUCCESS",
        "collector_result": {
            "paths": {
                "archive": source_archive,
            },
        },
        "pipeline_result": {
            "paths": {
                "parquet": output_parquet,
            },
        },
        "execution_manifest": {
            "manifest": {
                "started_at": "2026-07-29T10:00:00+09:00",
                "completed_at": "2026-07-29T10:01:00+09:00",
            },
        },
    }


def test_dry_run_does_not_call_provenance_stages(tmp_path):
    calls = []

    def daily(*args, **kwargs):
        calls.append("daily")
        assert kwargs["dry_run"] is True
        return {"status": "DRY_RUN"}

    def forbidden(*args, **kwargs):
        pytest.fail("provenance stage must not run in dry-run")

    result = bound.run_pre_night_bound_daily(
        RACE_DATE,
        tmp_path,
        venue_code=VENUE_CODE,
        run_id=RUN_ID,
        dry_run=True,
        daily_runner=daily,
        deadline_collector=forbidden,
        binding_publisher=forbidden,
        manifest_publisher=forbidden,
    )

    assert calls == ["daily"]
    assert result["status"] == "DRY_RUN"
    assert result["stage2_called"] is False
    assert result["stage3_called"] is False
    assert result["stage4_called"] is False
    assert not tmp_path.exists() or list(tmp_path.iterdir()) == []


def test_live_run_wires_stages_in_order(tmp_path):
    events = []
    expected_source_sha256 = hashlib.sha256(
        b"exact-program-source"
    ).hexdigest()

    def daily(*args, **kwargs):
        events.append("daily")
        assert kwargs["dry_run"] is False
        return _daily_result(tmp_path)

    def stage2(root, **kwargs):
        events.append("stage2")
        assert Path(root) == tmp_path
        assert kwargs == {
            "race_date": RACE_DATE,
            "expected_venue_codes": [VENUE_CODE],
        }
        return {
            "deadline_evidence_collection_sha256": "c" * 64,
        }

    def stage3(root, **kwargs):
        events.append("stage3")
        assert kwargs[
            "program_source_sha256_by_venue"
        ] == {
            VENUE_CODE: expected_source_sha256,
        }
        assert len(kwargs["program_entries"]) == 72
        assert {
            entry["venue_code"]
            for entry in kwargs["program_entries"]
        } == {VENUE_CODE}
        return {
            "program_entries_binding_sha256": "b" * 64,
        }

    def stage4(root, **kwargs):
        events.append("stage4")
        snapshot = (
            Path(root) / kwargs["snapshot_relative_path"]
        )
        assert snapshot.is_file()
        assert snapshot.read_bytes() == (
            tmp_path / "legacy/program.parquet"
        ).read_bytes()
        assert kwargs["branch"] == "main"
        assert kwargs["head"] == "a" * 40
        return {
            "pipeline_manifest_sha256": "d" * 64,
            "execution_manifest_sha256": "e" * 64,
        }

    result = bound.run_pre_night_bound_daily(
        RACE_DATE,
        tmp_path,
        venue_code=VENUE_CODE,
        run_id=RUN_ID,
        dry_run=False,
        deadline_evidence={"venue_code": VENUE_CODE},
        authorization_state={"approved": True},
        test_state={"focused": "PASSED"},
        daily_runner=daily,
        deadline_collector=stage2,
        binding_publisher=stage3,
        manifest_publisher=stage4,
        branch_provider=lambda: "main",
        head_provider=lambda: "a" * 40,
    )

    assert events == [
        "daily",
        "stage2",
        "stage3",
        "stage4",
    ]
    assert result["status"] == "SUCCESS"
    assert result["program_entry_count"] == 72
    assert result[
        "program_source_sha256_by_venue"
    ] == {
        VENUE_CODE: expected_source_sha256,
    }
    assert result["snapshot_path"].is_file()


def test_rejects_mixed_venue_parquet(tmp_path):
    def daily(*args, **kwargs):
        result = _daily_result(tmp_path)
        path = result["pipeline_result"]["paths"]["parquet"]
        frame = pd.read_parquet(path)
        frame.loc[0, "venue_code"] = "02"
        frame.to_parquet(path, index=False)
        return result

    with pytest.raises(
        bound.PreNightBoundDailyIntegrityError,
        match="venue coverage",
    ):
        bound.run_pre_night_bound_daily(
            RACE_DATE,
            tmp_path,
            venue_code=VENUE_CODE,
            run_id=RUN_ID,
            dry_run=False,
            deadline_evidence={"venue_code": VENUE_CODE},
            authorization_state={"approved": True},
            test_state={"focused": "PASSED"},
            daily_runner=daily,
        )


def test_rejects_deadline_evidence_for_other_venue(tmp_path):
    with pytest.raises(
        bound.PreNightBoundDailyContractError,
        match="venue_code mismatch",
    ):
        bound.run_pre_night_bound_daily(
            RACE_DATE,
            tmp_path,
            venue_code="01",
            run_id=RUN_ID,
            dry_run=False,
            deadline_evidence={"venue_code": "02"},
            authorization_state={"approved": True},
            test_state={"focused": "PASSED"},
            daily_runner=lambda *args, **kwargs: (
                _daily_result(tmp_path)
            ),
        )
