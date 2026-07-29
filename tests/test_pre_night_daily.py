import datetime as dt
import hashlib
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from boatrace_ai.ingestion import pre_night_deadlines as deadlines
from boatrace_ai.pipelines import pre_night_daily


JST = ZoneInfo("Asia/Tokyo")
RACE_DATE = dt.date(2026, 8, 10)
BEFORE_AS_OF = dt.datetime(
    2026,
    8,
    9,
    21,
    0,
    tzinfo=JST,
)
AFTER_AS_OF = dt.datetime(
    2026,
    8,
    9,
    21,
    31,
    tzinfo=JST,
)


def make_deadline_binding(venue_code="01"):
    raw = (
        b"<html><body>"
        b"official scheduled deadline test"
        b"</body></html>"
    )
    fetched_at = dt.datetime(
        2026, 8, 9, 12, 0, tzinfo=dt.timezone.utc
    )
    deadline_base = dt.datetime(
        2026, 8, 10, 10, 0, tzinfo=JST
    )

    evidence = deadlines.build_deadline_evidence(
        raw_source_bytes=raw + venue_code.encode("ascii"),
        source_locator=(
            "https://example.invalid/deadline-schedule/"
            + venue_code
        ),
        source_name="BOAT RACE official test page",
        source_authority="BOAT RACE",
        request_started_at=(
            fetched_at - dt.timedelta(seconds=2)
        ),
        fetched_at=fetched_at,
        http_status=200,
        response_headers={
            "Content-Type": "text/html; charset=UTF-8",
        },
        race_date=RACE_DATE.isoformat(),
        venue_code=venue_code,
        source_timezone=(
            "explicit-source-timezone-evidence"
        ),
        race_deadlines=[
            {
                "race_no": race_no,
                "deadline_kind": deadlines.DEADLINE_KIND,
                "scheduled_deadline_at": (
                    deadline_base
                    + dt.timedelta(minutes=race_no)
                ),
            }
            for race_no in range(1, 13)
        ],
    )

    digest = hashlib.sha256(
        deadlines.canonical_deadline_evidence_bytes(
            evidence
        )
    ).hexdigest()

    return evidence, digest


VALID_DEADLINE_EVIDENCE, VALID_DEADLINE_SHA256 = (
    make_deadline_binding()
)


def make_fake_functions(calls):

    def collector(
        race_date,
        data_root,
        now_fn=None,
        *,
        deadline_evidence=None,
    ):
        calls["collector"] += 1
        assert deadline_evidence == VALID_DEADLINE_EVIDENCE

        root = Path(data_root)
        directory = (
            root
            / "snapshots"
            / "pre_night_v2"
            / "program"
            / race_date.isoformat()
        )
        directory.mkdir(parents=True, exist_ok=True)

        archive = directory / "b260810.lzh"
        metadata = directory / "b260810.lzh.json"
        archive.write_bytes(b"test-program-archive")

        metadata_payload = {
            "race_date": race_date.isoformat(),
            "eligible_for_pre_night": True,
            "deadline_evidence": VALID_DEADLINE_EVIDENCE,
            "deadline_evidence_sha256": VALID_DEADLINE_SHA256,
        }
        metadata.write_text(
            json.dumps(metadata_payload),
            encoding="utf-8",
        )

        return {
            "paths": {
                "archive": archive,
                "metadata": metadata,
            },
            "metadata": metadata_payload,
            "cached": False,
            "eligible_for_pre_night": True,
        }

    def pipeline(
        race_date,
        data_root,
        overwrite=False,
        now_fn=None,
    ):
        calls["pipeline"] += 1

        root = Path(data_root)
        directory = (
            root
            / "features"
            / "pre_night_v2"
            / race_date.isoformat()
        )
        directory.mkdir(parents=True, exist_ok=True)

        parquet = directory / "program.parquet"
        manifest = directory / "program.parquet.json"
        parquet.write_bytes(b"test-parquet-content")

        manifest_payload = {
            "race_date": race_date.isoformat(),
            "status": "SUCCESS",
            "as_of_time": "2026-08-09T21:30:00+09:00",
            "eligibility_status": "ELIGIBLE",
            "eligible_for_pre_night": True,
            "eligibility_reason": (
                "All PRE_NIGHT PIT checks passed"
            ),
            "pit_eligibility": {
                "status": "ELIGIBLE",
                "eligible": True,
                "reason": (
                    "All PRE_NIGHT PIT checks passed"
                ),
                "race_date": "2026-08-10",
                "as_of_time": (
                    "2026-08-09T21:30:00+09:00"
                ),
                "details": {},
            },
            "deadline_evidence": VALID_DEADLINE_EVIDENCE,
            "deadline_evidence_sha256": VALID_DEADLINE_SHA256,
        }
        manifest.write_text(
            json.dumps(manifest_payload),
            encoding="utf-8",
        )

        return {
            "paths": {
                "parquet": parquet,
                "manifest": manifest,
            },
            "manifest": manifest_payload,
            "skipped": False,
        }

    return collector, pipeline


def test_dry_run_has_no_http_or_data_writes(tmp_path):
    calls = {
        "collector": 0,
        "pipeline": 0,
    }
    collector, pipeline = make_fake_functions(calls)

    result = pre_night_daily.run_pre_night_daily(
        RACE_DATE,
        tmp_path,
        dry_run=True,
        collector=collector,
        pipeline=pipeline,
        now_fn=lambda: BEFORE_AS_OF,
    )

    assert result["status"] == "DRY_RUN"
    assert result["dry_run"] is True
    assert result["eligible_to_start"] is True
    assert result["network_performed"] is False
    assert result["data_files_written"] is False
    assert result["collector_called"] is False
    assert result["pipeline_called"] is False
    assert calls == {
        "collector": 0,
        "pipeline": 0,
    }
    assert list(tmp_path.iterdir()) == []


def test_dry_run_reports_blocked_after_as_of(tmp_path):
    calls = {
        "collector": 0,
        "pipeline": 0,
    }
    collector, pipeline = make_fake_functions(calls)

    result = pre_night_daily.run_pre_night_daily(
        RACE_DATE,
        tmp_path,
        dry_run=True,
        collector=collector,
        pipeline=pipeline,
        now_fn=lambda: AFTER_AS_OF,
    )

    assert result["status"] == "BLOCKED_AFTER_AS_OF"
    assert result["eligible_to_start"] is False
    assert result["block_reason"] == (
        "CURRENT_TIME_AFTER_AS_OF"
    )
    assert calls == {
        "collector": 0,
        "pipeline": 0,
    }
    assert not tmp_path.exists() or (
        list(tmp_path.iterdir()) == []
    )


def test_live_run_is_blocked_before_collector_after_as_of(
    tmp_path,
):
    calls = {
        "collector": 0,
        "pipeline": 0,
    }
    collector, pipeline = make_fake_functions(calls)

    with pytest.raises(
        pre_night_daily.PreNightDailyDeadlineError
    ):
        pre_night_daily.run_pre_night_daily(
            RACE_DATE,
            tmp_path,
            dry_run=False,
            deadline_evidence=VALID_DEADLINE_EVIDENCE,
            collector=collector,
            pipeline=pipeline,
            now_fn=lambda: AFTER_AS_OF,
        )

    assert calls == {
        "collector": 0,
        "pipeline": 0,
    }
    assert not tmp_path.exists() or (
        list(tmp_path.iterdir()) == []
    )


def test_successful_live_run_writes_execution_manifest(
    tmp_path,
):
    calls = {
        "collector": 0,
        "pipeline": 0,
    }
    collector, pipeline = make_fake_functions(calls)

    result = pre_night_daily.run_pre_night_daily(
        RACE_DATE,
        tmp_path,
        dry_run=False,
        deadline_evidence=VALID_DEADLINE_EVIDENCE,
        collector=collector,
        pipeline=pipeline,
        now_fn=lambda: BEFORE_AS_OF,
    )

    assert result["status"] == "SUCCESS"
    assert result["cached"] is False
    assert result["skipped"] is False
    assert result["collector_called"] is True
    assert result["pipeline_called"] is True
    assert calls == {
        "collector": 1,
        "pipeline": 1,
    }

    manifest_path = (
        pre_night_daily.build_execution_manifest_path(
            RACE_DATE,
            tmp_path,
        )
    )

    assert manifest_path.is_file()

    validated = (
        pre_night_daily.validate_execution_manifest(
            manifest_path,
            tmp_path,
            race_date=RACE_DATE,
        )
    )

    assert validated["cached"] is True
    assert validated["manifest"]["status"] == "SUCCESS"
    assert set(validated["artifacts"]) == {
        "source_archive",
        "source_metadata",
        "output_parquet",
        "pipeline_manifest",
    }


def test_reuses_valid_execution_manifest_without_calls(
    tmp_path,
):
    calls = {
        "collector": 0,
        "pipeline": 0,
    }
    collector, pipeline = make_fake_functions(calls)

    first = pre_night_daily.run_pre_night_daily(
        RACE_DATE,
        tmp_path,
        dry_run=False,
        deadline_evidence=VALID_DEADLINE_EVIDENCE,
        collector=collector,
        pipeline=pipeline,
        now_fn=lambda: BEFORE_AS_OF,
    )

    assert first["cached"] is False
    assert calls == {
        "collector": 1,
        "pipeline": 1,
    }

    second = pre_night_daily.run_pre_night_daily(
        RACE_DATE,
        tmp_path,
        dry_run=False,
        deadline_evidence=VALID_DEADLINE_EVIDENCE,
        collector=collector,
        pipeline=pipeline,
        now_fn=lambda: AFTER_AS_OF,
    )

    assert second["status"] == "SUCCESS"
    assert second["cached"] is True
    assert second["skipped"] is True
    assert second["collector_called"] is False
    assert second["pipeline_called"] is False
    assert calls == {
        "collector": 1,
        "pipeline": 1,
    }


def test_rejects_tampered_source_archive(tmp_path):
    calls = {
        "collector": 0,
        "pipeline": 0,
    }
    collector, pipeline = make_fake_functions(calls)

    result = pre_night_daily.run_pre_night_daily(
        RACE_DATE,
        tmp_path,
        dry_run=False,
        deadline_evidence=VALID_DEADLINE_EVIDENCE,
        collector=collector,
        pipeline=pipeline,
        now_fn=lambda: BEFORE_AS_OF,
    )

    archive = (
        result["execution_manifest"]["artifacts"]
        ["source_archive"]["path"]
    )
    archive.write_bytes(b"tampered-archive")

    with pytest.raises(
        pre_night_daily.PreNightDailyIntegrityError
    ):
        pre_night_daily.run_pre_night_daily(
            RACE_DATE,
            tmp_path,
            dry_run=False,
            deadline_evidence=VALID_DEADLINE_EVIDENCE,
            collector=collector,
            pipeline=pipeline,
            now_fn=lambda: BEFORE_AS_OF,
        )

    assert calls == {
        "collector": 1,
        "pipeline": 1,
    }


def test_rejects_tampered_output_parquet(tmp_path):
    calls = {
        "collector": 0,
        "pipeline": 0,
    }
    collector, pipeline = make_fake_functions(calls)

    result = pre_night_daily.run_pre_night_daily(
        RACE_DATE,
        tmp_path,
        dry_run=False,
        deadline_evidence=VALID_DEADLINE_EVIDENCE,
        collector=collector,
        pipeline=pipeline,
        now_fn=lambda: BEFORE_AS_OF,
    )

    parquet = (
        result["execution_manifest"]["artifacts"]
        ["output_parquet"]["path"]
    )
    parquet.write_bytes(b"tampered-parquet")

    with pytest.raises(
        pre_night_daily.PreNightDailyIntegrityError
    ):
        pre_night_daily.run_pre_night_daily(
            RACE_DATE,
            tmp_path,
            dry_run=False,
            deadline_evidence=VALID_DEADLINE_EVIDENCE,
            collector=collector,
            pipeline=pipeline,
            now_fn=lambda: BEFORE_AS_OF,
        )

    assert calls == {
        "collector": 1,
        "pipeline": 1,
    }


def test_rejects_naive_clock_without_calls(tmp_path):
    calls = {
        "collector": 0,
        "pipeline": 0,
    }
    collector, pipeline = make_fake_functions(calls)

    with pytest.raises(
        pre_night_daily.PreNightDailyContractError
    ):
        pre_night_daily.run_pre_night_daily(
            RACE_DATE,
            tmp_path,
            dry_run=False,
            deadline_evidence=VALID_DEADLINE_EVIDENCE,
            collector=collector,
            pipeline=pipeline,
            now_fn=lambda: dt.datetime(
                2026,
                8,
                9,
                21,
                0,
            ),
        )

    assert calls == {
        "collector": 0,
        "pipeline": 0,
    }
    assert not tmp_path.exists() or (
        list(tmp_path.iterdir()) == []
    )

# BEGIN PRE_NIGHT_PIT_SAFETY_GATE_V1_DAILY_TEST
def test_daily_manifest_records_explicit_pit_status():
    from boatrace_ai.pipelines.pre_night_daily import (
        build_pre_night_eligibility_manifest_fields,
    )
    from boatrace_ai.pipelines.pre_night_eligibility import (
        eligible_decision,
    )

    decision = eligible_decision(
        race_date="2026-07-27",
        as_of_time="2026-07-26T21:30:00+09:00",
    )
    fields = build_pre_night_eligibility_manifest_fields(decision)

    assert fields["eligibility_status"] == "ELIGIBLE"
    assert fields["eligible_for_pre_night"] is True
    assert fields["pit_eligibility"]["status"] == "ELIGIBLE"
# END PRE_NIGHT_PIT_SAFETY_GATE_V1_DAILY_TEST


# BEGIN PRE_NIGHT_PIT_SAFETY_GATE_AST_RETRY_TESTS


def test_dry_run_records_explicit_pit_decision(tmp_path):
    result = pre_night_daily.run_pre_night_daily(
        RACE_DATE,
        tmp_path,
        dry_run=True,
        now_fn=lambda: BEFORE_AS_OF,
    )

    assert result["eligibility_status"] == "ELIGIBLE"
    assert result["eligible_for_pre_night"] is True
    assert result["eligibility_reason"]
    assert result["pit_eligibility"]["status"] == "ELIGIBLE"
    assert result["pit_eligibility"]["eligible"] is True
    assert not tmp_path.exists() or list(tmp_path.iterdir()) == []


def test_blocked_dry_run_records_fail_closed_pit_decision(
    tmp_path,
):
    result = pre_night_daily.run_pre_night_daily(
        RACE_DATE,
        tmp_path,
        dry_run=True,
        now_fn=lambda: AFTER_AS_OF,
    )

    assert result["eligible_for_pre_night"] is False
    assert result["eligibility_status"] != "ELIGIBLE"
    assert result["pit_eligibility"]["eligible"] is False
    assert not tmp_path.exists() or list(tmp_path.iterdir()) == []


def test_execution_manifest_copies_pipeline_pit_decision(
    tmp_path,
):
    calls = {"collector": 0, "pipeline": 0}
    collector, pipeline = make_fake_functions(calls)

    result = pre_night_daily.run_pre_night_daily(
        RACE_DATE,
        tmp_path,
        dry_run=False,
        deadline_evidence=VALID_DEADLINE_EVIDENCE,
        collector=collector,
        pipeline=pipeline,
        now_fn=lambda: BEFORE_AS_OF,
    )

    execution = result["execution_manifest"]["manifest"]
    pipeline_fields = result["pipeline_result"]["manifest"]

    for field_name in (
        "eligibility_status",
        "eligible_for_pre_night",
        "eligibility_reason",
        "pit_eligibility",
    ):
        assert execution[field_name] == pipeline_fields[field_name]


def test_cached_execution_rejects_missing_pit_field(tmp_path):
    calls = {"collector": 0, "pipeline": 0}
    collector, pipeline = make_fake_functions(calls)

    pre_night_daily.run_pre_night_daily(
        RACE_DATE,
        tmp_path,
        dry_run=False,
        deadline_evidence=VALID_DEADLINE_EVIDENCE,
        collector=collector,
        pipeline=pipeline,
        now_fn=lambda: BEFORE_AS_OF,
    )

    path = pre_night_daily.build_execution_manifest_path(
        RACE_DATE,
        tmp_path,
    )
    manifest = json.loads(path.read_text(encoding="utf-8"))
    manifest.pop("eligibility_status")
    path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(
        pre_night_daily.PreNightDailyIntegrityError,
        match="eligibility",
    ):
        pre_night_daily.run_pre_night_daily(
            RACE_DATE,
            tmp_path,
            dry_run=False,
            deadline_evidence=VALID_DEADLINE_EVIDENCE,
            collector=collector,
            pipeline=pipeline,
            now_fn=lambda: AFTER_AS_OF,
        )


def test_cached_execution_rejects_pipeline_pit_divergence(
    tmp_path,
):
    calls = {"collector": 0, "pipeline": 0}
    collector, pipeline = make_fake_functions(calls)

    first = pre_night_daily.run_pre_night_daily(
        RACE_DATE,
        tmp_path,
        dry_run=False,
        deadline_evidence=VALID_DEADLINE_EVIDENCE,
        collector=collector,
        pipeline=pipeline,
        now_fn=lambda: BEFORE_AS_OF,
    )

    pipeline_path = (
        first["execution_manifest"]["artifacts"]
        ["pipeline_manifest"]["path"]
    )
    manifest = json.loads(
        pipeline_path.read_text(encoding="utf-8")
    )
    manifest["eligibility_reason"] = "tampered reason"
    pipeline_path.write_text(
        json.dumps(manifest),
        encoding="utf-8",
    )

    execution_path = (
        pre_night_daily.build_execution_manifest_path(
            RACE_DATE,
            tmp_path,
        )
    )
    execution = json.loads(
        execution_path.read_text(encoding="utf-8")
    )
    record = execution["artifacts"]["pipeline_manifest"]
    record["size"] = pipeline_path.stat().st_size
    record["sha256"] = pre_night_daily.sha256_file(
        pipeline_path
    )
    execution_path.write_text(
        json.dumps(execution),
        encoding="utf-8",
    )

    with pytest.raises(
        pre_night_daily.PreNightDailyIntegrityError,
        match="PIT eligibility",
    ):
        pre_night_daily.run_pre_night_daily(
            RACE_DATE,
            tmp_path,
            dry_run=False,
            deadline_evidence=VALID_DEADLINE_EVIDENCE,
            collector=collector,
            pipeline=pipeline,
            now_fn=lambda: AFTER_AS_OF,
        )


# BEGIN PRE_NIGHT_PROSPECTIVE_DATASET_V1_TESTS


def _prospective_files(root):
    directory = (
        Path(root)
        / "manifests"
        / "pre_night_prospective_v1"
        / RACE_DATE.isoformat()
    )
    return (
        sorted(directory.glob("*.json"))
        if directory.exists()
        else []
    )


def test_prospective_dry_run_calls_no_new_dependencies(
    tmp_path,
):
    calls = {
        "revision": 0,
        "writer": 0,
        "validator": 0,
    }

    def revision_provider():
        calls["revision"] += 1
        return "a" * 40

    def writer(*args, **kwargs):
        calls["writer"] += 1
        pytest.fail("writer must not run")

    def validator(*args, **kwargs):
        calls["validator"] += 1
        pytest.fail("validator must not run")

    result = pre_night_daily.run_pre_night_daily(
        RACE_DATE,
        tmp_path,
        dry_run=True,
        now_fn=lambda: BEFORE_AS_OF,
        repository_commit_provider=revision_provider,
        prospective_writer=writer,
        prospective_validator=validator,
    )

    assert result["dry_run"] is True
    assert calls == {
        "revision": 0,
        "writer": 0,
        "validator": 0,
    }
    assert _prospective_files(tmp_path) == []


def test_live_run_creates_and_returns_prospective_manifest(
    tmp_path,
):
    calls = {"collector": 0, "pipeline": 0}
    collector, pipeline = make_fake_functions(calls)

    result = pre_night_daily.run_pre_night_daily(
        RACE_DATE,
        tmp_path,
        dry_run=False,
        deadline_evidence=VALID_DEADLINE_EVIDENCE,
        collector=collector,
        pipeline=pipeline,
        now_fn=lambda: BEFORE_AS_OF,
        repository_commit_provider=lambda: "a" * 40,
    )

    assert result["cached"] is False
    assert "prospective_manifest" in result
    assert result["prospective_manifest"]["cached"] is True

    files = _prospective_files(tmp_path)
    assert len(files) == 1

    payload = json.loads(
        files[0].read_text(encoding="utf-8")
    )
    assert payload["repository_commit"] == "a" * 40
    assert payload["eligible_for_pre_night"] is True
    assert payload["pit_eligibility"]["status"] == "ELIGIBLE"


def test_cached_run_validates_prospective_without_calls(
    tmp_path,
):
    calls = {"collector": 0, "pipeline": 0}
    collector, pipeline = make_fake_functions(calls)

    first = pre_night_daily.run_pre_night_daily(
        RACE_DATE,
        tmp_path,
        dry_run=False,
        deadline_evidence=VALID_DEADLINE_EVIDENCE,
        collector=collector,
        pipeline=pipeline,
        now_fn=lambda: BEFORE_AS_OF,
        repository_commit_provider=lambda: "a" * 40,
    )

    second = pre_night_daily.run_pre_night_daily(
        RACE_DATE,
        tmp_path,
        dry_run=False,
        deadline_evidence=VALID_DEADLINE_EVIDENCE,
        collector=collector,
        pipeline=pipeline,
        now_fn=lambda: AFTER_AS_OF,
        repository_commit_provider=lambda: "a" * 40,
    )

    assert first["cached"] is False
    assert second["cached"] is True
    assert second["collector_called"] is False
    assert second["pipeline_called"] is False
    assert second["prospective_manifest"]["cached"] is True
    assert calls == {"collector": 1, "pipeline": 1}


def test_cached_execution_without_prospective_fails_closed(
    tmp_path,
):
    calls = {"collector": 0, "pipeline": 0}
    collector, pipeline = make_fake_functions(calls)

    pre_night_daily.run_pre_night_daily(
        RACE_DATE,
        tmp_path,
        dry_run=False,
        deadline_evidence=VALID_DEADLINE_EVIDENCE,
        collector=collector,
        pipeline=pipeline,
        now_fn=lambda: BEFORE_AS_OF,
        repository_commit_provider=lambda: "a" * 40,
    )

    files = _prospective_files(tmp_path)
    assert len(files) == 1
    files[0].unlink()

    with pytest.raises(
        pre_night_daily.PreNightDailyIntegrityError,
        match="PROSPECTIVE_MANIFEST_MISSING",
    ):
        pre_night_daily.run_pre_night_daily(
            RACE_DATE,
            tmp_path,
            dry_run=False,
            deadline_evidence=VALID_DEADLINE_EVIDENCE,
            collector=collector,
            pipeline=pipeline,
            now_fn=lambda: AFTER_AS_OF,
            repository_commit_provider=lambda: "a" * 40,
        )

    assert _prospective_files(tmp_path) == []
    assert calls == {"collector": 1, "pipeline": 1}


def test_cached_tampered_prospective_fails_closed(
    tmp_path,
):
    calls = {"collector": 0, "pipeline": 0}
    collector, pipeline = make_fake_functions(calls)

    pre_night_daily.run_pre_night_daily(
        RACE_DATE,
        tmp_path,
        dry_run=False,
        deadline_evidence=VALID_DEADLINE_EVIDENCE,
        collector=collector,
        pipeline=pipeline,
        now_fn=lambda: BEFORE_AS_OF,
        repository_commit_provider=lambda: "a" * 40,
    )

    prospective_path = _prospective_files(tmp_path)[0]
    payload = json.loads(
        prospective_path.read_text(encoding="utf-8")
    )
    payload["repository_commit"] = "b" * 40
    prospective_path.write_text(
        json.dumps(payload),
        encoding="utf-8",
    )

    with pytest.raises(
        pre_night_daily.PreNightDailyIntegrityError,
        match="prospective",
    ):
        pre_night_daily.run_pre_night_daily(
            RACE_DATE,
            tmp_path,
            dry_run=False,
            deadline_evidence=VALID_DEADLINE_EVIDENCE,
            collector=collector,
            pipeline=pipeline,
            now_fn=lambda: AFTER_AS_OF,
            repository_commit_provider=lambda: "a" * 40,
        )

    assert calls == {"collector": 1, "pipeline": 1}


def test_prospective_writer_failure_prevents_success_return(
    tmp_path,
):
    calls = {"collector": 0, "pipeline": 0}
    collector, pipeline = make_fake_functions(calls)

    from boatrace_ai.pipelines.pre_night_prospective import (
        PreNightProspectiveIntegrityError,
    )

    def failing_writer(*args, **kwargs):
        raise PreNightProspectiveIntegrityError(
            "injected prospective write failure"
        )

    with pytest.raises(
        pre_night_daily.PreNightDailyIntegrityError,
        match="Prospective manifest creation failed",
    ):
        pre_night_daily.run_pre_night_daily(
            RACE_DATE,
            tmp_path,
            dry_run=False,
            deadline_evidence=VALID_DEADLINE_EVIDENCE,
            collector=collector,
            pipeline=pipeline,
            now_fn=lambda: BEFORE_AS_OF,
            repository_commit_provider=lambda: "a" * 40,
            prospective_writer=failing_writer,
        )

    assert calls == {"collector": 1, "pipeline": 1}


def test_prospective_preserves_execution_pit_decision(
    tmp_path,
):
    calls = {"collector": 0, "pipeline": 0}
    collector, pipeline = make_fake_functions(calls)

    result = pre_night_daily.run_pre_night_daily(
        RACE_DATE,
        tmp_path,
        dry_run=False,
        deadline_evidence=VALID_DEADLINE_EVIDENCE,
        collector=collector,
        pipeline=pipeline,
        now_fn=lambda: BEFORE_AS_OF,
        repository_commit_provider=lambda: "a" * 40,
    )

    execution = result["execution_manifest"]["manifest"]
    prospective_payload = (
        result["prospective_manifest"]["manifest"]
    )

    assert prospective_payload["pit_eligibility"] == (
        execution["pit_eligibility"]
    )
    assert prospective_payload[
        "eligible_for_pre_night"
    ] is execution["eligible_for_pre_night"]


# END PRE_NIGHT_PROSPECTIVE_DATASET_V1_TESTS
# BEGIN PHASE1_D1B3_TESTS


def d1b3_live(tmp_path, *, evidence=VALID_DEADLINE_EVIDENCE):
    calls = {"collector": 0, "pipeline": 0}
    collector, pipeline = make_fake_functions(calls)
    result = pre_night_daily.run_pre_night_daily(
        RACE_DATE,
        tmp_path,
        dry_run=False,
        deadline_evidence=evidence,
        collector=collector,
        pipeline=pipeline,
        now_fn=lambda: BEFORE_AS_OF,
        repository_commit_provider=lambda: "a" * 40,
    )
    return result, calls


def test_d1b3_t01_live_run_requires_deadline_evidence(tmp_path):
    calls = {"collector": 0, "pipeline": 0}
    collector, pipeline = make_fake_functions(calls)

    with pytest.raises(
        pre_night_daily.PreNightDailyContractError,
        match="deadline_evidence",
    ):
        pre_night_daily.run_pre_night_daily(
            RACE_DATE,
            tmp_path,
            dry_run=False,
            collector=collector,
            pipeline=pipeline,
            now_fn=lambda: BEFORE_AS_OF,
        )

    assert calls == {"collector": 0, "pipeline": 0}


def test_d1b3_t02_invalid_evidence_precedes_collector(tmp_path):
    calls = {"collector": 0, "pipeline": 0}
    collector, pipeline = make_fake_functions(calls)

    with pytest.raises(
        pre_night_daily.PreNightDailyContractError,
        match="deadline_evidence",
    ):
        pre_night_daily.run_pre_night_daily(
            RACE_DATE,
            tmp_path,
            dry_run=False,
            deadline_evidence={"invalid": True},
            collector=collector,
            pipeline=pipeline,
            now_fn=lambda: BEFORE_AS_OF,
        )

    assert calls == {"collector": 0, "pipeline": 0}


def test_d1b3_t03_validated_evidence_is_passed_to_collector(
    tmp_path,
):
    captured = {}
    calls = {"collector": 0, "pipeline": 0}
    collector, pipeline = make_fake_functions(calls)

    def capturing_collector(*args, **kwargs):
        captured["deadline_evidence"] = kwargs.get(
            "deadline_evidence"
        )
        return collector(*args, **kwargs)

    pre_night_daily.run_pre_night_daily(
        RACE_DATE,
        tmp_path,
        dry_run=False,
        deadline_evidence=VALID_DEADLINE_EVIDENCE,
        collector=capturing_collector,
        pipeline=pipeline,
        now_fn=lambda: BEFORE_AS_OF,
        repository_commit_provider=lambda: "a" * 40,
    )

    assert captured["deadline_evidence"] == (
        VALID_DEADLINE_EVIDENCE
    )


def test_d1b3_t04_execution_records_canonical_digest(tmp_path):
    result, _ = d1b3_live(tmp_path)
    execution = result["execution_manifest"]["manifest"]
    assert execution["deadline_evidence_sha256"] == (
        VALID_DEADLINE_SHA256
    )


def test_d1b3_t05_source_pipeline_execution_digests_match(
    tmp_path,
):
    result, _ = d1b3_live(tmp_path)
    execution = result["execution_manifest"]["manifest"]
    artifacts = result["execution_manifest"]["artifacts"]

    source = json.loads(
        artifacts["source_metadata"]["path"].read_text(
            encoding="utf-8"
        )
    )
    pipeline = json.loads(
        artifacts["pipeline_manifest"]["path"].read_text(
            encoding="utf-8"
        )
    )

    assert {
        source["deadline_evidence_sha256"],
        pipeline["deadline_evidence_sha256"],
        execution["deadline_evidence_sha256"],
    } == {VALID_DEADLINE_SHA256}



def _d1b3_missing_binding_case(
    tmp_path,
    *,
    target,
    field,
):
    calls = {"collector": 0, "pipeline": 0}
    collector, pipeline = make_fake_functions(calls)

    if target == "source":
        original_collector = collector

        def collector(*args, **kwargs):
            result = original_collector(*args, **kwargs)
            path = result["paths"]["metadata"]
            payload = json.loads(
                path.read_text(encoding="utf-8")
            )
            payload.pop(field)
            path.write_text(
                json.dumps(payload),
                encoding="utf-8",
            )
            result["metadata"] = payload
            return result

    else:
        original_pipeline = pipeline

        def pipeline(*args, **kwargs):
            result = original_pipeline(*args, **kwargs)
            path = result["paths"]["manifest"]
            payload = json.loads(
                path.read_text(encoding="utf-8")
            )
            payload.pop(field)
            path.write_text(
                json.dumps(payload),
                encoding="utf-8",
            )
            result["manifest"] = payload
            return result

    with pytest.raises(
        pre_night_daily.PreNightDailyIntegrityError,
        match="deadline_evidence",
    ):
        pre_night_daily.run_pre_night_daily(
            RACE_DATE,
            tmp_path,
            dry_run=False,
            deadline_evidence=VALID_DEADLINE_EVIDENCE,
            collector=collector,
            pipeline=pipeline,
            now_fn=lambda: BEFORE_AS_OF,
        )


def test_d1b3_t06_missing_source_evidence_fails_closed(
    tmp_path,
):
    _d1b3_missing_binding_case(
        tmp_path,
        target="source",
        field="deadline_evidence",
    )


def test_d1b3_t07_missing_source_digest_fails_closed(
    tmp_path,
):
    _d1b3_missing_binding_case(
        tmp_path,
        target="source",
        field="deadline_evidence_sha256",
    )


def test_d1b3_t08_source_canonical_mismatch_fails_closed(
    tmp_path,
):
    calls = {"collector": 0, "pipeline": 0}
    collector, pipeline = make_fake_functions(calls)
    original_collector = collector

    def collector(*args, **kwargs):
        result = original_collector(*args, **kwargs)
        path = result["paths"]["metadata"]
        payload = json.loads(
            path.read_text(encoding="utf-8")
        )
        payload["deadline_evidence_sha256"] = "f" * 64
        path.write_text(
            json.dumps(payload),
            encoding="utf-8",
        )
        result["metadata"] = payload
        return result

    with pytest.raises(
        pre_night_daily.PreNightDailyIntegrityError,
        match="deadline_evidence_sha256",
    ):
        pre_night_daily.run_pre_night_daily(
            RACE_DATE,
            tmp_path,
            dry_run=False,
            deadline_evidence=VALID_DEADLINE_EVIDENCE,
            collector=collector,
            pipeline=pipeline,
            now_fn=lambda: BEFORE_AS_OF,
        )


def test_d1b3_t09_missing_pipeline_evidence_fails_closed(
    tmp_path,
):
    _d1b3_missing_binding_case(
        tmp_path,
        target="pipeline",
        field="deadline_evidence",
    )


def test_d1b3_t10_missing_pipeline_digest_fails_closed(
    tmp_path,
):
    _d1b3_missing_binding_case(
        tmp_path,
        target="pipeline",
        field="deadline_evidence_sha256",
    )


def test_d1b3_t11_pipeline_canonical_mismatch_fails_closed(
    tmp_path,
):
    calls = {"collector": 0, "pipeline": 0}
    collector, pipeline = make_fake_functions(calls)
    original = pipeline

    def pipeline(*args, **kwargs):
        result = original(*args, **kwargs)
        path = result["paths"]["manifest"]
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["deadline_evidence_sha256"] = "f" * 64
        path.write_text(json.dumps(payload), encoding="utf-8")
        return result

    with pytest.raises(
        pre_night_daily.PreNightDailyIntegrityError,
        match="deadline_evidence_sha256",
    ):
        pre_night_daily.run_pre_night_daily(
            RACE_DATE,
            tmp_path,
            dry_run=False,
            deadline_evidence=VALID_DEADLINE_EVIDENCE,
            collector=collector,
            pipeline=pipeline,
            now_fn=lambda: BEFORE_AS_OF,
        )


def test_d1b3_t12_source_pipeline_divergence_fails_closed(
    tmp_path,
):
    alternate_evidence, alternate_digest = (
        make_deadline_binding("02")
    )
    calls = {"collector": 0, "pipeline": 0}
    collector, pipeline = make_fake_functions(calls)
    original = pipeline

    def pipeline(*args, **kwargs):
        result = original(*args, **kwargs)
        path = result["paths"]["manifest"]
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["deadline_evidence"] = alternate_evidence
        payload["deadline_evidence_sha256"] = alternate_digest
        path.write_text(json.dumps(payload), encoding="utf-8")
        return result

    with pytest.raises(
        pre_night_daily.PreNightDailyIntegrityError,
        match="differ",
    ):
        pre_night_daily.run_pre_night_daily(
            RACE_DATE,
            tmp_path,
            dry_run=False,
            deadline_evidence=VALID_DEADLINE_EVIDENCE,
            collector=collector,
            pipeline=pipeline,
            now_fn=lambda: BEFORE_AS_OF,
        )


def test_d1b3_t13_cached_execution_missing_digest_fails(
    tmp_path,
):
    d1b3_live(tmp_path)
    path = pre_night_daily.build_execution_manifest_path(
        RACE_DATE,
        tmp_path,
    )
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload.pop("deadline_evidence_sha256")
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(
        pre_night_daily.PreNightDailyIntegrityError,
        match="deadline_evidence_sha256",
    ):
        pre_night_daily.run_pre_night_daily(
            RACE_DATE,
            tmp_path,
            dry_run=False,
            now_fn=lambda: AFTER_AS_OF,
            repository_commit_provider=lambda: "a" * 40,
        )


def test_d1b3_t14_cached_execution_digest_mismatch_fails(
    tmp_path,
):
    d1b3_live(tmp_path)
    path = pre_night_daily.build_execution_manifest_path(
        RACE_DATE,
        tmp_path,
    )
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["deadline_evidence_sha256"] = "e" * 64
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(
        pre_night_daily.PreNightDailyIntegrityError,
        match="deadline_evidence_sha256",
    ):
        pre_night_daily.run_pre_night_daily(
            RACE_DATE,
            tmp_path,
            dry_run=False,
            now_fn=lambda: AFTER_AS_OF,
            repository_commit_provider=lambda: "a" * 40,
        )


def test_d1b3_t15_matching_cache_is_reused(tmp_path):
    first, calls = d1b3_live(tmp_path)

    collector, pipeline = make_fake_functions(calls)
    second = pre_night_daily.run_pre_night_daily(
        RACE_DATE,
        tmp_path,
        dry_run=False,
        collector=collector,
        pipeline=pipeline,
        now_fn=lambda: AFTER_AS_OF,
        repository_commit_provider=lambda: "a" * 40,
    )

    assert first["cached"] is False
    assert second["cached"] is True
    assert second["skipped"] is True
    assert calls == {"collector": 1, "pipeline": 1}


def test_d1b3_t16_supplied_evidence_must_match_cache(
    tmp_path,
):
    d1b3_live(tmp_path)
    alternate, _ = make_deadline_binding("02")

    with pytest.raises(
        pre_night_daily.PreNightDailyIntegrityError,
        match="Requested",
    ):
        pre_night_daily.run_pre_night_daily(
            RACE_DATE,
            tmp_path,
            dry_run=False,
            deadline_evidence=alternate,
            now_fn=lambda: AFTER_AS_OF,
            repository_commit_provider=lambda: "a" * 40,
        )


def test_d1b3_t17_dry_run_does_not_require_evidence(tmp_path):
    result = pre_night_daily.run_pre_night_daily(
        RACE_DATE,
        tmp_path,
        dry_run=True,
        now_fn=lambda: BEFORE_AS_OF,
    )

    assert result["dry_run"] is True
    assert "deadline_evidence_sha256" not in result
    assert not tmp_path.exists() or list(tmp_path.iterdir()) == []


def test_d1b3_t18_execution_has_no_full_deadline_payload(
    tmp_path,
):
    result, _ = d1b3_live(tmp_path)
    execution = result["execution_manifest"]["manifest"]
    serialized = json.dumps(execution, sort_keys=True)

    assert "deadline_evidence_sha256" in execution
    assert "deadline_evidence" not in execution
    assert "raw_source_bytes" not in serialized
    assert "raw_html" not in serialized
    assert "canonical_bytes" not in serialized
    assert "eligibility_cutoff_at" not in serialized
    assert "safety_margin_seconds" not in serialized


def test_d1b3_t19_keyword_only_api_extension():
    import inspect

    signature = inspect.signature(
        pre_night_daily.run_pre_night_daily
    )
    parameters = signature.parameters

    assert parameters["deadline_evidence"].kind is (
        inspect.Parameter.KEYWORD_ONLY
    )
    assert list(parameters)[-1] == "deadline_evidence"


def test_d1b3_t20_existing_return_shapes_are_preserved(
    tmp_path,
):
    result, _ = d1b3_live(tmp_path)

    assert set(result) == {
        "status",
        "race_date",
        "dry_run",
        "cached",
        "skipped",
        "collector_called",
        "pipeline_called",
        "collector_result",
        "pipeline_result",
        "execution_manifest",
        "prospective_manifest",
    }




def test_d1b3_t21_execution_manifest_contract_is_v2(
    tmp_path,
):
    result, _ = d1b3_live(tmp_path)
    execution = result["execution_manifest"]["manifest"]

    assert pre_night_daily.EXECUTION_CONTRACT_VERSION == (
        "pre_night_daily_execution_v2"
    )
    assert execution["execution_contract_version"] == (
        "pre_night_daily_execution_v2"
    )
    assert execution["deadline_evidence_sha256"] == (
        VALID_DEADLINE_SHA256
    )


# END PHASE1_D1B3_TESTS
