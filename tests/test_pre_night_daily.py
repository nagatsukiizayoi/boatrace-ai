import datetime as dt
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

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


def make_fake_functions(calls):

    def collector(race_date, data_root, now_fn=None):
        calls['collector'] += 1
        root = Path(data_root)
        directory = root / 'snapshots' / 'pre_night_v2' / 'program' / race_date.isoformat()
        directory.mkdir(parents=True, exist_ok=True)
        archive = directory / 'b260810.lzh'
        metadata = directory / 'b260810.lzh.json'
        archive.write_bytes(b'test-program-archive')
        metadata.write_text(json.dumps({'race_date': race_date.isoformat(), 'eligible_for_pre_night': True}), encoding='utf-8')
        return {'paths': {'archive': archive, 'metadata': metadata}, 'metadata': {'race_date': race_date.isoformat(), 'eligible_for_pre_night': True}, 'cached': False, 'eligible_for_pre_night': True}

    def pipeline(race_date, data_root, overwrite=False, now_fn=None):
        calls['pipeline'] += 1
        root = Path(data_root)
        directory = root / 'features' / 'pre_night_v2' / race_date.isoformat()
        directory.mkdir(parents=True, exist_ok=True)
        parquet = directory / 'program.parquet'
        manifest = directory / 'program.parquet.json'
        parquet.write_bytes(b'test-parquet-content')
        manifest.write_text(json.dumps({'race_date': race_date.isoformat(), 'status': 'SUCCESS', 'eligibility_status': 'ELIGIBLE', 'eligible_for_pre_night': True, 'eligibility_reason': 'All PRE_NIGHT PIT checks passed', 'pit_eligibility': {'status': 'ELIGIBLE', 'eligible': True, 'reason': 'All PRE_NIGHT PIT checks passed', 'race_date': '2026-08-10', 'as_of_time': '2026-08-09T21:30:00+09:00', 'details': {}}}), encoding='utf-8')
        return {'paths': {'parquet': parquet, 'manifest': manifest}, 'manifest': {'race_date': race_date.isoformat(), 'status': 'SUCCESS', 'eligibility_status': 'ELIGIBLE', 'eligible_for_pre_night': True, 'eligibility_reason': 'All PRE_NIGHT PIT checks passed', 'pit_eligibility': {'status': 'ELIGIBLE', 'eligible': True, 'reason': 'All PRE_NIGHT PIT checks passed', 'race_date': '2026-08-10', 'as_of_time': '2026-08-09T21:30:00+09:00', 'details': {}}}, 'skipped': False}
    return (collector, pipeline)


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
            collector=collector,
            pipeline=pipeline,
            now_fn=lambda: AFTER_AS_OF,
        )
