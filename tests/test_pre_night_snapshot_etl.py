import datetime as dt
import hashlib
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from boatrace_ai.ingestion import (
    pre_night_deadlines as deadlines,
)
from boatrace_ai.ingestion import (
    pre_night_snapshots as snapshots,
)
from boatrace_ai.pipelines import (
    pre_night_snapshot_etl as pipeline,
)


JST = ZoneInfo("Asia/Tokyo")
UTC = dt.timezone.utc


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def valid_program_frame():
    return pd.DataFrame(
        [
            {
                "race_date": "2026-08-10",
                "venue_code": "01",
                "race_no": 1,
                "boat_no": boat_no,
                "racer_id": (
                    4000 + boat_no
                ),
            }
            for boat_no in range(1, 7)
        ]
    )


def build_snapshot(
    tmp_path,
    fetched_at=(
        "2026-08-09T12:00:00+00:00"
    ),
    eligible=True,
):
    paths = snapshots.build_snapshot_paths(
        "2026-08-10",
        tmp_path,
    )

    paths["directory"].mkdir(
        parents=True,
        exist_ok=True,
    )

    archive_bytes = b"program-archive"
    paths["archive"].write_bytes(
        archive_bytes
    )

    digest = sha256_bytes(
        archive_bytes
    )

    deadline_raw = (
        b"<html><body>"
        b"test deadline schedule"
        b"</body></html>"
    )
    deadline_fetched_at = dt.datetime(
        2026,
        8,
        9,
        12,
        0,
        tzinfo=UTC,
    )
    deadline_base = dt.datetime(
        2026,
        8,
        10,
        10,
        0,
        tzinfo=JST,
    )
    deadline_evidence = (
        deadlines.build_deadline_evidence(
            raw_source_bytes=deadline_raw,
            source_locator=(
                "https://example.invalid/"
                "deadline-schedule"
            ),
            source_name=(
                "BOAT RACE official test page"
            ),
            source_authority="BOAT RACE",
            request_started_at=(
                deadline_fetched_at
                - dt.timedelta(seconds=2)
            ),
            fetched_at=deadline_fetched_at,
            http_status=200,
            response_headers={
                "Content-Type": (
                    "text/html; charset=UTF-8"
                ),
            },
            race_date="2026-08-10",
            venue_code="01",
            source_timezone=(
                "explicit-source-timezone-evidence"
            ),
            race_deadlines=[
                {
                    "race_no": race_no,
                    "deadline_kind": (
                        deadlines.DEADLINE_KIND
                    ),
                    "scheduled_deadline_at": (
                        deadline_base
                        + dt.timedelta(
                            minutes=race_no
                        )
                    ),
                }
                for race_no in range(1, 13)
            ],
        )
    )
    deadline_evidence_sha256 = (
        sha256_bytes(
            deadlines
            .canonical_deadline_evidence_bytes(
                deadline_evidence
            )
        )
    )

    metadata = {
        "contract_version": (
            snapshots.CONTRACT_VERSION
        ),
        "collector_version": (
            snapshots.COLLECTOR_VERSION
        ),
        "source_type": "program",
        "archive_type": "program",
        "snapshot_type": "PRE_NIGHT",
        "race_date": "2026-08-10",
        "as_of_rule": (
            snapshots.AS_OF_RULE
        ),
        "as_of_time": (
            "2026-08-09T21:30:00+09:00"
        ),
        "snapshot_at": (
            "2026-08-09T21:30:00+09:00"
        ),
        "request_started_at": (
            "2026-08-09T11:59:00+00:00"
        ),
        "fetched_at": fetched_at,
        "source_url": (
            "https://example.invalid/"
            "B260810.LZH"
        ),
        "http_status": 200,
        "response_size": len(
            archive_bytes
        ),
        "source_response_sha256": digest,
        "archive_sha256": digest,
        "archive_path": str(
            paths["archive"]
        ),
        "eligible_for_pre_night": (
            eligible
        ),
        "eligibility_reason": (
            "FETCHED_BY_AS_OF"
            if eligible
            else "FETCHED_AFTER_AS_OF"
        ),
        "deadline_evidence": (
            deadline_evidence
        ),
        "deadline_evidence_sha256": (
            deadline_evidence_sha256
        ),
    }

    paths["metadata"].write_text(
        json.dumps(metadata),
        encoding="utf-8",
    )

    return {
        "paths": paths,
        "metadata": metadata,
        "cached": True,
        "eligible_for_pre_night": (
            eligible
        ),
    }


def make_validator(snapshot):
    def validator(race_date, data_root):
        return snapshot

    return validator


def fake_extractor(
    archive_path,
    data_root,
    archive_type,
    overwrite=False,
):
    assert archive_type == "program"

    destination = (
        Path(data_root)
        / "temporary-test"
        / "B260810.TXT"
    )

    destination.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    destination.write_text(
        "test",
        encoding="utf-8",
    )

    return [destination]


def fixed_clock():
    return dt.datetime(
        2026,
        8,
        9,
        12,
        5,
        tzinfo=UTC,
    )


def test_writes_program_only_parquet_with_provenance(
    tmp_path,
):
    snapshot = build_snapshot(tmp_path)

    result = (
        pipeline
        .build_pre_night_program_parquet(
            "2026-08-10",
            tmp_path,
            parser=lambda *args, **kwargs: (
                valid_program_frame()
            ),
            extractor=fake_extractor,
            snapshot_validator=(
                make_validator(snapshot)
            ),
            now_fn=fixed_clock,
        )
    )

    assert result["skipped"] is False
    assert result["pit"][
        "future_source_rows"
    ] == 0

    frame = pd.read_parquet(
        result["paths"]["parquet"]
    )

    assert len(frame) == 6
    assert set(
        pipeline.PROVENANCE_COLUMNS
    ).issubset(frame.columns)

    assert frame[
        "provenance_status"
    ].eq("ELIGIBLE").all()

    assert frame[
        "feature_source_type"
    ].eq("program").all()

    assert not (
        pipeline.PROHIBITED_COLUMNS
        & set(frame.columns)
    )

    source_time = pd.to_datetime(
        frame["feature_source_max_time"],
        utc=True,
    )

    as_of_time = pd.to_datetime(
        frame["as_of_time"],
        utc=True,
    )

    assert (
        source_time <= as_of_time
    ).all()

    assert (
        result["paths"]["manifest"]
        .is_file()
    )


def test_reuses_valid_parquet_cache(
    tmp_path,
):
    snapshot = build_snapshot(tmp_path)

    first = (
        pipeline
        .build_pre_night_program_parquet(
            "2026-08-10",
            tmp_path,
            parser=lambda *args, **kwargs: (
                valid_program_frame()
            ),
            extractor=fake_extractor,
            snapshot_validator=(
                make_validator(snapshot)
            ),
            now_fn=fixed_clock,
        )
    )

    second = (
        pipeline
        .build_pre_night_program_parquet(
            "2026-08-10",
            tmp_path,
            parser=lambda *args, **kwargs: (
                pytest.fail(
                    "parser must not run"
                )
            ),
            extractor=lambda *args, **kwargs: (
                pytest.fail(
                    "extractor must not run"
                )
            ),
            snapshot_validator=(
                make_validator(snapshot)
            ),
            now_fn=fixed_clock,
        )
    )

    assert first["skipped"] is False
    assert second["skipped"] is True


def test_rejects_future_source_time(
    tmp_path,
):
    snapshot = build_snapshot(
        tmp_path,
        fetched_at=(
            "2026-08-09T13:00:00+00:00"
        ),
        eligible=True,
    )

    with pytest.raises(
        pipeline.PreNightPointInTimeError,
        match="after as_of_time",
    ):
        pipeline.build_pre_night_program_parquet(
            "2026-08-10",
            tmp_path,
            parser=lambda *args, **kwargs: (
                valid_program_frame()
            ),
            extractor=fake_extractor,
            snapshot_validator=(
                make_validator(snapshot)
            ),
            now_fn=fixed_clock,
        )


def test_rejects_ineligible_snapshot(
    tmp_path,
):
    snapshot = build_snapshot(
        tmp_path,
        eligible=False,
    )

    with pytest.raises(
        pipeline.PreNightPointInTimeError,
        match="not eligible",
    ):
        pipeline.build_pre_night_program_parquet(
            "2026-08-10",
            tmp_path,
            parser=lambda *args, **kwargs: (
                valid_program_frame()
            ),
            extractor=fake_extractor,
            snapshot_validator=(
                make_validator(snapshot)
            ),
            now_fn=fixed_clock,
        )


def test_rejects_prohibited_result_column(
    tmp_path,
):
    snapshot = build_snapshot(tmp_path)

    frame = valid_program_frame()
    frame["finish_position"] = [
        1, 2, 3, 4, 5, 6
    ]

    with pytest.raises(
        pipeline.PreNightFeatureContractError,
        match="Prohibited post-race",
    ):
        pipeline.build_pre_night_program_parquet(
            "2026-08-10",
            tmp_path,
            parser=lambda *args, **kwargs: (
                frame
            ),
            extractor=fake_extractor,
            snapshot_validator=(
                make_validator(snapshot)
            ),
            now_fn=fixed_clock,
        )


def test_rejects_invalid_race_size(
    tmp_path,
):
    snapshot = build_snapshot(tmp_path)

    frame = (
        valid_program_frame()
        .iloc[:5]
        .copy()
    )

    with pytest.raises(
        pipeline.PreNightFeatureContractError,
        match="six-boat",
    ):
        pipeline.build_pre_night_program_parquet(
            "2026-08-10",
            tmp_path,
            parser=lambda *args, **kwargs: (
                frame
            ),
            extractor=fake_extractor,
            snapshot_validator=(
                make_validator(snapshot)
            ),
            now_fn=fixed_clock,
        )


def test_rejects_duplicate_row_key(
    tmp_path,
):
    snapshot = build_snapshot(tmp_path)

    frame = valid_program_frame()
    frame = pd.concat(
        [
            frame,
            frame.iloc[[0]],
        ],
        ignore_index=True,
    )

    with pytest.raises(
        pipeline.PreNightFeatureContractError,
        match="Duplicate",
    ):
        pipeline.build_pre_night_program_parquet(
            "2026-08-10",
            tmp_path,
            parser=lambda *args, **kwargs: (
                frame
            ),
            extractor=fake_extractor,
            snapshot_validator=(
                make_validator(snapshot)
            ),
            now_fn=fixed_clock,
        )


def test_rejects_tampered_parquet_cache(
    tmp_path,
):
    snapshot = build_snapshot(tmp_path)

    result = (
        pipeline
        .build_pre_night_program_parquet(
            "2026-08-10",
            tmp_path,
            parser=lambda *args, **kwargs: (
                valid_program_frame()
            ),
            extractor=fake_extractor,
            snapshot_validator=(
                make_validator(snapshot)
            ),
            now_fn=fixed_clock,
        )
    )

    result["paths"]["parquet"].write_bytes(
        b"tampered"
    )

    with pytest.raises(
        pipeline.PreNightOutputIntegrityError,
        match="SHA-256",
    ):
        pipeline.build_pre_night_program_parquet(
            "2026-08-10",
            tmp_path,
            parser=lambda *args, **kwargs: (
                pytest.fail(
                    "parser must not run"
                )
            ),
            extractor=lambda *args, **kwargs: (
                pytest.fail(
                    "extractor must not run"
                )
            ),
            snapshot_validator=(
                make_validator(snapshot)
            ),
            now_fn=fixed_clock,
        )

# BEGIN PRE_NIGHT_PIT_SAFETY_GATE_V1_TEST
def test_explicit_pit_gate_exposes_validation_outcome():
    from boatrace_ai.pipelines.pre_night_eligibility import (
        PreNightEligibilityStatus,
    )
    from boatrace_ai.pipelines.pre_night_snapshot_etl import (
        evaluate_pre_night_pit_eligibility,
    )

    decision = evaluate_pre_night_pit_eligibility(
        race_date="2026-07-27",
        as_of_time="2026-07-26T21:30:00+09:00",
        validation_error=ValueError("fetched after as_of time"),
    )

    assert decision.eligible is False
    assert decision.status is (
        PreNightEligibilityStatus.SKIPPED_FETCHED_AFTER_AS_OF
    )
# END PRE_NIGHT_PIT_SAFETY_GATE_V1_TEST


# BEGIN PRE_NIGHT_PIT_SAFETY_GATE_AST_RETRY_TESTS


def test_pipeline_manifest_records_explicit_pit_decision(tmp_path):
    snapshot = build_snapshot(tmp_path)

    result = pipeline.build_pre_night_program_parquet(
        "2026-08-10",
        tmp_path,
        parser=lambda *args, **kwargs: valid_program_frame(),
        extractor=fake_extractor,
        snapshot_validator=make_validator(snapshot),
        now_fn=fixed_clock,
    )

    manifest = result["manifest"]

    assert manifest["eligibility_status"] == "ELIGIBLE"
    assert manifest["eligible_for_pre_night"] is True
    assert manifest["eligibility_reason"]
    assert manifest["pit_eligibility"]["status"] == "ELIGIBLE"
    assert manifest["pit_eligibility"]["eligible"] is True
    assert manifest["pit_eligibility"]["race_date"] == "2026-08-10"
    assert manifest["pit_eligibility"]["as_of_time"] == (
        "2026-08-09T21:30:00+09:00"
    )


def test_cached_pipeline_manifest_rejects_missing_pit_fields(
    tmp_path,
):
    snapshot = build_snapshot(tmp_path)

    first = pipeline.build_pre_night_program_parquet(
        "2026-08-10",
        tmp_path,
        parser=lambda *args, **kwargs: valid_program_frame(),
        extractor=fake_extractor,
        snapshot_validator=make_validator(snapshot),
        now_fn=fixed_clock,
    )

    manifest_path = first["paths"]["manifest"]
    manifest = json.loads(
        manifest_path.read_text(encoding="utf-8")
    )
    manifest.pop("eligibility_status")
    manifest_path.write_text(
        json.dumps(manifest),
        encoding="utf-8",
    )

    with pytest.raises(
        pipeline.PreNightOutputIntegrityError,
        match="eligibility",
    ):
        pipeline.build_pre_night_program_parquet(
            "2026-08-10",
            tmp_path,
            parser=lambda *args, **kwargs: pytest.fail(
                "parser must not run"
            ),
            extractor=lambda *args, **kwargs: pytest.fail(
                "extractor must not run"
            ),
            snapshot_validator=make_validator(snapshot),
            now_fn=fixed_clock,
        )


def test_cached_pipeline_manifest_rejects_nested_pit_mismatch(
    tmp_path,
):
    snapshot = build_snapshot(tmp_path)

    first = pipeline.build_pre_night_program_parquet(
        "2026-08-10",
        tmp_path,
        parser=lambda *args, **kwargs: valid_program_frame(),
        extractor=fake_extractor,
        snapshot_validator=make_validator(snapshot),
        now_fn=fixed_clock,
    )

    manifest_path = first["paths"]["manifest"]
    manifest = json.loads(
        manifest_path.read_text(encoding="utf-8")
    )
    manifest["pit_eligibility"]["eligible"] = False
    manifest_path.write_text(
        json.dumps(manifest),
        encoding="utf-8",
    )

    with pytest.raises(
        pipeline.PreNightOutputIntegrityError,
        match="eligibility",
    ):
        pipeline.build_pre_night_program_parquet(
            "2026-08-10",
            tmp_path,
            parser=lambda *args, **kwargs: pytest.fail(
                "parser must not run"
            ),
            extractor=lambda *args, **kwargs: pytest.fail(
                "extractor must not run"
            ),
            snapshot_validator=make_validator(snapshot),
            now_fn=fixed_clock,
        )


# BEGIN PHASE1_D1B2_TESTS


def _d1b2_build(tmp_path, snapshot=None):
    if snapshot is None:
        snapshot = build_snapshot(tmp_path)

    return pipeline.build_pre_night_program_parquet(
        "2026-08-10",
        tmp_path,
        parser=lambda *args, **kwargs: (
            valid_program_frame()
        ),
        extractor=fake_extractor,
        snapshot_validator=make_validator(snapshot),
        now_fn=fixed_clock,
    )


def test_d1b2_t01_program_entries_accept_snapshot_binding(
    tmp_path,
):
    snapshot = build_snapshot(tmp_path)
    result = _d1b2_build(tmp_path, snapshot)
    frame = pd.read_parquet(result["paths"]["parquet"])

    assert "deadline_evidence_sha256" in frame.columns


def test_d1b2_t02_every_row_has_identical_digest(tmp_path):
    snapshot = build_snapshot(tmp_path)
    expected = snapshot["metadata"][
        "deadline_evidence_sha256"
    ]

    result = _d1b2_build(tmp_path, snapshot)
    frame = pd.read_parquet(result["paths"]["parquet"])

    assert frame["deadline_evidence_sha256"].notna().all()
    assert frame["deadline_evidence_sha256"].eq(
        expected
    ).all()


def test_d1b2_t03_manifest_contains_validated_evidence(
    tmp_path,
):
    snapshot = build_snapshot(tmp_path)
    result = _d1b2_build(tmp_path, snapshot)

    expected = deadlines.validate_deadline_evidence(
        snapshot["metadata"]["deadline_evidence"]
    )

    assert result["manifest"]["deadline_evidence"] == (
        expected
    )


def test_d1b2_t04_manifest_digest_is_canonical_sha256(
    tmp_path,
):
    snapshot = build_snapshot(tmp_path)
    result = _d1b2_build(tmp_path, snapshot)

    validated = deadlines.validate_deadline_evidence(
        snapshot["metadata"]["deadline_evidence"]
    )
    expected = sha256_bytes(
        deadlines.canonical_deadline_evidence_bytes(
            validated
        )
    )

    assert result["manifest"][
        "deadline_evidence_sha256"
    ] == expected


def test_d1b2_t05_snapshot_manifest_and_rows_match(
    tmp_path,
):
    snapshot = build_snapshot(tmp_path)
    result = _d1b2_build(tmp_path, snapshot)
    frame = pd.read_parquet(result["paths"]["parquet"])

    expected = snapshot["metadata"][
        "deadline_evidence_sha256"
    ]

    assert result["manifest"][
        "deadline_evidence_sha256"
    ] == expected
    assert set(
        frame["deadline_evidence_sha256"]
    ) == {expected}


def test_d1b2_t06_missing_evidence_fails_before_output(
    tmp_path,
):
    snapshot = build_snapshot(tmp_path)
    snapshot["metadata"].pop("deadline_evidence")

    output_paths = pipeline.build_output_paths(
        "2026-08-10",
        tmp_path,
    )

    with pytest.raises(
        pipeline.PreNightFeatureContractError,
        match="deadline_evidence is missing",
    ):
        _d1b2_build(tmp_path, snapshot)

    assert not output_paths["directory"].exists()
    assert not output_paths["parquet"].exists()
    assert not output_paths["manifest"].exists()


def test_d1b2_t07_missing_digest_fails_before_output(
    tmp_path,
):
    snapshot = build_snapshot(tmp_path)
    snapshot["metadata"].pop(
        "deadline_evidence_sha256"
    )

    output_paths = pipeline.build_output_paths(
        "2026-08-10",
        tmp_path,
    )

    with pytest.raises(
        pipeline.PreNightFeatureContractError,
        match="deadline_evidence_sha256 is missing",
    ):
        _d1b2_build(tmp_path, snapshot)

    assert not output_paths["directory"].exists()
    assert not output_paths["parquet"].exists()
    assert not output_paths["manifest"].exists()


def test_d1b2_t08_malformed_evidence_fails_closed(
    tmp_path,
):
    snapshot = build_snapshot(tmp_path)
    snapshot["metadata"]["deadline_evidence"] = {
        "invalid": True,
    }

    with pytest.raises(
        pipeline.PreNightFeatureContractError,
        match="validation failed",
    ):
        _d1b2_build(tmp_path, snapshot)


def test_d1b2_t09_snapshot_digest_mismatch_fails_closed(
    tmp_path,
):
    snapshot = build_snapshot(tmp_path)
    snapshot["metadata"][
        "deadline_evidence_sha256"
    ] = "f" * 64

    with pytest.raises(
        pipeline.PreNightFeatureContractError,
        match="SHA-256|sha256",
    ):
        _d1b2_build(tmp_path, snapshot)


def test_d1b2_t10_cached_manifest_digest_mismatch(
    tmp_path,
):
    snapshot = build_snapshot(tmp_path)
    result = _d1b2_build(tmp_path, snapshot)

    manifest_path = result["paths"]["manifest"]
    manifest = json.loads(
        manifest_path.read_text(encoding="utf-8")
    )
    manifest["deadline_evidence_sha256"] = "e" * 64
    manifest_path.write_text(
        json.dumps(manifest),
        encoding="utf-8",
    )

    with pytest.raises(
        pipeline.PreNightOutputIntegrityError,
        match="deadline evidence",
    ):
        _d1b2_build(tmp_path, snapshot)


def test_d1b2_t11_cached_parquet_digest_mismatch(
    tmp_path,
):
    snapshot = build_snapshot(tmp_path)
    result = _d1b2_build(tmp_path, snapshot)

    parquet_path = result["paths"]["parquet"]
    manifest_path = result["paths"]["manifest"]

    frame = pd.read_parquet(parquet_path)
    frame["deadline_evidence_sha256"] = "d" * 64
    frame.to_parquet(parquet_path, index=False)

    manifest = json.loads(
        manifest_path.read_text(encoding="utf-8")
    )
    manifest["parquet_sha256"] = pipeline.sha256_file(
        parquet_path
    )
    manifest_path.write_text(
        json.dumps(manifest),
        encoding="utf-8",
    )

    with pytest.raises(
        pipeline.PreNightOutputIntegrityError,
        match="Parquet deadline evidence",
    ):
        _d1b2_build(tmp_path, snapshot)


def test_d1b2_t12_matching_cache_is_reused(tmp_path):
    snapshot = build_snapshot(tmp_path)

    first = _d1b2_build(tmp_path, snapshot)
    second = pipeline.build_pre_night_program_parquet(
        "2026-08-10",
        tmp_path,
        parser=lambda *args, **kwargs: pytest.fail(
            "parser must not run"
        ),
        extractor=lambda *args, **kwargs: pytest.fail(
            "extractor must not run"
        ),
        snapshot_validator=make_validator(snapshot),
        now_fn=fixed_clock,
    )

    assert first["skipped"] is False
    assert second["skipped"] is True
    assert second["manifest"][
        "deadline_evidence_sha256"
    ] == snapshot["metadata"][
        "deadline_evidence_sha256"
    ]


def test_d1b2_t13_no_prohibited_deadline_payloads(
    tmp_path,
):
    snapshot = build_snapshot(tmp_path)
    result = _d1b2_build(tmp_path, snapshot)
    manifest = result["manifest"]

    serialized = json.dumps(
        manifest,
        sort_keys=True,
    )

    assert "raw_source_bytes" not in serialized
    assert "raw_html" not in serialized
    assert "canonical_bytes" not in serialized
    assert "eligibility_cutoff_at" not in serialized
    assert "safety_margin_seconds" not in serialized

    frame = pd.read_parquet(result["paths"]["parquet"])

    assert "deadline_evidence" not in frame.columns
    assert "raw_source_bytes" not in frame.columns
    assert "raw_html" not in frame.columns


def test_d1b2_t14_validation_precedes_output_paths(
    monkeypatch,
    tmp_path,
):
    snapshot = build_snapshot(tmp_path)
    snapshot["metadata"].pop("deadline_evidence")
    events = []

    def forbidden_paths(*args, **kwargs):
        events.append("build_output_paths")
        raise AssertionError(
            "output paths must not be reached"
        )

    monkeypatch.setattr(
        pipeline,
        "build_output_paths",
        forbidden_paths,
    )

    with pytest.raises(
        pipeline.PreNightFeatureContractError,
        match="deadline_evidence is missing",
    ):
        _d1b2_build(tmp_path, snapshot)

    assert events == []


def test_d1b2_t15_existing_manifest_and_return_shape(
    tmp_path,
):
    result = _d1b2_build(tmp_path)
    manifest = result["manifest"]

    assert set(result) == {
        "paths",
        "manifest",
        "structure",
        "pit",
        "skipped",
    }

    for field in (
        "contract_version",
        "feature_version",
        "race_date",
        "as_of_rule",
        "as_of_time",
        "feature_source_sha256",
        "parquet_sha256",
        "row_count",
        "race_count",
        "provenance_status",
        "eligibility_status",
        "pit_eligibility",
        "deadline_evidence",
        "deadline_evidence_sha256",
    ):
        assert field in manifest


def test_d1b2_t16_public_api_signature_is_preserved():
    import inspect

    signature = inspect.signature(
        pipeline.build_pre_night_program_parquet
    )

    assert list(signature.parameters) == [
        "race_date",
        "data_root",
        "overwrite",
        "parser",
        "extractor",
        "snapshot_validator",
        "now_fn",
    ]


# END PHASE1_D1B2_TESTS
