import datetime as dt
import hashlib
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

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
