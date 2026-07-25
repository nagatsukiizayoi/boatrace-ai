import hashlib
import json
from datetime import date

import pandas as pd
import pytest

from boatrace_ai.ingestion.post_race_label_sources import (
    COLLECTOR_VERSION,
    CONTRACT_VERSION,
    atomic_write_json,
    build_source_paths,
)
from boatrace_ai.pipelines.post_race_label_etl import (
    LABEL_PROVENANCE_COLUMNS,
    PostRaceLabelContractError,
    PostRaceLabelIntegrityError,
    build_output_paths,
    build_post_race_label_parquet,
)


RACE_DATE = date(2026, 8, 10)
FETCHED_AT = "2026-08-10T12:00:00+00:00"
ARCHIVE_BYTES = b"\x00-lh5-post-race-label"


def sha256_bytes(value):
    return hashlib.sha256(value).hexdigest()


def build_source(tmp_path):
    paths = build_source_paths(
        RACE_DATE,
        tmp_path,
    )
    paths["directory"].mkdir(
        parents=True,
        exist_ok=True,
    )
    paths["archive"].write_bytes(
        ARCHIVE_BYTES
    )

    digest = sha256_bytes(ARCHIVE_BYTES)

    metadata = {
        "contract_version": CONTRACT_VERSION,
        "collector_version": COLLECTOR_VERSION,
        "source_type": "result",
        "archive_type": "result",
        "snapshot_type": "post_race_label",
        "race_date": RACE_DATE.isoformat(),
        "request_started_at": FETCHED_AT,
        "fetched_at": FETCHED_AT,
        "source_url": (
            "https://example.invalid/k260810.lzh"
        ),
        "http_status": 200,
        "response_size": len(ARCHIVE_BYTES),
        "source_response_sha256": digest,
        "archive_sha256": digest,
        "archive_path": str(paths["archive"]),
        "label_eligible": True,
        "provenance_status": "VERIFIED",
    }

    atomic_write_json(
        metadata,
        paths["metadata"],
    )

    return paths


def fake_extractor(archive, destination):
    text_file = destination / "K260810.TXT"
    text_file.write_text(
        "fixture",
        encoding="utf-8",
    )
    return [text_file]


def valid_result_frame():
    return pd.DataFrame(
        [
            {
                "race_date": RACE_DATE.isoformat(),
                "venue_code": "01",
                "race_no": 1,
                "boat_no": 1,
                "racer_id": "1001",
                "finish_position": 1,
            },
            {
                "race_date": RACE_DATE.isoformat(),
                "venue_code": "01",
                "race_no": 1,
                "boat_no": 2,
                "racer_id": "1002",
                "finish_position": 2,
            },
        ]
    )


def valid_payout_frame():
    return pd.DataFrame(
        [
            {
                "race_date": RACE_DATE.isoformat(),
                "venue_code": "01",
                "race_no": 1,
                "bet_type": "WIN",
                "selection_no": 1,
                "payout_status": "NORMAL",
                "combination": "1",
                "payout_yen": 120,
            }
        ]
    )


def result_parser(path, race_date=None):
    return valid_result_frame()


def payout_parser(path, race_date=None):
    return valid_payout_frame()


def source_validator(race_date, data_root):
    from boatrace_ai.ingestion.post_race_label_sources import (
        validate_cached_source,
    )

    return validate_cached_source(
        race_date,
        data_root,
        validator=lambda path: None,
    )


def test_build_output_paths(tmp_path):
    paths = build_output_paths(
        RACE_DATE,
        tmp_path,
    )

    assert paths["result"].name == (
        "race_results.parquet"
    )
    assert paths["payout"].name == (
        "race_payouts.parquet"
    )
    assert paths["manifest"].name == (
        "label_manifest.json"
    )


def test_writes_separate_label_outputs_with_provenance(
    tmp_path,
):
    build_source(tmp_path)

    result = build_post_race_label_parquet(
        RACE_DATE,
        tmp_path,
        result_parser=result_parser,
        payout_parser=payout_parser,
        extractor=fake_extractor,
        source_validator=source_validator,
    )

    paths = result["paths"]
    result_frame = pd.read_parquet(
        paths["result"]
    )
    payout_frame = pd.read_parquet(
        paths["payout"]
    )

    for column in LABEL_PROVENANCE_COLUMNS:
        assert column in result_frame.columns
        assert column in payout_frame.columns

    assert (
        result_frame["label_source_max_time"]
        == FETCHED_AT
    ).all()
    assert (
        payout_frame["label_source_max_time"]
        == FETCHED_AT
    ).all()

    assert not any(
        column.startswith("feature_source_")
        for column in result_frame.columns
    )

    assert result["manifest"][
        "feature_provenance_included"
    ] is False


def test_rejects_duplicate_result_row_key(tmp_path):
    build_source(tmp_path)

    duplicate = valid_result_frame()
    duplicate = pd.concat(
        [duplicate, duplicate.iloc[[0]]],
        ignore_index=True,
    )

    with pytest.raises(
        PostRaceLabelContractError
    ):
        build_post_race_label_parquet(
            RACE_DATE,
            tmp_path,
            result_parser=lambda *args, **kwargs: duplicate,
            payout_parser=payout_parser,
            extractor=fake_extractor,
            source_validator=source_validator,
        )


def test_reuses_valid_output_without_parser_calls(
    tmp_path,
):
    build_source(tmp_path)

    build_post_race_label_parquet(
        RACE_DATE,
        tmp_path,
        result_parser=result_parser,
        payout_parser=payout_parser,
        extractor=fake_extractor,
        source_validator=source_validator,
    )

    def fail(*args, **kwargs):
        raise AssertionError(
            "Parser or extractor must not be called"
        )

    result = build_post_race_label_parquet(
        RACE_DATE,
        tmp_path,
        result_parser=fail,
        payout_parser=fail,
        extractor=fail,
        source_validator=source_validator,
    )

    assert result["status"] == "CACHED"


def test_rejects_tampered_label_parquet(tmp_path):
    build_source(tmp_path)

    result = build_post_race_label_parquet(
        RACE_DATE,
        tmp_path,
        result_parser=result_parser,
        payout_parser=payout_parser,
        extractor=fake_extractor,
        source_validator=source_validator,
    )

    result["paths"]["result"].write_bytes(
        b"tampered parquet"
    )

    with pytest.raises(
        PostRaceLabelIntegrityError
    ):
        build_post_race_label_parquet(
            RACE_DATE,
            tmp_path,
            result_parser=result_parser,
            payout_parser=payout_parser,
            extractor=fake_extractor,
            source_validator=source_validator,
        )
