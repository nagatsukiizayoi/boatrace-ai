from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from boatrace_ai.ingestion.post_race_label_sources import (
    COLLECTOR_VERSION,
    build_source_paths,
    parse_timestamp,
    sha256_file,
    validate_cached_source,
)
from boatrace_ai.parsers.payout import (
    OUTPUT_COLUMNS as PAYOUT_OUTPUT_COLUMNS,
    PRIMARY_KEY as PAYOUT_PRIMARY_KEY,
    parse_payout_file,
)
from boatrace_ai.parsers.result import (
    OUTPUT_COLUMNS as RESULT_OUTPUT_COLUMNS,
    parse_result_file,
)


OUTPUT_CONTRACT_VERSION = "post_race_label_parquet_v1"
PIPELINE_VERSION = "post_race_label_etl_v1"

RESULT_ROW_KEYS = [
    "race_date",
    "venue_code",
    "race_no",
    "boat_no",
]

PAYOUT_ROW_KEYS = list(PAYOUT_PRIMARY_KEY)

LABEL_PROVENANCE_COLUMNS = [
    "label_source_fetched_at",
    "label_source_max_time",
    "label_source_sha256",
    "label_source_url",
    "label_collector_version",
    "label_contract_version",
    "label_provenance_status",
]

PROHIBITED_FEATURE_PROVENANCE_COLUMNS = {
    "as_of_time",
    "snapshot_at",
    "feature_source_fetched_at",
    "feature_source_max_time",
    "feature_source_sha256",
    "feature_source_url",
    "feature_collector_version",
    "feature_contract_version",
    "provenance_status",
}


class PostRaceLabelETLError(RuntimeError):
    pass


class PostRaceLabelContractError(PostRaceLabelETLError):
    pass


class PostRaceLabelIntegrityError(PostRaceLabelETLError):
    pass


def normalize_race_date(
    value: date | datetime | str,
) -> date:
    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, date):
        return value

    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise PostRaceLabelContractError(
            f"Invalid race_date: {value!r}"
        ) from exc


def build_output_paths(
    race_date: date | datetime | str,
    data_root: str | Path,
) -> dict[str, Path]:
    normalized = normalize_race_date(race_date)
    directory = (
        Path(data_root)
        / "labels"
        / "race_results"
        / f"{normalized:%Y}"
        / f"{normalized:%m}"
        / f"{normalized:%d}"
    )

    return {
        "directory": directory,
        "result": directory / "race_results.parquet",
        "payout": directory / "race_payouts.parquet",
        "manifest": directory / "label_manifest.json",
    }


def atomic_write_json(
    value: dict,
    path: str | Path,
) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)

    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{target.name}.",
        suffix=".part",
        dir=target.parent,
    )
    temporary_path = Path(temporary_name)

    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(
                value,
                handle,
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())

        os.replace(temporary_path, target)

    finally:
        if temporary_path.exists():
            temporary_path.unlink()


def atomic_write_parquet(
    frame: pd.DataFrame,
    path: str | Path,
) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)

    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{target.name}.",
        suffix=".part",
        dir=target.parent,
    )
    os.close(fd)
    temporary_path = Path(temporary_name)

    try:
        frame.to_parquet(
            temporary_path,
            index=False,
        )

        with temporary_path.open("rb") as handle:
            os.fsync(handle.fileno())

        os.replace(temporary_path, target)

    finally:
        if temporary_path.exists():
            temporary_path.unlink()


def extract_lzh_archive(
    archive_path: str | Path,
    destination: str | Path,
    timeout: int = 120,
) -> list[Path]:
    executable = shutil.which("7zz") or shutil.which("7z")

    if executable is None:
        raise PostRaceLabelETLError(
            "7z or 7zz is required to extract LZH archives"
        )

    destination_path = Path(destination)
    destination_path.mkdir(
        parents=True,
        exist_ok=True,
    )

    result = subprocess.run(
        [
            executable,
            "x",
            "-y",
            f"-o{destination_path}",
            str(Path(archive_path)),
        ],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        timeout=timeout,
    )

    if result.returncode != 0:
        detail = (
            result.stderr or result.stdout
        ).strip()
        raise PostRaceLabelETLError(
            f"LZH extraction failed: {detail}"
        )

    return sorted(
        path
        for path in destination_path.rglob("*")
        if path.is_file()
    )


def select_result_text_file(
    files: list[Path],
) -> Path:
    candidates = [
        Path(path)
        for path in files
        if Path(path).is_file()
    ]

    if not candidates:
        raise PostRaceLabelETLError(
            "No extracted result files were found"
        )

    preferred = [
        path
        for path in candidates
        if path.name.lower().startswith("k")
    ]

    selected = preferred or candidates

    if len(selected) != 1:
        raise PostRaceLabelETLError(
            "Expected exactly one result text file; "
            f"found {len(selected)}"
        )

    return selected[0]


def _validate_race_date_column(
    frame: pd.DataFrame,
    race_date: date,
    frame_name: str,
) -> None:
    if "race_date" not in frame.columns:
        raise PostRaceLabelContractError(
            f"{frame_name} has no race_date column"
        )

    normalized = {
        str(value)[:10]
        for value in frame["race_date"].dropna()
    }

    if normalized != {race_date.isoformat()}:
        raise PostRaceLabelContractError(
            f"{frame_name} race_date does not match"
        )


def validate_result_frame(
    frame: pd.DataFrame,
    race_date: date,
) -> None:
    required = set(RESULT_ROW_KEYS) | {
        "finish_position",
        "racer_id",
    }
    missing = sorted(required - set(frame.columns))

    if missing:
        raise PostRaceLabelContractError(
            f"Missing result columns: {missing}"
        )

    prohibited = sorted(
        PROHIBITED_FEATURE_PROVENANCE_COLUMNS
        & set(frame.columns)
    )

    if prohibited:
        raise PostRaceLabelContractError(
            "Result frame contains feature provenance: "
            f"{prohibited}"
        )

    _validate_race_date_column(
        frame,
        race_date,
        "result",
    )

    if frame.duplicated(
        RESULT_ROW_KEYS,
        keep=False,
    ).any():
        raise PostRaceLabelContractError(
            "Duplicate result row key"
        )


def validate_payout_frame(
    frame: pd.DataFrame,
    race_date: date,
) -> None:
    required = set(PAYOUT_ROW_KEYS) | {
        "combination",
        "payout_yen",
    }
    missing = sorted(required - set(frame.columns))

    if missing:
        raise PostRaceLabelContractError(
            f"Missing payout columns: {missing}"
        )

    prohibited = sorted(
        PROHIBITED_FEATURE_PROVENANCE_COLUMNS
        & set(frame.columns)
    )

    if prohibited:
        raise PostRaceLabelContractError(
            "Payout frame contains feature provenance: "
            f"{prohibited}"
        )

    _validate_race_date_column(
        frame,
        race_date,
        "payout",
    )

    if frame.duplicated(
        PAYOUT_ROW_KEYS,
        keep=False,
    ).any():
        raise PostRaceLabelContractError(
            "Duplicate payout row key"
        )


def attach_label_provenance(
    frame: pd.DataFrame,
    metadata: dict,
) -> pd.DataFrame:
    output = frame.copy()

    fetched_at = parse_timestamp(
        metadata["fetched_at"],
        "fetched_at",
    ).isoformat()

    output["label_source_fetched_at"] = fetched_at
    output["label_source_max_time"] = fetched_at
    output["label_source_sha256"] = (
        metadata["archive_sha256"]
    )
    output["label_source_url"] = (
        metadata["source_url"]
    )
    output["label_collector_version"] = (
        metadata["collector_version"]
    )
    output["label_contract_version"] = (
        OUTPUT_CONTRACT_VERSION
    )
    output["label_provenance_status"] = "VERIFIED"

    return output


def validate_existing_output(
    race_date: date | datetime | str,
    data_root: str | Path,
) -> dict:
    normalized = normalize_race_date(race_date)
    paths = build_output_paths(normalized, data_root)

    existence = {
        key: paths[key].is_file()
        for key in ("result", "payout", "manifest")
    }

    if not any(existence.values()):
        raise PostRaceLabelIntegrityError(
            "Post-race label output is not cached"
        )

    if not all(existence.values()):
        raise PostRaceLabelIntegrityError(
            "Post-race label output is incomplete"
        )

    try:
        with paths["manifest"].open(
            "r",
            encoding="utf-8",
        ) as handle:
            manifest = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise PostRaceLabelIntegrityError(
            "Unable to load label manifest"
        ) from exc

    if (
        manifest.get("contract_version")
        != OUTPUT_CONTRACT_VERSION
    ):
        raise PostRaceLabelContractError(
            "Unsupported label output contract"
        )

    if manifest.get("race_date") != normalized.isoformat():
        raise PostRaceLabelContractError(
            "Label manifest race_date mismatch"
        )

    source = validate_cached_source(
        normalized,
        data_root,
    )
    metadata = source["metadata"]

    if (
        manifest.get("source_archive_sha256")
        != metadata["archive_sha256"]
    ):
        raise PostRaceLabelIntegrityError(
            "Label manifest source hash mismatch"
        )

    expected_hashes = {
        "result": sha256_file(paths["result"]),
        "payout": sha256_file(paths["payout"]),
    }

    if manifest.get("output_sha256") != expected_hashes:
        raise PostRaceLabelIntegrityError(
            "Label output SHA-256 mismatch"
        )

    result_frame = pd.read_parquet(paths["result"])
    payout_frame = pd.read_parquet(paths["payout"])

    validate_result_frame(result_frame, normalized)
    validate_payout_frame(payout_frame, normalized)

    for column in LABEL_PROVENANCE_COLUMNS:
        if column not in result_frame.columns:
            raise PostRaceLabelContractError(
                f"Result output lacks {column}"
            )

        if column not in payout_frame.columns:
            raise PostRaceLabelContractError(
                f"Payout output lacks {column}"
            )

    return {
        "status": "CACHED",
        "paths": paths,
        "manifest": manifest,
        "result_rows": len(result_frame),
        "payout_rows": len(payout_frame),
    }


def build_post_race_label_parquet(
    race_date: date | datetime | str,
    data_root: str | Path,
    overwrite: bool = False,
    result_parser=None,
    payout_parser=None,
    extractor=None,
    source_validator=None,
) -> dict:
    normalized = normalize_race_date(race_date)
    paths = build_output_paths(normalized, data_root)

    existing = any(
        paths[key].exists()
        for key in ("result", "payout", "manifest")
    )

    if existing and not overwrite:
        return validate_existing_output(
            normalized,
            data_root,
        )

    validate_source = (
        source_validator or validate_cached_source
    )
    source = validate_source(
        normalized,
        data_root,
    )
    metadata = source["metadata"]
    source_paths = build_source_paths(
        normalized,
        data_root,
    )

    parse_result = (
        result_parser or parse_result_file
    )
    parse_payout = (
        payout_parser or parse_payout_file
    )
    extract = extractor or extract_lzh_archive

    with tempfile.TemporaryDirectory(
        prefix="post-race-label-"
    ) as temporary_directory:
        extracted = extract(
            source_paths["archive"],
            Path(temporary_directory),
        )
        result_text = select_result_text_file(
            list(extracted)
        )

        result_frame = parse_result(
            result_text,
            race_date=normalized,
        )
        payout_frame = parse_payout(
            result_text,
            race_date=normalized,
        )

    validate_result_frame(
        result_frame,
        normalized,
    )
    validate_payout_frame(
        payout_frame,
        normalized,
    )

    result_output = attach_label_provenance(
        result_frame,
        metadata,
    )
    payout_output = attach_label_provenance(
        payout_frame,
        metadata,
    )

    paths["directory"].mkdir(
        parents=True,
        exist_ok=True,
    )

    atomic_write_parquet(
        result_output,
        paths["result"],
    )
    atomic_write_parquet(
        payout_output,
        paths["payout"],
    )

    manifest = {
        "contract_version": OUTPUT_CONTRACT_VERSION,
        "pipeline_version": PIPELINE_VERSION,
        "race_date": normalized.isoformat(),
        "source_type": "result",
        "source_archive_path": str(
            source_paths["archive"]
        ),
        "source_metadata_path": str(
            source_paths["metadata"]
        ),
        "source_archive_sha256":
            metadata["archive_sha256"],
        "source_metadata_sha256":
            sha256_file(source_paths["metadata"]),
        "label_source_fetched_at":
            metadata["fetched_at"],
        "label_source_max_time":
            metadata["fetched_at"],
        "label_source_url":
            metadata["source_url"],
        "label_collector_version":
            metadata["collector_version"],
        "feature_provenance_included": False,
        "result_rows": len(result_output),
        "payout_rows": len(payout_output),
        "output_sha256": {
            "result": sha256_file(paths["result"]),
            "payout": sha256_file(paths["payout"]),
        },
    }

    atomic_write_json(
        manifest,
        paths["manifest"],
    )

    return validate_existing_output(
        normalized,
        data_root,
    )
