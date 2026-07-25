"""Single-day PRE_NIGHT program snapshot orchestration.

This module connects the prospective program-only collector to the
program-only Parquet ETL.

Dry-run mode is deliberately side-effect free:
- no HTTP request
- no collector invocation
- no pipeline invocation
- no directory creation
- no manifest write
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import uuid
from pathlib import Path
from typing import Callable

from boatrace_ai.ingestion.pre_night_snapshots import (
    build_pre_night_as_of,
    build_snapshot_paths,
    collect_pre_night_program_snapshot,
    normalize_race_date,
)
from boatrace_ai.pipelines.pre_night_snapshot_etl import (
    build_output_paths,
    build_pre_night_program_parquet,
)


ORCHESTRATOR_VERSION = "pre_night_daily_orchestrator_v1"
EXECUTION_CONTRACT_VERSION = "pre_night_daily_execution_v1"
EXECUTION_MODE = "PRE_NIGHT_PROGRAM_ONLY"
MANIFEST_STATUS_SUCCESS = "SUCCESS"
MANIFEST_STATUS_DRY_RUN = "DRY_RUN"
MANIFEST_STATUS_BLOCKED = "BLOCKED_AFTER_AS_OF"


class PreNightDailyError(RuntimeError):
    """Base error for the PRE_NIGHT daily orchestrator."""


class PreNightDailyContractError(PreNightDailyError):
    """Raised when an invocation violates the execution contract."""


class PreNightDailyDeadlineError(PreNightDailyError):
    """Raised when a live acquisition would start after as_of_time."""


class PreNightDailyIntegrityError(PreNightDailyError):
    """Raised when a cached execution artifact fails validation."""


def require_aware_datetime(
    value: dt.datetime,
    field_name: str,
) -> dt.datetime:
    if not isinstance(value, dt.datetime):
        raise PreNightDailyContractError(
            f"{field_name} must be datetime: {value!r}"
        )

    if value.tzinfo is None or value.utcoffset() is None:
        raise PreNightDailyContractError(
            f"{field_name} must be timezone-aware: {value!r}"
        )

    return value


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        for block in iter(
            lambda: handle.read(1024 * 1024),
            b"",
        ):
            digest.update(block)

    return digest.hexdigest()


def atomic_write_json(
    payload: dict,
    destination: Path,
) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)

    temporary = destination.with_name(
        destination.name + f".{uuid.uuid4().hex}.tmp"
    )

    try:
        with temporary.open(
            "w",
            encoding="utf-8",
            newline="\n",
        ) as handle:
            json.dump(
                payload,
                handle,
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())

        temporary.replace(destination)

    finally:
        if temporary.exists():
            temporary.unlink()


def build_execution_manifest_path(
    race_date,
    data_root,
) -> Path:
    date_value = normalize_race_date(race_date)
    root = Path(data_root)

    return (
        root
        / "manifests"
        / "pre_night_v2"
        / "daily"
        / f"{date_value.isoformat()}.json"
    )


def _path_within_root(
    path: Path,
    data_root: Path,
) -> Path:
    resolved_path = path.resolve()
    resolved_root = data_root.resolve()

    try:
        resolved_path.relative_to(resolved_root)
    except ValueError as exc:
        raise PreNightDailyIntegrityError(
            "Manifest artifact is outside data_root: "
            f"path={resolved_path}, data_root={resolved_root}"
        ) from exc

    return resolved_path


def _required_result_path(
    result: dict,
    section_name: str,
    path_name: str,
) -> Path:
    if not isinstance(result, dict):
        raise PreNightDailyContractError(
            f"{section_name} result must be dict"
        )

    paths = result.get("paths")

    if not isinstance(paths, dict):
        raise PreNightDailyContractError(
            f"{section_name}.paths must be dict"
        )

    value = paths.get(path_name)

    if value is None:
        raise PreNightDailyContractError(
            f"{section_name}.paths.{path_name} is missing"
        )

    return Path(value)


def _artifact_record(
    path: Path,
    data_root: Path,
) -> dict:
    resolved = _path_within_root(path, data_root)

    if not resolved.is_file():
        raise PreNightDailyIntegrityError(
            f"Expected artifact does not exist: {resolved}"
        )

    return {
        "path": str(resolved),
        "size": int(resolved.stat().st_size),
        "sha256": sha256_file(resolved),
    }


def build_dry_run_plan(
    race_date,
    data_root,
    *,
    now_fn=None,
) -> dict:
    """Build a side-effect-free execution plan."""

    date_value = normalize_race_date(race_date)
    root = Path(data_root)

    clock = now_fn or (
        lambda: dt.datetime.now(dt.timezone.utc)
    )
    current_time = require_aware_datetime(
        clock(),
        "current_time",
    )

    as_of_time = build_pre_night_as_of(date_value)
    snapshot_paths = build_snapshot_paths(
        date_value,
        root,
    )
    output_paths = build_output_paths(
        date_value,
        root,
    )
    execution_manifest = build_execution_manifest_path(
        date_value,
        root,
    )

    eligible_to_start = current_time <= as_of_time

    return {
        "status": (
            MANIFEST_STATUS_DRY_RUN
            if eligible_to_start
            else MANIFEST_STATUS_BLOCKED
        ),
        "dry_run": True,
        "race_date": date_value.isoformat(),
        "execution_mode": EXECUTION_MODE,
        "orchestrator_version": ORCHESTRATOR_VERSION,
        "execution_contract_version": (
            EXECUTION_CONTRACT_VERSION
        ),
        "as_of_time": as_of_time.isoformat(),
        "checked_at": current_time.isoformat(),
        "eligible_to_start": eligible_to_start,
        "block_reason": (
            None
            if eligible_to_start
            else "CURRENT_TIME_AFTER_AS_OF"
        ),
        "planned_snapshot_archive": str(
            snapshot_paths["archive"]
        ),
        "planned_snapshot_metadata": str(
            snapshot_paths["metadata"]
        ),
        "planned_output_parquet": str(
            output_paths["parquet"]
        ),
        "planned_pipeline_manifest": str(
            output_paths["manifest"]
        ),
        "planned_execution_manifest": str(
            execution_manifest
        ),
        "network_performed": False,
        "data_files_written": False,
        "collector_called": False,
        "pipeline_called": False,
    }


def validate_execution_manifest(
    manifest_path: Path,
    data_root,
    race_date=None,
) -> dict:
    root = Path(data_root)
    resolved_manifest = _path_within_root(
        Path(manifest_path),
        root,
    )

    if not resolved_manifest.is_file():
        raise PreNightDailyIntegrityError(
            "Execution manifest does not exist: "
            f"{resolved_manifest}"
        )

    try:
        manifest = json.loads(
            resolved_manifest.read_text(
                encoding="utf-8"
            )
        )
    except Exception as exc:
        raise PreNightDailyIntegrityError(
            "Execution manifest is malformed: "
            f"{resolved_manifest}"
        ) from exc

    if not isinstance(manifest, dict):
        raise PreNightDailyIntegrityError(
            "Execution manifest must be a JSON object"
        )

    expected_values = {
        "status": MANIFEST_STATUS_SUCCESS,
        "execution_mode": EXECUTION_MODE,
        "orchestrator_version": ORCHESTRATOR_VERSION,
        "execution_contract_version": (
            EXECUTION_CONTRACT_VERSION
        ),
    }

    for field_name, expected in expected_values.items():
        actual = manifest.get(field_name)
        if actual != expected:
            raise PreNightDailyIntegrityError(
                f"Execution manifest field mismatch: "
                f"{field_name}: expected={expected!r}, "
                f"actual={actual!r}"
            )

    if race_date is not None:
        expected_date = normalize_race_date(
            race_date
        ).isoformat()

        if manifest.get("race_date") != expected_date:
            raise PreNightDailyIntegrityError(
                "Execution manifest race_date mismatch: "
                f"expected={expected_date}, "
                f"actual={manifest.get('race_date')}"
            )

    artifacts = manifest.get("artifacts")

    if not isinstance(artifacts, dict):
        raise PreNightDailyIntegrityError(
            "Execution manifest artifacts must be dict"
        )

    required_artifacts = {
        "source_archive",
        "source_metadata",
        "output_parquet",
        "pipeline_manifest",
    }

    missing = required_artifacts - set(artifacts)
    if missing:
        raise PreNightDailyIntegrityError(
            "Execution manifest artifact entries missing: "
            f"{sorted(missing)}"
        )

    validated_artifacts = {}

    for artifact_name in sorted(required_artifacts):
        record = artifacts.get(artifact_name)

        if not isinstance(record, dict):
            raise PreNightDailyIntegrityError(
                f"Invalid artifact record: {artifact_name}"
            )

        path_text = record.get("path")
        expected_sha256 = str(
            record.get("sha256", "")
        ).lower()
        expected_size = record.get("size")

        if not path_text:
            raise PreNightDailyIntegrityError(
                f"Artifact path missing: {artifact_name}"
            )

        artifact_path = _path_within_root(
            Path(path_text),
            root,
        )

        if not artifact_path.is_file():
            raise PreNightDailyIntegrityError(
                f"Artifact missing: {artifact_name}: "
                f"{artifact_path}"
            )

        actual_size = int(
            artifact_path.stat().st_size
        )
        actual_sha256 = sha256_file(
            artifact_path
        ).lower()

        if expected_size != actual_size:
            raise PreNightDailyIntegrityError(
                f"Artifact size mismatch: {artifact_name}: "
                f"expected={expected_size}, "
                f"actual={actual_size}"
            )

        if expected_sha256 != actual_sha256:
            raise PreNightDailyIntegrityError(
                f"Artifact SHA-256 mismatch: "
                f"{artifact_name}: "
                f"expected={expected_sha256}, "
                f"actual={actual_sha256}"
            )

        validated_artifacts[artifact_name] = {
            "path": artifact_path,
            "size": actual_size,
            "sha256": actual_sha256,
        }

    return {
        "manifest_path": resolved_manifest,
        "manifest": manifest,
        "artifacts": validated_artifacts,
        "cached": True,
        "skipped": True,
    }


def run_pre_night_daily(
    race_date,
    data_root,
    *,
    dry_run=True,
    overwrite=False,
    collector: Callable | None = None,
    pipeline: Callable | None = None,
    now_fn=None,
) -> dict:
    """Run or plan one PRE_NIGHT program-only collection day."""

    date_value = normalize_race_date(race_date)
    root = Path(data_root)

    clock = now_fn or (
        lambda: dt.datetime.now(dt.timezone.utc)
    )

    current_time = require_aware_datetime(
        clock(),
        "current_time",
    )
    as_of_time = build_pre_night_as_of(
        date_value
    )

    if dry_run:
        return build_dry_run_plan(
            date_value,
            root,
            now_fn=lambda: current_time,
        )

    execution_manifest_path = (
        build_execution_manifest_path(
            date_value,
            root,
        )
    )

    # A completed run can be validated after the deadline because
    # this branch performs no acquisition.
    if (
        execution_manifest_path.is_file()
        and not overwrite
    ):
        cached = validate_execution_manifest(
            execution_manifest_path,
            root,
            race_date=date_value,
        )

        return {
            "status": MANIFEST_STATUS_SUCCESS,
            "race_date": date_value.isoformat(),
            "dry_run": False,
            "cached": True,
            "skipped": True,
            "collector_called": False,
            "pipeline_called": False,
            "execution_manifest": cached,
        }

    # Never start or retry network acquisition after the PIT cutoff.
    if current_time > as_of_time:
        raise PreNightDailyDeadlineError(
            "Live PRE_NIGHT acquisition cannot start "
            "after as_of_time: "
            f"current_time={current_time.isoformat()}, "
            f"as_of_time={as_of_time.isoformat()}"
        )

    collector_function = (
        collector
        or collect_pre_night_program_snapshot
    )
    pipeline_function = (
        pipeline
        or build_pre_night_program_parquet
    )

    collector_result = collector_function(
        date_value,
        root,
        now_fn=clock,
    )

    pipeline_result = pipeline_function(
        date_value,
        root,
        overwrite=overwrite,
        now_fn=clock,
    )

    source_archive = _required_result_path(
        collector_result,
        "collector",
        "archive",
    )
    source_metadata = _required_result_path(
        collector_result,
        "collector",
        "metadata",
    )
    output_parquet = _required_result_path(
        pipeline_result,
        "pipeline",
        "parquet",
    )
    pipeline_manifest = _required_result_path(
        pipeline_result,
        "pipeline",
        "manifest",
    )

    completed_at = require_aware_datetime(
        clock(),
        "completed_at",
    )

    artifacts = {
        "source_archive": _artifact_record(
            source_archive,
            root,
        ),
        "source_metadata": _artifact_record(
            source_metadata,
            root,
        ),
        "output_parquet": _artifact_record(
            output_parquet,
            root,
        ),
        "pipeline_manifest": _artifact_record(
            pipeline_manifest,
            root,
        ),
    }

    execution_manifest = {
        "status": MANIFEST_STATUS_SUCCESS,
        "race_date": date_value.isoformat(),
        "execution_mode": EXECUTION_MODE,
        "orchestrator_version": ORCHESTRATOR_VERSION,
        "execution_contract_version": (
            EXECUTION_CONTRACT_VERSION
        ),
        "as_of_time": as_of_time.isoformat(),
        "started_at": current_time.isoformat(),
        "completed_at": completed_at.isoformat(),
        "dry_run": False,
        "collector_called": True,
        "pipeline_called": True,
        "artifacts": artifacts,
    }

    atomic_write_json(
        execution_manifest,
        execution_manifest_path,
    )

    validated = validate_execution_manifest(
        execution_manifest_path,
        root,
        race_date=date_value,
    )

    return {
        "status": MANIFEST_STATUS_SUCCESS,
        "race_date": date_value.isoformat(),
        "dry_run": False,
        "cached": False,
        "skipped": False,
        "collector_called": True,
        "pipeline_called": True,
        "collector_result": collector_result,
        "pipeline_result": pipeline_result,
        "execution_manifest": validated,
    }
