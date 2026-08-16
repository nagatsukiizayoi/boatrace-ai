"""Runtime wiring for one-venue PRE_NIGHT provenance stages."""

from __future__ import annotations

import datetime as dt
import hashlib
import os
import platform
import re
import subprocess
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import pandas as pd

from boatrace_ai.pipelines.pre_night_daily import (
    run_pre_night_daily,
)
from boatrace_ai.pipelines.pre_night_deadline_collection import (
    collect_pre_night_deadline_evidence,
)
from boatrace_ai.pipelines.pre_night_manifest_chain import (
    publish_pre_night_manifest_chain,
)
from boatrace_ai.pipelines.pre_night_program_binding import (
    publish_pre_night_program_entries_binding,
)


RUNTIME_WIRING_VERSION = "pre_night_bound_daily_v1"
PIPELINE_NAME = "pre-night"
PIPELINE_VERSION = "venue-binding-runtime-v1"

_VENUE_RE = re.compile(r"^(0[1-9]|1[0-9]|2[0-4])$")


class PreNightBoundDailyError(Exception):
    """Base runtime-wiring error."""


class PreNightBoundDailyContractError(PreNightBoundDailyError):
    """Raised when caller input violates the runtime contract."""


class PreNightBoundDailyIntegrityError(PreNightBoundDailyError):
    """Raised when an artifact or stage result is inconsistent."""


def _require_race_date(value: Any) -> str:
    if isinstance(value, dt.datetime):
        raise PreNightBoundDailyContractError(
            "race_date must not be datetime"
        )

    if isinstance(value, dt.date):
        return value.isoformat()

    if not isinstance(value, str):
        raise PreNightBoundDailyContractError(
            "race_date must be ISO date"
        )

    try:
        normalized = dt.date.fromisoformat(value).isoformat()
    except ValueError as error:
        raise PreNightBoundDailyContractError(
            "race_date must be ISO date"
        ) from error

    if normalized != value:
        raise PreNightBoundDailyContractError(
            "race_date must be canonical"
        )

    return normalized


def _require_venue_code(value: Any) -> str:
    if isinstance(value, bool):
        raise PreNightBoundDailyContractError(
            "venue_code must be 01 through 24"
        )

    if isinstance(value, int):
        value = f"{value:02d}"

    if (
        not isinstance(value, str)
        or _VENUE_RE.fullmatch(value) is None
    ):
        raise PreNightBoundDailyContractError(
            "venue_code must be 01 through 24"
        )

    return value


def _require_mapping(
    value: Any,
    field_name: str,
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise PreNightBoundDailyContractError(
            f"{field_name} must be mapping"
        )

    return dict(value)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)

    return digest.hexdigest()


def _path_within_root(
    value: Any,
    root: Path,
    field_name: str,
) -> Path:
    path = Path(value)

    if path.is_symlink():
        raise PreNightBoundDailyIntegrityError(
            f"{field_name} must not be symlink"
        )

    try:
        path.resolve().relative_to(root.resolve())
    except ValueError as error:
        raise PreNightBoundDailyIntegrityError(
            f"{field_name} escapes data_root"
        ) from error

    if not path.is_file():
        raise PreNightBoundDailyIntegrityError(
            f"{field_name} does not exist: {path}"
        )

    return path


def _result_artifact_path(
    result: Mapping[str, Any],
    *,
    fresh_section: str,
    fresh_key: str,
    cached_artifact: str,
    root: Path,
) -> Path:
    section = result.get(fresh_section)

    if isinstance(section, Mapping):
        paths = section.get("paths")

        if (
            isinstance(paths, Mapping)
            and paths.get(fresh_key) is not None
        ):
            return _path_within_root(
                paths[fresh_key],
                root,
                f"{fresh_section}.{fresh_key}",
            )

    execution = result.get("execution_manifest")

    if isinstance(execution, Mapping):
        artifacts = execution.get("artifacts")

        if isinstance(artifacts, Mapping):
            artifact = artifacts.get(cached_artifact)

            if (
                isinstance(artifact, Mapping)
                and artifact.get("path") is not None
            ):
                return _path_within_root(
                    artifact["path"],
                    root,
                    f"execution.{cached_artifact}",
                )

    raise PreNightBoundDailyIntegrityError(
        f"daily result does not provide {cached_artifact}"
    )


def _normalize_frame_venue(value: Any) -> str:
    if pd.isna(value):
        raise PreNightBoundDailyIntegrityError(
            "program venue_code contains null"
        )

    if isinstance(value, bool):
        raise PreNightBoundDailyIntegrityError(
            "program venue_code is invalid"
        )

    if isinstance(value, int):
        return _require_venue_code(value)

    if isinstance(value, float) and value.is_integer():
        return _require_venue_code(int(value))

    text = str(value)

    if text.isdigit():
        return _require_venue_code(int(text))

    return _require_venue_code(text)


def _normalize_frame_date(value: Any) -> str:
    if isinstance(value, pd.Timestamp):
        value = value.date()

    if isinstance(value, dt.datetime):
        value = value.date()

    if isinstance(value, dt.date):
        return value.isoformat()

    return str(value)


def _program_entries_from_parquet(
    parquet_path: Path,
    *,
    race_date: str,
    venue_code: str,
) -> list[dict[str, Any]]:
    frame = pd.read_parquet(parquet_path)

    required = {
        "race_date",
        "venue_code",
        "race_no",
        "boat_no",
    }
    missing = required - set(frame.columns)

    if missing:
        raise PreNightBoundDailyIntegrityError(
            "program parquet identity columns missing: "
            f"{sorted(missing)}"
        )

    normalized_dates = frame["race_date"].map(
        _normalize_frame_date
    )
    normalized_venues = frame["venue_code"].map(
        _normalize_frame_venue
    )

    observed_dates = set(normalized_dates)
    observed_venues = set(normalized_venues)

    if observed_dates != {race_date}:
        raise PreNightBoundDailyIntegrityError(
            "program parquet race_date coverage mismatch"
        )

    if observed_venues != {venue_code}:
        raise PreNightBoundDailyIntegrityError(
            "program parquet venue coverage mismatch"
        )

    entries = [
        {
            "race_date": race_date,
            "venue_code": venue_code,
            "race_no": int(row.race_no),
            "boat_no": int(row.boat_no),
        }
        for row in frame[
            ["race_no", "boat_no"]
        ].itertuples(index=False)
    ]

    if len(entries) != 72:
        raise PreNightBoundDailyIntegrityError(
            "one-venue program grid must contain 72 rows"
        )

    return entries


def _publish_snapshot_link(
    source: Path,
    *,
    root: Path,
    race_date: str,
    run_id: str,
) -> Path:
    year, month, day = race_date.split("-")

    destination = (
        root
        / "prospective"
        / "pre_night"
        / "runs"
        / year
        / month
        / day
        / run_id
        / "snapshot.parquet"
    )

    destination.parent.mkdir(parents=True, exist_ok=True)

    if destination.is_symlink():
        raise PreNightBoundDailyIntegrityError(
            "snapshot destination must not be symlink"
        )

    if destination.exists():
        if not destination.is_file():
            raise PreNightBoundDailyIntegrityError(
                "snapshot destination is not a file"
            )

        if _sha256_file(destination) != _sha256_file(source):
            raise PreNightBoundDailyIntegrityError(
                "snapshot cache digest conflict"
            )

        return destination

    try:
        os.link(source, destination)
    except FileExistsError:
        if _sha256_file(destination) != _sha256_file(source):
            raise PreNightBoundDailyIntegrityError(
                "snapshot concurrent publication conflict"
            )
    except OSError as error:
        raise PreNightBoundDailyIntegrityError(
            "snapshot immutable publication failed"
        ) from error

    if _sha256_file(destination) != _sha256_file(source):
        raise PreNightBoundDailyIntegrityError(
            "published snapshot digest mismatch"
        )

    return destination


def _git_value(root: Path, *arguments: str) -> str:
    result = subprocess.run(
        ["git", *arguments],
        cwd=root,
        text=True,
        capture_output=True,
    )

    if result.returncode != 0:
        raise PreNightBoundDailyContractError(
            f"git {' '.join(arguments)} failed"
        )

    return result.stdout.strip()


def run_pre_night_bound_daily(
    race_date,
    data_root,
    *,
    venue_code,
    run_id,
    dry_run=True,
    overwrite=False,
    deadline_evidence=None,
    authorization_state=None,
    test_state=None,
    daily_runner: Callable = run_pre_night_daily,
    deadline_collector: Callable = (
        collect_pre_night_deadline_evidence
    ),
    binding_publisher: Callable = (
        publish_pre_night_program_entries_binding
    ),
    manifest_publisher: Callable = (
        publish_pre_night_manifest_chain
    ),
    branch_provider: Callable | None = None,
    head_provider: Callable | None = None,
) -> dict[str, Any]:
    """Run legacy PRE_NIGHT and wire one venue through Stages 2-4."""

    race_date = _require_race_date(race_date)
    venue_code = _require_venue_code(venue_code)
    root = Path(data_root)

    # Fail closed before the daily runner performs network or file work.
    authorization: dict[str, Any] | None = None
    tests: dict[str, Any] | None = None

    if not dry_run:
        if not isinstance(deadline_evidence, Mapping):
            raise PreNightBoundDailyContractError(
                "live run requires deadline_evidence"
            )

        evidence_venue = deadline_evidence.get("venue_code")

        if evidence_venue is not None:
            evidence_venue = _require_venue_code(
                evidence_venue
            )

            if evidence_venue != venue_code:
                raise PreNightBoundDailyContractError(
                    "deadline evidence venue_code mismatch"
                )

        authorization = _require_mapping(
            authorization_state,
            "authorization_state",
        )
        tests = _require_mapping(
            test_state,
            "test_state",
        )

    daily_result = daily_runner(
        race_date,
        root,
        dry_run=dry_run,
        overwrite=overwrite,
        deadline_evidence=deadline_evidence,
    )

    if not isinstance(daily_result, Mapping):
        raise PreNightBoundDailyIntegrityError(
            "daily runner result must be mapping"
        )

    daily_result = dict(daily_result)

    if dry_run:
        return {
            "status": "DRY_RUN",
            "runtime_wiring_version": RUNTIME_WIRING_VERSION,
            "race_date": race_date,
            "venue_code": venue_code,
            "run_id": run_id,
            "dry_run": True,
            "daily_result": daily_result,
            "planned_stages": [
                "deadline_evidence_collection",
                "program_entries_binding",
                "snapshot_publication",
                "manifest_chain",
            ],
            "stage2_called": False,
            "stage3_called": False,
            "stage4_called": False,
        }

    # At this point these values were validated above.
    if authorization is None or tests is None:
        raise PreNightBoundDailyIntegrityError(
            "live-run state validation was not completed"
        )

    source_archive = _result_artifact_path(
        daily_result,
        fresh_section="collector_result",
        fresh_key="archive",
        cached_artifact="source_archive",
        root=root,
    )

    output_parquet = _result_artifact_path(
        daily_result,
        fresh_section="pipeline_result",
        fresh_key="parquet",
        cached_artifact="output_parquet",
        root=root,
    )

    program_entries = _program_entries_from_parquet(
        output_parquet,
        race_date=race_date,
        venue_code=venue_code,
    )

    program_source_sha256 = _sha256_file(
        source_archive
    )

    stage2 = deadline_collector(
        root,
        race_date=race_date,
        expected_venue_codes=[venue_code],
    )

    if not isinstance(stage2, Mapping):
        raise PreNightBoundDailyIntegrityError(
            "Stage 2 result must be mapping"
        )

    stage2 = dict(stage2)

    collection_sha256 = stage2.get(
        "deadline_evidence_collection_sha256"
    )

    if not isinstance(collection_sha256, str):
        raise PreNightBoundDailyIntegrityError(
            "Stage 2 collection digest is missing"
        )

    stage3 = binding_publisher(
        root,
        run_id=run_id,
        race_date=race_date,
        deadline_evidence_collection_sha256=(
            collection_sha256
        ),
        program_source_sha256_by_venue={
            venue_code: program_source_sha256,
        },
        program_entries=program_entries,
    )

    if not isinstance(stage3, Mapping):
        raise PreNightBoundDailyIntegrityError(
            "Stage 3 result must be mapping"
        )

    stage3 = dict(stage3)

    snapshot = _publish_snapshot_link(
        output_parquet,
        root=root,
        race_date=race_date,
        run_id=run_id,
    )

    execution = daily_result.get(
        "execution_manifest",
        {},
    )

    if not isinstance(execution, Mapping):
        raise PreNightBoundDailyIntegrityError(
            "daily execution manifest must be mapping"
        )

    execution_payload = execution.get(
        "manifest",
        {},
    )

    if not isinstance(execution_payload, Mapping):
        raise PreNightBoundDailyIntegrityError(
            "daily execution manifest payload must be mapping"
        )

    started_at = execution_payload.get("started_at")
    completed_at = execution_payload.get("completed_at")

    if not isinstance(started_at, str):
        raise PreNightBoundDailyIntegrityError(
            "daily execution started_at is missing"
        )

    if not isinstance(completed_at, str):
        raise PreNightBoundDailyIntegrityError(
            "daily execution completed_at is missing"
        )

    repository_root = Path(__file__).resolve().parents[3]

    if branch_provider is not None:
        branch = branch_provider()
    else:
        branch = _git_value(
            repository_root,
            "branch",
            "--show-current",
        )

    if head_provider is not None:
        head = head_provider()
    else:
        head = _git_value(
            repository_root,
            "rev-parse",
            "HEAD",
        )

    if not isinstance(branch, str) or not branch:
        raise PreNightBoundDailyIntegrityError(
            "git branch is missing"
        )

    if not isinstance(head, str) or not head:
        raise PreNightBoundDailyIntegrityError(
            "git HEAD is missing"
        )

    snapshot_relative_path = snapshot.relative_to(
        root
    ).as_posix()

    stage4 = manifest_publisher(
        root,
        race_date=race_date,
        run_id=run_id,
        snapshot_relative_path=snapshot_relative_path,
        pipeline_name=PIPELINE_NAME,
        pipeline_version=PIPELINE_VERSION,
        branch=branch,
        head=head,
        started_at=started_at,
        completed_at=completed_at,
        authorization_state=authorization,
        runtime={
            "python": platform.python_version(),
            "runtime_wiring_version": (
                RUNTIME_WIRING_VERSION
            ),
        },
        test_state=tests,
    )

    if not isinstance(stage4, Mapping):
        raise PreNightBoundDailyIntegrityError(
            "Stage 4 result must be mapping"
        )

    return {
        "status": "SUCCESS",
        "runtime_wiring_version": RUNTIME_WIRING_VERSION,
        "race_date": race_date,
        "venue_code": venue_code,
        "run_id": run_id,
        "dry_run": False,
        "daily_result": daily_result,
        "deadline_collection": stage2,
        "program_binding": stage3,
        "manifest_chain": dict(stage4),
        "program_source_sha256_by_venue": {
            venue_code: program_source_sha256,
        },
        "program_entry_count": len(program_entries),
        "snapshot_path": snapshot,
    }

__all__ = [
    "RUNTIME_WIRING_VERSION",
    "PreNightBoundDailyError",
    "PreNightBoundDailyContractError",
    "PreNightBoundDailyIntegrityError",
    "run_pre_night_bound_daily",
]
