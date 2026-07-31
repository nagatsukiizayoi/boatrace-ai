"""Stage 5 immutable Prospective Dataset v3 certification.

The existing Stage 4 snapshot exact bytes are certified as Dataset v3.
This module does not collect data, create a replacement Parquet file,
predict, join post-race information, stage, commit, or push.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import math
import os
import re
import secrets
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from boatrace_ai.pipelines.pre_night_deadline_collection import (
    COLLECTION_CONTRACT_VERSION,
)
from boatrace_ai.pipelines.pre_night_manifest_chain import (
    EXECUTION_MANIFEST_VERSION,
    PIPELINE_MANIFEST_VERSION,
)
from boatrace_ai.pipelines.pre_night_program_binding import (
    PROGRAM_BINDING_CONTRACT_VERSION,
    canonical_program_entries_binding_bytes,
)


CONTRACT_VERSION = "pre_night_prospective_dataset_v3"
ARTIFACT_TYPE = "PROSPECTIVE_DATASET_V3_CERTIFICATION"
CLASSIFICATION = "PROSPECTIVE_PIT_CERTIFIED"

AUTHORIZED_CONTRACT_ID = (
    "D1B5-STAGE5-PROSPECTIVE-DATASET-V3-V1-R1"
)
AUTHORIZED_CONTRACT_SHA256 = (
    "7c3b2031793e2a044750146a0a4ecab6"
    "277f5abc252e8be698ce42a2ae70766e"
)
AUTHORIZED_BRANCH = (
    "feature/pre-night-authoritative-deadline-pit-contract-v2"
)
AUTHORIZED_HEAD = (
    "f1a5c036c6d1fd3e1b32811bca31acfb4633a263"
)

EXPECTED_PANDAS_VERSION = "2.2.2"
EXPECTED_PYARROW_VERSION = "18.1.0"

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_HEAD_RE = re.compile(r"^[0-9a-f]{40}$")
_RUN_ID_RE = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$"
)
_VENUE_RE = re.compile(r"^\d{2}$")
_RACER_ID_RE = re.compile(r"^\d{4}$")

ARTIFACT_RECORD_KEYS = {
    "relative_path",
    "byte_length",
    "sha256",
    "contract_version",
}

COLLECTION_KEYS = {
    "contract_version",
    "race_date",
    "expected_venue_codes",
    "entry_count",
    "entries",
}

COLLECTION_ENTRY_KEYS = {
    "race_date",
    "venue_code",
    "relative_path",
    "deadline_evidence_sha256",
    "byte_length",
    "contract_version",
}

PIPELINE_KEYS = {
    "manifest_version",
    "manifest_role",
    "pipeline_name",
    "pipeline_version",
    "race_date",
    "run_id",
    "branch",
    "head",
    "started_at",
    "completed_at",
    "authorization_state",
    "stage1_contract_id",
    "stage2_contract_version",
    "stage3_contract_version",
    "deadline_evidence_collection_sha256",
    "program_entries_binding_sha256",
    "input_artifacts",
    "output_artifacts",
}

EXECUTION_KEYS = {
    "manifest_version",
    "manifest_role",
    "run_id",
    "race_date",
    "phase",
    "branch",
    "head",
    "repository_relative_path",
    "authorization_state",
    "runtime",
    "input_digests",
    "output_digests",
    "deadline_evidence_collection_sha256",
    "program_entries_binding_sha256",
    "pipeline_manifest_sha256",
    "test_state",
}

MANIFEST_KEYS = {
    "contract_version",
    "artifact_type",
    "classification",
    "race_date",
    "run_id",
    "branch",
    "head",
    "dataset_schema",
    "parent_artifacts",
    "parent_digests",
}

PARENT_NAMES = (
    "deadline_evidence_collection",
    "program_entries_binding",
    "snapshot",
    "pipeline_manifest",
    "execution_manifest",
)

PARENT_CONTRACT_VERSIONS = {
    "deadline_evidence_collection": (
        "pre_night_deadline_evidence_collection_v1"
    ),
    "program_entries_binding": (
        "pre_night_program_entries_binding_v1"
    ),
    "snapshot": "snapshot_exact_bytes_v1",
    "pipeline_manifest": PIPELINE_MANIFEST_VERSION,
    "execution_manifest": EXECUTION_MANIFEST_VERSION,
}

DATASET_COLUMNS = (
    "race_date",
    "venue_code",
    "race_no",
    "boat_no",
    "racer_id",
    "racer_name",
    "age",
    "branch",
    "weight_kg",
    "class",
    "national_win_rate",
    "national_place2_rate_pct",
    "local_win_rate",
    "local_place2_rate_pct",
    "motor_no",
    "motor_place2_rate_pct",
    "boat_no_equipment",
    "boat_place2_rate_pct",
    "series_results_raw",
    "source_file",
    "as_of_time",
    "snapshot_at",
    "feature_version",
    "feature_contract_version",
    "feature_source_type",
    "feature_source_url",
    "feature_source_sha256",
    "feature_source_fetched_at",
    "feature_source_max_time",
    "source_max_time",
    "feature_collector_version",
    "provenance_status",
    "deadline_evidence_sha256",
)

MODEL_FEATURE_COLUMNS = frozenset(
    {
        "age",
        "boat_place2_rate_pct",
        "branch",
        "class",
        "local_place2_rate_pct",
        "local_win_rate",
        "motor_place2_rate_pct",
        "national_place2_rate_pct",
        "national_win_rate",
        "weight_kg",
    }
)

ARROW_TYPES = {
    "race_date": "string",
    "venue_code": "string",
    "race_no": "int64",
    "boat_no": "int64",
    "racer_id": "string",
    "racer_name": "string",
    "age": "int64",
    "branch": "string",
    "weight_kg": "int64",
    "class": "string",
    "national_win_rate": "double",
    "national_place2_rate_pct": "double",
    "local_win_rate": "double",
    "local_place2_rate_pct": "double",
    "motor_no": "int64",
    "motor_place2_rate_pct": "double",
    "boat_no_equipment": "int64",
    "boat_place2_rate_pct": "double",
    "series_results_raw": "string",
    "source_file": "string",
    "as_of_time": "timestamp[ms, tz=+09:00]",
    "snapshot_at": "timestamp[ms, tz=+09:00]",
    "feature_version": "string",
    "feature_contract_version": "string",
    "feature_source_type": "string",
    "feature_source_url": "string",
    "feature_source_sha256": "string",
    "feature_source_fetched_at": "timestamp[ms, tz=UTC]",
    "feature_source_max_time": "timestamp[ms, tz=UTC]",
    "source_max_time": "timestamp[ms, tz=UTC]",
    "feature_collector_version": "string",
    "provenance_status": "string",
    "deadline_evidence_sha256": "string",
}

FORBIDDEN_KEYS = frozenset(
    {
        "actual",
        "arrival",
        "bet_type",
        "combination",
        "ev",
        "exhibition",
        "finish",
        "finish_position",
        "finish_raw",
        "kimarite",
        "label",
        "labels",
        "odds",
        "payoff",
        "payout",
        "payout_status",
        "payout_yen",
        "payouts",
        "prediction",
        "predictions",
        "probability",
        "probabilities",
        "race_cancelled",
        "race_time_raw",
        "recommendation",
        "recommendations",
        "refund",
        "result",
        "result_available",
        "results",
        "target",
        "winner",
        "winning",
        "raw_html",
        "raw_source_bytes",
        "canonical_bytes",
        "canonical_deadline_evidence_bytes",
        "deadline_evidence",
        "eligibility_cutoff_at",
        "safety_margin_seconds",
        "着順",
        "払戻",
        "結果",
        "決まり手",
    }
)


class ProspectiveV3Error(Exception):
    """Base Stage 5 error."""


class ProspectiveV3ContractError(ProspectiveV3Error):
    """Invalid caller input, schema, identity, or unsafe path."""


class ProspectiveV3CacheError(ProspectiveV3Error):
    """Missing, malformed, noncanonical, or conflicting artifact."""


class ProspectiveV3IntegrityError(ProspectiveV3Error):
    """Digest, byte-length, publication, or durability failure."""


class ProspectiveV3ConflictError(ProspectiveV3Error):
    """Existing immutable destination has different exact bytes."""


def _canonical_json_bytes(value: Any) -> bytes:
    try:
        text = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as error:
        raise ProspectiveV3ContractError(
            "value is not canonical JSON compatible"
        ) from error

    return (text + "\n").encode("utf-8")


def canonical_prospective_v3_manifest_bytes(
    value: Mapping[str, Any],
) -> bytes:
    """Return canonical UTF-8 JSON with exactly one trailing LF."""
    if not isinstance(value, Mapping):
        raise ProspectiveV3ContractError(
            "prospective manifest must be mapping"
        )

    return _canonical_json_bytes(dict(value))


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _require_sha256(value: Any, field: str) -> str:
    if (
        not isinstance(value, str)
        or _SHA256_RE.fullmatch(value) is None
    ):
        raise ProspectiveV3ContractError(
            f"{field} must be lowercase SHA-256"
        )

    return value


def _require_race_date(value: Any) -> str:
    if (
        not isinstance(value, str)
        or value != value.strip()
    ):
        raise ProspectiveV3ContractError(
            "race_date must be canonical string"
        )

    try:
        normalized = dt.date.fromisoformat(
            value
        ).isoformat()
    except ValueError as error:
        raise ProspectiveV3ContractError(
            "race_date is invalid"
        ) from error

    if value != normalized:
        raise ProspectiveV3ContractError(
            "race_date must use strict YYYY-MM-DD"
        )

    return value


def _require_run_id(value: Any) -> str:
    if (
        not isinstance(value, str)
        or value in {".", ".."}
        or _RUN_ID_RE.fullmatch(value) is None
    ):
        raise ProspectiveV3ContractError(
            "run_id is invalid"
        )

    return value


def _require_branch(value: Any) -> str:
    if value != AUTHORIZED_BRANCH:
        raise ProspectiveV3ContractError(
            "branch does not match authorized branch"
        )

    return value


def _require_head(value: Any) -> str:
    if (
        not isinstance(value, str)
        or _HEAD_RE.fullmatch(value) is None
    ):
        raise ProspectiveV3ContractError(
            "head must be lowercase 40-character Git SHA"
        )

    if value != AUTHORIZED_HEAD:
        raise ProspectiveV3ContractError(
            "head does not match authorized HEAD"
        )

    return value


def _dataset_schema_value() -> dict[str, Any]:
    column_order = list(DATASET_COLUMNS)

    physical = [
        {
            "name": name,
            "arrow_type": ARROW_TYPES[name],
            "physical_nullable": True,
            "logical_null_allowed": False,
        }
        for name in DATASET_COLUMNS
    ]

    return {
        "schema_name": "pre_night_prospective_dataset_v3",
        "schema_version": "3.0.0",
        "physical_format": "PARQUET",
        "column_count": 33,
        "column_order_sha256": _sha256_bytes(
            _canonical_json_bytes(column_order)
        ),
        "physical_schema_sha256": _sha256_bytes(
            _canonical_json_bytes(physical)
        ),
        "logical_null_policy": "NO_NULLS_IN_ANY_COLUMN",
        "series_results_raw_policy": (
            "RETAINED_PRE_RACE_SOURCE_METADATA_NOT_MODEL_FEATURE"
        ),
    }


DATASET_SCHEMA_VALUE = _dataset_schema_value()


def _paths(
    data_root: Any,
    race_date: str,
    run_id: str,
) -> dict[str, Path]:
    raw_root = Path(data_root)

    if raw_root.exists() and raw_root.is_symlink():
        raise ProspectiveV3ContractError(
            "data_root must not be symlink"
        )

    root = raw_root.resolve(strict=False)
    date_value = dt.date.fromisoformat(race_date)

    run_directory = (
        root
        / "prospective"
        / "pre_night"
        / "runs"
        / f"{date_value.year:04d}"
        / f"{date_value.month:02d}"
        / f"{date_value.day:02d}"
        / run_id
    )

    collection = (
        root
        / "prospective"
        / "pre_night"
        / "deadline_evidence_collections"
        / f"{date_value.year:04d}"
        / f"{date_value.month:02d}"
        / f"{date_value.day:02d}"
        / "deadline_evidence_collection.json"
    )

    return {
        "root": root,
        "directory": run_directory,
        "collection": collection,
        "binding": (
            run_directory
            / "program_entries_binding.json"
        ),
        "pipeline": (
            run_directory / "pipeline_manifest.json"
        ),
        "execution": (
            run_directory / "execution_manifest.json"
        ),
        "destination": (
            run_directory
            / "prospective_dataset_v3_manifest.json"
        ),
        "lock": (
            run_directory
            / ".prospective_dataset_v3.lock"
        ),
    }


def _assert_safe(
    root: Path,
    target: Path,
    label: str,
) -> None:
    try:
        relative = target.relative_to(root)
    except ValueError as error:
        raise ProspectiveV3ContractError(
            f"{label} path escapes data_root"
        ) from error

    resolved = target.resolve(strict=False)

    try:
        resolved.relative_to(root)
    except ValueError as error:
        raise ProspectiveV3ContractError(
            f"{label} resolved path escapes data_root"
        ) from error

    current = root

    for component in relative.parts:
        if component in {".", ".."}:
            raise ProspectiveV3ContractError(
                f"{label} contains unsafe component"
            )

        current = current / component

        if current.exists() and current.is_symlink():
            raise ProspectiveV3ContractError(
                f"{label} path contains symlink"
            )


def _safe_relative_path(
    root: Path,
    value: Any,
    label: str,
) -> Path:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or "\\" in value
    ):
        raise ProspectiveV3ContractError(
            f"{label} must be safe POSIX relative path"
        )

    relative = Path(value)

    if (
        relative.is_absolute()
        or relative.as_posix() != value
        or "." in relative.parts
        or ".." in relative.parts
    ):
        raise ProspectiveV3ContractError(
            f"{label} must be safe POSIX relative path"
        )

    target = root / relative
    _assert_safe(root, target, label)
    return target


# BEGIN STAGE5 SOURCE PART 2


def _exact_keys(
    value: Any,
    expected: set[str],
    label: str,
) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ProspectiveV3CacheError(
            f"{label} must be JSON object"
        )

    if set(value) != expected:
        raise ProspectiveV3CacheError(
            f"{label} fields mismatch"
        )

    return value


def _load_json(
    path: Path,
    *,
    root: Path,
    label: str,
    canonicalizer=None,
) -> tuple[dict[str, Any], bytes]:
    _assert_safe(root, path, label)

    if (
        not path.exists()
        or path.is_symlink()
        or not path.is_file()
    ):
        raise ProspectiveV3CacheError(
            f"{label} must be regular file"
        )

    stored = path.read_bytes()

    try:
        payload = json.loads(
            stored.decode("utf-8")
        )
    except (
        UnicodeDecodeError,
        json.JSONDecodeError,
    ) as error:
        raise ProspectiveV3CacheError(
            f"{label} is not valid UTF-8 JSON"
        ) from error

    if not isinstance(payload, dict):
        raise ProspectiveV3CacheError(
            f"{label} must be JSON object"
        )

    try:
        canonical = (
            _canonical_json_bytes(payload)
            if canonicalizer is None
            else canonicalizer(payload)
        )
    except Exception as error:
        raise ProspectiveV3CacheError(
            f"{label} contract validation failed"
        ) from error

    if canonical != stored:
        raise ProspectiveV3CacheError(
            f"{label} is non-canonical"
        )

    return payload, stored


def _record_file(
    path: Path,
    root: Path,
    contract_version: str,
) -> dict[str, Any]:
    stored = path.read_bytes()

    return {
        "relative_path": (
            path.relative_to(root).as_posix()
        ),
        "byte_length": len(stored),
        "sha256": _sha256_bytes(stored),
        "contract_version": contract_version,
    }


def _validate_artifact_record(
    record: Any,
    *,
    root: Path,
    expected_path: Path,
    expected_contract_version: str,
    label: str,
) -> dict[str, Any]:
    normalized = _exact_keys(
        record,
        ARTIFACT_RECORD_KEYS,
        label,
    )

    path = _safe_relative_path(
        root,
        normalized["relative_path"],
        f"{label}.relative_path",
    )

    if path != expected_path:
        raise ProspectiveV3CacheError(
            f"{label} relative path mismatch"
        )

    if (
        not path.exists()
        or path.is_symlink()
        or not path.is_file()
    ):
        raise ProspectiveV3CacheError(
            f"{label} artifact is missing"
        )

    byte_length = normalized["byte_length"]

    if (
        isinstance(byte_length, bool)
        or not isinstance(byte_length, int)
        or byte_length <= 0
    ):
        raise ProspectiveV3CacheError(
            f"{label} byte_length is invalid"
        )

    if (
        normalized["contract_version"]
        != expected_contract_version
    ):
        raise ProspectiveV3CacheError(
            f"{label} contract_version mismatch"
        )

    try:
        recorded_sha256 = _require_sha256(
            normalized["sha256"],
            f"{label}.sha256",
        )
    except ProspectiveV3ContractError as error:
        raise ProspectiveV3CacheError(
            f"{label} SHA-256 is invalid"
        ) from error

    stored = path.read_bytes()
    actual_sha256 = _sha256_bytes(stored)

    if len(stored) != byte_length:
        raise ProspectiveV3IntegrityError(
            f"{label} byte-length mismatch"
        )

    if actual_sha256 != recorded_sha256:
        raise ProspectiveV3IntegrityError(
            f"{label} SHA-256 mismatch"
        )

    return {
        "relative_path": (
            path.relative_to(root).as_posix()
        ),
        "byte_length": len(stored),
        "sha256": actual_sha256,
        "contract_version": (
            expected_contract_version
        ),
    }


def _scan_forbidden(
    value: Any,
    location: str = "$",
) -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            normalized = str(key).strip().casefold()

            if normalized in FORBIDDEN_KEYS:
                raise ProspectiveV3ContractError(
                    f"forbidden key: {location}.{key}"
                )

            _scan_forbidden(
                child,
                f"{location}.{key}",
            )

    elif isinstance(value, list):
        for index, child in enumerate(value):
            _scan_forbidden(
                child,
                f"{location}[{index}]",
            )


def _validate_collection(
    paths: Mapping[str, Path],
    race_date: str,
) -> tuple[dict[str, Any], bytes]:
    payload, stored = _load_json(
        paths["collection"],
        root=paths["root"],
        label="Stage 2 collection",
    )

    _exact_keys(
        payload,
        COLLECTION_KEYS,
        "Stage 2 collection",
    )

    if (
        payload["contract_version"]
        != COLLECTION_CONTRACT_VERSION
    ):
        raise ProspectiveV3CacheError(
            "Stage 2 contract version mismatch"
        )

    if payload["race_date"] != race_date:
        raise ProspectiveV3CacheError(
            "Stage 2 race_date mismatch"
        )

    venues = payload["expected_venue_codes"]
    entries = payload["entries"]

    if (
        not isinstance(venues, list)
        or not venues
    ):
        raise ProspectiveV3CacheError(
            "Stage 2 venue list is invalid"
        )

    try:
        sorted_venues = sorted(
            set(venues),
            key=int,
        )
    except (TypeError, ValueError) as error:
        raise ProspectiveV3CacheError(
            "Stage 2 venue list is invalid"
        ) from error

    if venues != sorted_venues:
        raise ProspectiveV3CacheError(
            "Stage 2 venue order is invalid"
        )

    if (
        not isinstance(entries, list)
        or isinstance(
            payload["entry_count"],
            bool,
        )
        or payload["entry_count"] != len(entries)
        or len(entries) != len(venues)
    ):
        raise ProspectiveV3CacheError(
            "Stage 2 entry count mismatch"
        )

    for index, entry in enumerate(entries):
        entry = _exact_keys(
            entry,
            COLLECTION_ENTRY_KEYS,
            f"Stage 2 entry {index}",
        )

        venue = entry["venue_code"]

        if (
            not isinstance(venue, str)
            or _VENUE_RE.fullmatch(venue) is None
            or not 1 <= int(venue) <= 24
            or venue != venues[index]
            or entry["race_date"] != race_date
        ):
            raise ProspectiveV3CacheError(
                "Stage 2 entry identity mismatch"
            )

        try:
            _require_sha256(
                entry["deadline_evidence_sha256"],
                "deadline_evidence_sha256",
            )
        except ProspectiveV3ContractError as error:
            raise ProspectiveV3CacheError(
                "Stage 2 entry digest is invalid"
            ) from error

    return payload, stored


def _validate_dataset(
    path: Path,
    *,
    race_date: str,
) -> dict[str, Any]:
    if pd.__version__ != EXPECTED_PANDAS_VERSION:
        raise ProspectiveV3ContractError(
            "pandas version does not match "
            "authorized writer binding"
        )

    if pa.__version__ != EXPECTED_PYARROW_VERSION:
        raise ProspectiveV3ContractError(
            "pyarrow version does not match "
            "authorized writer binding"
        )

    try:
        schema = pq.read_schema(path)
    except Exception as error:
        raise ProspectiveV3CacheError(
            "snapshot is not readable Parquet"
        ) from error

    if schema.names != list(DATASET_COLUMNS):
        raise ProspectiveV3ContractError(
            "Dataset v3 column order mismatch"
        )

    if len(schema) != 33:
        raise ProspectiveV3ContractError(
            "Dataset v3 must contain exactly 33 columns"
        )

    for index, field in enumerate(schema):
        expected_name = DATASET_COLUMNS[index]
        expected_type = ARROW_TYPES[
            expected_name
        ]

        if field.name != expected_name:
            raise ProspectiveV3ContractError(
                "Dataset v3 field name mismatch"
            )

        if str(field.type) != expected_type:
            raise ProspectiveV3ContractError(
                "Dataset v3 Arrow type mismatch: "
                f"{field.name}"
            )

        if field.nullable is not True:
            raise ProspectiveV3ContractError(
                "Dataset v3 physical nullable mismatch: "
                f"{field.name}"
            )

    try:
        frame = pd.read_parquet(
            path,
            engine="pyarrow",
        )
    except Exception as error:
        raise ProspectiveV3CacheError(
            "snapshot Parquet read failed"
        ) from error

    if list(frame.columns) != list(DATASET_COLUMNS):
        raise ProspectiveV3ContractError(
            "Dataset v3 readback column order mismatch"
        )

    if frame.empty:
        raise ProspectiveV3ContractError(
            "Dataset v3 must not be empty"
        )

    null_count = sum(
        int(frame[column].isna().sum())
        for column in DATASET_COLUMNS
    )

    if null_count != 0:
        raise ProspectiveV3ContractError(
            "Dataset v3 contains logical null values"
        )

    if "series_results_raw" in MODEL_FEATURE_COLUMNS:
        raise ProspectiveV3ContractError(
            "series_results_raw must not be model feature"
        )

    if (
        set(frame["race_date"].astype(str))
        != {race_date}
    ):
        raise ProspectiveV3ContractError(
            "Dataset v3 race_date mismatch"
        )

    venues = frame["venue_code"].astype(str)

    if any(
        _VENUE_RE.fullmatch(value) is None
        or not 1 <= int(value) <= 24
        for value in venues
    ):
        raise ProspectiveV3ContractError(
            "Dataset v3 venue_code is invalid"
        )

    racer_ids = frame["racer_id"].astype(str)

    if any(
        _RACER_ID_RE.fullmatch(value) is None
        for value in racer_ids
    ):
        raise ProspectiveV3ContractError(
            "Dataset v3 racer_id is invalid"
        )

    integer_ranges = {
        "race_no": (1, 12),
        "boat_no": (1, 6),
        "age": (15, 100),
        "weight_kg": (30, 100),
        "motor_no": (1, 999),
        "boat_no_equipment": (1, 999),
    }

    for column, limits in integer_ranges.items():
        minimum, maximum = limits

        if not frame[column].between(
            minimum,
            maximum,
            inclusive="both",
        ).all():
            raise ProspectiveV3ContractError(
                "Dataset v3 integer range violation: "
                f"{column}"
            )

    float_ranges = {
        "national_win_rate": (0.0, 10.0),
        "national_place2_rate_pct": (
            0.0,
            100.0,
        ),
        "local_win_rate": (0.0, 10.0),
        "local_place2_rate_pct": (
            0.0,
            100.0,
        ),
        "motor_place2_rate_pct": (
            0.0,
            100.0,
        ),
        "boat_place2_rate_pct": (
            0.0,
            100.0,
        ),
    }

    for column, limits in float_ranges.items():
        minimum, maximum = limits
        values = frame[column].astype(float)

        if not all(
            math.isfinite(value)
            for value in values
        ):
            raise ProspectiveV3ContractError(
                "Dataset v3 non-finite value: "
                f"{column}"
            )

        if not values.between(
            minimum,
            maximum,
            inclusive="both",
        ).all():
            raise ProspectiveV3ContractError(
                "Dataset v3 float range violation: "
                f"{column}"
            )

    for column in (
        "racer_name",
        "branch",
        "class",
        "source_file",
        "feature_source_url",
        "feature_collector_version",
    ):
        if any(
            not str(value).strip()
            for value in frame[column]
        ):
            raise ProspectiveV3ContractError(
                f"Dataset v3 empty text: {column}"
            )

    fixed_values = {
        "feature_version": (
            "pre_night_program_snapshot_v1"
        ),
        "feature_contract_version": (
            "pre_night_program_parquet_v1"
        ),
        "feature_source_type": "program",
        "provenance_status": "ELIGIBLE",
    }

    for column, expected in fixed_values.items():
        if (
            set(frame[column].astype(str))
            != {expected}
        ):
            raise ProspectiveV3ContractError(
                "Dataset v3 fixed value mismatch: "
                f"{column}"
            )

    for column in (
        "feature_source_sha256",
        "deadline_evidence_sha256",
    ):
        values = set(
            frame[column].astype(str)
        )

        if (
            len(values) != 1
            or any(
                _SHA256_RE.fullmatch(value)
                is None
                for value in values
            )
        ):
            raise ProspectiveV3ContractError(
                "Dataset v3 digest column invalid: "
                f"{column}"
            )

    key_columns = [
        "race_date",
        "venue_code",
        "race_no",
        "boat_no",
    ]

    if frame.duplicated(key_columns).any():
        raise ProspectiveV3ContractError(
            "Dataset v3 contains duplicate primary keys"
        )

    identities = [
        (
            str(row.race_date),
            str(row.venue_code),
            int(row.race_no),
            int(row.boat_no),
        )
        for row in frame[
            key_columns
        ].itertuples(index=False)
    ]

    expected_order = sorted(
        identities,
        key=lambda item: (
            item[0],
            int(item[1]),
            item[2],
            item[3],
        ),
    )

    if identities != expected_order:
        raise ProspectiveV3ContractError(
            "Dataset v3 row order mismatch"
        )

    grouped = frame.groupby(
        [
            "race_date",
            "venue_code",
            "race_no",
        ],
        dropna=False,
        sort=False,
    )

    for race_key, race_frame in grouped:
        if len(race_frame) != 6:
            raise ProspectiveV3ContractError(
                "Dataset v3 race row count mismatch: "
                f"{race_key}"
            )

        if (
            set(map(int, race_frame["boat_no"]))
            != set(range(1, 7))
        ):
            raise ProspectiveV3ContractError(
                "Dataset v3 boat set mismatch: "
                f"{race_key}"
            )

        if (
            race_frame["source_file"]
            .nunique(dropna=False)
            != 1
        ):
            raise ProspectiveV3ContractError(
                "Dataset v3 source_file mismatch: "
                f"{race_key}"
            )

    timestamp_columns = {
        "as_of_time": pd.to_datetime(
            frame["as_of_time"],
            utc=True,
            errors="coerce",
        ),
        "snapshot_at": pd.to_datetime(
            frame["snapshot_at"],
            utc=True,
            errors="coerce",
        ),
        "fetched": pd.to_datetime(
            frame["feature_source_fetched_at"],
            utc=True,
            errors="coerce",
        ),
        "source_max": pd.to_datetime(
            frame["feature_source_max_time"],
            utc=True,
            errors="coerce",
        ),
        "source_alias": pd.to_datetime(
            frame["source_max_time"],
            utc=True,
            errors="coerce",
        ),
    }

    if any(
        values.isna().any()
        for values in timestamp_columns.values()
    ):
        raise ProspectiveV3ContractError(
            "Dataset v3 contains invalid timestamp"
        )

    as_of = timestamp_columns["as_of_time"]
    snapshot_at = timestamp_columns["snapshot_at"]
    fetched = timestamp_columns["fetched"]
    source_max = timestamp_columns["source_max"]
    source_alias = timestamp_columns[
        "source_alias"
    ]

    if not as_of.equals(snapshot_at):
        raise ProspectiveV3ContractError(
            "Dataset v3 as_of_time and snapshot_at differ"
        )

    if (
        (fetched > as_of).any()
        or (source_max > as_of).any()
    ):
        raise ProspectiveV3ContractError(
            "Dataset v3 contains post-deadline source time"
        )

    if not source_max.equals(source_alias):
        raise ProspectiveV3ContractError(
            "Dataset v3 source_max_time alias mismatch"
        )

    race_day = dt.date.fromisoformat(race_date)
    jst = dt.timezone(
        dt.timedelta(hours=9)
    )

    expected_as_of = pd.Timestamp(
        dt.datetime.combine(
            race_day - dt.timedelta(days=1),
            dt.time(21, 30),
            tzinfo=jst,
        )
    ).tz_convert("UTC")

    if not (as_of == expected_as_of).all():
        raise ProspectiveV3ContractError(
            "Dataset v3 as_of_time is not "
            "previous-day 21:30 JST"
        )

    return {
        "row_count": int(len(frame)),
        "race_count": int(
            frame[
                [
                    "race_date",
                    "venue_code",
                    "race_no",
                ]
            ]
            .drop_duplicates()
            .shape[0]
        ),
        "column_count": 33,
        "null_count": 0,
    }


def _prepare(
    data_root: Any,
    *,
    race_date: Any,
    run_id: Any,
    branch: Any,
    head: Any,
) -> dict[str, Any]:
    race_date = _require_race_date(race_date)
    run_id = _require_run_id(run_id)
    branch = _require_branch(branch)
    head = _require_head(head)

    paths = _paths(
        data_root,
        race_date,
        run_id,
    )
    root = paths["root"]

    for label in (
        "directory",
        "collection",
        "binding",
        "pipeline",
        "execution",
        "destination",
        "lock",
    ):
        _assert_safe(
            root,
            paths[label],
            label,
        )

    _, collection_bytes = _validate_collection(
        paths,
        race_date,
    )
    collection_sha256 = _sha256_bytes(
        collection_bytes
    )

    binding, binding_bytes = _load_json(
        paths["binding"],
        root=root,
        label="Stage 3 binding",
        canonicalizer=(
            canonical_program_entries_binding_bytes
        ),
    )
    binding_sha256 = _sha256_bytes(
        binding_bytes
    )

    if (
        binding.get("contract_version")
        != PROGRAM_BINDING_CONTRACT_VERSION
    ):
        raise ProspectiveV3CacheError(
            "Stage 3 contract version mismatch"
        )

    if binding.get("race_date") != race_date:
        raise ProspectiveV3CacheError(
            "Stage 3 race_date mismatch"
        )

    if (
        binding.get(
            "deadline_evidence_collection_sha256"
        )
        != collection_sha256
    ):
        raise ProspectiveV3IntegrityError(
            "Stage 3 collection digest mismatch"
        )

    pipeline, pipeline_bytes = _load_json(
        paths["pipeline"],
        root=root,
        label="Pipeline Manifest",
    )
    execution, execution_bytes = _load_json(
        paths["execution"],
        root=root,
        label="Execution Manifest",
    )

    _exact_keys(
        pipeline,
        PIPELINE_KEYS,
        "Pipeline Manifest",
    )
    _exact_keys(
        execution,
        EXECUTION_KEYS,
        "Execution Manifest",
    )

    if (
        pipeline["manifest_version"]
        != PIPELINE_MANIFEST_VERSION
        or pipeline["manifest_role"]
        != "PIPELINE_MANIFEST"
    ):
        raise ProspectiveV3CacheError(
            "Pipeline Manifest identity mismatch"
        )

    if (
        execution["manifest_version"]
        != EXECUTION_MANIFEST_VERSION
        or execution["manifest_role"]
        != "EXECUTION_MANIFEST"
        or execution["phase"]
        != "PRE_NIGHT_MANIFEST_CHAIN"
    ):
        raise ProspectiveV3CacheError(
            "Execution Manifest identity mismatch"
        )

    for field, expected in (
        ("race_date", race_date),
        ("run_id", run_id),
        ("branch", branch),
        ("head", head),
    ):
        if (
            pipeline.get(field) != expected
            or execution.get(field) != expected
        ):
            raise ProspectiveV3IntegrityError(
                "Pipeline/Execution identity mismatch: "
                f"{field}"
            )

    expected_authorization = {
        "approved": True,
    }

    if (
        pipeline.get("authorization_state")
        != expected_authorization
        or execution.get("authorization_state")
        != expected_authorization
    ):
        raise ProspectiveV3ContractError(
            "authorization_state must equal "
            "{'approved': True}"
        )

    if (
        pipeline["stage2_contract_version"]
        != COLLECTION_CONTRACT_VERSION
        or pipeline["stage3_contract_version"]
        != PROGRAM_BINDING_CONTRACT_VERSION
    ):
        raise ProspectiveV3CacheError(
            "Pipeline parent contract version mismatch"
        )

    pipeline_sha256 = _sha256_bytes(
        pipeline_bytes
    )
    execution_sha256 = _sha256_bytes(
        execution_bytes
    )

    if (
        execution["pipeline_manifest_sha256"]
        != pipeline_sha256
    ):
        raise ProspectiveV3IntegrityError(
            "Execution pipeline manifest digest mismatch"
        )

    collection_digests = {
        collection_sha256,
        pipeline[
            "deadline_evidence_collection_sha256"
        ],
        execution[
            "deadline_evidence_collection_sha256"
        ],
        execution[
            "input_digests"
        ].get(
            "deadline_evidence_collection"
        ),
    }

    if collection_digests != {
        collection_sha256
    }:
        raise ProspectiveV3IntegrityError(
            "deadline collection digest chain mismatch"
        )

    binding_digests = {
        binding_sha256,
        pipeline[
            "program_entries_binding_sha256"
        ],
        execution[
            "program_entries_binding_sha256"
        ],
        execution[
            "input_digests"
        ].get(
            "program_entries_binding"
        ),
    }

    if binding_digests != {
        binding_sha256
    }:
        raise ProspectiveV3IntegrityError(
            "program binding digest chain mismatch"
        )

    input_artifacts = _exact_keys(
        pipeline["input_artifacts"],
        {
            "deadline_evidence_collection",
            "program_entries_binding",
        },
        "Pipeline input_artifacts",
    )

    output_artifacts = _exact_keys(
        pipeline["output_artifacts"],
        {"snapshot"},
        "Pipeline output_artifacts",
    )

    collection_record = (
        _validate_artifact_record(
            input_artifacts[
                "deadline_evidence_collection"
            ],
            root=root,
            expected_path=paths["collection"],
            expected_contract_version=(
                COLLECTION_CONTRACT_VERSION
            ),
            label=(
                "deadline_evidence_collection"
            ),
        )
    )

    binding_record = _validate_artifact_record(
        input_artifacts[
            "program_entries_binding"
        ],
        root=root,
        expected_path=paths["binding"],
        expected_contract_version=(
            PROGRAM_BINDING_CONTRACT_VERSION
        ),
        label="program_entries_binding",
    )

    snapshot_raw = _exact_keys(
        output_artifacts["snapshot"],
        ARTIFACT_RECORD_KEYS,
        "snapshot",
    )

    dataset_path = _safe_relative_path(
        root,
        snapshot_raw["relative_path"],
        "snapshot.relative_path",
    )

    snapshot_record = _validate_artifact_record(
        snapshot_raw,
        root=root,
        expected_path=dataset_path,
        expected_contract_version=(
            "snapshot_exact_bytes_v1"
        ),
        label="snapshot",
    )

    if (
        execution["output_digests"].get(
            "snapshot"
        )
        != snapshot_record["sha256"]
    ):
        raise ProspectiveV3IntegrityError(
            "Execution snapshot digest mismatch"
        )

    if (
        execution["output_digests"].get(
            "pipeline_manifest"
        )
        != pipeline_sha256
    ):
        raise ProspectiveV3IntegrityError(
            "Execution output pipeline digest mismatch"
        )

    dataset_validation = _validate_dataset(
        dataset_path,
        race_date=race_date,
    )

    parent_artifacts = {
        "deadline_evidence_collection": (
            collection_record
        ),
        "program_entries_binding": (
            binding_record
        ),
        "snapshot": snapshot_record,
        "pipeline_manifest": _record_file(
            paths["pipeline"],
            root,
            PIPELINE_MANIFEST_VERSION,
        ),
        "execution_manifest": _record_file(
            paths["execution"],
            root,
            EXECUTION_MANIFEST_VERSION,
        ),
    }

    parent_digests = {
        name: parent_artifacts[name]["sha256"]
        for name in PARENT_NAMES
    }

    manifest = {
        "contract_version": CONTRACT_VERSION,
        "artifact_type": ARTIFACT_TYPE,
        "classification": CLASSIFICATION,
        "race_date": race_date,
        "run_id": run_id,
        "branch": branch,
        "head": head,
        "dataset_schema": DATASET_SCHEMA_VALUE,
        "parent_artifacts": parent_artifacts,
        "parent_digests": parent_digests,
    }

    _scan_forbidden(manifest)

    canonical = (
        canonical_prospective_v3_manifest_bytes(
            manifest
        )
    )

    return {
        "manifest": manifest,
        "canonical": canonical,
        "paths": paths,
        "dataset_path": dataset_path,
        "dataset_record": snapshot_record,
        "dataset_validation": (
            dataset_validation
        ),
        "collection_sha256": (
            collection_sha256
        ),
        "binding_sha256": binding_sha256,
        "pipeline_sha256": pipeline_sha256,
        "execution_sha256": execution_sha256,
    }


def _validate_destination(
    prepared: Mapping[str, Any],
) -> None:
    path = prepared["paths"]["destination"]
    root = prepared["paths"]["root"]

    payload, stored = _load_json(
        path,
        root=root,
        label=(
            "Prospective Dataset v3 Manifest"
        ),
    )

    if set(payload) != MANIFEST_KEYS:
        raise ProspectiveV3CacheError(
            "Prospective Dataset v3 Manifest "
            "fields mismatch"
        )

    if payload != prepared["manifest"]:
        raise ProspectiveV3ConflictError(
            "Prospective Dataset v3 Manifest "
            "payload conflict"
        )

    if stored != prepared["canonical"]:
        raise ProspectiveV3ConflictError(
            "Prospective Dataset v3 Manifest "
            "byte conflict"
        )


def _receipt(
    prepared: Mapping[str, Any],
    *,
    cached: bool,
    publication_status: str,
) -> dict[str, Any]:
    paths = prepared["paths"]
    manifest_bytes = (
        paths["destination"].read_bytes()
    )
    dataset = prepared["dataset_record"]

    return {
        "race_date": (
            prepared["manifest"]["race_date"]
        ),
        "run_id": (
            prepared["manifest"]["run_id"]
        ),
        "classification": CLASSIFICATION,
        "dataset_relative_path": (
            dataset["relative_path"]
        ),
        "dataset_sha256": dataset["sha256"],
        "dataset_byte_length": (
            dataset["byte_length"]
        ),
        "prospective_manifest_relative_path": (
            paths["destination"]
            .relative_to(paths["root"])
            .as_posix()
        ),
        "prospective_manifest_sha256": (
            _sha256_bytes(manifest_bytes)
        ),
        "prospective_manifest_byte_length": (
            len(manifest_bytes)
        ),
        "cached": cached,
        "publication_status": (
            publication_status
        ),
        "paths": {
            "directory": paths["directory"],
            "dataset": prepared["dataset_path"],
            "prospective_manifest": (
                paths["destination"]
            ),
        },
    }


def _fsync_file(path: Path) -> None:
    try:
        with path.open("rb") as handle:
            os.fsync(handle.fileno())
    except OSError as error:
        raise ProspectiveV3IntegrityError(
            "destination file fsync failed"
        ) from error


def _fsync_directory(path: Path) -> None:
    descriptor = None

    try:
        descriptor = os.open(
            path,
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0),
        )
        os.fsync(descriptor)
    except OSError as error:
        raise ProspectiveV3IntegrityError(
            "destination directory fsync failed"
        ) from error
    finally:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError as error:
                raise ProspectiveV3IntegrityError(
                    "directory descriptor close failed"
                ) from error


def validate_prospective_v3(
    data_root,
    *,
    race_date,
    run_id,
    branch,
    head,
) -> dict[str, Any]:
    """Validate an existing certification without side effects."""
    prepared = _prepare(
        data_root,
        race_date=race_date,
        run_id=run_id,
        branch=branch,
        head=head,
    )

    _validate_destination(prepared)

    return _receipt(
        prepared,
        cached=True,
        publication_status="VALIDATED_REUSE",
    )


def publish_prospective_v3(
    data_root,
    *,
    race_date,
    run_id,
    branch,
    head,
) -> dict[str, Any]:
    """Publish or validate one immutable Dataset v3 certification."""
    prepared = _prepare(
        data_root,
        race_date=race_date,
        run_id=run_id,
        branch=branch,
        head=head,
    )

    paths = prepared["paths"]
    destination = paths["destination"]

    if destination.exists():
        _validate_destination(prepared)

        return _receipt(
            prepared,
            cached=True,
            publication_status=(
                "VALIDATED_REUSE"
            ),
        )

    if not paths["directory"].is_dir():
        raise ProspectiveV3CacheError(
            "Stage 4 run directory does not exist"
        )

    temporary = paths["directory"] / (
        ".prospective_dataset_v3_manifest.json."
        + secrets.token_hex(16)
        + ".tmp"
    )

    _assert_safe(
        paths["root"],
        temporary,
        "temporary",
    )

    lock_fd = None
    lock_acquired = False
    lock_identity = None
    temporary_created = False
    destination_created = False

    try:
        try:
            lock_fd = os.open(
                paths["lock"],
                (
                    os.O_CREAT
                    | os.O_EXCL
                    | os.O_WRONLY
                ),
            )
            lock_acquired = True
            lock_stat = os.fstat(lock_fd)
            lock_identity = (
                lock_stat.st_dev,
                lock_stat.st_ino,
            )
        except FileExistsError as error:
            raise ProspectiveV3Error(
                "prospective v3 publication "
                "lock exists"
            ) from error
        except OSError as error:
            raise ProspectiveV3Error(
                "prospective v3 lock "
                "acquisition failed"
            ) from error

        try:
            os.close(lock_fd)
        except OSError as error:
            raise ProspectiveV3IntegrityError(
                "lock descriptor close failed"
            ) from error
        else:
            lock_fd = None

        if destination.exists():
            _validate_destination(prepared)

            return _receipt(
                prepared,
                cached=True,
                publication_status=(
                    "VALIDATED_REUSE"
                ),
            )

        try:
            with temporary.open("xb") as handle:
                temporary_created = True
                handle.write(
                    prepared["canonical"]
                )
                handle.flush()
                os.fsync(handle.fileno())
        except Exception as error:
            raise ProspectiveV3IntegrityError(
                "temporary write failed"
            ) from error

        temporary_bytes = (
            temporary.read_bytes()
        )

        if temporary_bytes != prepared["canonical"]:
            raise ProspectiveV3IntegrityError(
                "temporary byte mismatch"
            )

        if (
            _sha256_bytes(temporary_bytes)
            != _sha256_bytes(
                prepared["canonical"]
            )
        ):
            raise ProspectiveV3IntegrityError(
                "temporary digest mismatch"
            )

        try:
            os.link(
                temporary,
                destination,
            )
            destination_created = True
        except FileExistsError:
            _validate_destination(prepared)

            return _receipt(
                prepared,
                cached=True,
                publication_status=(
                    "VALIDATED_REUSE"
                ),
            )
        except OSError as error:
            raise ProspectiveV3IntegrityError(
                "atomic no-overwrite "
                "publication failed"
            ) from error

        _fsync_file(destination)
        _fsync_directory(
            destination.parent
        )
        _validate_destination(prepared)

        return _receipt(
            prepared,
            cached=False,
            publication_status="CREATED",
        )

    except Exception:
        if destination_created:
            try:
                destination.unlink(
                    missing_ok=True
                )
            except OSError as cleanup_error:
                raise ProspectiveV3IntegrityError(
                    "failed publication cleanup failed"
                ) from cleanup_error

        raise

    finally:
        cleanup_error = None

        if temporary_created:
            try:
                temporary.unlink(
                    missing_ok=True
                )
            except OSError as error:
                cleanup_error = error

        if lock_fd is not None:
            try:
                os.close(lock_fd)
            except OSError as error:
                cleanup_error = (
                    cleanup_error or error
                )

        if lock_acquired:
            try:
                current = paths["lock"].lstat()
            except FileNotFoundError:
                current = None
            except OSError as error:
                current = None
                cleanup_error = (
                    cleanup_error or error
                )

            if current is not None:
                identity = (
                    current.st_dev,
                    current.st_ino,
                )

                if identity != lock_identity:
                    cleanup_error = (
                        cleanup_error
                        or RuntimeError(
                            "lock ownership "
                            "identity changed"
                        )
                    )
                else:
                    try:
                        paths["lock"].unlink()
                    except OSError as error:
                        cleanup_error = (
                            cleanup_error
                            or error
                        )

        if cleanup_error is not None:
            raise ProspectiveV3IntegrityError(
                "owned-resource cleanup failed"
            ) from cleanup_error


__all__ = [
    "ARTIFACT_TYPE",
    "AUTHORIZED_BRANCH",
    "AUTHORIZED_CONTRACT_ID",
    "AUTHORIZED_CONTRACT_SHA256",
    "AUTHORIZED_HEAD",
    "CLASSIFICATION",
    "CONTRACT_VERSION",
    "DATASET_COLUMNS",
    "DATASET_SCHEMA_VALUE",
    "ProspectiveV3CacheError",
    "ProspectiveV3ConflictError",
    "ProspectiveV3ContractError",
    "ProspectiveV3Error",
    "ProspectiveV3IntegrityError",
    "canonical_prospective_v3_manifest_bytes",
    "publish_prospective_v3",
    "validate_prospective_v3",
]


# END STAGE5 SOURCE PART 2
