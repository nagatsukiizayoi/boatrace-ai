"""Immutable Stage 6 pre-night Feature Matrix publication.

This module consumes one validated Stage 5 Prospective Dataset v3,
reuses the existing program-only feature contract, and publishes one
four-artifact Feature Matrix unit.

Production publication requires separate operational authorization.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import math
import os
import secrets
from pathlib import Path
from typing import Any, Mapping

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from boatrace_ai.pipelines.pre_night_features import (
    EXCLUDED_COLUMNS,
    METADATA_COLUMNS,
    MODEL_FEATURE_COLUMNS,
    OUTPUT_COLUMNS,
    PRIMARY_KEY_COLUMNS,
    PROGRAM_INPUT_COLUMNS,
    SCHEMA_NAME as SOURCE_FEATURE_SCHEMA_NAME,
    SCHEMA_VERSION as SOURCE_FEATURE_SCHEMA_VERSION,
    build_program_only_features,
)
from boatrace_ai.pipelines.prospective_v3 import (
    CLASSIFICATION as PARENT_CLASSIFICATION,
    CONTRACT_VERSION as PARENT_CONTRACT_VERSION,
    DATASET_COLUMNS,
    canonical_prospective_v3_manifest_bytes,
    validate_prospective_v3,
)


CONTRACT_VERSION = "pre_night_feature_matrix_v1"
ARTIFACT_TYPE = "PRE_NIGHT_FEATURE_MATRIX"
CLASSIFICATION = "PROSPECTIVE_PIT_CERTIFIED_FEATURE_MATRIX"
SCHEMA_NAME = "pre-night-program-only-feature-matrix"
SCHEMA_VERSION = "1.0.0"

AUTHORIZED_CONTRACT_ID = "D1B5-STAGE6-FEATURE-MATRIX-V1-R1"
AUTHORIZED_CONTRACT_SHA256 = (
    "52e316990d324fe812cfa3981b350102c75b9e7927a19535c3b7dad1f4195e01"
)

PYTHON_VERSION = "3.12.13"
PANDAS_VERSION = "2.2.2"
PYARROW_VERSION = "18.1.0"

ARTIFACT_RECORD_KEYS = {
    "relative_path",
    "byte_length",
    "sha256",
    "artifact_type",
    "contract_version",
}

SCHEMA_KEYS = {
    "contract_version",
    "schema_name",
    "schema_version",
    "race_date",
    "run_id",
    "column_count",
    "column_order",
    "primary_key_columns",
    "model_feature_columns",
    "metadata_columns",
    "excluded_parent_columns",
    "column_contracts",
    "physical_runtime",
}

METADATA_KEYS = {
    "contract_version",
    "artifact_type",
    "classification",
    "race_date",
    "run_id",
    "branch",
    "head",
    "parent_prospective_manifest_sha256",
    "parent_dataset_sha256",
    "source_feature_contract_sha256",
    "row_count",
    "column_count",
    "primary_key_columns",
    "model_feature_columns",
    "metadata_columns",
    "excluded_parent_columns",
    "sort_key",
    "runtime",
}

MANIFEST_KEYS = {
    "contract_version",
    "artifact_type",
    "classification",
    "race_date",
    "run_id",
    "branch",
    "head",
    "parent_contract_id",
    "parent_contract_sha256",
    "parent_prospective_manifest",
    "parent_dataset",
    "feature_schema",
    "feature_matrix",
    "feature_matrix_schema",
    "feature_matrix_metadata",
}

OUTPUT_PATH_NAMES = (
    "feature_matrix",
    "feature_matrix_schema",
    "feature_matrix_metadata",
    "feature_matrix_manifest",
)

COLUMN_CONTRACTS = (
    {
        "position": 1,
        "name": "race_date",
        "role": "PRIMARY_KEY",
        "model_feature": False,
        "arrow_type": "string",
        "pandas_dtype": "string",
        "nullable": False,
        "missing_value_policy": "REJECT",
        "implicit_coercion_allowed": False,
        "value_contract": {
            "format": "YYYY-MM-DD",
            "semantic_validation": "valid calendar date",
            "canonical": True,
        },
    },
    {
        "position": 2,
        "name": "venue_code",
        "role": "PRIMARY_KEY",
        "model_feature": False,
        "arrow_type": "string",
        "pandas_dtype": "string",
        "nullable": False,
        "missing_value_policy": "REJECT",
        "implicit_coercion_allowed": False,
        "value_contract": {
            "regex": r"^\d{2}$",
            "numeric_minimum": 1,
            "numeric_maximum": 24,
            "canonical": True,
        },
    },
    {
        "position": 3,
        "name": "race_no",
        "role": "PRIMARY_KEY",
        "model_feature": False,
        "arrow_type": "int64",
        "pandas_dtype": "int64",
        "nullable": False,
        "missing_value_policy": "REJECT",
        "implicit_coercion_allowed": False,
        "value_contract": {
            "minimum": 1,
            "maximum": 12,
            "bool_allowed": False,
        },
    },
    {
        "position": 4,
        "name": "boat_no",
        "role": "PRIMARY_KEY",
        "model_feature": False,
        "arrow_type": "int64",
        "pandas_dtype": "int64",
        "nullable": False,
        "missing_value_policy": "REJECT",
        "implicit_coercion_allowed": False,
        "value_contract": {
            "minimum": 1,
            "maximum": 6,
            "bool_allowed": False,
        },
    },
    {
        "position": 5,
        "name": "age",
        "role": "MODEL_FEATURE",
        "model_feature": True,
        "arrow_type": "int64",
        "pandas_dtype": "int64",
        "nullable": False,
        "missing_value_policy": "REJECT",
        "implicit_coercion_allowed": False,
        "value_contract": {
            "minimum": 15,
            "maximum": 100,
            "bool_allowed": False,
        },
    },
    {
        "position": 6,
        "name": "boat_place2_rate_pct",
        "role": "MODEL_FEATURE",
        "model_feature": True,
        "arrow_type": "double",
        "pandas_dtype": "float64",
        "nullable": False,
        "missing_value_policy": "REJECT",
        "implicit_coercion_allowed": False,
        "value_contract": {
            "minimum": 0.0,
            "maximum": 100.0,
            "finite": True,
        },
    },
    {
        "position": 7,
        "name": "branch",
        "role": "MODEL_FEATURE",
        "model_feature": True,
        "arrow_type": "string",
        "pandas_dtype": "string",
        "nullable": False,
        "missing_value_policy": "REJECT",
        "implicit_coercion_allowed": False,
        "value_contract": {
            "nonempty": True,
            "canonical_text": True,
            "encoding": (
                "PRESERVE_NORMALIZED_STRING_"
                "NO_RUNTIME_CATEGORY_CODE"
            ),
        },
    },
    {
        "position": 8,
        "name": "class",
        "role": "MODEL_FEATURE",
        "model_feature": True,
        "arrow_type": "string",
        "pandas_dtype": "string",
        "nullable": False,
        "missing_value_policy": "REJECT",
        "implicit_coercion_allowed": False,
        "value_contract": {
            "nonempty": True,
            "canonical_text": True,
            "encoding": (
                "PRESERVE_NORMALIZED_STRING_"
                "NO_RUNTIME_CATEGORY_CODE"
            ),
        },
    },
    {
        "position": 9,
        "name": "local_place2_rate_pct",
        "role": "MODEL_FEATURE",
        "model_feature": True,
        "arrow_type": "double",
        "pandas_dtype": "float64",
        "nullable": False,
        "missing_value_policy": "REJECT",
        "implicit_coercion_allowed": False,
        "value_contract": {
            "minimum": 0.0,
            "maximum": 100.0,
            "finite": True,
        },
    },
    {
        "position": 10,
        "name": "local_win_rate",
        "role": "MODEL_FEATURE",
        "model_feature": True,
        "arrow_type": "double",
        "pandas_dtype": "float64",
        "nullable": False,
        "missing_value_policy": "REJECT",
        "implicit_coercion_allowed": False,
        "value_contract": {
            "minimum": 0.0,
            "maximum": 10.0,
            "finite": True,
        },
    },
    {
        "position": 11,
        "name": "motor_place2_rate_pct",
        "role": "MODEL_FEATURE",
        "model_feature": True,
        "arrow_type": "double",
        "pandas_dtype": "float64",
        "nullable": False,
        "missing_value_policy": "REJECT",
        "implicit_coercion_allowed": False,
        "value_contract": {
            "minimum": 0.0,
            "maximum": 100.0,
            "finite": True,
        },
    },
    {
        "position": 12,
        "name": "national_place2_rate_pct",
        "role": "MODEL_FEATURE",
        "model_feature": True,
        "arrow_type": "double",
        "pandas_dtype": "float64",
        "nullable": False,
        "missing_value_policy": "REJECT",
        "implicit_coercion_allowed": False,
        "value_contract": {
            "minimum": 0.0,
            "maximum": 100.0,
            "finite": True,
        },
    },
    {
        "position": 13,
        "name": "national_win_rate",
        "role": "MODEL_FEATURE",
        "model_feature": True,
        "arrow_type": "double",
        "pandas_dtype": "float64",
        "nullable": False,
        "missing_value_policy": "REJECT",
        "implicit_coercion_allowed": False,
        "value_contract": {
            "minimum": 0.0,
            "maximum": 10.0,
            "finite": True,
        },
    },
    {
        "position": 14,
        "name": "weight_kg",
        "role": "MODEL_FEATURE",
        "model_feature": True,
        "arrow_type": "double",
        "pandas_dtype": "float64",
        "nullable": False,
        "missing_value_policy": "REJECT",
        "implicit_coercion_allowed": False,
        "value_contract": {
            "minimum": 30.0,
            "maximum": 100.0,
            "finite": True,
        },
    },
    {
        "position": 15,
        "name": "boat_no_equipment",
        "role": "METADATA",
        "model_feature": False,
        "arrow_type": "int64",
        "pandas_dtype": "int64",
        "nullable": False,
        "missing_value_policy": "REJECT",
        "implicit_coercion_allowed": False,
        "value_contract": {
            "minimum": 1,
            "maximum": 999,
            "bool_allowed": False,
        },
    },
    {
        "position": 16,
        "name": "motor_no",
        "role": "METADATA",
        "model_feature": False,
        "arrow_type": "int64",
        "pandas_dtype": "int64",
        "nullable": False,
        "missing_value_policy": "REJECT",
        "implicit_coercion_allowed": False,
        "value_contract": {
            "minimum": 1,
            "maximum": 999,
            "bool_allowed": False,
        },
    },
    {
        "position": 17,
        "name": "racer_id",
        "role": "METADATA",
        "model_feature": False,
        "arrow_type": "string",
        "pandas_dtype": "string",
        "nullable": False,
        "missing_value_policy": "REJECT",
        "implicit_coercion_allowed": False,
        "value_contract": {
            "nonempty": True,
            "numeric_text": True,
            "regex": r"^\d+$",
            "preserve_leading_zeroes": True,
            "numeric_positive_rule": False,
        },
    },
    {
        "position": 18,
        "name": "racer_name",
        "role": "METADATA",
        "model_feature": False,
        "arrow_type": "string",
        "pandas_dtype": "string",
        "nullable": False,
        "missing_value_policy": "REJECT",
        "implicit_coercion_allowed": False,
        "value_contract": {
            "nonempty": True,
            "canonical_text": True,
        },
    },
    {
        "position": 19,
        "name": "source_file",
        "role": "METADATA",
        "model_feature": False,
        "arrow_type": "string",
        "pandas_dtype": "string",
        "nullable": False,
        "missing_value_policy": "REJECT",
        "implicit_coercion_allowed": False,
        "value_contract": {
            "nonempty": True,
            "same_value_within_race": True,
            "model_feature": False,
        },
    },
)


class FeatureMatrixError(RuntimeError):
    """Base Stage 6 Feature Matrix error."""


class FeatureMatrixContractError(FeatureMatrixError):
    """Invalid input, identity, schema, or unsafe path."""


class FeatureMatrixCacheError(FeatureMatrixError):
    """Missing, malformed, noncanonical, or partial artifact unit."""


class FeatureMatrixIntegrityError(FeatureMatrixError):
    """Digest, byte-length, write, fsync, or verification failure."""


class FeatureMatrixConflictError(FeatureMatrixError):
    """Existing immutable artifact differs from expected bytes."""


def canonical_feature_matrix_json_bytes(
    value: Mapping[str, Any],
) -> bytes:
    """Return exact canonical JSON with one trailing LF."""
    if not isinstance(value, Mapping):
        raise FeatureMatrixContractError(
            "canonical JSON value must be a mapping"
        )

    try:
        text = json.dumps(
            dict(value),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as error:
        raise FeatureMatrixContractError(
            "value is not canonical JSON compatible"
        ) from error

    return (text + "\n").encode("utf-8")


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _require_sha256(value: Any, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or value.lower() != value
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise FeatureMatrixContractError(
            f"{label} must be lowercase SHA-256"
        )
    return value


def _require_git_commit(value: Any, label: str) -> str:
    """Require one canonical 40-character Git SHA-1 object ID."""
    if (
        not isinstance(value, str)
        or len(value) != 40
        or value.lower() != value
        or any(
            character not in "0123456789abcdef"
            for character in value
        )
    ):
        raise FeatureMatrixContractError(
            f"{label} must be lowercase 40-character Git commit ID"
        )

    return value


def _require_race_date(value: Any) -> str:
    if not isinstance(value, str) or value != value.strip():
        raise FeatureMatrixContractError(
            "race_date must be canonical string"
        )

    try:
        normalized = dt.date.fromisoformat(value).isoformat()
    except ValueError as error:
        raise FeatureMatrixContractError(
            "race_date must be valid YYYY-MM-DD"
        ) from error

    if normalized != value:
        raise FeatureMatrixContractError(
            "race_date must be canonical YYYY-MM-DD"
        )

    return value


def _require_identity(value: Any, label: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > 128
        or any(
            character not in (
                "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                "abcdefghijklmnopqrstuvwxyz"
                "0123456789._/-"
            )
            for character in value
        )
    ):
        raise FeatureMatrixContractError(
            f"{label} is not canonical"
        )
    return value


def _safe_root(data_root: Any) -> Path:
    root = Path(data_root)

    if not root.is_absolute():
        raise FeatureMatrixContractError(
            "data_root must be absolute"
        )

    if not root.exists() or not root.is_dir() or root.is_symlink():
        raise FeatureMatrixContractError(
            "data_root must be an existing non-symlink directory"
        )

    return root.resolve(strict=True)


def _assert_safe(root: Path, path: Path, label: str) -> None:
    try:
        path.absolute().relative_to(root)
    except ValueError as error:
        raise FeatureMatrixContractError(
            f"{label} escapes data_root"
        ) from error

    current = path
    while current != root:
        if current.exists() and current.is_symlink():
            raise FeatureMatrixContractError(
                f"{label} contains symlink"
            )
        current = current.parent


def _paths(
    data_root: Any,
    *,
    race_date: str,
    run_id: str,
) -> dict[str, Path]:
    root = _safe_root(data_root)
    race_date = _require_race_date(race_date)
    run_id = _require_identity(run_id, "run_id")

    date = dt.date.fromisoformat(race_date)
    directory = (
        root
        / "prospective"
        / "pre_night"
        / "runs"
        / f"{date.year:04d}"
        / f"{date.month:02d}"
        / f"{date.day:02d}"
        / run_id
    )

    paths = {
        "root": root,
        "directory": directory,
        "parent_manifest": (
            directory / "prospective_dataset_v3_manifest.json"
        ),
        "feature_matrix": directory / "feature_matrix.parquet",
        "feature_matrix_schema": (
            directory / "feature_matrix.schema.json"
        ),
        "feature_matrix_metadata": (
            directory / "feature_matrix.metadata.json"
        ),
        "feature_matrix_manifest": (
            directory / "feature_matrix.manifest.json"
        ),
        "lock": directory / ".feature_matrix.lock",
    }

    for label, path in paths.items():
        if label != "root":
            _assert_safe(root, path, label)

    return paths


def _exact_keys(
    value: Any,
    expected: set[str],
    label: str,
) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise FeatureMatrixCacheError(
            f"{label} must be JSON object"
        )

    if set(value) != expected:
        raise FeatureMatrixCacheError(
            f"{label} fields mismatch"
        )

    return value


def _load_canonical_json(
    path: Path,
    *,
    root: Path,
    label: str,
    expected_keys: set[str] | None = None,
) -> tuple[dict[str, Any], bytes]:
    _assert_safe(root, path, label)

    if (
        not path.exists()
        or not path.is_file()
        or path.is_symlink()
    ):
        raise FeatureMatrixCacheError(
            f"{label} is missing or unsafe"
        )

    stored = path.read_bytes()

    try:
        payload = json.loads(stored.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise FeatureMatrixCacheError(
            f"{label} is malformed"
        ) from error

    if not isinstance(payload, dict):
        raise FeatureMatrixCacheError(
            f"{label} must be JSON object"
        )

    canonical = canonical_feature_matrix_json_bytes(payload)

    if stored != canonical:
        raise FeatureMatrixCacheError(
            f"{label} is non-canonical"
        )

    if expected_keys is not None:
        _exact_keys(payload, expected_keys, label)

    return payload, stored


def _artifact_record(
    path: Path,
    *,
    root: Path,
    artifact_type: str,
    contract_version: str,
) -> dict[str, Any]:
    stored = path.read_bytes()
    return {
        "relative_path": path.relative_to(root).as_posix(),
        "byte_length": len(stored),
        "sha256": _sha256_bytes(stored),
        "artifact_type": artifact_type,
        "contract_version": contract_version,
    }


def _validate_record(
    record: Any,
    *,
    root: Path,
    expected_path: Path,
    expected_artifact_type: str,
    expected_contract_version: str,
    label: str,
) -> dict[str, Any]:
    record = _exact_keys(
        record,
        ARTIFACT_RECORD_KEYS,
        label,
    )

    if record["artifact_type"] != expected_artifact_type:
        raise FeatureMatrixCacheError(
            f"{label} artifact_type mismatch"
        )

    if record["contract_version"] != expected_contract_version:
        raise FeatureMatrixCacheError(
            f"{label} contract_version mismatch"
        )

    relative_path = record["relative_path"]
    if not isinstance(relative_path, str):
        raise FeatureMatrixCacheError(
            f"{label} relative_path must be string"
        )

    actual_path = root / relative_path
    _assert_safe(root, actual_path, label)

    if actual_path != expected_path:
        raise FeatureMatrixCacheError(
            f"{label} path mismatch"
        )

    if (
        not actual_path.is_file()
        or actual_path.is_symlink()
    ):
        raise FeatureMatrixCacheError(
            f"{label} artifact missing or unsafe"
        )

    stored = actual_path.read_bytes()
    expected_length = record["byte_length"]

    if (
        isinstance(expected_length, bool)
        or not isinstance(expected_length, int)
        or expected_length < 0
        or len(stored) != expected_length
    ):
        raise FeatureMatrixIntegrityError(
            f"{label} byte length mismatch"
        )

    expected_digest = _require_sha256(
        record["sha256"],
        f"{label}.sha256",
    )

    if _sha256_bytes(stored) != expected_digest:
        raise FeatureMatrixIntegrityError(
            f"{label} SHA-256 mismatch"
        )

    return record


def _cast_feature_frame(
    rows: list[dict[str, Any]],
) -> pd.DataFrame:
    frame = pd.DataFrame(rows, columns=list(OUTPUT_COLUMNS))

    string_columns = [
        "race_date",
        "venue_code",
        "branch",
        "class",
        "racer_id",
        "racer_name",
        "source_file",
    ]
    int_columns = [
        "race_no",
        "boat_no",
        "age",
        "boat_no_equipment",
        "motor_no",
    ]
    float_columns = [
        "boat_place2_rate_pct",
        "local_place2_rate_pct",
        "local_win_rate",
        "motor_place2_rate_pct",
        "national_place2_rate_pct",
        "national_win_rate",
        "weight_kg",
    ]

    for column in string_columns:
        frame[column] = frame[column].astype("string")

    for column in int_columns:
        frame[column] = frame[column].astype("int64")

    for column in float_columns:
        frame[column] = frame[column].astype("float64")

    return frame.loc[:, list(OUTPUT_COLUMNS)]


def _validate_feature_frame(frame: pd.DataFrame) -> None:
    if list(frame.columns) != list(OUTPUT_COLUMNS):
        raise FeatureMatrixContractError(
            "Feature Matrix column order mismatch"
        )

    if frame.empty or len(frame) % 6 != 0:
        raise FeatureMatrixContractError(
            "Feature Matrix row count must be positive multiple of 6"
        )

    if int(frame.isna().sum().sum()) != 0:
        raise FeatureMatrixContractError(
            "Feature Matrix must contain no nulls"
        )

    if frame.duplicated(
        subset=list(PRIMARY_KEY_COLUMNS)
    ).any():
        raise FeatureMatrixContractError(
            "duplicate Feature Matrix primary key"
        )

    expected = frame.sort_values(
        list(PRIMARY_KEY_COLUMNS),
        kind="stable",
    ).reset_index(drop=True)

    if not frame.reset_index(drop=True).equals(expected):
        raise FeatureMatrixContractError(
            "Feature Matrix row order mismatch"
        )

    for column in MODEL_FEATURE_COLUMNS:
        if column in {"branch", "class"}:
            continue
        values = frame[column].to_numpy()
        if not all(math.isfinite(float(value)) for value in values):
            raise FeatureMatrixContractError(
                f"{column} contains nonfinite value"
            )


def _build_frame(dataset_path: Path) -> pd.DataFrame:
    try:
        parent = pd.read_parquet(
            dataset_path,
            engine="pyarrow",
        )
    except Exception as error:
        raise FeatureMatrixCacheError(
            "parent Dataset v3 Parquet read failed"
        ) from error

    if list(parent.columns) != list(DATASET_COLUMNS):
        raise FeatureMatrixContractError(
            "parent Dataset v3 columns mismatch"
        )

    if int(parent.isna().sum().sum()) != 0:
        raise FeatureMatrixContractError(
            "parent Dataset v3 contains nulls"
        )

    program_rows = (
        parent.loc[:, list(PROGRAM_INPUT_COLUMNS)]
        .to_dict(orient="records")
    )

    try:
        feature_rows = build_program_only_features(
            program_rows
        )
    except Exception as error:
        raise FeatureMatrixContractError(
            "program-only feature construction failed"
        ) from error

    frame = _cast_feature_frame(feature_rows)
    _validate_feature_frame(frame)
    return frame


def _schema_payload(
    *,
    race_date: str,
    run_id: str,
) -> dict[str, Any]:
    return {
        "contract_version": CONTRACT_VERSION,
        "schema_name": SCHEMA_NAME,
        "schema_version": SCHEMA_VERSION,
        "race_date": race_date,
        "run_id": run_id,
        "column_count": len(OUTPUT_COLUMNS),
        "column_order": list(OUTPUT_COLUMNS),
        "primary_key_columns": list(PRIMARY_KEY_COLUMNS),
        "model_feature_columns": list(MODEL_FEATURE_COLUMNS),
        "metadata_columns": list(METADATA_COLUMNS),
        "excluded_parent_columns": list(EXCLUDED_COLUMNS),
        "column_contracts": [
            dict(value) for value in COLUMN_CONTRACTS
        ],
        "physical_runtime": {
            "python_version": PYTHON_VERSION,
            "pandas_version": PANDAS_VERSION,
            "pyarrow_version": PYARROW_VERSION,
            "parquet_engine": "pyarrow",
            "write_index": False,
            "readback_validation": True,
        },
    }


def _source_feature_contract_sha256() -> str:
    payload = {
        "schema_name": SOURCE_FEATURE_SCHEMA_NAME,
        "schema_version": SOURCE_FEATURE_SCHEMA_VERSION,
        "primary_key_columns": list(PRIMARY_KEY_COLUMNS),
        "model_feature_columns": list(MODEL_FEATURE_COLUMNS),
        "metadata_columns": list(METADATA_COLUMNS),
        "excluded_columns": list(EXCLUDED_COLUMNS),
        "output_columns": list(OUTPUT_COLUMNS),
        "program_input_columns": list(PROGRAM_INPUT_COLUMNS),
    }
    return _sha256_bytes(
        canonical_feature_matrix_json_bytes(payload)
    )


# BEGIN STAGE6 SOURCE PART 2

import io
import platform


def _parquet_bytes(frame: pd.DataFrame) -> bytes:
    buffer = io.BytesIO()

    try:
        frame.to_parquet(
            buffer,
            index=False,
            engine="pyarrow",
        )
    except Exception as error:
        raise FeatureMatrixIntegrityError(
            "Feature Matrix Parquet serialization failed"
        ) from error

    stored = buffer.getvalue()

    if not stored:
        raise FeatureMatrixIntegrityError(
            "Feature Matrix Parquet is empty"
        )

    try:
        readback = pd.read_parquet(
            io.BytesIO(stored),
            engine="pyarrow",
        )
    except Exception as error:
        raise FeatureMatrixIntegrityError(
            "Feature Matrix Parquet readback failed"
        ) from error

    _validate_feature_frame(readback)

    if len(readback) != len(frame):
        raise FeatureMatrixIntegrityError(
            "Feature Matrix readback row count mismatch"
        )

    if list(readback.columns) != list(OUTPUT_COLUMNS):
        raise FeatureMatrixIntegrityError(
            "Feature Matrix readback column order mismatch"
        )

    return stored


def _runtime_payload() -> dict[str, Any]:
    return {
        "python_version": platform.python_version(),
        "pandas_version": pd.__version__,
        "pyarrow_version": pa.__version__,
        "parquet_engine": "pyarrow",
        "write_index": False,
    }


def _require_runtime() -> None:
    runtime = _runtime_payload()

    if runtime["pandas_version"] != PANDAS_VERSION:
        raise FeatureMatrixContractError(
            "pandas version mismatch"
        )

    if runtime["pyarrow_version"] != PYARROW_VERSION:
        raise FeatureMatrixContractError(
            "pyarrow version mismatch"
        )


def _load_parent_manifest(
    paths: Mapping[str, Path],
    *,
    race_date: str,
    run_id: str,
    branch: str,
    head: str,
) -> tuple[dict[str, Any], bytes]:
    root = paths["root"]
    path = paths["parent_manifest"]

    _assert_safe(root, path, "parent manifest")

    if (
        not path.exists()
        or not path.is_file()
        or path.is_symlink()
    ):
        raise FeatureMatrixCacheError(
            "parent Prospective Dataset v3 manifest is missing"
        )

    stored = path.read_bytes()

    try:
        payload = json.loads(stored.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise FeatureMatrixCacheError(
            "parent manifest is malformed"
        ) from error

    if not isinstance(payload, dict):
        raise FeatureMatrixCacheError(
            "parent manifest must be JSON object"
        )

    try:
        canonical = canonical_prospective_v3_manifest_bytes(
            payload
        )
    except Exception as error:
        raise FeatureMatrixCacheError(
            "parent manifest validation failed"
        ) from error

    if stored != canonical:
        raise FeatureMatrixCacheError(
            "parent manifest is non-canonical"
        )

    expected_identity = {
        "contract_version": PARENT_CONTRACT_VERSION,
        "classification": PARENT_CLASSIFICATION,
        "race_date": race_date,
        "run_id": run_id,
        "branch": branch,
        "head": head,
    }

    for field, expected in expected_identity.items():
        if payload.get(field) != expected:
            raise FeatureMatrixCacheError(
                f"parent manifest {field} mismatch"
            )

    return payload, stored


def _prepare(
    data_root,
    *,
    race_date,
    run_id,
    branch,
    head,
) -> dict[str, Any]:
    race_date = _require_race_date(race_date)
    run_id = _require_identity(run_id, "run_id")
    branch = _require_identity(branch, "branch")
    head = _require_git_commit(head, "head")

    _require_runtime()

    paths = _paths(
        data_root,
        race_date=race_date,
        run_id=run_id,
    )

    parent_receipt = validate_prospective_v3(
        data_root,
        race_date=race_date,
        run_id=run_id,
        branch=branch,
        head=head,
    )

    if (
        parent_receipt.get("classification")
        != PARENT_CLASSIFICATION
    ):
        raise FeatureMatrixCacheError(
            "parent receipt classification mismatch"
        )

    parent_manifest, parent_manifest_bytes = (
        _load_parent_manifest(
            paths,
            race_date=race_date,
            run_id=run_id,
            branch=branch,
            head=head,
        )
    )

    parent_manifest_sha256 = _sha256_bytes(
        parent_manifest_bytes
    )

    receipt_manifest_sha256 = _require_sha256(
        parent_receipt.get(
            "prospective_manifest_sha256"
        ),
        "parent receipt manifest digest",
    )

    if receipt_manifest_sha256 != parent_manifest_sha256:
        raise FeatureMatrixIntegrityError(
            "parent manifest digest mismatch"
        )

    dataset_path = parent_receipt.get(
        "paths",
        {},
    ).get("dataset")

    if not isinstance(dataset_path, Path):
        raise FeatureMatrixCacheError(
            "parent receipt dataset path missing"
        )

    _assert_safe(
        paths["root"],
        dataset_path,
        "parent dataset",
    )

    if (
        not dataset_path.is_file()
        or dataset_path.is_symlink()
    ):
        raise FeatureMatrixCacheError(
            "parent dataset missing or unsafe"
        )

    dataset_bytes = dataset_path.read_bytes()
    dataset_sha256 = _sha256_bytes(dataset_bytes)

    if dataset_sha256 != _require_sha256(
        parent_receipt.get("dataset_sha256"),
        "parent dataset digest",
    ):
        raise FeatureMatrixIntegrityError(
            "parent dataset digest mismatch"
        )

    if len(dataset_bytes) != parent_receipt.get(
        "dataset_byte_length"
    ):
        raise FeatureMatrixIntegrityError(
            "parent dataset byte length mismatch"
        )

    frame = _build_frame(dataset_path)
    parquet_bytes = _parquet_bytes(frame)

    schema_payload = _schema_payload(
        race_date=race_date,
        run_id=run_id,
    )
    schema_bytes = canonical_feature_matrix_json_bytes(
        schema_payload
    )

    metadata_payload = {
        "contract_version": CONTRACT_VERSION,
        "artifact_type": ARTIFACT_TYPE,
        "classification": CLASSIFICATION,
        "race_date": race_date,
        "run_id": run_id,
        "branch": branch,
        "head": head,
        "parent_prospective_manifest_sha256": (
            parent_manifest_sha256
        ),
        "parent_dataset_sha256": dataset_sha256,
        "source_feature_contract_sha256": (
            _source_feature_contract_sha256()
        ),
        "row_count": len(frame),
        "column_count": len(OUTPUT_COLUMNS),
        "primary_key_columns": list(
            PRIMARY_KEY_COLUMNS
        ),
        "model_feature_columns": list(
            MODEL_FEATURE_COLUMNS
        ),
        "metadata_columns": list(METADATA_COLUMNS),
        "excluded_parent_columns": list(
            EXCLUDED_COLUMNS
        ),
        "sort_key": list(PRIMARY_KEY_COLUMNS),
        "runtime": _runtime_payload(),
    }
    metadata_bytes = canonical_feature_matrix_json_bytes(
        metadata_payload
    )

    parent_manifest_record = {
        "relative_path": (
            paths["parent_manifest"]
            .relative_to(paths["root"])
            .as_posix()
        ),
        "byte_length": len(parent_manifest_bytes),
        "sha256": parent_manifest_sha256,
        "artifact_type": (
            "PROSPECTIVE_DATASET_V3_MANIFEST"
        ),
        "contract_version": PARENT_CONTRACT_VERSION,
    }

    parent_dataset_record = {
        "relative_path": (
            dataset_path
            .relative_to(paths["root"])
            .as_posix()
        ),
        "byte_length": len(dataset_bytes),
        "sha256": dataset_sha256,
        "artifact_type": "PROSPECTIVE_DATASET_V3",
        "contract_version": PARENT_CONTRACT_VERSION,
    }

    feature_matrix_record = {
        "relative_path": (
            paths["feature_matrix"]
            .relative_to(paths["root"])
            .as_posix()
        ),
        "byte_length": len(parquet_bytes),
        "sha256": _sha256_bytes(parquet_bytes),
        "artifact_type": "FEATURE_MATRIX_PARQUET",
        "contract_version": CONTRACT_VERSION,
    }

    feature_schema_record = {
        "relative_path": (
            paths["feature_matrix_schema"]
            .relative_to(paths["root"])
            .as_posix()
        ),
        "byte_length": len(schema_bytes),
        "sha256": _sha256_bytes(schema_bytes),
        "artifact_type": "FEATURE_MATRIX_SCHEMA",
        "contract_version": CONTRACT_VERSION,
    }

    feature_metadata_record = {
        "relative_path": (
            paths["feature_matrix_metadata"]
            .relative_to(paths["root"])
            .as_posix()
        ),
        "byte_length": len(metadata_bytes),
        "sha256": _sha256_bytes(metadata_bytes),
        "artifact_type": "FEATURE_MATRIX_METADATA",
        "contract_version": CONTRACT_VERSION,
    }

    manifest_payload = {
        "contract_version": CONTRACT_VERSION,
        "artifact_type": ARTIFACT_TYPE,
        "classification": CLASSIFICATION,
        "race_date": race_date,
        "run_id": run_id,
        "branch": branch,
        "head": head,
        "parent_contract_id": (
            "D1B5-STAGE5-PROSPECTIVE-DATASET-V3-V1-R1"
        ),
        "parent_contract_sha256": (
            "7c3b2031793e2a044750146a0a4ecab6277f5abc252e8be698ce42a2ae70766e"
        ),
        "parent_prospective_manifest": (
            parent_manifest_record
        ),
        "parent_dataset": parent_dataset_record,
        "feature_schema": feature_schema_record,
        "feature_matrix": feature_matrix_record,
        "feature_matrix_schema": feature_schema_record,
        "feature_matrix_metadata": feature_metadata_record,
    }
    manifest_bytes = canonical_feature_matrix_json_bytes(
        manifest_payload
    )

    payloads = {
        "feature_matrix": parquet_bytes,
        "feature_matrix_schema": schema_bytes,
        "feature_matrix_metadata": metadata_bytes,
        "feature_matrix_manifest": manifest_bytes,
    }

    return {
        "paths": paths,
        "frame": frame,
        "payloads": payloads,
        "parent_manifest": parent_manifest,
        "parent_manifest_record": parent_manifest_record,
        "parent_dataset_record": parent_dataset_record,
        "schema_payload": schema_payload,
        "metadata_payload": metadata_payload,
        "manifest_payload": manifest_payload,
    }


def _validate_parquet_bytes(
    stored: bytes,
    expected_frame: pd.DataFrame,
) -> None:
    try:
        frame = pd.read_parquet(
            io.BytesIO(stored),
            engine="pyarrow",
        )
    except Exception as error:
        raise FeatureMatrixCacheError(
            "cached Feature Matrix Parquet is malformed"
        ) from error

    _validate_feature_frame(frame)

    if len(frame) != len(expected_frame):
        raise FeatureMatrixConflictError(
            "cached Feature Matrix row count conflict"
        )

    if not frame.equals(expected_frame):
        raise FeatureMatrixConflictError(
            "cached Feature Matrix content conflict"
        )


def _validate_complete_unit(
    prepared: Mapping[str, Any],
) -> None:
    paths = prepared["paths"]
    root = paths["root"]
    payloads = prepared["payloads"]

    existence = {
        name: paths[name].exists()
        for name in OUTPUT_PATH_NAMES
    }

    if not all(existence.values()):
        if any(existence.values()):
            raise FeatureMatrixCacheError(
                "partial Feature Matrix artifact unit exists"
            )
        raise FeatureMatrixCacheError(
            "Feature Matrix artifact unit does not exist"
        )

    for name in OUTPUT_PATH_NAMES:
        path = paths[name]
        _assert_safe(root, path, name)

        if not path.is_file() or path.is_symlink():
            raise FeatureMatrixCacheError(
                f"{name} is missing or unsafe"
            )

        stored = path.read_bytes()
        expected = payloads[name]

        if stored != expected:
            raise FeatureMatrixConflictError(
                f"{name} exact byte conflict"
            )

        if _sha256_bytes(stored) != _sha256_bytes(
            expected
        ):
            raise FeatureMatrixIntegrityError(
                f"{name} digest mismatch"
            )

    _validate_parquet_bytes(
        paths["feature_matrix"].read_bytes(),
        prepared["frame"],
    )

    schema, _ = _load_canonical_json(
        paths["feature_matrix_schema"],
        root=root,
        label="Feature Matrix schema",
        expected_keys=SCHEMA_KEYS,
    )
    metadata, _ = _load_canonical_json(
        paths["feature_matrix_metadata"],
        root=root,
        label="Feature Matrix metadata",
        expected_keys=METADATA_KEYS,
    )
    manifest, _ = _load_canonical_json(
        paths["feature_matrix_manifest"],
        root=root,
        label="Feature Matrix manifest",
        expected_keys=MANIFEST_KEYS,
    )

    if schema != prepared["schema_payload"]:
        raise FeatureMatrixConflictError(
            "Feature Matrix schema payload conflict"
        )

    if metadata != prepared["metadata_payload"]:
        raise FeatureMatrixConflictError(
            "Feature Matrix metadata payload conflict"
        )

    if manifest != prepared["manifest_payload"]:
        raise FeatureMatrixConflictError(
            "Feature Matrix manifest payload conflict"
        )


def _receipt(
    prepared: Mapping[str, Any],
    *,
    cached: bool,
    publication_status: str,
) -> dict[str, Any]:
    paths = prepared["paths"]
    payloads = prepared["payloads"]

    def relative(name: str) -> str:
        return (
            paths[name]
            .relative_to(paths["root"])
            .as_posix()
        )

    return {
        "race_date": (
            prepared["manifest_payload"]["race_date"]
        ),
        "run_id": (
            prepared["manifest_payload"]["run_id"]
        ),
        "classification": CLASSIFICATION,
        "feature_matrix_relative_path": (
            relative("feature_matrix")
        ),
        "feature_matrix_sha256": _sha256_bytes(
            payloads["feature_matrix"]
        ),
        "feature_matrix_byte_length": len(
            payloads["feature_matrix"]
        ),
        "feature_matrix_schema_relative_path": (
            relative("feature_matrix_schema")
        ),
        "feature_matrix_schema_sha256": _sha256_bytes(
            payloads["feature_matrix_schema"]
        ),
        "feature_matrix_schema_byte_length": len(
            payloads["feature_matrix_schema"]
        ),
        "feature_matrix_metadata_relative_path": (
            relative("feature_matrix_metadata")
        ),
        "feature_matrix_metadata_sha256": _sha256_bytes(
            payloads["feature_matrix_metadata"]
        ),
        "feature_matrix_metadata_byte_length": len(
            payloads["feature_matrix_metadata"]
        ),
        "feature_matrix_manifest_relative_path": (
            relative("feature_matrix_manifest")
        ),
        "feature_matrix_manifest_sha256": _sha256_bytes(
            payloads["feature_matrix_manifest"]
        ),
        "feature_matrix_manifest_byte_length": len(
            payloads["feature_matrix_manifest"]
        ),
        "row_count": len(prepared["frame"]),
        "column_count": len(OUTPUT_COLUMNS),
        "cached": cached,
        "publication_status": publication_status,
        "paths": {
            "directory": paths["directory"],
            "feature_matrix": paths["feature_matrix"],
            "feature_matrix_schema": (
                paths["feature_matrix_schema"]
            ),
            "feature_matrix_metadata": (
                paths["feature_matrix_metadata"]
            ),
            "feature_matrix_manifest": (
                paths["feature_matrix_manifest"]
            ),
        },
    }


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY

    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY

    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise FeatureMatrixIntegrityError(
            "directory open for fsync failed"
        ) from error

    try:
        os.fsync(descriptor)
    except OSError as error:
        raise FeatureMatrixIntegrityError(
            "directory fsync failed"
        ) from error
    finally:
        try:
            os.close(descriptor)
        except OSError as error:
            raise FeatureMatrixIntegrityError(
                "directory descriptor close failed"
            ) from error


def validate_feature_matrix(
    data_root,
    *,
    race_date,
    run_id,
    branch,
    head,
) -> dict[str, Any]:
    """Validate and reuse one complete immutable Feature Matrix unit."""
    prepared = _prepare(
        data_root,
        race_date=race_date,
        run_id=run_id,
        branch=branch,
        head=head,
    )
    _validate_complete_unit(prepared)

    return _receipt(
        prepared,
        cached=True,
        publication_status="VALIDATED_REUSE",
    )


def publish_feature_matrix(
    data_root,
    *,
    race_date,
    run_id,
    branch,
    head,
) -> dict[str, Any]:
    """Publish or validate one immutable four-artifact unit."""
    prepared = _prepare(
        data_root,
        race_date=race_date,
        run_id=run_id,
        branch=branch,
        head=head,
    )
    paths = prepared["paths"]
    directory = paths["directory"]

    if not directory.is_dir() or directory.is_symlink():
        raise FeatureMatrixCacheError(
            "Stage 5 run directory is missing or unsafe"
        )

    existing = {
        name: paths[name].exists()
        for name in OUTPUT_PATH_NAMES
    }

    if all(existing.values()):
        _validate_complete_unit(prepared)
        return _receipt(
            prepared,
            cached=True,
            publication_status="VALIDATED_REUSE",
        )

    if any(existing.values()):
        raise FeatureMatrixCacheError(
            "partial Feature Matrix artifact unit exists"
        )

    lock_fd = None
    lock_acquired = False
    lock_identity = None
    temporaries: dict[str, Path] = {}
    created_destinations: list[Path] = []

    try:
        try:
            lock_fd = os.open(
                paths["lock"],
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
            )
            lock_acquired = True
            lock_stat = os.fstat(lock_fd)
            lock_identity = (
                lock_stat.st_dev,
                lock_stat.st_ino,
            )
        except FileExistsError as error:
            raise FeatureMatrixError(
                "Feature Matrix publication lock exists"
            ) from error
        except OSError as error:
            raise FeatureMatrixError(
                "Feature Matrix lock acquisition failed"
            ) from error

        try:
            os.close(lock_fd)
        except OSError as error:
            raise FeatureMatrixIntegrityError(
                "lock descriptor close failed"
            ) from error
        else:
            lock_fd = None

        existing_after_lock = {
            name: paths[name].exists()
            for name in OUTPUT_PATH_NAMES
        }

        if all(existing_after_lock.values()):
            _validate_complete_unit(prepared)
            return _receipt(
                prepared,
                cached=True,
                publication_status="VALIDATED_REUSE",
            )

        if any(existing_after_lock.values()):
            raise FeatureMatrixCacheError(
                "partial Feature Matrix artifact unit exists"
            )

        for name in OUTPUT_PATH_NAMES:
            temporary = directory / (
                f".{paths[name].name}."
                f"{secrets.token_hex(16)}.tmp"
            )
            _assert_safe(
                paths["root"],
                temporary,
                f"{name} temporary",
            )

            try:
                with temporary.open("xb") as handle:
                    handle.write(
                        prepared["payloads"][name]
                    )
                    handle.flush()
                    os.fsync(handle.fileno())
            except Exception as error:
                raise FeatureMatrixIntegrityError(
                    f"{name} temporary write failed"
                ) from error

            if temporary.read_bytes() != prepared[
                "payloads"
            ][name]:
                raise FeatureMatrixIntegrityError(
                    f"{name} temporary byte mismatch"
                )

            temporaries[name] = temporary

        # Manifest is intentionally last.
        for name in OUTPUT_PATH_NAMES:
            try:
                os.link(
                    temporaries[name],
                    paths[name],
                )
                created_destinations.append(paths[name])
            except FileExistsError as error:
                raise FeatureMatrixConflictError(
                    f"{name} destination appeared during publication"
                ) from error
            except OSError as error:
                raise FeatureMatrixIntegrityError(
                    f"{name} no-overwrite publication failed"
                ) from error

        _fsync_directory(directory)
        _validate_complete_unit(prepared)

        return _receipt(
            prepared,
            cached=False,
            publication_status="CREATED",
        )

    except Exception:
        cleanup_error = None

        for path in reversed(created_destinations):
            try:
                path.unlink(missing_ok=True)
            except OSError as error:
                cleanup_error = cleanup_error or error

        if cleanup_error is not None:
            raise FeatureMatrixIntegrityError(
                "owned publication cleanup failed"
            ) from cleanup_error

        raise

    finally:
        cleanup_error = None

        for temporary in temporaries.values():
            try:
                temporary.unlink(missing_ok=True)
            except OSError as error:
                cleanup_error = cleanup_error or error

        if lock_fd is not None:
            try:
                os.close(lock_fd)
            except OSError as error:
                cleanup_error = cleanup_error or error

        if lock_acquired:
            try:
                current = paths["lock"].lstat()
            except FileNotFoundError:
                current = None
            except OSError as error:
                current = None
                cleanup_error = cleanup_error or error

            if current is not None:
                current_identity = (
                    current.st_dev,
                    current.st_ino,
                )

                if current_identity != lock_identity:
                    cleanup_error = cleanup_error or RuntimeError(
                        "lock ownership identity changed"
                    )
                else:
                    try:
                        paths["lock"].unlink()
                    except OSError as error:
                        cleanup_error = cleanup_error or error

        if cleanup_error is not None:
            raise FeatureMatrixIntegrityError(
                "Feature Matrix cleanup failed"
            ) from cleanup_error


__all__ = [
    "CONTRACT_VERSION",
    "ARTIFACT_TYPE",
    "CLASSIFICATION",
    "SCHEMA_NAME",
    "SCHEMA_VERSION",
    "AUTHORIZED_CONTRACT_ID",
    "AUTHORIZED_CONTRACT_SHA256",
    "COLUMN_CONTRACTS",
    "FeatureMatrixError",
    "FeatureMatrixContractError",
    "FeatureMatrixCacheError",
    "FeatureMatrixIntegrityError",
    "FeatureMatrixConflictError",
    "canonical_feature_matrix_json_bytes",
    "validate_feature_matrix",
    "publish_feature_matrix",
]


# END STAGE6 SOURCE PART 2
