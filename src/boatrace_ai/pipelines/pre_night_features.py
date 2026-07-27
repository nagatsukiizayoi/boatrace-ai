"""Program-only pre-night feature dataset contract v1.

This module transforms the verified 20-column program-parser output into the
fixed 19-column ``pre-night-program-only-features`` schema version 1.0.0.

Only pre-race program information is accepted. ``series_results_raw`` is
explicitly excluded from both the output and the model feature set.
"""

from __future__ import annotations

import datetime as dt
import math
from collections import defaultdict
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any


SCHEMA_NAME = "pre-night-program-only-features"
SCHEMA_VERSION = "1.0.0"

PRIMARY_KEY_COLUMNS = (
    "race_date",
    "venue_code",
    "race_no",
    "boat_no",
)

MODEL_FEATURE_COLUMNS = (
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
)

METADATA_COLUMNS = (
    "boat_no_equipment",
    "motor_no",
    "racer_id",
    "racer_name",
    "source_file",
)

EXCLUDED_COLUMNS = (
    "series_results_raw",
)

OUTPUT_COLUMNS = (
    *PRIMARY_KEY_COLUMNS,
    *MODEL_FEATURE_COLUMNS,
    *METADATA_COLUMNS,
)

PROGRAM_INPUT_COLUMNS = (
    *OUTPUT_COLUMNS,
    *EXCLUDED_COLUMNS,
)

FORBIDDEN_RESULT_COLUMNS = frozenset(
    {
        "actual",
        "arrival",
        "collector_result",
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
        "pipeline_result",
        "refund",
        "result",
        "result_available",
        "results",
        "target",
        "winner",
        "winning",
        "着順",
        "払戻",
        "結果",
        "決まり手",
    }
)


class ProgramOnlyFeatureContractError(ValueError):
    """Raised when input violates the Program-only feature contract."""


def schema_contract() -> dict[str, Any]:
    """Return an immutable-style description of schema version 1.0.0."""
    return {
        "schema_name": SCHEMA_NAME,
        "schema_version": SCHEMA_VERSION,
        "primary_key_columns": list(PRIMARY_KEY_COLUMNS),
        "model_feature_columns": list(MODEL_FEATURE_COLUMNS),
        "metadata_columns": list(METADATA_COLUMNS),
        "excluded_columns": list(EXCLUDED_COLUMNS),
        "output_columns": list(OUTPUT_COLUMNS),
        "program_input_columns": list(PROGRAM_INPUT_COLUMNS),
    }


def _fail(message: str) -> None:
    raise ProgramOnlyFeatureContractError(message)


def _require_non_bool_int(
    value: Any,
    *,
    column: str,
    minimum: int,
    maximum: int,
) -> int:
    if isinstance(value, bool):
        _fail(f"{column} must be an integer, not bool")

    if isinstance(value, float):
        if not math.isfinite(value) or not value.is_integer():
            _fail(f"{column} must be an integer")

    try:
        converted = int(value)
    except (TypeError, ValueError, OverflowError):
        _fail(f"{column} must be an integer")

    if not minimum <= converted <= maximum:
        _fail(
            f"{column} is out of range: "
            f"expected {minimum}..{maximum}, got {converted}"
        )

    return converted


def _require_finite_float(
    value: Any,
    *,
    column: str,
    minimum: float,
    maximum: float,
) -> float:
    if isinstance(value, bool):
        _fail(f"{column} must be numeric, not bool")

    try:
        converted = float(value)
    except (TypeError, ValueError, OverflowError):
        _fail(f"{column} must be numeric")

    if not math.isfinite(converted):
        _fail(f"{column} must be finite")

    if not minimum <= converted <= maximum:
        _fail(
            f"{column} is out of range: "
            f"expected {minimum}..{maximum}, got {converted}"
        )

    return converted


def _require_text(value: Any, *, column: str) -> str:
    if value is None:
        _fail(f"{column} must not be missing")

    if isinstance(value, float) and not math.isfinite(value):
        _fail(f"{column} must be finite text-compatible data")

    converted = str(value).strip()

    if not converted:
        _fail(f"{column} must not be empty")

    return converted


def _normalize_race_date(value: Any) -> str:
    if isinstance(value, dt.datetime):
        value = value.date()

    if isinstance(value, dt.date):
        return value.isoformat()

    text = _require_text(value, column="race_date")

    try:
        parsed = dt.date.fromisoformat(text)
    except ValueError:
        _fail(
            "race_date must use ISO YYYY-MM-DD format: "
            f"got {text!r}"
        )

    return parsed.isoformat()


def _normalize_venue_code(value: Any) -> str:
    if isinstance(value, bool):
        _fail("venue_code must not be bool")

    if isinstance(value, int):
        numeric = value
    else:
        text = _require_text(value, column="venue_code")

        if not text.isdigit():
            _fail(f"venue_code must be numeric text: got {text!r}")

        numeric = int(text)

    if not 1 <= numeric <= 24:
        _fail(
            "venue_code is out of range: "
            f"expected 1..24, got {numeric}"
        )

    return f"{numeric:02d}"


def _normalize_racer_id(value: Any) -> str:
    if isinstance(value, bool):
        _fail("racer_id must not be bool")

    text = _require_text(value, column="racer_id")

    if not text.isdigit():
        _fail(f"racer_id must be numeric text: got {text!r}")

    return text


def _normalize_source_file(value: Any) -> str:
    if isinstance(value, Path):
        value = value.as_posix()

    return _require_text(value, column="source_file")


def _validate_input_columns(row: Mapping[str, Any], row_number: int) -> None:
    actual = set(row)
    expected = set(PROGRAM_INPUT_COLUMNS)

    forbidden = sorted(actual & FORBIDDEN_RESULT_COLUMNS)
    if forbidden:
        _fail(
            f"row {row_number} contains forbidden result/post-race columns: "
            f"{forbidden}"
        )

    missing = sorted(expected - actual)
    unexpected = sorted(actual - expected)

    if missing or unexpected:
        _fail(
            f"row {row_number} must contain exactly the verified "
            f"20-column parser schema; missing={missing}, "
            f"unexpected={unexpected}"
        )


def _normalize_row(
    row: Mapping[str, Any],
    *,
    row_number: int,
) -> dict[str, Any]:
    _validate_input_columns(row, row_number)

    result: dict[str, Any] = {
        "race_date": _normalize_race_date(row["race_date"]),
        "venue_code": _normalize_venue_code(row["venue_code"]),
        "race_no": _require_non_bool_int(
            row["race_no"],
            column="race_no",
            minimum=1,
            maximum=12,
        ),
        "boat_no": _require_non_bool_int(
            row["boat_no"],
            column="boat_no",
            minimum=1,
            maximum=6,
        ),
        "age": _require_non_bool_int(
            row["age"],
            column="age",
            minimum=15,
            maximum=100,
        ),
        "boat_place2_rate_pct": _require_finite_float(
            row["boat_place2_rate_pct"],
            column="boat_place2_rate_pct",
            minimum=0.0,
            maximum=100.0,
        ),
        "branch": _require_text(row["branch"], column="branch"),
        "class": _require_text(row["class"], column="class"),
        "local_place2_rate_pct": _require_finite_float(
            row["local_place2_rate_pct"],
            column="local_place2_rate_pct",
            minimum=0.0,
            maximum=100.0,
        ),
        "local_win_rate": _require_finite_float(
            row["local_win_rate"],
            column="local_win_rate",
            minimum=0.0,
            maximum=10.0,
        ),
        "motor_place2_rate_pct": _require_finite_float(
            row["motor_place2_rate_pct"],
            column="motor_place2_rate_pct",
            minimum=0.0,
            maximum=100.0,
        ),
        "national_place2_rate_pct": _require_finite_float(
            row["national_place2_rate_pct"],
            column="national_place2_rate_pct",
            minimum=0.0,
            maximum=100.0,
        ),
        "national_win_rate": _require_finite_float(
            row["national_win_rate"],
            column="national_win_rate",
            minimum=0.0,
            maximum=10.0,
        ),
        "weight_kg": _require_finite_float(
            row["weight_kg"],
            column="weight_kg",
            minimum=30.0,
            maximum=100.0,
        ),
        "boat_no_equipment": _require_non_bool_int(
            row["boat_no_equipment"],
            column="boat_no_equipment",
            minimum=1,
            maximum=999,
        ),
        "motor_no": _require_non_bool_int(
            row["motor_no"],
            column="motor_no",
            minimum=1,
            maximum=999,
        ),
        "racer_id": _normalize_racer_id(row["racer_id"]),
        "racer_name": _require_text(
            row["racer_name"],
            column="racer_name",
        ),
        "source_file": _normalize_source_file(row["source_file"]),
    }

    # Reconstruct in the contract's fixed order.
    return {column: result[column] for column in OUTPUT_COLUMNS}


def build_program_only_features(
    rows: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Build deterministic Program-only feature records.

    Contract rules:

    * input must use the verified 20-column parser schema;
    * ``series_results_raw`` is excluded;
    * result, payout, odds and label columns are rejected;
    * each race must contain boats 1 through 6 exactly once;
    * duplicate primary keys are rejected;
    * all numeric model values must be finite and in range;
    * all rows in one race must have the same ``source_file``;
    * output is sorted by date, venue, race and boat.
    """
    if rows is None:
        _fail("rows must not be None")

    normalized: list[dict[str, Any]] = []

    for row_number, row in enumerate(rows, start=1):
        if not isinstance(row, Mapping):
            _fail(f"row {row_number} must be a mapping")

        normalized.append(
            _normalize_row(row, row_number=row_number)
        )

    if not normalized:
        _fail("at least one race is required")

    seen_primary_keys: set[tuple[Any, ...]] = set()
    race_groups: dict[
        tuple[str, str, int],
        list[dict[str, Any]],
    ] = defaultdict(list)

    for row in normalized:
        primary_key = tuple(row[column] for column in PRIMARY_KEY_COLUMNS)

        if primary_key in seen_primary_keys:
            _fail(f"duplicate primary key: {primary_key}")

        seen_primary_keys.add(primary_key)

        race_key = (
            row["race_date"],
            row["venue_code"],
            row["race_no"],
        )
        race_groups[race_key].append(row)

    required_boats = {1, 2, 3, 4, 5, 6}

    for race_key, race_rows in race_groups.items():
        boats = {row["boat_no"] for row in race_rows}

        if len(race_rows) != 6 or boats != required_boats:
            _fail(
                f"race {race_key} must contain exactly boats 1..6; "
                f"row_count={len(race_rows)}, boats={sorted(boats)}"
            )

        source_files = {row["source_file"] for row in race_rows}

        if len(source_files) != 1:
            _fail(
                f"race {race_key} contains mixed source_file values: "
                f"{sorted(source_files)}"
            )

    normalized.sort(
        key=lambda row: (
            row["race_date"],
            row["venue_code"],
            row["race_no"],
            row["boat_no"],
        )
    )

    return normalized


def build_pre_night_features(
    rows: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Backward-readable alias for :func:`build_program_only_features`."""
    return build_program_only_features(rows)


__all__ = [
    "SCHEMA_NAME",
    "SCHEMA_VERSION",
    "PRIMARY_KEY_COLUMNS",
    "MODEL_FEATURE_COLUMNS",
    "METADATA_COLUMNS",
    "EXCLUDED_COLUMNS",
    "OUTPUT_COLUMNS",
    "PROGRAM_INPUT_COLUMNS",
    "FORBIDDEN_RESULT_COLUMNS",
    "ProgramOnlyFeatureContractError",
    "schema_contract",
    "build_program_only_features",
    "build_pre_night_features",
]
