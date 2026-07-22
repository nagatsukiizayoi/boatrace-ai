"""Semantic validation for curated payout rows.

Payout rows have two valid forms.

Normal payout
-------------
Identified by:

    race_date + venue_code + race_no + bet_type + combination

Semantic placeholder/status row
-------------------------------
Identified by:

    race_date + venue_code + race_no + bet_type

For a placeholder row, ``combination`` is null and ``payout_status`` is one
of ``CANCELLED``, ``NOT_ESTABLISHED``, or ``SPECIAL_PAYOUT``.

``CANCELLED`` and ``NOT_ESTABLISHED`` have a null ``payout_yen``.
``SPECIAL_PAYOUT`` has ``payout_yen == 70``.

This module is read-only. It never modifies the supplied DataFrames.
"""

from __future__ import annotations

from typing import Any

import pandas as pd


BASE_RACE_KEY = [
    "race_date",
    "venue_code",
    "race_no",
]

NORMAL_PAYOUT_KEY = [
    "race_date",
    "venue_code",
    "race_no",
    "bet_type",
    "combination",
]

PLACEHOLDER_KEY = [
    "race_date",
    "venue_code",
    "race_no",
    "bet_type",
]

ALLOWED_PLACEHOLDER_STATUSES = frozenset(
    {
        "CANCELLED",
        "NOT_ESTABLISHED",
        "SPECIAL_PAYOUT",
    }
)

_REQUIRED_PAYOUT_COLUMNS = set(
    NORMAL_PAYOUT_KEY
    + [
        "payout_yen",
        "payout_status",
    ]
)


class PayoutSemanticAuditError(AssertionError):
    """Raised when a payout DataFrame violates the semantic contract."""

    def __init__(self, report: dict[str, Any]) -> None:
        self.report = report
        failed = ", ".join(report.get("failed_checks", []))
        super().__init__(f"Payout semantic audit failed: {failed}")


def _blank_mask(series: pd.Series) -> pd.Series:
    """Return a bool Series selecting non-null blank strings."""

    if not (
        pd.api.types.is_object_dtype(series.dtype)
        or pd.api.types.is_string_dtype(series.dtype)
    ):
        return pd.Series(False, index=series.index, dtype=bool)

    values = series.astype("string").str.strip()
    return values.eq("").astype("boolean").fillna(False).astype(bool)


def _duplicate_summary(
    frame: pd.DataFrame,
    key: list[str],
) -> tuple[int, int]:
    """Return duplicate-row and duplicate-group counts."""

    if frame.empty:
        return 0, 0

    mask = frame.duplicated(key, keep=False)
    row_count = int(mask.sum())

    if not row_count:
        return 0, 0

    group_count = int(
        frame.loc[mask, key]
        .drop_duplicates()
        .shape[0]
    )

    return row_count, group_count


def _race_set(
    frame: pd.DataFrame,
) -> set[tuple[Any, ...]]:
    """Return distinct race keys."""

    if frame.empty:
        return set()

    return set(
        frame[BASE_RACE_KEY]
        .drop_duplicates()
        .itertuples(index=False, name=None)
    )


def audit_payout_semantics(
    payout: pd.DataFrame,
    program: pd.DataFrame | None = None,
) -> dict[str, Any]:
    """Audit payout rows without modifying the supplied DataFrames.

    Args:
        payout:
            Curated payout DataFrame.
        program:
            Optional curated program DataFrame. When supplied, payout and
            program race-key sets must match exactly.

    Returns:
        A JSON-compatible-style dictionary containing status, checks,
        failure names, and metrics. Scalar values may still use NumPy scalar
        types inherited from pandas.

    The function reports all detectable failures rather than stopping on the
    first invalid row.
    """

    missing_payout_columns = sorted(
        _REQUIRED_PAYOUT_COLUMNS - set(payout.columns)
    )

    missing_program_columns: list[str] = []

    if program is not None:
        missing_program_columns = sorted(
            set(BASE_RACE_KEY) - set(program.columns)
        )

    if missing_payout_columns or missing_program_columns:
        checks = {
            "required_payout_columns_present": (
                not missing_payout_columns
            ),
            "required_program_columns_present": (
                not missing_program_columns
            ),
        }

        failed_checks = [
            name
            for name, passed in checks.items()
            if not passed
        ]

        return {
            "status": "FAILED",
            "checks": checks,
            "failed_checks": failed_checks,
            "missing_payout_columns": missing_payout_columns,
            "missing_program_columns": missing_program_columns,
        }

    # Only local masks/subsets are created. The input is not changed.
    combination_null = payout["combination"].isna()
    combination_blank = _blank_mask(payout["combination"])

    placeholder = payout.loc[combination_null]
    normal = payout.loc[
        ~combination_null & ~combination_blank
    ]

    base_key_null_counts = {
        column: int(payout[column].isna().sum())
        for column in BASE_RACE_KEY
    }

    bet_type_null_count = int(
        payout["bet_type"].isna().sum()
    )

    normal_key_null_counts = {
        column: int(normal[column].isna().sum())
        for column in NORMAL_PAYOUT_KEY
    }

    normal_duplicate_rows, normal_duplicate_groups = (
        _duplicate_summary(normal, NORMAL_PAYOUT_KEY)
    )

    placeholder_duplicate_rows, placeholder_duplicate_groups = (
        _duplicate_summary(placeholder, PLACEHOLDER_KEY)
    )

    placeholder_status = (
        placeholder["payout_status"]
        .astype("string")
        .str.strip()
        .str.upper()
    )

    status_for_count = placeholder_status.fillna("<NULL>")

    placeholder_status_counts = {
        str(status): int(count)
        for status, count in status_for_count
        .value_counts(dropna=False)
        .items()
    }

    invalid_status = (
        ~placeholder_status.isin(
            ALLOWED_PLACEHOLDER_STATUSES
        )
    ).astype("boolean").fillna(True).astype(bool)

    invalid_placeholder_status_rows = int(
        invalid_status.sum()
    )

    cancelled_or_not_established = (
        placeholder_status.isin(
            {
                "CANCELLED",
                "NOT_ESTABLISHED",
            }
        )
        .astype("boolean")
        .fillna(False)
        .astype(bool)
    )

    invalid_cancelled_payout = (
        cancelled_or_not_established
        & placeholder["payout_yen"].notna()
    )

    invalid_cancelled_payout_rows = int(
        invalid_cancelled_payout.sum()
    )

    special_mask = (
        placeholder_status.eq("SPECIAL_PAYOUT")
        .astype("boolean")
        .fillna(False)
        .astype(bool)
    )

    special_values = pd.to_numeric(
        placeholder.loc[special_mask, "payout_yen"],
        errors="coerce",
    )

    invalid_special_payout_rows = int(
        (
            special_values.isna()
            | special_values.ne(70)
        ).sum()
    )

    payout_races = _race_set(payout)
    program_races: set[tuple[Any, ...]] | None = None

    payout_only_races: set[tuple[Any, ...]] = set()
    program_only_races: set[tuple[Any, ...]] = set()

    if program is not None:
        program_races = _race_set(program)
        payout_only_races = payout_races - program_races
        program_only_races = program_races - payout_races

    checks = {
        "required_payout_columns_present": True,
        "required_program_columns_present": True,
        "combination_has_no_blank_strings": (
            int(combination_blank.sum()) == 0
        ),
        "base_race_keys_non_null": (
            all(
                count == 0
                for count in base_key_null_counts.values()
            )
        ),
        "bet_type_non_null": (
            bet_type_null_count == 0
        ),
        "normal_full_keys_non_null": (
            all(
                count == 0
                for count in normal_key_null_counts.values()
            )
        ),
        "normal_full_keys_unique": (
            normal_duplicate_rows == 0
        ),
        "placeholder_keys_unique": (
            placeholder_duplicate_rows == 0
        ),
        "placeholder_statuses_allowed": (
            invalid_placeholder_status_rows == 0
        ),
        "cancelled_not_established_payout_is_null": (
            invalid_cancelled_payout_rows == 0
        ),
        "special_payout_is_70_yen": (
            invalid_special_payout_rows == 0
        ),
        "payout_has_no_unexpected_races": (
            program is None or not payout_only_races
        ),
        "payout_covers_all_program_races": (
            program is None or not program_only_races
        ),
    }

    failed_checks = [
        name
        for name, passed in checks.items()
        if not passed
    ]

    return {
        "status": (
            "SUCCESS" if not failed_checks else "FAILED"
        ),
        "checks": checks,
        "failed_checks": failed_checks,
        "metrics": {
            "payout_rows": int(len(payout)),
            "normal_payout_rows": int(len(normal)),
            "placeholder_rows": int(len(placeholder)),
            "blank_combination_rows": int(
                combination_blank.sum()
            ),
            "base_key_null_counts": base_key_null_counts,
            "bet_type_null_count": bet_type_null_count,
            "normal_key_null_counts": (
                normal_key_null_counts
            ),
            "normal_duplicate_rows": (
                normal_duplicate_rows
            ),
            "normal_duplicate_groups": (
                normal_duplicate_groups
            ),
            "placeholder_duplicate_rows": (
                placeholder_duplicate_rows
            ),
            "placeholder_duplicate_groups": (
                placeholder_duplicate_groups
            ),
            "placeholder_status_counts": (
                placeholder_status_counts
            ),
            "invalid_placeholder_status_rows": (
                invalid_placeholder_status_rows
            ),
            "invalid_cancelled_or_not_established_payout_rows": (
                invalid_cancelled_payout_rows
            ),
            "invalid_special_payout_rows": (
                invalid_special_payout_rows
            ),
            "payout_race_count": len(payout_races),
            "program_race_count": (
                len(program_races)
                if program_races is not None
                else None
            ),
            "payout_only_race_count": (
                len(payout_only_races)
            ),
            "program_only_race_count": (
                len(program_only_races)
            ),
        },
        "contract": {
            "normal_payout_key": list(NORMAL_PAYOUT_KEY),
            "placeholder_key": list(PLACEHOLDER_KEY),
            "allowed_placeholder_statuses": sorted(
                ALLOWED_PLACEHOLDER_STATUSES
            ),
            "special_payout_yen": 70,
        },
    }


def assert_payout_semantics(
    payout: pd.DataFrame,
    program: pd.DataFrame | None = None,
) -> dict[str, Any]:
    """Audit payout semantics and raise on failure."""

    report = audit_payout_semantics(
        payout=payout,
        program=program,
    )

    if report["status"] != "SUCCESS":
        raise PayoutSemanticAuditError(report)

    return report
