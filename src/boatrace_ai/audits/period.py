"""Cross-month semantic audits for normalized race data.

This module operates on already loaded pandas DataFrames. It does not
discover files, run ETL, or modify source artifacts.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

import pandas as pd

from .payout_semantics import audit_payout_semantics


RACE_KEY = ["race_date", "venue_code", "race_no"]
ENTRY_KEY = RACE_KEY + ["boat_no"]
NORMAL_PAYOUT_KEY = RACE_KEY + [
    "bet_type",
    "combination",
]
PLACEHOLDER_KEY = RACE_KEY + ["bet_type"]

EXPECTED_BET_TYPES = {
    "WIN",
    "PLACE",
    "EXACTA",
    "QUINELLA",
    "QUINELLA_PLACE",
    "TRIFECTA",
    "TRIO",
}

REQUIRED_FRAMES = {
    "program",
    "result",
    "merged",
    "payout",
}

REQUIRED_COLUMNS = {
    "program": set(ENTRY_KEY),
    "result": set(ENTRY_KEY),
    "merged": set(ENTRY_KEY),
    "payout": set(
        NORMAL_PAYOUT_KEY
        + ["payout_status", "payout_yen"]
    ),
}

MONTH_PATTERN = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


class PeriodAuditError(AssertionError):
    """Raised when a period semantic audit fails."""

    def __init__(self, report: Mapping[str, Any]):
        self.report = dict(report)
        count = len(self.report.get("failed_checks", []))
        super().__init__(
            f"Period semantic audit failed with "
            f"{count} failed check(s)"
        )


def _add_failure(
    failures: list[dict[str, Any]],
    *,
    scope: str,
    check: str,
    expected: Any = None,
    actual: Any = None,
    details: Any = None,
) -> None:
    item: dict[str, Any] = {
        "scope": scope,
        "check": check,
    }

    if expected is not None:
        item["expected"] = expected

    if actual is not None:
        item["actual"] = actual

    if details is not None:
        item["details"] = details

    failures.append(item)


def _blank_mask(series: pd.Series) -> pd.Series:
    values = series.astype("string")
    return values.notna() & values.str.strip().eq("")


def _blank_cell_count(
    frame: pd.DataFrame,
    columns: list[str],
) -> int:
    count = 0

    for column in columns:
        if column not in frame.columns:
            continue

        # Keep sum() inside int(). This also works for several rows.
        count += int(_blank_mask(frame[column]).sum())

    return count


def _null_cell_count(
    frame: pd.DataFrame,
    columns: list[str],
) -> int:
    existing = [
        column
        for column in columns
        if column in frame.columns
    ]

    if not existing:
        return 0

    return int(frame[existing].isna().sum().sum())


def _duplicate_row_count(
    frame: pd.DataFrame,
    keys: list[str],
) -> int:
    if frame.empty:
        return 0

    return int(
        frame.duplicated(keys, keep=False).sum()
    )


def _race_set(
    frame: pd.DataFrame,
) -> set[tuple[str, str, int]]:
    if frame.empty:
        return set()

    work = frame[RACE_KEY].copy()

    work["race_date"] = (
        work["race_date"].astype("string").str.strip()
    )
    work["venue_code"] = (
        work["venue_code"].astype("string").str.strip()
    )
    work["race_no"] = pd.to_numeric(
        work["race_no"],
        errors="coerce",
    ).astype("Int64")

    work = work.dropna().drop_duplicates()

    return {
        (
            str(row.race_date),
            str(row.venue_code),
            int(row.race_no),
        )
        for row in work.itertuples(index=False)
    }


def _normalize_month_payload(
    payload: Mapping[str, pd.DataFrame],
) -> dict[str, pd.DataFrame]:
    return {
        name: payload[name].copy(deep=False)
        for name in REQUIRED_FRAMES
    }


def audit_period(
    months: Mapping[
        str,
        Mapping[str, pd.DataFrame],
    ],
) -> dict[str, Any]:
    """Audit normalized DataFrames across one or more months.

    Parameters
    ----------
    months:
        Mapping of ``YYYY-MM`` labels to mappings containing
        ``program``, ``result``, ``merged`` and ``payout``
        DataFrames.

    Returns
    -------
    dict
        A JSON-compatible report with monthly metrics,
        cross-period metrics and failed checks.
    """

    failures: list[dict[str, Any]] = []
    monthly_reports: dict[str, dict[str, Any]] = {}

    if not isinstance(months, Mapping) or not months:
        return {
            "status": "FAILED",
            "classification": "PERIOD_AUDIT_FAILED",
            "month_count": 0,
            "monthly_audits": {},
            "cross_period_metrics": {},
            "failed_checks": [{
                "scope": "period",
                "check": "months_non_empty_mapping",
                "expected": True,
                "actual": False,
            }],
        }

    normalized_months: dict[
        str,
        dict[str, pd.DataFrame],
    ] = {}

    for month, payload in months.items():
        if not isinstance(month, str) or not MONTH_PATTERN.fullmatch(
            month
        ):
            _add_failure(
                failures,
                scope=str(month),
                check="month_label_format",
                expected="YYYY-MM",
                actual=month,
            )
            continue

        if not isinstance(payload, Mapping):
            _add_failure(
                failures,
                scope=month,
                check="month_payload_mapping",
                expected=True,
                actual=False,
            )
            continue

        missing_frames = sorted(
            REQUIRED_FRAMES - set(payload)
        )

        if missing_frames:
            _add_failure(
                failures,
                scope=month,
                check="required_frames",
                expected=sorted(REQUIRED_FRAMES),
                actual=sorted(payload),
                details={"missing": missing_frames},
            )
            continue

        non_dataframes = sorted(
            name
            for name in REQUIRED_FRAMES
            if not isinstance(payload[name], pd.DataFrame)
        )

        if non_dataframes:
            _add_failure(
                failures,
                scope=month,
                check="frame_types",
                expected="pandas.DataFrame",
                actual=non_dataframes,
            )
            continue

        normalized_months[month] = _normalize_month_payload(
            payload
        )

    all_frames: dict[str, list[pd.DataFrame]] = {
        name: [] for name in REQUIRED_FRAMES
    }

    for month in sorted(normalized_months):
        frames = normalized_months[month]
        month_failures: list[dict[str, Any]] = []

        schema_ok = True

        for name, required in REQUIRED_COLUMNS.items():
            missing = sorted(
                required - set(frames[name].columns)
            )

            if missing:
                schema_ok = False
                _add_failure(
                    month_failures,
                    scope=month,
                    check=f"{name}_required_columns",
                    expected=sorted(required),
                    actual=sorted(frames[name].columns),
                    details={"missing": missing},
                )

        if not schema_ok:
            failures.extend(month_failures)
            monthly_reports[month] = {
                "status": "FAILED",
                "failed_checks": month_failures,
            }
            continue

        program = frames["program"]
        result = frames["result"]
        merged = frames["merged"]
        payout = frames["payout"]

        combination = payout["combination"]
        blank_combination = _blank_mask(combination)
        placeholder_mask = combination.isna()
        normal_mask = (
            combination.notna()
            & ~blank_combination
        )

        normal = payout.loc[normal_mask]
        placeholder = payout.loc[placeholder_mask]

        key_metrics = {
            "program_null_key_cells":
                _null_cell_count(program, ENTRY_KEY),
            "program_blank_key_cells":
                _blank_cell_count(program, ENTRY_KEY),
            "program_duplicate_rows":
                _duplicate_row_count(program, ENTRY_KEY),
            "result_null_key_cells":
                _null_cell_count(result, ENTRY_KEY),
            "result_blank_key_cells":
                _blank_cell_count(result, ENTRY_KEY),
            "result_duplicate_rows":
                _duplicate_row_count(result, ENTRY_KEY),
            "merged_null_key_cells":
                _null_cell_count(merged, ENTRY_KEY),
            "merged_blank_key_cells":
                _blank_cell_count(merged, ENTRY_KEY),
            "merged_duplicate_rows":
                _duplicate_row_count(merged, ENTRY_KEY),
            "normal_payout_null_key_cells":
                _null_cell_count(
                    normal,
                    NORMAL_PAYOUT_KEY,
                ),
            "normal_payout_blank_key_cells":
                _blank_cell_count(
                    normal,
                    NORMAL_PAYOUT_KEY,
                ),
            "normal_payout_duplicate_rows":
                _duplicate_row_count(
                    normal,
                    NORMAL_PAYOUT_KEY,
                ),
            "placeholder_null_base_key_cells":
                _null_cell_count(
                    placeholder,
                    PLACEHOLDER_KEY,
                ),
            "placeholder_blank_base_key_cells":
                _blank_cell_count(
                    placeholder,
                    PLACEHOLDER_KEY,
                ),
            "placeholder_duplicate_rows":
                _duplicate_row_count(
                    placeholder,
                    PLACEHOLDER_KEY,
                ),
            "blank_combination_rows":
                int(blank_combination.sum()),
        }

        for check, actual in key_metrics.items():
            if actual:
                _add_failure(
                    month_failures,
                    scope=month,
                    check=check,
                    expected=0,
                    actual=actual,
                )

        program_races = _race_set(program)
        result_races = _race_set(result)
        merged_races = _race_set(merged)
        payout_races = _race_set(payout)

        program_only = program_races - payout_races
        payout_only = payout_races - program_races

        if program_only:
            _add_failure(
                month_failures,
                scope=month,
                check="program_only_races",
                expected=0,
                actual=len(program_only),
                details=sorted(program_only)[:10],
            )

        if payout_only:
            _add_failure(
                month_failures,
                scope=month,
                check="payout_only_races",
                expected=0,
                actual=len(payout_only),
                details=sorted(payout_only)[:10],
            )

        if program_races != merged_races:
            _add_failure(
                month_failures,
                scope=month,
                check="program_merged_race_set",
                expected=len(program_races),
                actual=len(merged_races),
            )

        groups = payout[
            RACE_KEY + ["bet_type"]
        ].copy()

        groups["bet_type"] = (
            groups["bet_type"]
            .astype("string")
            .str.strip()
            .str.upper()
        )
        groups = groups.drop_duplicates()

        actual_bet_types = set(
            groups["bet_type"].dropna().astype(str)
        )

        if actual_bet_types != EXPECTED_BET_TYPES:
            _add_failure(
                month_failures,
                scope=month,
                check="bet_type_set",
                expected=sorted(EXPECTED_BET_TYPES),
                actual=sorted(actual_bet_types),
            )

        per_race_groups = (
            groups.groupby(
                RACE_KEY,
                dropna=False,
            ).size()
        )

        invalid_group_counts = int(
            per_race_groups.ne(7).sum()
        )

        if invalid_group_counts:
            _add_failure(
                month_failures,
                scope=month,
                check="races_without_exactly_7_bet_types",
                expected=0,
                actual=invalid_group_counts,
            )

        semantic_result = audit_payout_semantics(
            payout=payout,
            program=program,
        )

        semantic_status = semantic_result.get("status")
        semantic_failures = semantic_result.get(
            "failed_checks", []
        )

        if semantic_status != "SUCCESS" or semantic_failures:
            _add_failure(
                month_failures,
                scope=month,
                check="payout_semantic_audit",
                expected="SUCCESS",
                actual=semantic_status,
                details=semantic_failures,
            )

        month_status = (
            "SUCCESS"
            if not month_failures
            else "FAILED"
        )

        monthly_reports[month] = {
            "status": month_status,
            "rows": {
                name: len(frame)
                for name, frame in frames.items()
            },
            "program_race_count": len(program_races),
            "result_race_count": len(result_races),
            "merged_race_count": len(merged_races),
            "payout_race_count": len(payout_races),
            "normal_payout_rows": len(normal),
            "placeholder_rows": len(placeholder),
            "payout_group_count": len(groups),
            "key_metrics": key_metrics,
            "payout_semantic_status": semantic_status,
            "failed_checks": month_failures,
        }

        failures.extend(month_failures)

        for name, frame in frames.items():
            all_frames[name].append(frame)

    combined = {
        name: (
            pd.concat(
                frames,
                ignore_index=True,
                sort=False,
            )
            if frames
            else pd.DataFrame()
        )
        for name, frames in all_frames.items()
    }

    cross_metrics: dict[str, Any] = {
        "month_count": len(normalized_months),
    }

    if all(combined[name].shape[1] for name in REQUIRED_FRAMES):
        payout = combined["payout"]
        combination = payout["combination"]
        blank_combination = _blank_mask(combination)

        normal = payout.loc[
            combination.notna()
            & ~blank_combination
        ]
        placeholder = payout.loc[
            combination.isna()
        ]

        cross_metrics.update({
            "program_rows": len(combined["program"]),
            "result_rows": len(combined["result"]),
            "merged_rows": len(combined["merged"]),
            "payout_rows": len(payout),
            "normal_payout_rows": len(normal),
            "placeholder_rows": len(placeholder),
            "program_race_count":
                len(_race_set(combined["program"])),
            "result_race_count":
                len(_race_set(combined["result"])),
            "payout_race_count":
                len(_race_set(payout)),
            "program_duplicate_rows":
                _duplicate_row_count(
                    combined["program"],
                    ENTRY_KEY,
                ),
            "result_duplicate_rows":
                _duplicate_row_count(
                    combined["result"],
                    ENTRY_KEY,
                ),
            "merged_duplicate_rows":
                _duplicate_row_count(
                    combined["merged"],
                    ENTRY_KEY,
                ),
            "normal_payout_duplicate_rows":
                _duplicate_row_count(
                    normal,
                    NORMAL_PAYOUT_KEY,
                ),
            "placeholder_duplicate_rows":
                _duplicate_row_count(
                    placeholder,
                    PLACEHOLDER_KEY,
                ),
            "blank_combination_rows":
                int(blank_combination.sum()),
        })

        for check in (
            "program_duplicate_rows",
            "result_duplicate_rows",
            "merged_duplicate_rows",
            "normal_payout_duplicate_rows",
            "placeholder_duplicate_rows",
            "blank_combination_rows",
        ):
            actual = cross_metrics[check]

            if actual:
                _add_failure(
                    failures,
                    scope="period",
                    check=check,
                    expected=0,
                    actual=actual,
                )

    status = "SUCCESS" if not failures else "FAILED"

    return {
        "status": status,
        "classification": (
            "PERIOD_AUDIT_SUCCESS"
            if status == "SUCCESS"
            else "PERIOD_AUDIT_FAILED"
        ),
        "month_count": len(normalized_months),
        "months": sorted(normalized_months),
        "monthly_audits": monthly_reports,
        "cross_period_metrics": cross_metrics,
        "failed_checks": failures,
    }


def assert_period_audit(
    months: Mapping[
        str,
        Mapping[str, pd.DataFrame],
    ],
) -> dict[str, Any]:
    """Run :func:`audit_period` and raise on failure."""

    report = audit_period(months)

    if report["status"] != "SUCCESS":
        raise PeriodAuditError(report)

    return report
