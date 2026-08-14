"""Pure source-agnostic pre-night deadline-evidence contract.

This module intentionally performs no network access and no filesystem
publication. Acquisition and pipeline integration are outside D1-A.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timezone
from typing import Any


CONTRACT_VERSION = "pre_night_deadline_evidence_v1"
DEADLINE_KIND = "OFFICIAL_SCHEDULED_BETTING_CLOSE"

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_RACE_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_VENUE_CODE_RE = re.compile(r"^\d{2}$")

_TOP_LEVEL_FIELDS = frozenset(
    {
        "contract_version",
        "source_name",
        "source_authority",
        "source_locator",
        "source_timezone",
        "request_started_at",
        "fetched_at",
        "http_status",
        "response_headers",
        "raw_source_sha256",
        "race_date",
        "venue_code",
        "race_deadlines",
    }
)

_RACE_DEADLINE_FIELDS = frozenset(
    {
        "race_no",
        "deadline_kind",
        "scheduled_deadline_at",
    }
)


class PreNightDeadlineEvidenceError(ValueError):
    """Base fail-closed D1-A deadline-evidence error."""


class PreNightDeadlineSchemaError(PreNightDeadlineEvidenceError):
    """Raised for missing, unknown or structurally invalid fields."""


class PreNightDeadlineTimestampError(PreNightDeadlineEvidenceError):
    """Raised for invalid, naive or incorrectly ordered timestamps."""


class PreNightDeadlineIdentityError(PreNightDeadlineEvidenceError):
    """Raised for invalid or duplicate race identities."""


class PreNightDeadlineIntegrityError(PreNightDeadlineEvidenceError):
    """Raised for raw-source integrity failures."""


def _require_exact_fields(
    value: Mapping[str, Any],
    expected: frozenset[str],
    *,
    context: str,
) -> None:
    actual = set(value)
    missing = sorted(expected - actual)
    unknown = sorted(actual - expected)

    if missing or unknown:
        raise PreNightDeadlineSchemaError(
            f"{context} field mismatch: "
            f"missing={missing}, unknown={unknown}"
        )


def _require_text(value: Any, *, field: str) -> str:
    if not isinstance(value, str):
        raise PreNightDeadlineSchemaError(
            f"{field} must be a string"
        )

    if not value:
        raise PreNightDeadlineSchemaError(
            f"{field} must not be empty"
        )

    if value != value.strip():
        raise PreNightDeadlineSchemaError(
            f"{field} must not contain leading or trailing whitespace"
        )

    return value


def _parse_aware_timestamp(
    value: Any,
    *,
    field: str,
) -> datetime:
    if isinstance(value, datetime):
        parsed = value

    elif isinstance(value, str):
        text = _require_text(value, field=field)

        if text.endswith("Z"):
            text = text[:-1] + "+00:00"

        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise PreNightDeadlineTimestampError(
                f"{field} must be a valid ISO-8601 timestamp"
            ) from exc

    else:
        raise PreNightDeadlineTimestampError(
            f"{field} must be datetime or ISO-8601 string"
        )

    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise PreNightDeadlineTimestampError(
            f"{field} must be timezone-aware"
        )

    return parsed


def _format_utc_timestamp(value: datetime) -> str:
    normalized = value.astimezone(timezone.utc)

    if normalized.microsecond:
        text = normalized.isoformat(
            timespec="microseconds"
        )
    else:
        text = normalized.isoformat(
            timespec="seconds"
        )

    # normalized is UTC, so isoformat must end with +00:00.
    # Use slicing instead of str.replace so this pure string operation
    # cannot be confused with filesystem replacement.
    if not text.endswith("+00:00"):
        raise PreNightDeadlineTimestampError(
            "internal UTC timestamp normalization failed"
        )

    return text[:-6] + "Z"


def _normalize_race_date(value: Any) -> str:
    if not isinstance(value, str):
        raise PreNightDeadlineIdentityError(
            "race_date must be a string"
        )

    if not _RACE_DATE_RE.fullmatch(value):
        raise PreNightDeadlineIdentityError(
            "race_date must use strict YYYY-MM-DD format"
        )

    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise PreNightDeadlineIdentityError(
            "race_date is not a valid calendar date"
        ) from exc

    if parsed.isoformat() != value:
        raise PreNightDeadlineIdentityError(
            "race_date is not canonical"
        )

    return value


def _normalize_venue_code(value: Any) -> str:
    if not isinstance(value, str):
        raise PreNightDeadlineIdentityError(
            "venue_code must be a string"
        )

    if not _VENUE_CODE_RE.fullmatch(value):
        raise PreNightDeadlineIdentityError(
            "venue_code must be exactly two decimal digits"
        )

    return value


def _normalize_race_no(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise PreNightDeadlineIdentityError(
            "race_no must be an integer and must not be bool"
        )

    if not 1 <= value <= 12:
        raise PreNightDeadlineIdentityError(
            "race_no must be between 1 and 12"
        )

    return value


def _normalize_http_status(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise PreNightDeadlineSchemaError(
            "http_status must be an integer"
        )

    if not 200 <= value <= 299:
        raise PreNightDeadlineSchemaError(
            "http_status must be in the 200-299 range"
        )

    return value


def _normalize_headers(value: Any) -> dict[str, str]:
    if not isinstance(value, Mapping):
        raise PreNightDeadlineSchemaError(
            "response_headers must be a mapping"
        )

    output: dict[str, str] = {}
    casefolded_keys: set[str] = set()

    for raw_key, raw_value in value.items():
        key = _require_text(
            raw_key,
            field="response_headers key",
        )

        if not isinstance(raw_value, str):
            raise PreNightDeadlineSchemaError(
                f"response_headers[{key!r}] must be a string"
            )

        if raw_value != raw_value.strip():
            raise PreNightDeadlineSchemaError(
                f"response_headers[{key!r}] must not contain "
                "leading or trailing whitespace"
            )

        folded = key.casefold()

        if folded in casefolded_keys:
            raise PreNightDeadlineSchemaError(
                "response_headers contains duplicate "
                "case-insensitive keys"
            )

        casefolded_keys.add(folded)
        output[key] = raw_value

    return dict(sorted(output.items()))


def _normalize_sha256(value: Any) -> str:
    if not isinstance(value, str):
        raise PreNightDeadlineIntegrityError(
            "raw_source_sha256 must be a string"
        )

    if not _SHA256_RE.fullmatch(value):
        raise PreNightDeadlineIntegrityError(
            "raw_source_sha256 must be 64 lowercase hexadecimal characters"
        )

    return value


def _normalize_race_deadlines(
    value: Any,
    *,
    fetched_at: datetime,
) -> list[dict[str, Any]]:
    if (
        isinstance(value, (str, bytes, bytearray))
        or not isinstance(value, Sequence)
    ):
        raise PreNightDeadlineSchemaError(
            "race_deadlines must be a sequence of mappings"
        )

    normalized: list[dict[str, Any]] = []
    seen: set[int] = set()

    for index, item in enumerate(value):
        if not isinstance(item, Mapping):
            raise PreNightDeadlineSchemaError(
                f"race_deadlines[{index}] must be a mapping"
            )

        _require_exact_fields(
            item,
            _RACE_DEADLINE_FIELDS,
            context=f"race_deadlines[{index}]",
        )

        race_no = _normalize_race_no(item["race_no"])

        if race_no in seen:
            raise PreNightDeadlineIdentityError(
                f"duplicate race_no: {race_no}"
            )

        seen.add(race_no)

        deadline_kind = _require_text(
            item["deadline_kind"],
            field=f"race_deadlines[{index}].deadline_kind",
        )

        if deadline_kind != DEADLINE_KIND:
            raise PreNightDeadlineSchemaError(
                f"race_deadlines[{index}].deadline_kind "
                f"must equal {DEADLINE_KIND}"
            )

        scheduled = _parse_aware_timestamp(
            item["scheduled_deadline_at"],
            field=(
                f"race_deadlines[{index}]."
                "scheduled_deadline_at"
            ),
        )

        if scheduled <= fetched_at:
            raise PreNightDeadlineTimestampError(
                f"race {race_no} scheduled deadline "
                "must be later than fetched_at"
            )

        normalized.append(
            {
                "race_no": race_no,
                "deadline_kind": DEADLINE_KIND,
                "scheduled_deadline_at": (
                    _format_utc_timestamp(scheduled)
                ),
            }
        )

    expected = set(range(1, 13))

    if seen != expected:
        missing = sorted(expected - seen)
        unexpected = sorted(seen - expected)

        raise PreNightDeadlineIdentityError(
            "race_deadlines must contain exactly 1R-12R: "
            f"missing={missing}, unexpected={unexpected}"
        )

    return sorted(
        normalized,
        key=lambda item: item["race_no"],
    )


def validate_deadline_evidence(
    evidence: Mapping[str, Any],
    *,
    raw_source_bytes: bytes | None = None,
) -> dict[str, Any]:
    """Validate and normalize a deadline-evidence mapping."""

    if not isinstance(evidence, Mapping):
        raise PreNightDeadlineSchemaError(
            "evidence must be a mapping"
        )

    _require_exact_fields(
        evidence,
        _TOP_LEVEL_FIELDS,
        context="evidence",
    )

    contract_version = _require_text(
        evidence["contract_version"],
        field="contract_version",
    )

    if contract_version != CONTRACT_VERSION:
        raise PreNightDeadlineSchemaError(
            f"contract_version must equal {CONTRACT_VERSION}"
        )

    source_name = _require_text(
        evidence["source_name"],
        field="source_name",
    )

    source_authority = _require_text(
        evidence["source_authority"],
        field="source_authority",
    )

    source_locator = _require_text(
        evidence["source_locator"],
        field="source_locator",
    )

    # This is explicit caller-supplied evidence. No default or inference
    # such as Asia/Tokyo is performed here.
    source_timezone = _require_text(
        evidence["source_timezone"],
        field="source_timezone",
    )

    request_started = _parse_aware_timestamp(
        evidence["request_started_at"],
        field="request_started_at",
    )

    fetched = _parse_aware_timestamp(
        evidence["fetched_at"],
        field="fetched_at",
    )

    if request_started > fetched:
        raise PreNightDeadlineTimestampError(
            "request_started_at must be less than "
            "or equal to fetched_at"
        )

    http_status = _normalize_http_status(
        evidence["http_status"]
    )

    response_headers = _normalize_headers(
        evidence["response_headers"]
    )

    recorded_sha256 = _normalize_sha256(
        evidence["raw_source_sha256"]
    )

    if raw_source_bytes is not None:
        if not isinstance(raw_source_bytes, bytes):
            raise PreNightDeadlineIntegrityError(
                "raw_source_bytes must be bytes"
            )

        if not raw_source_bytes:
            raise PreNightDeadlineIntegrityError(
                "raw_source_bytes must not be empty"
            )

        actual_sha256 = hashlib.sha256(
            raw_source_bytes
        ).hexdigest()

        if actual_sha256 != recorded_sha256:
            raise PreNightDeadlineIntegrityError(
                "raw_source_sha256 does not match "
                "the exact raw source bytes"
            )

    race_date = _normalize_race_date(
        evidence["race_date"]
    )

    venue_code = _normalize_venue_code(
        evidence["venue_code"]
    )

    race_deadlines = _normalize_race_deadlines(
        evidence["race_deadlines"],
        fetched_at=fetched,
    )

    return {
        "contract_version": CONTRACT_VERSION,
        "source_name": source_name,
        "source_authority": source_authority,
        "source_locator": source_locator,
        "source_timezone": source_timezone,
        "request_started_at": (
            _format_utc_timestamp(request_started)
        ),
        "fetched_at": _format_utc_timestamp(fetched),
        "http_status": http_status,
        "response_headers": response_headers,
        "raw_source_sha256": recorded_sha256,
        "race_date": race_date,
        "venue_code": venue_code,
        "race_deadlines": race_deadlines,
    }


def build_deadline_evidence(
    *,
    raw_source_bytes: bytes,
    source_locator: str,
    source_name: str,
    source_authority: str,
    request_started_at: datetime | str,
    fetched_at: datetime | str,
    http_status: int,
    response_headers: Mapping[str, str],
    race_date: str,
    venue_code: str,
    source_timezone: str,
    race_deadlines: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Build validated deadline evidence from already-acquired data."""

    if not isinstance(raw_source_bytes, bytes):
        raise PreNightDeadlineIntegrityError(
            "raw_source_bytes must be bytes"
        )

    if not raw_source_bytes:
        raise PreNightDeadlineIntegrityError(
            "raw_source_bytes must not be empty"
        )

    evidence = {
        "contract_version": CONTRACT_VERSION,
        "source_name": source_name,
        "source_authority": source_authority,
        "source_locator": source_locator,
        "source_timezone": source_timezone,
        "request_started_at": request_started_at,
        "fetched_at": fetched_at,
        "http_status": http_status,
        "response_headers": response_headers,
        "raw_source_sha256": hashlib.sha256(
            raw_source_bytes
        ).hexdigest(),
        "race_date": race_date,
        "venue_code": venue_code,
        "race_deadlines": race_deadlines,
    }

    return validate_deadline_evidence(
        evidence,
        raw_source_bytes=raw_source_bytes,
    )


def canonical_deadline_evidence_bytes(
    evidence: Mapping[str, Any],
) -> bytes:
    """Return deterministic UTF-8 canonical JSON with one trailing LF."""

    normalized = validate_deadline_evidence(evidence)

    text = json.dumps(
        normalized,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )

    return (text + "\n").encode("utf-8")
