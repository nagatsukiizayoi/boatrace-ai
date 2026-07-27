"""Explicit point-in-time eligibility contract for PRE_NIGHT.

This module converts validation outcomes into a deterministic, fail-closed
eligibility decision. It does not perform collection, prediction, model
execution, scheduling, or live-data access.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Any, Mapping


class PreNightEligibilityStatus(str, Enum):
    """Stable PRE_NIGHT PIT eligibility statuses."""

    ELIGIBLE = "ELIGIBLE"
    SKIPPED_CONTRACT_VERSION_UNSUPPORTED = (
        "SKIPPED_CONTRACT_VERSION_UNSUPPORTED"
    )
    SKIPPED_FETCHED_AFTER_AS_OF = "SKIPPED_FETCHED_AFTER_AS_OF"
    SKIPPED_HASH_MISMATCH = "SKIPPED_HASH_MISMATCH"
    SKIPPED_INVALID_RACE_STRUCTURE = "SKIPPED_INVALID_RACE_STRUCTURE"
    SKIPPED_METADATA_INVALID = "SKIPPED_METADATA_INVALID"
    SKIPPED_OUTPUT_INTEGRITY_ERROR = (
        "SKIPPED_OUTPUT_INTEGRITY_ERROR"
    )
    SKIPPED_PROGRAM_UNAVAILABLE = "SKIPPED_PROGRAM_UNAVAILABLE"
    SKIPPED_PROHIBITED_FEATURE = "SKIPPED_PROHIBITED_FEATURE"
    SKIPPED_PROVENANCE_MISSING = "SKIPPED_PROVENANCE_MISSING"


def _json_value(value: Any) -> Any:
    """Convert supported values into deterministic JSON-safe values."""

    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {
            str(key): _json_value(value[key])
            for key in sorted(value, key=lambda item: str(item))
        }
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


@dataclass(frozen=True)
class PreNightEligibilityDecision:
    """Serializable and fail-closed PRE_NIGHT PIT decision."""

    status: PreNightEligibilityStatus
    eligible: bool
    reason: str
    race_date: str
    as_of_time: str
    details: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        status = self.status
        if not isinstance(status, PreNightEligibilityStatus):
            status = PreNightEligibilityStatus(str(status))
            object.__setattr__(self, "status", status)

        if not isinstance(self.eligible, bool):
            raise TypeError("eligible must be bool")

        expected_eligible = (
            status is PreNightEligibilityStatus.ELIGIBLE
        )
        if self.eligible is not expected_eligible:
            raise ValueError(
                "eligible must be true exactly when status is ELIGIBLE"
            )

        if not str(self.reason).strip():
            raise ValueError("reason must be non-empty")
        if not str(self.race_date).strip():
            raise ValueError("race_date must be non-empty")
        if not str(self.as_of_time).strip():
            raise ValueError("as_of_time must be non-empty")

        normalized_details = _json_value(dict(self.details))
        object.__setattr__(self, "details", normalized_details)

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of_time": str(self.as_of_time),
            "details": _json_value(self.details),
            "eligible": self.eligible,
            "race_date": str(self.race_date),
            "reason": str(self.reason),
            "status": self.status.value,
        }

    def to_json(self) -> str:
        return json.dumps(
            self.to_dict(),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )


def eligible_decision(
    *,
    race_date: str,
    as_of_time: str,
    reason: str = "All PRE_NIGHT PIT checks passed",
    details: Mapping[str, Any] | None = None,
) -> PreNightEligibilityDecision:
    return PreNightEligibilityDecision(
        status=PreNightEligibilityStatus.ELIGIBLE,
        eligible=True,
        reason=reason,
        race_date=str(race_date),
        as_of_time=str(as_of_time),
        details={} if details is None else details,
    )


def skipped_decision(
    *,
    status: PreNightEligibilityStatus,
    reason: str,
    race_date: str,
    as_of_time: str,
    details: Mapping[str, Any] | None = None,
) -> PreNightEligibilityDecision:
    status = PreNightEligibilityStatus(status)
    if status is PreNightEligibilityStatus.ELIGIBLE:
        raise ValueError("skipped_decision cannot use ELIGIBLE")
    return PreNightEligibilityDecision(
        status=status,
        eligible=False,
        reason=reason,
        race_date=str(race_date),
        as_of_time=str(as_of_time),
        details={} if details is None else details,
    )


_TEXT_RULES: tuple[
    tuple[PreNightEligibilityStatus, tuple[str, ...]], ...
] = (
    (
        PreNightEligibilityStatus.SKIPPED_CONTRACT_VERSION_UNSUPPORTED,
        (
            "contract version",
            "contract_version",
            "unsupported contract",
            "version unsupported",
        ),
    ),
    (
        PreNightEligibilityStatus.SKIPPED_FETCHED_AFTER_AS_OF,
        (
            "fetched after",
            "after as_of",
            "after as of",
            "future source",
            "future_source",
            "late live run",
        ),
    ),
    (
        PreNightEligibilityStatus.SKIPPED_HASH_MISMATCH,
        (
            "hash mismatch",
            "sha256 mismatch",
            "sha-256 mismatch",
            "response_sha256 mismatch",
        ),
    ),
    (
        PreNightEligibilityStatus.SKIPPED_PROVENANCE_MISSING,
        (
            "provenance missing",
            "missing provenance",
            "provenance_missing",
            "sidecar missing",
            "missing sidecar",
        ),
    ),
    (
        PreNightEligibilityStatus.SKIPPED_INVALID_RACE_STRUCTURE,
        (
            "invalid race structure",
            "race structure",
            "six boats",
            "6 boats",
            "six racers",
            "duplicate boat",
            "duplicate racer",
        ),
    ),
    (
        PreNightEligibilityStatus.SKIPPED_PROHIBITED_FEATURE,
        (
            "prohibited feature",
            "forbidden feature",
            "feature contract",
            "result feature",
            "payout feature",
            "odds feature",
            "exhibition feature",
        ),
    ),
    (
        PreNightEligibilityStatus.SKIPPED_PROGRAM_UNAVAILABLE,
        (
            "program unavailable",
            "program not found",
            "archive unavailable",
            "source unavailable",
            "download failed",
            "filenotfounderror",
        ),
    ),
    (
        PreNightEligibilityStatus.SKIPPED_OUTPUT_INTEGRITY_ERROR,
        (
            "output integrity",
            "output_integrity",
            "write integrity",
            "manifest integrity",
        ),
    ),
    (
        PreNightEligibilityStatus.SKIPPED_METADATA_INVALID,
        (
            "metadata invalid",
            "invalid metadata",
            "timezone",
            "naive datetime",
            "race date mismatch",
            "source type mismatch",
        ),
    ),
)


def classify_pre_night_exception(
    error: BaseException,
) -> PreNightEligibilityStatus:
    """Map a validation error to a stable status.

    The mapping is deliberately fail-closed. An unknown exception is
    classified as an output-integrity error and is never eligible.
    """

    if not isinstance(error, BaseException):
        raise TypeError("error must be an exception")

    explicit_status = getattr(error, "eligibility_status", None)
    if explicit_status is None:
        explicit_status = getattr(error, "status", None)

    if explicit_status is not None:
        try:
            status = PreNightEligibilityStatus(explicit_status)
        except (TypeError, ValueError):
            status = None
        if (
            status is not None
            and status is not PreNightEligibilityStatus.ELIGIBLE
        ):
            return status

    if isinstance(error, FileNotFoundError):
        return PreNightEligibilityStatus.SKIPPED_PROGRAM_UNAVAILABLE

    searchable = (
        f"{type(error).__module__}."
        f"{type(error).__name__}: {error}"
    ).casefold()

    for status, terms in _TEXT_RULES:
        if any(term.casefold() in searchable for term in terms):
            return status

    return PreNightEligibilityStatus.SKIPPED_OUTPUT_INTEGRITY_ERROR


def decision_from_exception(
    error: BaseException,
    *,
    race_date: str,
    as_of_time: str,
    details: Mapping[str, Any] | None = None,
) -> PreNightEligibilityDecision:
    status = classify_pre_night_exception(error)

    merged_details = {
        "exception_module": type(error).__module__,
        "exception_type": type(error).__name__,
    }
    if details:
        merged_details.update(dict(details))

    return skipped_decision(
        status=status,
        reason=str(error).strip() or type(error).__name__,
        race_date=str(race_date),
        as_of_time=str(as_of_time),
        details=merged_details,
    )


def manifest_eligibility_fields(
    decision: PreNightEligibilityDecision,
) -> dict[str, Any]:
    """Return stable fields suitable for a dry-run/live-run manifest."""

    if not isinstance(decision, PreNightEligibilityDecision):
        raise TypeError(
            "decision must be PreNightEligibilityDecision"
        )

    return {
        "eligibility_status": decision.status.value,
        "eligible_for_pre_night": decision.eligible,
        "eligibility_reason": decision.reason,
        "pit_eligibility": decision.to_dict(),
    }


__all__ = [
    "PreNightEligibilityDecision",
    "PreNightEligibilityStatus",
    "classify_pre_night_exception",
    "decision_from_exception",
    "eligible_decision",
    "manifest_eligibility_fields",
    "skipped_decision",
]
