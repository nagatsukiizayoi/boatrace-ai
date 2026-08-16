"""Pre-night authorization artifact generator.

This module generates authorization artifacts only. It never starts a
race pipeline, publishes a manifest, invokes live execution, or performs
Git operations.

Approval is not inferred from deadline evidence. A human reviewer must
supply the exact confirmation phrase.
"""

from __future__ import annotations

import hashlib
import json
import os
import pathlib
import re
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence


CONFIRMATION_TEMPLATE = (
    "CONFIRM DEADLINES {race_date} "
    "VENUE {venue_code} 1R-12R MATCH"
)

EXPECTED_RACES = tuple(range(1, 13))
HHMM_PATTERN = re.compile(
    r"^(?:[01]\d|2[0-3]):[0-5]\d$"
)


class AuthorizationContractError(ValueError):
    """Raised when authorization inputs violate the contract."""


@dataclass(frozen=True)
class GeneratedArtifacts:
    """Paths and SHA-256 hashes of generated artifacts."""

    authorization_path: pathlib.Path
    test_state_path: pathlib.Path
    receipt_path: pathlib.Path
    authorization_sha256: str
    test_state_sha256: str
    receipt_sha256: str


def sha256_file(path: pathlib.Path) -> str:
    """Return the SHA-256 digest of a file."""

    return hashlib.sha256(path.read_bytes()).hexdigest()


def _normalized_venue_code(value: str) -> str:
    venue_code = str(value).strip()

    if not venue_code.isdigit():
        raise AuthorizationContractError(
            "venue_code must contain decimal digits"
        )

    venue_code = venue_code.zfill(2)

    if len(venue_code) != 2:
        raise AuthorizationContractError(
            "venue_code must normalize to two digits"
        )

    return venue_code


def _parse_race_date(value: str) -> str:
    try:
        parsed = datetime.strptime(
            value,
            "%Y-%m-%d",
        )
    except ValueError as exc:
        raise AuthorizationContractError(
            "race_date must use YYYY-MM-DD"
        ) from exc

    return parsed.strftime("%Y-%m-%d")


def _deadline_minutes(value: str) -> int:
    if not isinstance(value, str):
        raise AuthorizationContractError(
            "deadline must be a string"
        )

    if not HHMM_PATTERN.fullmatch(value):
        raise AuthorizationContractError(
            f"invalid HH:MM deadline: {value!r}"
        )

    hour, minute = map(int, value.split(":"))
    return hour * 60 + minute


def validate_deadlines(
    deadlines: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Validate complete, unique, increasing 1R-12R deadlines."""

    if len(deadlines) != 12:
        raise AuthorizationContractError(
            "exactly 12 deadline records are required"
        )

    normalized: list[dict[str, Any]] = []

    for record in deadlines:
        if not isinstance(record, Mapping):
            raise AuthorizationContractError(
                "each deadline record must be a mapping"
            )

        if set(record) != {"race", "deadline"}:
            raise AuthorizationContractError(
                "each deadline record must contain exactly "
                "'race' and 'deadline'"
            )

        race = record["race"]

        if isinstance(race, bool) or not isinstance(race, int):
            raise AuthorizationContractError(
                "race must be an integer"
            )

        deadline = record["deadline"]
        minutes = _deadline_minutes(deadline)

        normalized.append(
            {
                "race": race,
                "deadline": deadline,
                "_minutes": minutes,
            }
        )

    races = tuple(item["race"] for item in normalized)

    if races != EXPECTED_RACES:
        raise AuthorizationContractError(
            "races must appear exactly once in 1R-12R order"
        )

    intervals: list[int] = []

    for previous, current in zip(
        normalized,
        normalized[1:],
    ):
        interval = (
            current["_minutes"] - previous["_minutes"]
        )

        if interval <= 0:
            raise AuthorizationContractError(
                "deadlines must be strictly increasing"
            )

        if not 5 <= interval <= 90:
            raise AuthorizationContractError(
                "each inter-race interval must be "
                "between 5 and 90 minutes"
            )

        intervals.append(interval)

    return [
        {
            "race": item["race"],
            "deadline": item["deadline"],
        }
        for item in normalized
    ]


def validate_test_state(
    test_state: Mapping[str, Any],
) -> dict[str, str]:
    """Validate the repository-defined pre-night test state."""

    if not isinstance(test_state, Mapping):
        raise AuthorizationContractError(
            "test_state must be a mapping"
        )

    expected = {
        "focused": "PASSED",
        "full_suite": "PASSED",
    }

    if dict(test_state) != expected:
        raise AuthorizationContractError(
            "test_state must exactly equal "
            "{'focused': 'PASSED', "
            "'full_suite': 'PASSED'}"
        )

    return dict(expected)


def _atomic_write_json(
    path: pathlib.Path,
    payload: Mapping[str, Any],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    data = (
        json.dumps(
            payload,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")

    file_descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
    )

    temporary_path = pathlib.Path(temporary_name)

    try:
        with os.fdopen(file_descriptor, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())

        os.replace(temporary_path, path)

    except Exception:
        temporary_path.unlink(missing_ok=True)
        raise


def generate_pre_night_authorization_artifacts(
    *,
    output_root: pathlib.Path,
    race_date: str,
    venue_code: str,
    venue_name: str,
    reviewer: str,
    confirmation_phrase: str,
    deadlines: Sequence[Mapping[str, Any]],
    visual_review_path: pathlib.Path,
    expected_visual_review_sha256: str,
    contract_review_path: pathlib.Path,
    expected_contract_review_sha256: str,
    test_state: Mapping[str, Any],
    overwrite: bool = False,
) -> GeneratedArtifacts:
    """Generate reviewed pre-night authorization artifacts.

    This function does not perform live execution.

    The authorization state intentionally remains exactly:
        {"approved": True}

    All audit metadata is written to authorization_receipt.json.
    """

    output_root = pathlib.Path(output_root)
    visual_review_path = pathlib.Path(visual_review_path)
    contract_review_path = pathlib.Path(contract_review_path)

    normalized_date = _parse_race_date(race_date)
    normalized_venue = _normalized_venue_code(venue_code)
    normalized_deadlines = validate_deadlines(deadlines)
    normalized_test_state = validate_test_state(test_state)

    reviewer = str(reviewer).strip()
    venue_name = str(venue_name).strip()

    if not reviewer:
        raise AuthorizationContractError(
            "reviewer must not be empty"
        )

    if not venue_name:
        raise AuthorizationContractError(
            "venue_name must not be empty"
        )

    expected_phrase = CONFIRMATION_TEMPLATE.format(
        race_date=normalized_date,
        venue_code=normalized_venue,
    )

    if confirmation_phrase != expected_phrase:
        raise AuthorizationContractError(
            "confirmation phrase does not match "
            "the required exact value"
        )

    if not visual_review_path.is_file():
        raise AuthorizationContractError(
            f"visual review file not found: "
            f"{visual_review_path}"
        )

    if not contract_review_path.is_file():
        raise AuthorizationContractError(
            f"contract review file not found: "
            f"{contract_review_path}"
        )

    actual_visual_hash = sha256_file(
        visual_review_path
    )
    actual_contract_hash = sha256_file(
        contract_review_path
    )

    if actual_visual_hash != expected_visual_review_sha256:
        raise AuthorizationContractError(
            "visual review SHA-256 mismatch"
        )

    if actual_contract_hash != expected_contract_review_sha256:
        raise AuthorizationContractError(
            "contract review SHA-256 mismatch"
        )

    visual_review = json.loads(
        visual_review_path.read_text(encoding="utf-8")
    )
    contract_review = json.loads(
        contract_review_path.read_text(encoding="utf-8")
    )

    if (
        visual_review.get("classification")
        != "PRE_NIGHT_DEADLINE_VISUAL_REVIEW_CONFIRMED"
    ):
        raise AuthorizationContractError(
            "visual review classification is not confirmed"
        )

    if visual_review.get("race_date") != normalized_date:
        raise AuthorizationContractError(
            "visual review race_date mismatch"
        )

    if (
        str(visual_review.get("venue_code", "")).zfill(2)
        != normalized_venue
    ):
        raise AuthorizationContractError(
            "visual review venue_code mismatch"
        )

    if visual_review.get("live_execution_performed") is not False:
        raise AuthorizationContractError(
            "visual review must report no live execution"
        )

    if (
        contract_review.get("classification")
        != "PRE_NIGHT_AUTHORIZATION_GENERATOR_NOT_FOUND"
    ):
        raise AuthorizationContractError(
            "unexpected prior contract review classification"
        )

    if contract_review.get("errors") != []:
        raise AuthorizationContractError(
            "prior contract review contains errors"
        )

    if (
        contract_review.get("live_execution_performed")
        is not False
    ):
        raise AuthorizationContractError(
            "contract review must report no live execution"
        )

    authorization_path = (
        output_root / "authorization_state.draft.json"
    )
    test_state_path = output_root / "test_state.json"
    receipt_path = (
        output_root / "authorization_receipt.json"
    )

    generated_paths = (
        authorization_path,
        test_state_path,
        receipt_path,
    )

    if not overwrite:
        existing = [
            str(path)
            for path in generated_paths
            if path.exists()
        ]

        if existing:
            raise FileExistsError(
                "refusing to overwrite existing artifacts: "
                + ", ".join(existing)
            )

    authorization_state = {
        "approved": True,
    }

    receipt = {
        "classification": (
            "PRE_NIGHT_AUTHORIZATION_ARTIFACTS_GENERATED"
        ),
        "generated_at": (
            datetime.now(timezone.utc).isoformat()
        ),
        "race_date": normalized_date,
        "venue_code": normalized_venue,
        "venue_name": venue_name,
        "reviewer": reviewer,
        "confirmation_phrase": confirmation_phrase,
        "human_approval_recorded": True,
        "authorization_created": True,
        "live_execution_performed": False,
        "deadlines": normalized_deadlines,
        "visual_review": {
            "path": str(visual_review_path),
            "sha256": actual_visual_hash,
        },
        "contract_review": {
            "path": str(contract_review_path),
            "sha256": actual_contract_hash,
        },
        "authorization_contract": {
            "exact_value": authorization_state,
        },
        "test_state_contract": normalized_test_state,
    }

    # Receipt first, authorization last. This minimizes the chance of an
    # authorization file existing without its audit record.
    _atomic_write_json(receipt_path, receipt)
    _atomic_write_json(
        test_state_path,
        normalized_test_state,
    )
    _atomic_write_json(
        authorization_path,
        authorization_state,
    )

    return GeneratedArtifacts(
        authorization_path=authorization_path,
        test_state_path=test_state_path,
        receipt_path=receipt_path,
        authorization_sha256=sha256_file(
            authorization_path
        ),
        test_state_sha256=sha256_file(
            test_state_path
        ),
        receipt_sha256=sha256_file(
            receipt_path
        ),
    )
