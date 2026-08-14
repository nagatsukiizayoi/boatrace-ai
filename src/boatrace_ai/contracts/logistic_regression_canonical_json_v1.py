"""Validation utilities for LOGISTIC_REGRESSION_CANONICAL_JSON_V1.

This module validates JSON-compatible in-memory objects only.
It does not load, deserialize, execute, predict with or train models.
"""

from __future__ import annotations

import hashlib
import json
import math
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping

from jsonschema.validators import validator_for


CONTRACT_ID = "LOGISTIC_REGRESSION_CANONICAL_JSON_V1"
MODEL_FAMILY = "logistic_regression_v1"

SCHEMA_PATH = (
    Path(__file__).resolve().parents[3]
    / "contracts"
    / "schemas"
    / "logistic_regression_canonical_json_v1.schema.json"
)


class ContractValidationError(ValueError):
    """Raised when a canonical contract document is invalid."""


@lru_cache(maxsize=1)
def load_schema() -> dict[str, Any]:
    """Load and statically validate the authoritative JSON Schema."""
    with SCHEMA_PATH.open("r", encoding="utf-8") as stream:
        schema = json.load(stream)

    validator_class = validator_for(schema)
    validator_class.check_schema(schema)
    return schema


def _reject_non_finite(
    value: Any,
    path: str = "$",
) -> None:
    if isinstance(value, bool) or value is None:
        return

    if isinstance(value, float):
        if not math.isfinite(value):
            raise ContractValidationError(
                f"Non-finite numeric value at {path}"
            )
        return

    if isinstance(value, Mapping):
        for key, child in value.items():
            _reject_non_finite(
                child,
                f"{path}.{key}",
            )
        return

    if isinstance(value, (list, tuple)):
        for index, child in enumerate(value):
            _reject_non_finite(
                child,
                f"{path}[{index}]",
            )


def validate_contract(
    document: Mapping[str, Any],
) -> None:
    """Validate a canonical logistic-regression contract document."""
    if not isinstance(document, Mapping):
        raise ContractValidationError(
            "Canonical contract root must be an object"
        )

    _reject_non_finite(document)

    schema = load_schema()
    validator_class = validator_for(schema)
    validator = validator_class(schema)

    errors = sorted(
        validator.iter_errors(document),
        key=lambda item: (
            tuple(
                str(part)
                for part in item.absolute_path
            ),
            item.message,
        ),
    )

    if errors:
        rendered = []

        for error in errors:
            location = "$"

            if error.absolute_path:
                location += "".join(
                    (
                        f"[{part}]"
                        if isinstance(part, int)
                        else f".{part}"
                    )
                    for part in error.absolute_path
                )

            rendered.append(
                f"{location}: {error.message}"
            )

        raise ContractValidationError(
            "Canonical contract validation failed: "
            + " | ".join(rendered)
        )


def canonical_json_bytes(
    document: Mapping[str, Any],
) -> bytes:
    """Return deterministic UTF-8 JSON after validation."""
    validate_contract(document)

    return (
        json.dumps(
            document,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def canonical_sha256(
    document: Mapping[str, Any],
) -> str:
    """Return SHA-256 of deterministic canonical JSON bytes."""
    return hashlib.sha256(
        canonical_json_bytes(document)
    ).hexdigest()


__all__ = [
    "CONTRACT_ID",
    "MODEL_FAMILY",
    "SCHEMA_PATH",
    "ContractValidationError",
    "load_schema",
    "validate_contract",
    "canonical_json_bytes",
    "canonical_sha256",
]
