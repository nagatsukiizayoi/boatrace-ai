"""Exact contracts for the approved declarative logistic model format."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import (
    Clamped,
    Decimal,
    DivisionByZero,
    FloatOperation,
    Inexact,
    InvalidOperation,
    Overflow,
    ROUND_HALF_EVEN,
    Rounded,
    Subnormal,
    Underflow,
    localcontext,
)
import json
import math
import unicodedata
from typing import Final


CONTRACT_ID: Final = "D1B5-STAGE7-PRODUCTION-MODEL-AUTHORITY-V1-R1"
CONTRACT_SHA256: Final = (
    "1108b266e11c9db5a13f823c29b3c6ce"
    "fccea33a983a370ccbe4345a5aa17a4f"
)

ARTIFACT_NAMES: Final = frozenset({
    "model.artifact.json",
    "model.sha256",
    "model_contract.json",
    "feature_contract.json",
    "training_manifest.json",
    "validation_report.json",
    "approval.json",
    "rollback.json",
    "authority_event.json",
    "authority_event_authorization.json",
    "package",
})


class ModelAuthorityError(Exception):
    """Base error containing only immutable, allowlisted metadata."""

    __slots__ = ("error_code", "validation_stage", "artifact_name", "_locked")

    def __init__(self, *, error_code: str, validation_stage: str,
                 artifact_name: str) -> None:
        if type(error_code) is not str or type(validation_stage) is not str:
            raise TypeError("error metadata must be exact str")
        if type(artifact_name) is not str or artifact_name not in ARTIFACT_NAMES:
            raise TypeError("artifact_name is not allowlisted")
        object.__setattr__(self, "error_code", error_code)
        object.__setattr__(self, "validation_stage", validation_stage)
        object.__setattr__(self, "artifact_name", artifact_name)
        object.__setattr__(self, "_locked", True)
        super().__init__(
            "model authority validation failed: "
            f"error_code={error_code}; "
            f"validation_stage={validation_stage}; "
            f"artifact_name={artifact_name}"
        )

    def __setattr__(self, name, value):
        if getattr(self, "_locked", False):
            raise AttributeError("exception metadata is immutable")
        object.__setattr__(self, name, value)


class ModelRegistryError(ModelAuthorityError):
    pass


class ModelIntegrityError(ModelAuthorityError):
    pass


class ModelContractError(ModelAuthorityError):
    pass


class ModelAuthorityStateError(ModelAuthorityError):
    pass


class ModelFormatError(ModelAuthorityError):
    pass


def _error(cls, stage: str, artifact: str):
    raise cls(
        error_code=stage,
        validation_stage=stage,
        artifact_name=artifact,
    )


def _reject_constant(_value: str):
    _error(ModelContractError, "S7L-014_JSON_STRICT_PARSE", "package")


def _reject_surrogates(value) -> None:
    if isinstance(value, str):
        if any(0xD800 <= ord(ch) <= 0xDFFF for ch in value):
            _error(ModelContractError, "S7L-014_JSON_STRICT_PARSE", "package")
    elif isinstance(value, list):
        for item in value:
            _reject_surrogates(item)
    elif isinstance(value, dict):
        for key, item in value.items():
            _reject_surrogates(key)
            _reject_surrogates(item)
    elif isinstance(value, float) and not math.isfinite(value):
        _error(ModelContractError, "S7L-014_JSON_STRICT_PARSE", "package")


def _normalize(value):
    if isinstance(value, str):
        result = unicodedata.normalize("NFC", value)
        _reject_surrogates(result)
        return result
    if isinstance(value, list):
        return [_normalize(item) for item in value]
    if isinstance(value, dict):
        result = {}
        for raw_key, raw_value in value.items():
            if type(raw_key) is not str:
                _error(ModelContractError, "S7L-014_JSON_STRICT_PARSE", "package")
            key = unicodedata.normalize("NFC", raw_key)
            if key in result:
                _error(
                    ModelIntegrityError,
                    "S7L-015_CANONICAL_JSON_BYTES",
                    "package",
                )
            result[key] = _normalize(raw_value)
        return result
    return value


def canonical_model_json_bytes(value) -> bytes:
    normalized = _normalize(value)
    try:
        text = json.dumps(
            normalized,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    except (TypeError, ValueError):
        _error(ModelContractError, "S7L-014_JSON_STRICT_PARSE", "package")
    return text.encode("utf-8") + b"\n"


def validate_model_json(value, *, artifact_name) -> dict:
    if type(artifact_name) is not str or artifact_name not in ARTIFACT_NAMES:
        _error(ModelContractError, "S7L-016_ARTIFACT_SCHEMA", "package")
    if type(value) is not dict:
        _error(ModelContractError, "S7L-016_ARTIFACT_SCHEMA", artifact_name)
    normalized = _normalize(value)
    if type(normalized) is not dict:
        _error(ModelContractError, "S7L-016_ARTIFACT_SCHEMA", artifact_name)
    canonical_model_json_bytes(normalized)
    return normalized


def parse_model_json_bytes(raw: bytes, *, artifact_name: str) -> dict:
    if type(raw) is not bytes:
        _error(ModelContractError, "S7L-014_JSON_STRICT_PARSE", artifact_name)
    if raw.startswith(b"\xef\xbb\xbf"):
        _error(ModelContractError, "S7L-014_JSON_STRICT_PARSE", artifact_name)
    try:
        text = raw.decode("utf-8", errors="strict")
        pairs_seen = []

        def hook(pairs):
            result = {}
            for key, value in pairs:
                if key in result:
                    _error(
                        ModelContractError,
                        "S7L-014_JSON_STRICT_PARSE",
                        artifact_name,
                    )
                result[key] = value
            pairs_seen.append(True)
            return result

        value = json.loads(
            text,
            object_pairs_hook=hook,
            parse_constant=lambda _v: _error(
                ModelContractError,
                "S7L-014_JSON_STRICT_PARSE",
                artifact_name,
            ),
        )
    except ModelAuthorityError:
        raise
    except Exception:
        _error(ModelContractError, "S7L-014_JSON_STRICT_PARSE", artifact_name)

    _reject_surrogates(value)
    normalized = validate_model_json(value, artifact_name=artifact_name)

    if canonical_model_json_bytes(normalized) != raw:
        _error(ModelIntegrityError, "S7L-015_CANONICAL_JSON_BYTES", artifact_name)
    return normalized


def _decimal_context():
    ctx = __import__("decimal").Context(
        prec=50,
        rounding=ROUND_HALF_EVEN,
        Emin=-999999,
        Emax=999999,
        capitals=1,
        clamp=0,
    )
    ctx.traps[InvalidOperation] = True
    ctx.traps[DivisionByZero] = True
    ctx.traps[Overflow] = True
    ctx.traps[FloatOperation] = True
    ctx.traps[Clamped] = False
    ctx.traps[Inexact] = False
    ctx.traps[Rounded] = False
    ctx.traps[Subnormal] = False
    ctx.traps[Underflow] = False
    return ctx


def _to_decimal(value) -> Decimal:
    if type(value) is bool or value is None:
        _error(ModelFormatError, "S7L-042_DECIMAL_RUNTIME_CONTEXT",
               "model.artifact.json")
    try:
        if type(value) is int:
            result = Decimal(value)
        elif type(value) is float:
            if not math.isfinite(value):
                raise ValueError
            result = Decimal.from_float(value)
        elif type(value) is str:
            import re
            if re.fullmatch(
                r"(0|-[1-9][0-9]*|[1-9][0-9]*)(\.[0-9]*[1-9])?",
                value,
            ) is None:
                raise ValueError
            result = Decimal(value)
        elif type(value) is Decimal:
            result = value
        else:
            raise ValueError
        if not result.is_finite():
            raise ValueError
        return result
    except Exception:
        _error(ModelFormatError, "S7L-042_DECIMAL_RUNTIME_CONTEXT",
               "model.artifact.json")


@dataclass(frozen=True, slots=True)
class ApprovedLogisticRegressionModel:
    model_id: str
    model_sha256: str
    feature_contract_sha256: str
    raw_feature_order: tuple[str, ...]
    numeric_features: tuple[str, ...]
    branch_vocabulary: tuple[str, ...]
    class_vocabulary: tuple[str, ...]
    encoded_feature_order: tuple[str, ...]
    intercept: Decimal
    coefficients: tuple[Decimal, ...]
    positive_class_label: str

    def _validate_row(self, row) -> None:
        if type(row) is not dict:
            _error(ModelContractError, "S7L-044_RUNTIME_OBJECT",
                   "model.artifact.json")
        if tuple(row.keys()) != self.raw_feature_order:
            if set(row) != set(self.raw_feature_order):
                _error(ModelContractError, "S7L-044_RUNTIME_OBJECT",
                       "model.artifact.json")
        for key in row:
            if type(key) is not str:
                _error(ModelContractError, "S7L-044_RUNTIME_OBJECT",
                       "model.artifact.json")

    def encode_row(self, row) -> tuple[Decimal, ...]:
        self._validate_row(row)
        values = tuple(_to_decimal(row[name]) for name in self.numeric_features)

        branch = row["branch"]
        class_value = row["class"]
        if type(branch) is not str or type(class_value) is not str:
            _error(ModelContractError, "S7L-044_RUNTIME_OBJECT",
                   "model.artifact.json")
        branch = unicodedata.normalize("NFC", branch)
        class_value = unicodedata.normalize("NFC", class_value)
        if not branch or not class_value:
            _error(ModelContractError, "S7L-044_RUNTIME_OBJECT",
                   "model.artifact.json")

        branch_index = (
            self.branch_vocabulary.index(branch)
            if branch in self.branch_vocabulary else 0
        )
        class_index = (
            self.class_vocabulary.index(class_value)
            if class_value in self.class_vocabulary else 0
        )

        branch_values = tuple(
            Decimal(1) if i == branch_index else Decimal(0)
            for i in range(len(self.branch_vocabulary))
        )
        class_values = tuple(
            Decimal(1) if i == class_index else Decimal(0)
            for i in range(len(self.class_vocabulary))
        )
        encoded = values + branch_values + class_values
        if len(encoded) != len(self.encoded_feature_order):
            _error(ModelFormatError, "S7L-041_DECIMAL_MODEL_FORMAT",
                   "model.artifact.json")
        return encoded

    def decision_function(self, row) -> Decimal:
        encoded = self.encode_row(row)
        if len(encoded) != len(self.coefficients):
            _error(ModelFormatError, "S7L-041_DECIMAL_MODEL_FORMAT",
                   "model.artifact.json")
        ctx = _decimal_context()
        with localcontext(ctx) as active:
            active.clear_flags()
            z = +self.intercept
            for coefficient, value in zip(self.coefficients, encoded):
                product = +(coefficient * value)
                z = +(z + product)
            if not z.is_finite():
                _error(ModelFormatError, "S7L-042_DECIMAL_RUNTIME_CONTEXT",
                       "model.artifact.json")
            return z

    def predict_probability(self, row) -> Decimal:
        z = self.decision_function(row)
        ctx = _decimal_context()
        with localcontext(ctx) as active:
            active.clear_flags()
            if z >= Decimal(36):
                p = Decimal(1)
            elif z <= Decimal(-36):
                p = Decimal(0)
            elif z >= Decimal(0):
                p = Decimal(1) / (Decimal(1) + (-z).exp())
            else:
                e = z.exp()
                p = e / (Decimal(1) + e)
            p = p.quantize(
                Decimal("0.000000000000001"),
                rounding=ROUND_HALF_EVEN,
            )
            if p.is_zero() and p.is_signed():
                p = Decimal("0.000000000000000")
            if not p.is_finite() or not Decimal(0) <= p <= Decimal(1):
                _error(ModelFormatError, "S7L-042_DECIMAL_RUNTIME_CONTEXT",
                       "model.artifact.json")
            return p
