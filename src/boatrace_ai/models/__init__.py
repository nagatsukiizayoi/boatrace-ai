"""Approved model authority public API."""

from .contracts import (
    ApprovedLogisticRegressionModel,
    ModelAuthorityError,
    ModelAuthorityStateError,
    ModelContractError,
    ModelFormatError,
    ModelIntegrityError,
    ModelRegistryError,
    canonical_model_json_bytes,
    validate_model_json,
)
from .loader import load_approved_model
from .registry import (
    resolve_approved_model,
    validate_approved_model_package,
    validate_model_authority_event_authorization,
    validate_model_authority_events,
)

__all__ = (
    "ApprovedLogisticRegressionModel",
    "ModelAuthorityError",
    "ModelAuthorityStateError",
    "ModelContractError",
    "ModelFormatError",
    "ModelIntegrityError",
    "ModelRegistryError",
    "canonical_model_json_bytes",
    "load_approved_model",
    "resolve_approved_model",
    "validate_approved_model_package",
    "validate_model_authority_event_authorization",
    "validate_model_authority_events",
    "validate_model_json",
)
