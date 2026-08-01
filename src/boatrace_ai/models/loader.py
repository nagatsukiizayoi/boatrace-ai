"""Approved declarative logistic-model loader."""

from __future__ import annotations

from decimal import Decimal

from .contracts import (
    ApprovedLogisticRegressionModel,
    ModelContractError,
    ModelFormatError,
)
from .registry import _package, validate_approved_model_package


def _raise(cls, stage, artifact):
    raise cls(
        error_code=stage,
        validation_stage=stage,
        artifact_name=artifact,
    )


def load_approved_model(
    model_root, *, model_id, expected_model_sha256,
    expected_feature_contract_sha256
) -> ApprovedLogisticRegressionModel:
    if (
        type(expected_feature_contract_sha256) is not str
        or len(expected_feature_contract_sha256) != 64
        or any(ch not in "0123456789abcdef"
               for ch in expected_feature_contract_sha256)
    ):
        _raise(ModelContractError, "S7L-018_CHILD_DIGEST_BINDINGS", "package")

    receipt = validate_approved_model_package(
        model_root,
        model_id=model_id,
        expected_model_sha256=expected_model_sha256,
    )
    if receipt["feature_contract_sha256"] != expected_feature_contract_sha256:
        _raise(ModelContractError, "S7L-018_CHILD_DIGEST_BINDINGS", "package")

    _root, _package_path, _raw, parsed = _package(
        model_root, model_id, expected_model_sha256
    )
    artifact = parsed["model.artifact.json"]
    feature = parsed["feature_contract.json"]

    try:
        coefficients = tuple(
            Decimal(value) for value in artifact["coefficients_decimal"]
        )
        intercept = Decimal(artifact["intercept_decimal"])
        encoded = tuple(artifact["encoded_feature_order"])
        if len(coefficients) != len(encoded):
            raise ValueError
        model = ApprovedLogisticRegressionModel(
            model_id=model_id,
            model_sha256=expected_model_sha256,
            feature_contract_sha256=expected_feature_contract_sha256,
            raw_feature_order=tuple(artifact["raw_feature_order"]),
            numeric_features=tuple(feature["numeric_features"]),
            branch_vocabulary=tuple(feature["branch_vocabulary"]),
            class_vocabulary=tuple(feature["class_vocabulary"]),
            encoded_feature_order=encoded,
            intercept=intercept,
            coefficients=coefficients,
            positive_class_label=artifact["positive_class_label"],
        )
    except Exception:
        _raise(ModelFormatError, "S7L-044_RUNTIME_OBJECT",
               "model.artifact.json")
    return model
