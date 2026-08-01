from __future__ import annotations

import json

import pytest

from boatrace_ai.models import (
    ModelContractError,
    ModelRegistryError,
    canonical_model_json_bytes,
    validate_model_json,
    validate_approved_model_package,
)


def test_canonical_json_exact_order_nfc_and_lf():
    value = {"z": "e\u0301", "a": 1}
    assert canonical_model_json_bytes(value) == (
        '{"a":1,"z":"é"}\n'.encode("utf-8")
    )


def test_normalized_key_collision_rejected():
    with pytest.raises(Exception):
        canonical_model_json_bytes({"é": 1, "e\u0301": 2})


def test_validate_model_json_requires_exact_dict():
    class Child(dict):
        pass

    with pytest.raises(ModelContractError):
        validate_model_json(Child(a=1), artifact_name="model_contract.json")


def test_registry_rejects_relative_root():
    with pytest.raises(ModelRegistryError):
        validate_approved_model_package(
            "relative",
            model_id="model-a",
            expected_model_sha256="0" * 64,
        )


def test_registry_rejects_bad_model_id(tmp_path):
    with pytest.raises(ModelContractError):
        validate_approved_model_package(
            str(tmp_path),
            model_id="BAD",
            expected_model_sha256="0" * 64,
        )
