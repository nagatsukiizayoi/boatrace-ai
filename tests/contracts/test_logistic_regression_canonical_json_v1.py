from __future__ import annotations

import importlib.util
import json
import math
import re
from pathlib import Path

import pytest
from jsonschema.validators import validator_for


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]

SCHEMA_PATH = (
    REPOSITORY_ROOT
    / "contracts"
    / "schemas"
    / "logistic_regression_canonical_json_v1.schema.json"
)

VALIDATOR_PATH = (
    REPOSITORY_ROOT
    / "src"
    / "boatrace_ai"
    / "contracts"
    / "logistic_regression_canonical_json_v1.py"
)

_spec = importlib.util.spec_from_file_location(
    "logistic_regression_canonical_json_v1",
    VALIDATOR_PATH,
)

assert _spec is not None
assert _spec.loader is not None

contract_module = importlib.util.module_from_spec(
    _spec
)
_spec.loader.exec_module(contract_module)


def _resolve_local_ref(root, ref):
    assert ref.startswith("#/")

    value = root

    for part in ref[2:].split("/"):
        part = (
            part
            .replace("~1", "/")
            .replace("~0", "~")
        )
        value = value[part]

    return value


def _merge_object_samples(left, right):
    if isinstance(left, dict) and isinstance(right, dict):
        merged = dict(left)
        merged.update(right)
        return merged

    return right


def _minimal_string(fragment, seed=0):
    if "const" in fragment:
        return fragment["const"]

    if "enum" in fragment:
        values = fragment["enum"]
        return values[seed % len(values)]

    pattern = fragment.get("pattern")
    minimum = max(
        1,
        int(fragment.get("minLength", 0)),
    )
    maximum = fragment.get("maxLength")

    # Fixed hexadecimal hash patterns used by the contract.
    if pattern in {
        "^[0-9a-f]{64}$",
        "[0-9a-f]{64}",
    }:
        return f"{seed:064x}"[-64:]

    if pattern in {
        "^[0-9a-f]{40}$",
        "[0-9a-f]{40}",
    }:
        return f"{seed:040x}"[-40:]

    # Use seed-dependent strings so uniqueItems arrays receive
    # distinct values.
    character = chr(
        ord("a") + (seed % 26)
    )
    candidate = character * minimum

    if maximum is not None:
        candidate = candidate[:int(maximum)]

    if (
        pattern is not None
        and re.fullmatch(pattern, candidate) is None
    ):
        candidates = [
            f"value{seed}",
            f"feature_{seed}",
            f"class_{seed}",
            character,
            character * max(minimum, 2),
            str(seed),
        ]

        for value in candidates:
            if len(value) < minimum:
                value += character * (
                    minimum - len(value)
                )

            if maximum is not None:
                value = value[:int(maximum)]

            if re.fullmatch(pattern, value) is not None:
                return value

        raise AssertionError(
            "Unable to construct a string matching "
            f"pattern: {pattern}"
        )

    return candidate


def _minimal_instance(
    fragment,
    root,
    seed=0,
):
    if "$ref" in fragment:
        return _minimal_instance(
            _resolve_local_ref(
                root,
                fragment["$ref"],
            ),
            root,
            seed,
        )

    if "const" in fragment:
        return fragment["const"]

    if "enum" in fragment:
        values = fragment["enum"]
        return values[seed % len(values)]

    if "allOf" in fragment:
        sample = {}

        for part in fragment["allOf"]:
            sample = _merge_object_samples(
                sample,
                _minimal_instance(
                    part,
                    root,
                    seed,
                ),
            )

        remaining = {
            key: value
            for key, value in fragment.items()
            if key != "allOf"
        }

        if remaining:
            sample = _merge_object_samples(
                sample,
                _minimal_instance(
                    remaining,
                    root,
                    seed,
                ),
            )

        return sample

    for choice_key in ("oneOf", "anyOf"):
        if choice_key in fragment:
            choices = fragment[choice_key]

            for choice_index, choice in enumerate(choices):
                try:
                    candidate = _minimal_instance(
                        choice,
                        root,
                        seed + choice_index,
                    )

                    resolved_choice = (
                        _resolve_local_ref(
                            root,
                            choice["$ref"],
                        )
                        if "$ref" in choice
                        else choice
                    )

                    choice_validator_class = validator_for(
                        resolved_choice
                    )

                    choice_validator_class(
                        resolved_choice
                    ).validate(candidate)

                    return candidate

                except Exception:
                    continue

            return _minimal_instance(
                choices[0],
                root,
                seed,
            )

    fragment_type = fragment.get("type")

    if isinstance(fragment_type, list):
        non_null_types = [
            item
            for item in fragment_type
            if item != "null"
        ]

        fragment_type = (
            non_null_types[0]
            if non_null_types
            else "null"
        )

    if (
        fragment_type is None
        and "properties" in fragment
    ):
        fragment_type = "object"

    if fragment_type == "object":
        properties = fragment.get(
            "properties",
            {},
        )

        required = fragment.get(
            "required",
            [],
        )

        return {
            name: _minimal_instance(
                properties[name],
                root,
                seed,
            )
            for name in required
            if name in properties
        }

    if fragment_type == "array":
        count = max(
            1,
            int(fragment.get("minItems", 0)),
        )

        item_schema = fragment.get(
            "items",
            {},
        )

        unique_items = bool(
            fragment.get("uniqueItems", False)
        )

        values = []

        for index in range(count):
            item_seed = (
                seed + index
                if unique_items
                else seed
            )

            value = _minimal_instance(
                item_schema,
                root,
                item_seed,
            )

            if unique_items:
                attempt = 0

                while (
                    value in values
                    and attempt < 100
                ):
                    attempt += 1

                    value = _minimal_instance(
                        item_schema,
                        root,
                        seed + index + attempt,
                    )

                assert value not in values, (
                    "Unable to construct unique "
                    "array items"
                )

            values.append(value)

        return values

    if fragment_type == "string":
        return _minimal_string(
            fragment,
            seed,
        )

    if fragment_type == "integer":
        if "minimum" in fragment:
            return int(
                math.ceil(fragment["minimum"])
            )

        if "exclusiveMinimum" in fragment:
            return (
                int(
                    math.floor(
                        fragment["exclusiveMinimum"]
                    )
                )
                + 1
            )

        return seed

    if fragment_type == "number":
        if "minimum" in fragment:
            return float(fragment["minimum"])

        if "exclusiveMinimum" in fragment:
            return (
                float(fragment["exclusiveMinimum"])
                + 1.0
            )

        return float(seed)

    if fragment_type == "boolean":
        return False

    if fragment_type == "null":
        return None

    if "default" in fragment:
        return fragment["default"]

    return {}


def test_schema_is_valid_and_has_required_identity():
    schema = json.loads(
        SCHEMA_PATH.read_text(
            encoding="utf-8"
        )
    )

    schema_validator_class = validator_for(schema)
    schema_validator_class.check_schema(schema)

    rendered = json.dumps(
        schema,
        ensure_ascii=False,
    )

    assert (
        "LOGISTIC_REGRESSION_CANONICAL_JSON_V1"
        in rendered
    )

    assert "logistic_regression_v1" in rendered


def test_repository_schema_matches_validator_schema():
    expected = json.loads(
        SCHEMA_PATH.read_text(
            encoding="utf-8"
        )
    )

    assert contract_module.load_schema() == expected

    assert contract_module.CONTRACT_ID == (
        "LOGISTIC_REGRESSION_CANONICAL_JSON_V1"
    )

    assert (
        contract_module.MODEL_FAMILY
        == "logistic_regression_v1"
    )


def test_minimal_schema_instance_conforms_when_constructible():
    schema = json.loads(
        SCHEMA_PATH.read_text(
            encoding="utf-8"
        )
    )

    instance = _minimal_instance(
        schema,
        schema,
    )

    schema_validator_class = validator_for(schema)
    schema_validator_class(schema).validate(instance)

    contract_module.validate_contract(instance)


def test_empty_document_is_rejected_when_schema_has_requirements():
    schema = json.loads(
        SCHEMA_PATH.read_text(
            encoding="utf-8"
        )
    )

    if schema.get("required"):
        with pytest.raises(
            contract_module.ContractValidationError
        ):
            contract_module.validate_contract({})


def test_non_finite_values_are_rejected_before_serialization():
    with pytest.raises(
        contract_module.ContractValidationError
    ):
        contract_module.canonical_json_bytes({
            "value": float("nan")
        })


def test_canonical_serialization_is_deterministic_for_valid_instance():
    schema = json.loads(
        SCHEMA_PATH.read_text(
            encoding="utf-8"
        )
    )

    instance = _minimal_instance(
        schema,
        schema,
    )

    first = contract_module.canonical_json_bytes(
        instance
    )

    second = contract_module.canonical_json_bytes(
        instance
    )

    assert first == second
    assert first.endswith(b"\n")

    first_hash = contract_module.canonical_sha256(
        instance
    )

    second_hash = contract_module.canonical_sha256(
        instance
    )

    assert first_hash == second_hash
    assert re.fullmatch(
        r"[0-9a-f]{64}",
        first_hash,
    )
