from __future__ import annotations

from decimal import Decimal

import pytest

from boatrace_ai.models import ApprovedLogisticRegressionModel


RAW = (
    "age",
    "boat_place2_rate_pct",
    "branch",
    "class",
    "local_place2_rate_pct",
    "local_win_rate",
    "motor_place2_rate_pct",
    "national_place2_rate_pct",
    "national_win_rate",
    "weight_kg",
)
NUMERIC = (
    "age",
    "boat_place2_rate_pct",
    "local_place2_rate_pct",
    "local_win_rate",
    "motor_place2_rate_pct",
    "national_place2_rate_pct",
    "national_win_rate",
    "weight_kg",
)
BRANCH = ("__UNKNOWN__", "A")
CLASS = ("__UNKNOWN__", "B")
ENCODED = NUMERIC + (
    "branch__one_hot__0000",
    "branch__one_hot__0001",
    "class__one_hot__0000",
    "class__one_hot__0001",
)


def model(intercept="0"):
    return ApprovedLogisticRegressionModel(
        model_id="model-a",
        model_sha256="0" * 64,
        feature_contract_sha256="1" * 64,
        raw_feature_order=RAW,
        numeric_features=NUMERIC,
        branch_vocabulary=BRANCH,
        class_vocabulary=CLASS,
        encoded_feature_order=ENCODED,
        intercept=Decimal(intercept),
        coefficients=tuple(Decimal(0) for _ in ENCODED),
        positive_class_label="1",
    )


def row():
    return {
        "age": 20,
        "boat_place2_rate_pct": "0",
        "branch": "A",
        "class": "B",
        "local_place2_rate_pct": Decimal("0"),
        "local_win_rate": 0.0,
        "motor_place2_rate_pct": 0,
        "national_place2_rate_pct": 0,
        "national_win_rate": 0,
        "weight_kg": 50,
    }


def test_runtime_object_is_frozen_and_uses_tuple_state():
    value = model()
    assert type(value.coefficients) is tuple
    with pytest.raises(Exception):
        value.model_id = "other"


def test_probability_exact_fifteen_places():
    assert model().predict_probability(row()) == Decimal(
        "0.500000000000000"
    )


def test_sigmoid_boundaries():
    assert model("36").predict_probability(row()) == Decimal(
        "1.000000000000000"
    )
    assert model("-36").predict_probability(row()) == Decimal(
        "0.000000000000000"
    )


def test_row_must_be_exact_dict():
    class Child(dict):
        pass

    with pytest.raises(Exception):
        model().predict_probability(Child(row()))
