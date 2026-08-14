from __future__ import annotations

import hashlib
import inspect
import json
import os
from pathlib import Path

import pandas as pd
import pytest

from boatrace_ai.pipelines import pre_night_feature_matrix as fm


RACE_DATE = "2026-08-10"
RUN_ID = "pre-night-20260810-stage6-test"
BRANCH = "feature/pre-night-authoritative-deadline-pit-contract-v2"
HEAD = "5e8b2d3186241258810d9fab7e0c6668a44ffd09"


def digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sample_frame() -> pd.DataFrame:
    rows = []

    for boat_no in range(1, 7):
        rows.append(
            {
                "race_date": RACE_DATE,
                "venue_code": "01",
                "race_no": 1,
                "boat_no": boat_no,
                "age": 30 + boat_no,
                "boat_place2_rate_pct": 30.0 + boat_no,
                "branch": "東京",
                "class": "A1",
                "local_place2_rate_pct": 40.0 + boat_no,
                "local_win_rate": 5.0 + boat_no / 10.0,
                "motor_place2_rate_pct": 35.0 + boat_no,
                "national_place2_rate_pct": 45.0 + boat_no,
                "national_win_rate": 6.0 + boat_no / 10.0,
                "weight_kg": 50.0 + boat_no / 10.0,
                "boat_no_equipment": boat_no,
                "motor_no": 10 + boat_no,
                "racer_id": f"{1000 + boat_no:04d}",
                "racer_name": f"選手{boat_no}",
                "source_file": "B260810.TXT",
            }
        )

    frame = pd.DataFrame(
        rows,
        columns=list(fm.OUTPUT_COLUMNS),
    )

    for column in [
        "race_date",
        "venue_code",
        "branch",
        "class",
        "racer_id",
        "racer_name",
        "source_file",
    ]:
        frame[column] = frame[column].astype("string")

    for column in [
        "race_no",
        "boat_no",
        "age",
        "boat_no_equipment",
        "motor_no",
    ]:
        frame[column] = frame[column].astype("int64")

    for column in [
        "boat_place2_rate_pct",
        "local_place2_rate_pct",
        "local_win_rate",
        "motor_place2_rate_pct",
        "national_place2_rate_pct",
        "national_win_rate",
        "weight_kg",
    ]:
        frame[column] = frame[column].astype("float64")

    return frame


@pytest.fixture
def prepared_root(tmp_path: Path, monkeypatch):
    root = tmp_path / "data"
    directory = (
        root
        / "prospective"
        / "pre_night"
        / "runs"
        / "2026"
        / "08"
        / "10"
        / RUN_ID
    )
    directory.mkdir(parents=True)

    dataset_path = directory / "snapshot.parquet"
    dataset_bytes = b"stage6-parent-dataset-test-bytes"
    dataset_path.write_bytes(dataset_bytes)

    parent_payload = {
        "contract_version": fm.PARENT_CONTRACT_VERSION,
        "classification": fm.PARENT_CLASSIFICATION,
        "race_date": RACE_DATE,
        "run_id": RUN_ID,
        "branch": BRANCH,
        "head": HEAD,
    }
    parent_bytes = (
        fm.canonical_feature_matrix_json_bytes(
            parent_payload
        )
    )

    parent_path = (
        directory
        / "prospective_dataset_v3_manifest.json"
    )
    parent_path.write_bytes(parent_bytes)

    parent_receipt = {
        "classification": fm.PARENT_CLASSIFICATION,
        "prospective_manifest_sha256": digest(parent_bytes),
        "dataset_sha256": digest(dataset_bytes),
        "dataset_byte_length": len(dataset_bytes),
        "paths": {
            "dataset": dataset_path,
            "prospective_manifest": parent_path,
        },
    }

    monkeypatch.setattr(
        fm,
        "validate_prospective_v3",
        lambda *args, **kwargs: parent_receipt,
    )
    monkeypatch.setattr(
        fm,
        "_load_parent_manifest",
        lambda *args, **kwargs: (
            parent_payload,
            parent_bytes,
        ),
    )
    monkeypatch.setattr(
        fm,
        "_build_frame",
        lambda *args, **kwargs: sample_frame(),
    )

    return {
        "root": root,
        "directory": directory,
        "dataset": dataset_path,
        "parent_manifest": parent_path,
    }


def publish(root: Path):
    return fm.publish_feature_matrix(
        root,
        race_date=RACE_DATE,
        run_id=RUN_ID,
        branch=BRANCH,
        head=HEAD,
    )


def validate(root: Path):
    return fm.validate_feature_matrix(
        root,
        race_date=RACE_DATE,
        run_id=RUN_ID,
        branch=BRANCH,
        head=HEAD,
    )


def test_contract_identity_and_exact_columns():
    assert (
        fm.AUTHORIZED_CONTRACT_ID
        == "D1B5-STAGE6-FEATURE-MATRIX-V1-R1"
    )
    assert (
        fm.AUTHORIZED_CONTRACT_SHA256
        == "52e316990d324fe812cfa3981b350102c75b9e7927a19535c3b7dad1f4195e01"
    )
    assert len(fm.OUTPUT_COLUMNS) == 19
    assert len(fm.PRIMARY_KEY_COLUMNS) == 4
    assert len(fm.MODEL_FEATURE_COLUMNS) == 10
    assert len(fm.METADATA_COLUMNS) == 5
    assert fm.EXCLUDED_COLUMNS == ("series_results_raw",)


def test_public_api_signatures_are_exact():
    for function in (
        fm.validate_feature_matrix,
        fm.publish_feature_matrix,
    ):
        signature = inspect.signature(function)
        assert list(signature.parameters) == [
            "data_root",
            "race_date",
            "run_id",
            "branch",
            "head",
        ]
        assert (
            signature.parameters["data_root"].kind
            == inspect.Parameter.POSITIONAL_OR_KEYWORD
        )
        for name in [
            "race_date",
            "run_id",
            "branch",
            "head",
        ]:
            assert (
                signature.parameters[name].kind
                == inspect.Parameter.KEYWORD_ONLY
            )


def test_canonical_json_is_deterministic():
    left = fm.canonical_feature_matrix_json_bytes(
        {"b": 2, "a": "日本語"}
    )
    right = fm.canonical_feature_matrix_json_bytes(
        {"a": "日本語", "b": 2}
    )
    assert left == right
    assert left.endswith(b"\n")
    assert not left.endswith(b"\n\n")


def test_column_contract_weight_and_racer_id():
    contracts = {
        item["name"]: item
        for item in fm.COLUMN_CONTRACTS
    }
    assert contracts["weight_kg"]["arrow_type"] == "double"
    assert (
        contracts["weight_kg"]["pandas_dtype"]
        == "float64"
    )
    assert (
        contracts["weight_kg"]["value_contract"]
        ["minimum"]
        == 30.0
    )
    assert contracts["racer_id"]["arrow_type"] == "string"
    assert (
        contracts["racer_id"]["value_contract"]
        ["numeric_text"]
        is True
    )
    assert (
        contracts["racer_id"]["value_contract"]
        ["numeric_positive_rule"]
        is False
    )


def test_valid_publication_creates_four_artifacts(
    prepared_root,
):
    receipt = publish(prepared_root["root"])

    assert receipt["cached"] is False
    assert receipt["publication_status"] == "CREATED"
    assert receipt["row_count"] == 6
    assert receipt["column_count"] == 19

    directory = prepared_root["directory"]
    expected = [
        directory / "feature_matrix.parquet",
        directory / "feature_matrix.schema.json",
        directory / "feature_matrix.metadata.json",
        directory / "feature_matrix.manifest.json",
    ]
    assert all(path.is_file() for path in expected)
    assert not (directory / ".feature_matrix.lock").exists()
    assert not list(directory.glob(".*.tmp"))


def test_identical_publication_is_validated_reuse(
    prepared_root,
):
    created = publish(prepared_root["root"])
    reused = publish(prepared_root["root"])

    assert created["publication_status"] == "CREATED"
    assert reused["publication_status"] == "VALIDATED_REUSE"
    assert reused["cached"] is True
    assert (
        created["feature_matrix_manifest_sha256"]
        == reused["feature_matrix_manifest_sha256"]
    )


def test_validate_returns_validated_reuse(prepared_root):
    publish(prepared_root["root"])
    receipt = validate(prepared_root["root"])

    assert receipt["cached"] is True
    assert receipt["publication_status"] == "VALIDATED_REUSE"


def test_manifest_has_exact_fields_and_no_self_digest(
    prepared_root,
):
    publish(prepared_root["root"])
    manifest_path = (
        prepared_root["directory"]
        / "feature_matrix.manifest.json"
    )
    stored = manifest_path.read_bytes()
    payload = json.loads(stored.decode("utf-8"))

    assert set(payload) == fm.MANIFEST_KEYS
    assert "feature_matrix_manifest_sha256" not in payload
    assert (
        stored
        == fm.canonical_feature_matrix_json_bytes(payload)
    )


def test_feature_matrix_physical_schema(prepared_root):
    publish(prepared_root["root"])
    frame = pd.read_parquet(
        prepared_root["directory"]
        / "feature_matrix.parquet",
        engine="pyarrow",
    )

    assert list(frame.columns) == list(fm.OUTPUT_COLUMNS)
    assert len(frame) == 6
    assert int(frame.isna().sum().sum()) == 0
    assert str(frame["weight_kg"].dtype) == "float64"
    assert str(frame["racer_id"].dtype) in {
        "string",
        "string[python]",
        "object",
    }


def test_existing_byte_conflict_is_rejected(prepared_root):
    publish(prepared_root["root"])
    path = (
        prepared_root["directory"]
        / "feature_matrix.metadata.json"
    )
    path.write_bytes(b"{}\n")

    with pytest.raises(fm.FeatureMatrixConflictError):
        publish(prepared_root["root"])


def test_partial_unit_is_rejected(prepared_root):
    path = (
        prepared_root["directory"]
        / "feature_matrix.schema.json"
    )
    path.write_bytes(b"{}\n")

    with pytest.raises(fm.FeatureMatrixCacheError):
        publish(prepared_root["root"])


def test_existing_lock_is_rejected(prepared_root):
    lock = (
        prepared_root["directory"]
        / ".feature_matrix.lock"
    )
    lock.write_text("owned elsewhere", encoding="utf-8")

    with pytest.raises(fm.FeatureMatrixError):
        publish(prepared_root["root"])

    assert lock.exists()


def test_null_frame_is_rejected():
    frame = sample_frame()
    frame.loc[0, "branch"] = pd.NA

    with pytest.raises(fm.FeatureMatrixContractError):
        fm._validate_feature_frame(frame)


def test_duplicate_primary_key_is_rejected():
    frame = sample_frame()
    frame.loc[1, list(fm.PRIMARY_KEY_COLUMNS)] = (
        frame.loc[0, list(fm.PRIMARY_KEY_COLUMNS)]
    )

    with pytest.raises(fm.FeatureMatrixContractError):
        fm._validate_feature_frame(frame)


def test_wrong_column_order_is_rejected():
    frame = sample_frame()
    columns = list(frame.columns)
    columns[0], columns[1] = columns[1], columns[0]

    with pytest.raises(fm.FeatureMatrixContractError):
        fm._validate_feature_frame(frame.loc[:, columns])


def test_publication_failure_cleans_owned_artifacts(
    prepared_root,
    monkeypatch,
):
    real_link = os.link
    call_count = {"value": 0}

    def failing_link(source, destination):
        call_count["value"] += 1
        if call_count["value"] == 2:
            raise OSError("injected link failure")
        return real_link(source, destination)

    monkeypatch.setattr(fm.os, "link", failing_link)

    with pytest.raises(fm.FeatureMatrixIntegrityError):
        publish(prepared_root["root"])

    directory = prepared_root["directory"]
    for filename in [
        "feature_matrix.parquet",
        "feature_matrix.schema.json",
        "feature_matrix.metadata.json",
        "feature_matrix.manifest.json",
    ]:
        assert not (directory / filename).exists()

    assert not (directory / ".feature_matrix.lock").exists()
    assert not list(directory.glob(".*.tmp"))


def test_source_uses_link_and_not_replace():
    source = inspect.getsource(fm)
    assert "os.link(" in source
    assert "os.replace(" not in source
