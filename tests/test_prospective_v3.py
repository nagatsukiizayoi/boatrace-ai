from __future__ import annotations

import ast
import copy
import datetime as dt
import hashlib
import inspect
import json
from pathlib import Path

import pandas as pd
import pytest

from boatrace_ai.pipelines import prospective_v3 as v3
from boatrace_ai.pipelines.pre_night_deadline_collection import (
    COLLECTION_CONTRACT_VERSION,
)
from boatrace_ai.pipelines.pre_night_manifest_chain import (
    EXECUTION_MANIFEST_VERSION,
    PIPELINE_MANIFEST_VERSION,
    canonical_manifest_bytes,
)
from boatrace_ai.pipelines.pre_night_program_binding import (
    PROGRAM_BINDING_CONTRACT_VERSION,
    build_pre_night_program_entries_binding,
    canonical_program_entries_binding_bytes,
)


RACE_DATE = "2026-08-10"
RUN_ID = "pre-night-20260810-test"
BRANCH = v3.AUTHORIZED_BRANCH
HEAD = v3.AUTHORIZED_HEAD
D1 = "d" * 64
P1 = "a" * 64
JST = dt.timezone(dt.timedelta(hours=9))
UTC = dt.timezone.utc


def file_record(
    path: Path,
    root: Path,
    contract_version: str,
) -> dict:
    stored = path.read_bytes()

    return {
        "relative_path": (
            path.relative_to(root).as_posix()
        ),
        "byte_length": len(stored),
        "sha256": hashlib.sha256(
            stored
        ).hexdigest(),
        "contract_version": contract_version,
    }


def make_dataset_frame() -> pd.DataFrame:
    rows = []

    for boat_no in range(1, 7):
        rows.append(
            {
                "race_date": RACE_DATE,
                "venue_code": "01",
                "race_no": 1,
                "boat_no": boat_no,
                "racer_id": (
                    f"{4000 + boat_no:04d}"
                ),
                "racer_name": f"選手{boat_no}",
                "age": 20 + boat_no,
                "branch": "東京",
                "weight_kg": 50 + boat_no,
                "class": "A1",
                "national_win_rate": (
                    6.0 + boat_no / 10
                ),
                "national_place2_rate_pct": (
                    45.0 + boat_no
                ),
                "local_win_rate": (
                    5.0 + boat_no / 10
                ),
                "local_place2_rate_pct": (
                    40.0 + boat_no
                ),
                "motor_no": 20 + boat_no,
                "motor_place2_rate_pct": (
                    35.0 + boat_no
                ),
                "boat_no_equipment": (
                    10 + boat_no
                ),
                "boat_place2_rate_pct": (
                    30.0 + boat_no
                ),
                "series_results_raw": "1 2 3",
                "source_file": "B260810.TXT",
            }
        )

    frame = pd.DataFrame(
        rows,
        columns=list(v3.DATASET_COLUMNS[:20]),
    )

    for column in (
        "racer_id",
        "racer_name",
        "branch",
        "class",
        "series_results_raw",
    ):
        frame[column] = (
            frame[column].astype("string")
        )

    for column in (
        "race_no",
        "boat_no",
        "age",
        "weight_kg",
        "motor_no",
        "boat_no_equipment",
    ):
        frame[column] = (
            frame[column].astype("int64")
        )

    for column in (
        "national_win_rate",
        "national_place2_rate_pct",
        "local_win_rate",
        "local_place2_rate_pct",
        "motor_place2_rate_pct",
        "boat_place2_rate_pct",
    ):
        frame[column] = (
            frame[column].astype("float64")
        )

    as_of = pd.Timestamp(
        dt.datetime(
            2026,
            8,
            9,
            21,
            30,
            tzinfo=JST,
        )
    )
    fetched = pd.Timestamp(
        dt.datetime(
            2026,
            8,
            9,
            12,
            0,
            tzinfo=UTC,
        )
    )

    as_of_values = pd.Series(
        [as_of] * len(frame),
        index=frame.index,
        dtype=pd.DatetimeTZDtype(
            unit="ms",
            tz=JST,
        ),
    )
    fetched_values = pd.Series(
        [fetched] * len(frame),
        index=frame.index,
        dtype=pd.DatetimeTZDtype(
            unit="ms",
            tz=UTC,
        ),
    )

    frame["as_of_time"] = as_of_values
    frame["snapshot_at"] = as_of_values
    frame["feature_version"] = (
        "pre_night_program_snapshot_v1"
    )
    frame["feature_contract_version"] = (
        "pre_night_program_parquet_v1"
    )
    frame["feature_source_type"] = "program"
    frame["feature_source_url"] = (
        "https://example.invalid/B260810.LZH"
    )
    frame["feature_source_sha256"] = P1
    frame["feature_source_fetched_at"] = (
        fetched_values
    )
    frame["feature_source_max_time"] = (
        fetched_values
    )
    frame["source_max_time"] = fetched_values
    frame["feature_collector_version"] = (
        "collector-v1"
    )
    frame["provenance_status"] = "ELIGIBLE"
    frame["deadline_evidence_sha256"] = D1

    return frame


def install_stage4(
    root: Path,
    *,
    frame_mutator=None,
    authorization_state=None,
) -> dict[str, Path]:
    if authorization_state is None:
        authorization_state = {
            "approved": True,
        }

    collection_path = (
        root
        / "prospective"
        / "pre_night"
        / "deadline_evidence_collections"
        / "2026"
        / "08"
        / "10"
        / "deadline_evidence_collection.json"
    )
    collection_path.parent.mkdir(
        parents=True
    )

    collection_payload = {
        "contract_version": (
            COLLECTION_CONTRACT_VERSION
        ),
        "race_date": RACE_DATE,
        "expected_venue_codes": ["01"],
        "entry_count": 1,
        "entries": [
            {
                "race_date": RACE_DATE,
                "venue_code": "01",
                "relative_path": (
                    "prospective/pre_night/"
                    "deadline_evidence/"
                    "2026/08/10/01/"
                    "deadline_evidence.json"
                ),
                "deadline_evidence_sha256": D1,
                "byte_length": 10,
                "contract_version": (
                    "test-stage1-v1"
                ),
            }
        ],
    }

    collection_path.write_bytes(
        canonical_manifest_bytes(
            collection_payload
        )
    )

    collection_sha256 = hashlib.sha256(
        collection_path.read_bytes()
    ).hexdigest()

    program_entries = [
        {
            "race_date": RACE_DATE,
            "venue_code": "01",
            "race_no": race_no,
            "boat_no": boat_no,
        }
        for race_no in range(1, 13)
        for boat_no in range(1, 7)
    ]

    binding_payload = (
        build_pre_night_program_entries_binding(
            race_date=RACE_DATE,
            deadline_evidence_collection_sha256=(
                collection_sha256
            ),
            deadline_evidence_sha256_by_venue={
                "01": D1,
            },
            program_source_sha256_by_venue={
                "01": P1,
            },
            program_entries=program_entries,
        )
    )

    run_directory = (
        root
        / "prospective"
        / "pre_night"
        / "runs"
        / "2026"
        / "08"
        / "10"
        / RUN_ID
    )
    run_directory.mkdir(parents=True)

    binding_path = (
        run_directory
        / "program_entries_binding.json"
    )
    binding_path.write_bytes(
        canonical_program_entries_binding_bytes(
            binding_payload
        )
    )

    binding_sha256 = hashlib.sha256(
        binding_path.read_bytes()
    ).hexdigest()

    frame = make_dataset_frame()

    if frame_mutator is not None:
        changed = frame_mutator(frame)

        if changed is not None:
            frame = changed

    snapshot_path = (
        run_directory / "snapshot.parquet"
    )
    frame.to_parquet(
        snapshot_path,
        index=False,
        engine="pyarrow",
    )

    pipeline_path = (
        run_directory / "pipeline_manifest.json"
    )
    execution_path = (
        run_directory / "execution_manifest.json"
    )

    pipeline_payload = {
        "manifest_version": (
            PIPELINE_MANIFEST_VERSION
        ),
        "manifest_role": "PIPELINE_MANIFEST",
        "pipeline_name": "pre-night",
        "pipeline_version": "v1",
        "race_date": RACE_DATE,
        "run_id": RUN_ID,
        "branch": BRANCH,
        "head": HEAD,
        "started_at": (
            "2026-08-09T12:00:00Z"
        ),
        "completed_at": (
            "2026-08-09T12:01:00Z"
        ),
        "authorization_state": (
            copy.deepcopy(
                authorization_state
            )
        ),
        "stage1_contract_id": (
            "D1B5-STAGE1-DEADLINE-"
            "EVIDENCE-PUBLICATION-V2"
        ),
        "stage2_contract_version": (
            COLLECTION_CONTRACT_VERSION
        ),
        "stage3_contract_version": (
            PROGRAM_BINDING_CONTRACT_VERSION
        ),
        "deadline_evidence_collection_sha256": (
            collection_sha256
        ),
        "program_entries_binding_sha256": (
            binding_sha256
        ),
        "input_artifacts": {
            "deadline_evidence_collection": (
                file_record(
                    collection_path,
                    root,
                    COLLECTION_CONTRACT_VERSION,
                )
            ),
            "program_entries_binding": (
                file_record(
                    binding_path,
                    root,
                    PROGRAM_BINDING_CONTRACT_VERSION,
                )
            ),
        },
        "output_artifacts": {
            "snapshot": file_record(
                snapshot_path,
                root,
                "snapshot_exact_bytes_v1",
            ),
        },
    }

    pipeline_path.write_bytes(
        canonical_manifest_bytes(
            pipeline_payload
        )
    )

    pipeline_sha256 = hashlib.sha256(
        pipeline_path.read_bytes()
    ).hexdigest()

    snapshot_sha256 = hashlib.sha256(
        snapshot_path.read_bytes()
    ).hexdigest()

    execution_payload = {
        "manifest_version": (
            EXECUTION_MANIFEST_VERSION
        ),
        "manifest_role": (
            "EXECUTION_MANIFEST"
        ),
        "run_id": RUN_ID,
        "race_date": RACE_DATE,
        "phase": "PRE_NIGHT_MANIFEST_CHAIN",
        "branch": BRANCH,
        "head": HEAD,
        "repository_relative_path": (
            "src/boatrace_ai/pipelines/"
            "pre_night_manifest_chain.py"
        ),
        "authorization_state": (
            copy.deepcopy(
                authorization_state
            )
        ),
        "runtime": {
            "python": "3.12",
        },
        "input_digests": {
            "deadline_evidence_collection": (
                collection_sha256
            ),
            "program_entries_binding": (
                binding_sha256
            ),
        },
        "output_digests": {
            "snapshot": snapshot_sha256,
            "pipeline_manifest": (
                pipeline_sha256
            ),
        },
        "deadline_evidence_collection_sha256": (
            collection_sha256
        ),
        "program_entries_binding_sha256": (
            binding_sha256
        ),
        "pipeline_manifest_sha256": (
            pipeline_sha256
        ),
        "test_state": {
            "focused": "PASSED",
        },
    }

    execution_path.write_bytes(
        canonical_manifest_bytes(
            execution_payload
        )
    )

    return {
        "collection": collection_path,
        "binding": binding_path,
        "snapshot": snapshot_path,
        "pipeline": pipeline_path,
        "execution": execution_path,
        "directory": run_directory,
        "destination": (
            run_directory
            / "prospective_dataset_v3_manifest.json"
        ),
        "lock": (
            run_directory
            / ".prospective_dataset_v3.lock"
        ),
    }


def publish(root: Path) -> dict:
    return v3.publish_prospective_v3(
        root,
        race_date=RACE_DATE,
        run_id=RUN_ID,
        branch=BRANCH,
        head=HEAD,
    )


def validate(root: Path) -> dict:
    return v3.validate_prospective_v3(
        root,
        race_date=RACE_DATE,
        run_id=RUN_ID,
        branch=BRANCH,
        head=HEAD,
    )


def test_authorized_contract_binding():
    assert v3.AUTHORIZED_CONTRACT_ID == (
        "D1B5-STAGE5-PROSPECTIVE-"
        "DATASET-V3-V1-R1"
    )
    assert v3.AUTHORIZED_CONTRACT_SHA256 == (
        "7c3b2031793e2a044750146a0a4ecab6"
        "277f5abc252e8be698ce42a2ae70766e"
    )


def test_public_api_signatures_are_exact():
    assert list(
        inspect.signature(
            v3.canonical_prospective_v3_manifest_bytes
        ).parameters
    ) == ["value"]

    expected = [
        "data_root",
        "race_date",
        "run_id",
        "branch",
        "head",
    ]

    assert list(
        inspect.signature(
            v3.publish_prospective_v3
        ).parameters
    ) == expected

    assert list(
        inspect.signature(
            v3.validate_prospective_v3
        ).parameters
    ) == expected


def test_canonical_bytes_are_exact():
    first = {
        "b": "日本語",
        "a": 1,
    }
    second = {
        "a": 1,
        "b": "日本語",
    }

    actual = (
        v3.canonical_prospective_v3_manifest_bytes(
            first
        )
    )

    assert actual == (
        v3.canonical_prospective_v3_manifest_bytes(
            second
        )
    )
    assert actual == (
        '{"a":1,"b":"日本語"}\n'
    ).encode("utf-8")
    assert actual.endswith(b"\n")
    assert not actual.endswith(b"\n\n")


def test_canonical_bytes_reject_nan():
    with pytest.raises(
        v3.ProspectiveV3ContractError
    ):
        v3.canonical_prospective_v3_manifest_bytes(
            {"value": float("nan")}
        )


def test_creates_valid_certification(tmp_path):
    paths = install_stage4(tmp_path)
    snapshot_before = (
        paths["snapshot"].read_bytes()
    )
    snapshot_mtime = (
        paths["snapshot"].stat().st_mtime_ns
    )

    result = publish(tmp_path)

    assert result["publication_status"] == (
        "CREATED"
    )
    assert result["cached"] is False
    assert result["classification"] == (
        "PROSPECTIVE_PIT_CERTIFIED"
    )

    assert (
        paths["snapshot"].read_bytes()
        == snapshot_before
    )
    assert (
        paths["snapshot"].stat().st_mtime_ns
        == snapshot_mtime
    )

    manifest_bytes = (
        paths["destination"].read_bytes()
    )
    manifest = json.loads(
        manifest_bytes.decode("utf-8")
    )

    assert manifest_bytes == (
        v3.canonical_prospective_v3_manifest_bytes(
            manifest
        )
    )
    assert set(manifest) == v3.MANIFEST_KEYS
    assert manifest["run_id"] == RUN_ID
    assert manifest["race_date"] == RACE_DATE
    assert manifest["branch"] == BRANCH
    assert manifest["head"] == HEAD
    assert manifest["dataset_schema"] == (
        v3.DATASET_SCHEMA_VALUE
    )
    assert set(manifest["parent_artifacts"]) == (
        set(v3.PARENT_NAMES)
    )
    assert set(manifest["parent_digests"]) == (
        set(v3.PARENT_NAMES)
    )


def test_return_schema_is_exact(tmp_path):
    install_stage4(tmp_path)
    result = publish(tmp_path)

    assert set(result) == {
        "race_date",
        "run_id",
        "classification",
        "dataset_relative_path",
        "dataset_sha256",
        "dataset_byte_length",
        "prospective_manifest_relative_path",
        "prospective_manifest_sha256",
        "prospective_manifest_byte_length",
        "cached",
        "publication_status",
        "paths",
    }

    assert set(result["paths"]) == {
        "directory",
        "dataset",
        "prospective_manifest",
    }

    assert all(
        isinstance(value, Path)
        for value in result["paths"].values()
    )


def test_identical_existing_is_validated_reuse(
    tmp_path,
):
    paths = install_stage4(tmp_path)
    first = publish(tmp_path)

    manifest_mtime = (
        paths["destination"].stat().st_mtime_ns
    )
    dataset_mtime = (
        paths["snapshot"].stat().st_mtime_ns
    )

    second = publish(tmp_path)

    assert first["publication_status"] == (
        "CREATED"
    )
    assert second["publication_status"] == (
        "VALIDATED_REUSE"
    )
    assert second["cached"] is True
    assert (
        paths["destination"].stat().st_mtime_ns
        == manifest_mtime
    )
    assert (
        paths["snapshot"].stat().st_mtime_ns
        == dataset_mtime
    )


def test_validate_existing_has_no_write_side_effect(
    tmp_path,
):
    paths = install_stage4(tmp_path)
    publish(tmp_path)

    manifest_mtime = (
        paths["destination"].stat().st_mtime_ns
    )
    snapshot_mtime = (
        paths["snapshot"].stat().st_mtime_ns
    )

    result = validate(tmp_path)

    assert result["cached"] is True
    assert result["publication_status"] == (
        "VALIDATED_REUSE"
    )
    assert (
        paths["destination"].stat().st_mtime_ns
        == manifest_mtime
    )
    assert (
        paths["snapshot"].stat().st_mtime_ns
        == snapshot_mtime
    )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("race_date", "2026/08/10"),
        ("run_id", "../bad"),
        ("branch", "wrong-branch"),
        ("head", "A" * 40),
    ],
)
def test_rejects_invalid_caller_identity(
    tmp_path,
    field,
    value,
):
    install_stage4(tmp_path)

    arguments = {
        "race_date": RACE_DATE,
        "run_id": RUN_ID,
        "branch": BRANCH,
        "head": HEAD,
    }
    arguments[field] = value

    with pytest.raises(
        v3.ProspectiveV3ContractError
    ):
        v3.publish_prospective_v3(
            tmp_path,
            **arguments,
        )


@pytest.mark.parametrize(
    "artifact",
    [
        "collection",
        "binding",
        "pipeline",
        "execution",
        "snapshot",
    ],
)
def test_rejects_missing_parent(
    tmp_path,
    artifact,
):
    paths = install_stage4(tmp_path)
    paths[artifact].unlink()

    with pytest.raises(
        v3.ProspectiveV3Error
    ):
        publish(tmp_path)

    assert not paths["destination"].exists()


def test_rejects_noncanonical_pipeline(
    tmp_path,
):
    paths = install_stage4(tmp_path)
    payload = json.loads(
        paths["pipeline"].read_text(
            encoding="utf-8"
        )
    )

    paths["pipeline"].write_text(
        json.dumps(
            payload,
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(
        v3.ProspectiveV3CacheError,
        match="non-canonical",
    ):
        publish(tmp_path)

    assert not paths["destination"].exists()


def test_rejects_pipeline_execution_digest_mismatch(
    tmp_path,
):
    paths = install_stage4(tmp_path)
    execution = json.loads(
        paths["execution"].read_text(
            encoding="utf-8"
        )
    )
    execution[
        "pipeline_manifest_sha256"
    ] = "f" * 64

    paths["execution"].write_bytes(
        canonical_manifest_bytes(
            execution
        )
    )

    with pytest.raises(
        v3.ProspectiveV3IntegrityError,
        match="pipeline manifest digest",
    ):
        publish(tmp_path)

    assert not paths["destination"].exists()


def test_rejects_unapproved_authorization(
    tmp_path,
):
    paths = install_stage4(
        tmp_path,
        authorization_state={
            "approved": True,
            "reviewer": "unexpected",
        },
    )

    with pytest.raises(
        v3.ProspectiveV3ContractError,
        match="authorization_state",
    ):
        publish(tmp_path)

    assert not paths["destination"].exists()


def test_rejects_dataset_column_order_mismatch(
    tmp_path,
):
    def mutate(frame):
        columns = list(frame.columns)
        columns[0], columns[1] = (
            columns[1],
            columns[0],
        )
        return frame[columns]

    paths = install_stage4(
        tmp_path,
        frame_mutator=mutate,
    )

    with pytest.raises(
        v3.ProspectiveV3ContractError,
        match="column order",
    ):
        publish(tmp_path)

    assert not paths["destination"].exists()


def test_rejects_dataset_logical_null(
    tmp_path,
):
    def mutate(frame):
        frame.loc[0, "racer_name"] = pd.NA
        return frame

    paths = install_stage4(
        tmp_path,
        frame_mutator=mutate,
    )

    with pytest.raises(
        v3.ProspectiveV3ContractError,
        match="logical null",
    ):
        publish(tmp_path)

    assert not paths["destination"].exists()


def test_rejects_post_deadline_source_time(
    tmp_path,
):
    def mutate(frame):
        value = pd.Timestamp(
            dt.datetime(
                2026,
                8,
                9,
                13,
                0,
                tzinfo=UTC,
            )
        )
        values = pd.Series(
            [value] * len(frame),
            index=frame.index,
            dtype=pd.DatetimeTZDtype(
                unit="ms",
                tz=UTC,
            ),
        )
        frame[
            "feature_source_fetched_at"
        ] = values
        frame[
            "feature_source_max_time"
        ] = values
        frame["source_max_time"] = values
        return frame

    paths = install_stage4(
        tmp_path,
        frame_mutator=mutate,
    )

    with pytest.raises(
        v3.ProspectiveV3ContractError,
        match="post-deadline",
    ):
        publish(tmp_path)

    assert not paths["destination"].exists()


def test_series_results_retained_but_not_feature(
    tmp_path,
):
    paths = install_stage4(tmp_path)
    result = publish(tmp_path)

    frame = pd.read_parquet(
        result["paths"]["dataset"],
        engine="pyarrow",
    )

    assert "series_results_raw" in frame
    assert (
        "series_results_raw"
        not in v3.MODEL_FEATURE_COLUMNS
    )
    assert (
        v3.DATASET_SCHEMA_VALUE[
            "series_results_raw_policy"
        ]
        == (
            "RETAINED_PRE_RACE_SOURCE_METADATA_"
            "NOT_MODEL_FEATURE"
        )
    )
    assert paths["snapshot"].is_file()


def test_conflicting_existing_manifest_rejected(
    tmp_path,
):
    paths = install_stage4(tmp_path)
    publish(tmp_path)

    payload = json.loads(
        paths["destination"].read_text(
            encoding="utf-8"
        )
    )
    payload["classification"] = "CONFLICT"

    paths["destination"].write_bytes(
        v3.canonical_prospective_v3_manifest_bytes(
            payload
        )
    )

    with pytest.raises(
        v3.ProspectiveV3ConflictError
    ):
        publish(tmp_path)


def test_corrupt_existing_manifest_rejected(
    tmp_path,
):
    paths = install_stage4(tmp_path)
    paths["destination"].write_text(
        "{not-json",
        encoding="utf-8",
    )

    with pytest.raises(
        v3.ProspectiveV3CacheError,
        match="valid UTF-8 JSON",
    ):
        publish(tmp_path)


def test_foreign_lock_is_preserved(tmp_path):
    paths = install_stage4(tmp_path)
    paths["lock"].write_text(
        "foreign",
        encoding="utf-8",
    )

    with pytest.raises(
        v3.ProspectiveV3Error,
        match="lock exists",
    ):
        publish(tmp_path)

    assert (
        paths["lock"].read_text(
            encoding="utf-8"
        )
        == "foreign"
    )
    assert not paths["destination"].exists()


def test_success_cleans_lock_and_temporary(
    tmp_path,
):
    paths = install_stage4(tmp_path)
    publish(tmp_path)

    assert not paths["lock"].exists()
    assert not list(
        paths["directory"].glob(
            ".prospective_dataset_v3_"
            "manifest.json.*.tmp"
        )
    )


def test_uses_link_and_not_replace():
    tree = ast.parse(
        Path(v3.__file__).read_text(
            encoding="utf-8"
        )
    )
    attributes = {
        node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(
            node.func,
            ast.Attribute,
        )
    }

    assert "link" in attributes
    assert "replace" not in attributes


def test_does_not_write_new_parquet(
    tmp_path,
):
    paths = install_stage4(tmp_path)
    before_bytes = paths["snapshot"].read_bytes()
    before_mtime = (
        paths["snapshot"].stat().st_mtime_ns
    )

    publish(tmp_path)

    assert paths["snapshot"].read_bytes() == (
        before_bytes
    )
    assert (
        paths["snapshot"].stat().st_mtime_ns
        == before_mtime
    )

    parquet_files = list(
        paths["directory"].glob("*.parquet")
    )
    assert parquet_files == [
        paths["snapshot"]
    ]


def test_only_stage5_manifest_is_added(
    tmp_path,
):
    paths = install_stage4(tmp_path)

    before = {
        path.relative_to(tmp_path).as_posix()
        for path in tmp_path.rglob("*")
        if path.is_file()
    }

    publish(tmp_path)

    after = {
        path.relative_to(tmp_path).as_posix()
        for path in tmp_path.rglob("*")
        if path.is_file()
    }

    assert after - before == {
        paths["destination"]
        .relative_to(tmp_path)
        .as_posix()
    }
