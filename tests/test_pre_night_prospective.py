import datetime as dt
import hashlib
import json
import os
from pathlib import Path

import pytest

from boatrace_ai.pipelines import pre_night_prospective as prospective


RACE_DATE = "2026-08-10"
AS_OF_TIME = "2026-08-09T21:30:00+09:00"
COMPLETED_AT = "2026-08-09T21:05:00+09:00"
REPOSITORY_COMMIT = "a" * 40
DEADLINE_EVIDENCE_SHA256 = "d" * 64


def file_record(path):
    return {
        "path": str(path),
        "size": path.stat().st_size,
        "sha256": prospective.sha256_file(path),
    }


def pit_decision(reason="All PRE_NIGHT PIT checks passed"):
    return {
        "status": "ELIGIBLE",
        "eligible": True,
        "reason": reason,
        "race_date": RACE_DATE,
        "as_of_time": AS_OF_TIME,
        "details": {},
    }


def eligibility_fields(reason="All PRE_NIGHT PIT checks passed"):
    pit = pit_decision(reason)

    return {
        "eligibility_status": "ELIGIBLE",
        "eligible_for_pre_night": True,
        "eligibility_reason": reason,
        "pit_eligibility": pit,
    }


def build_context(tmp_path):
    root = Path(tmp_path)

    artifact_directory = root / "artifacts"
    artifact_directory.mkdir(parents=True)

    source_archive = artifact_directory / "program.lzh"
    source_metadata = artifact_directory / "program.lzh.json"
    output_parquet = artifact_directory / "program.parquet"
    pipeline_manifest = artifact_directory / "program.parquet.json"

    source_archive.write_bytes(b"program archive")
    source_metadata.write_text(
        json.dumps({"source_type": "program"}),
        encoding="utf-8",
    )
    output_parquet.write_bytes(b"program parquet")

    pipeline_payload = {
        "race_date": RACE_DATE,
        "as_of_time": AS_OF_TIME,
        **eligibility_fields(),
        "deadline_evidence_sha256": DEADLINE_EVIDENCE_SHA256,
    }
    pipeline_manifest.write_text(
        json.dumps(pipeline_payload),
        encoding="utf-8",
    )

    artifact_paths = {
        "source_archive": source_archive,
        "source_metadata": source_metadata,
        "output_parquet": output_parquet,
        "pipeline_manifest": pipeline_manifest,
    }

    execution_artifacts = {
        name: file_record(path)
        for name, path in artifact_paths.items()
    }

    execution_payload = {
        "status": "SUCCESS",
        "race_date": RACE_DATE,
        "as_of_time": AS_OF_TIME,
        "started_at": "2026-08-09T21:00:00+09:00",
        "completed_at": COMPLETED_AT,
        "dry_run": False,
        "artifacts": execution_artifacts,
        **eligibility_fields(),
        "deadline_evidence_sha256": DEADLINE_EVIDENCE_SHA256,
    }

    execution_path = (
        root
        / "manifests"
        / "pre_night_v2"
        / "daily"
        / f"{RACE_DATE}.json"
    )
    execution_path.parent.mkdir(parents=True)
    execution_path.write_text(
        json.dumps(execution_payload),
        encoding="utf-8",
    )

    validation = {
        "manifest_path": execution_path,
        "manifest": execution_payload,
        "artifacts": {
            name: {
                "path": path,
                "size": path.stat().st_size,
                "sha256": prospective.sha256_file(path),
            }
            for name, path in artifact_paths.items()
        },
        "cached": True,
        "skipped": True,
    }

    manifest = prospective.build_prospective_manifest(
        race_date=RACE_DATE,
        data_root=root,
        execution_manifest_path=execution_path,
        execution_validation=validation,
        repository_commit=REPOSITORY_COMMIT,
        created_at=COMPLETED_AT,
    )

    return {
        "root": root,
        "manifest": manifest,
        "execution_path": execution_path,
        "execution_validation": validation,
        "artifact_paths": artifact_paths,
    }


def test_builds_valid_manifest(tmp_path):
    context = build_context(tmp_path)
    manifest = context["manifest"]

    assert manifest["contract_version"] == (
        prospective.CONTRACT_VERSION
    )
    assert manifest["artifact_type"] == prospective.ARTIFACT_TYPE
    assert manifest["eligible_for_pre_night"] is True
    assert manifest["pit_eligibility"]["status"] == "ELIGIBLE"
    assert set(manifest["artifacts"]) == (
        prospective.REQUIRED_ARTIFACTS
    )


def test_run_id_is_deterministic(tmp_path):
    context = build_context(tmp_path)

    second = prospective.build_prospective_manifest(
        race_date=RACE_DATE,
        data_root=context["root"],
        execution_manifest_path=context["execution_path"],
        execution_validation=context["execution_validation"],
        repository_commit=REPOSITORY_COMMIT,
        created_at=COMPLETED_AT,
    )

    assert second["run_id"] == context["manifest"]["run_id"]


def test_directory_contract(tmp_path):
    directory = prospective.build_prospective_directory(
        RACE_DATE,
        tmp_path,
    )

    assert directory == (
        tmp_path
        / "manifests"
        / "pre_night_prospective_v1"
        / RACE_DATE
    )


def test_rejects_naive_created_at(tmp_path):
    context = build_context(tmp_path)

    with pytest.raises(
        prospective.PreNightProspectiveContractError,
        match="timezone",
    ):
        prospective.build_prospective_manifest(
            race_date=RACE_DATE,
            data_root=context["root"],
            execution_manifest_path=context["execution_path"],
            execution_validation=context["execution_validation"],
            repository_commit=REPOSITORY_COMMIT,
            created_at="2026-08-09T21:05:00",
        )


def test_rejects_created_at_different_from_completion(tmp_path):
    context = build_context(tmp_path)
    manifest = dict(context["manifest"])
    manifest["created_at"] = "2026-08-09T21:06:00+09:00"

    with pytest.raises(
        prospective.PreNightProspectiveContractError,
        match="completed_at",
    ):
        prospective.write_prospective_manifest(
            manifest,
            context["root"],
        )


@pytest.mark.parametrize(
    "invalid_date",
    ["2026/08/10", "2026-13-40", "", None],
)
def test_rejects_invalid_race_date(tmp_path, invalid_date):
    context = build_context(tmp_path)

    with pytest.raises(
        prospective.PreNightProspectiveContractError
    ):
        prospective.build_prospective_manifest(
            race_date=invalid_date,
            data_root=context["root"],
            execution_manifest_path=context["execution_path"],
            execution_validation=context["execution_validation"],
            repository_commit=REPOSITORY_COMMIT,
            created_at=COMPLETED_AT,
        )


@pytest.mark.parametrize(
    "invalid_sha",
    ["", "a" * 39, "a" * 41, "g" * 40, 123],
)
def test_rejects_invalid_repository_commit(tmp_path, invalid_sha):
    context = build_context(tmp_path)

    with pytest.raises(
        prospective.PreNightProspectiveContractError,
        match="repository_commit",
    ):
        prospective.build_prospective_manifest(
            race_date=RACE_DATE,
            data_root=context["root"],
            execution_manifest_path=context["execution_path"],
            execution_validation=context["execution_validation"],
            repository_commit=invalid_sha,
            created_at=COMPLETED_AT,
        )


def test_rejects_ineligible_manifest(tmp_path):
    context = build_context(tmp_path)
    manifest = dict(context["manifest"])
    manifest["eligible_for_pre_night"] = False

    with pytest.raises(
        prospective.PreNightProspectiveContractError,
        match="must be true",
    ):
        prospective.write_prospective_manifest(
            manifest,
            context["root"],
        )


def test_rejects_pit_mismatch(tmp_path):
    context = build_context(tmp_path)
    manifest = dict(context["manifest"])
    manifest["pit_eligibility"] = dict(
        manifest["pit_eligibility"]
    )
    manifest["pit_eligibility"]["reason"] = "tampered"

    with pytest.raises(
        prospective.PreNightProspectiveIntegrityError,
        match="PIT",
    ):
        prospective.write_prospective_manifest(
            manifest,
            context["root"],
        )


def test_rejects_missing_artifact(tmp_path):
    context = build_context(tmp_path)
    context["artifact_paths"]["source_archive"].unlink()

    with pytest.raises(
        prospective.PreNightProspectiveIntegrityError,
        match="missing|does not exist",
    ):
        prospective.write_prospective_manifest(
            context["manifest"],
            context["root"],
        )


def test_rejects_tampered_artifact(tmp_path):
    context = build_context(tmp_path)
    context["artifact_paths"]["output_parquet"].write_bytes(
        b"tampered"
    )

    with pytest.raises(
        prospective.PreNightProspectiveIntegrityError,
        match="SHA-256|size",
    ):
        prospective.write_prospective_manifest(
            context["manifest"],
            context["root"],
        )


def test_rejects_artifact_path_swap(tmp_path):
    context = build_context(tmp_path)
    manifest = json.loads(json.dumps(context["manifest"]))
    source = manifest["artifacts"]["source_archive"]
    source["path"] = str(
        context["artifact_paths"]["source_metadata"]
    )
    source["size"] = (
        context["artifact_paths"]["source_metadata"]
        .stat()
        .st_size
    )
    source["sha256"] = prospective.sha256_file(
        context["artifact_paths"]["source_metadata"]
    )

    with pytest.raises(
        prospective.PreNightProspectiveIntegrityError,
        match="path differs",
    ):
        prospective.write_prospective_manifest(
            manifest,
            context["root"],
        )


def test_rejects_path_outside_data_root(tmp_path):
    context = build_context(tmp_path)
    outside = tmp_path.parent / "outside-prospective-artifact"
    outside.write_bytes(b"outside")

    manifest = json.loads(json.dumps(context["manifest"]))
    record = manifest["artifacts"]["source_archive"]
    record["path"] = str(outside)
    record["size"] = outside.stat().st_size
    record["sha256"] = prospective.sha256_file(outside)

    try:
        with pytest.raises(
            prospective.PreNightProspectiveIntegrityError,
            match="outside data_root",
        ):
            prospective.write_prospective_manifest(
                manifest,
                context["root"],
            )
    finally:
        outside.unlink(missing_ok=True)


def test_rejects_unknown_top_level_key(tmp_path):
    context = build_context(tmp_path)
    manifest = dict(context["manifest"])
    manifest["unexpected"] = True

    with pytest.raises(
        prospective.PreNightProspectiveContractError,
        match="unknown",
    ):
        prospective.write_prospective_manifest(
            manifest,
            context["root"],
        )


@pytest.mark.parametrize(
    "forbidden_key",
    [
        "result",
        "payout",
        "label",
        "prediction",
        "odds",
        "probability",
        "recommendation",
        "ev",
    ],
)
def test_rejects_forbidden_recursive_key(
    tmp_path,
    forbidden_key,
):
    context = build_context(tmp_path)
    manifest = json.loads(json.dumps(context["manifest"]))
    manifest["pit_eligibility"]["details"] = {
        forbidden_key: "forbidden"
    }

    with pytest.raises(
        prospective.PreNightProspectiveContractError,
        match="Forbidden prospective key",
    ):
        prospective.write_prospective_manifest(
            manifest,
            context["root"],
        )


def test_atomic_write_and_validate(tmp_path):
    context = build_context(tmp_path)

    result = prospective.write_prospective_manifest(
        context["manifest"],
        context["root"],
    )

    assert result["cached"] is False
    assert result["manifest_path"].is_file()

    validated = prospective.validate_prospective_manifest(
        result["manifest_path"],
        context["root"],
        race_date=RACE_DATE,
        expected_repository_commit=REPOSITORY_COMMIT,
    )

    assert validated["run_id"] == context["manifest"]["run_id"]


def test_identical_write_is_idempotent(tmp_path):
    context = build_context(tmp_path)

    first = prospective.write_prospective_manifest(
        context["manifest"],
        context["root"],
    )
    second = prospective.write_prospective_manifest(
        context["manifest"],
        context["root"],
    )

    assert first["cached"] is False
    assert second["cached"] is True
    assert first["manifest_path"] == second["manifest_path"]


def test_publish_failure_cleans_temporary_file(
    tmp_path,
    monkeypatch,
):
    context = build_context(tmp_path)

    def fail_link(source, destination):
        raise OSError("injected publish failure")

    monkeypatch.setattr(os, "link", fail_link)

    with pytest.raises(
        OSError,
        match="injected publish failure",
    ):
        prospective.write_prospective_manifest(
            context["manifest"],
            context["root"],
        )

    directory = prospective.build_prospective_directory(
        RACE_DATE,
        context["root"],
    )

    assert not prospective.build_prospective_manifest_path(
        context["manifest"],
        context["root"],
    ).exists()

    if directory.exists():
        assert list(directory.glob("*.tmp")) == []


def test_corrupt_existing_manifest_is_rejected(tmp_path):
    context = build_context(tmp_path)
    destination = (
        prospective.build_prospective_manifest_path(
            context["manifest"],
            context["root"],
        )
    )
    destination.parent.mkdir(parents=True)
    destination.write_text("{not-json", encoding="utf-8")

    with pytest.raises(
        prospective.PreNightProspectiveIntegrityError,
        match="valid JSON",
    ):
        prospective.write_prospective_manifest(
            context["manifest"],
            context["root"],
        )


def test_same_run_id_different_content_conflicts(
    tmp_path,
    monkeypatch,
):
    context = build_context(tmp_path)
    first = prospective.write_prospective_manifest(
        context["manifest"],
        context["root"],
    )

    original_validate = (
        prospective.validate_prospective_manifest
    )

    def different_existing(*args, **kwargs):
        validated = original_validate(*args, **kwargs)
        validated["manifest"] = dict(validated["manifest"])
        validated["manifest"]["created_at"] = (
            "2026-08-09T21:05:01+09:00"
        )
        return validated

    monkeypatch.setattr(
        prospective,
        "validate_prospective_manifest",
        different_existing,
    )

    with pytest.raises(
        prospective.PreNightProspectiveConflictError
    ):
        prospective.write_prospective_manifest(
            context["manifest"],
            context["root"],
        )

    assert first["manifest_path"].is_file()


def test_manifest_filename_tampering_is_rejected(tmp_path):
    context = build_context(tmp_path)
    written = prospective.write_prospective_manifest(
        context["manifest"],
        context["root"],
    )

    wrong = written["manifest_path"].with_name(
        "0" * 64 + ".json"
    )
    written["manifest_path"].replace(wrong)

    with pytest.raises(
        prospective.PreNightProspectiveIntegrityError,
        match="filename",
    ):
        prospective.validate_prospective_manifest(
            wrong,
            context["root"],
        )


# BEGIN PHASE1_D1B4_TESTS


def _d1b4_build_with_execution(context, execution_manifest):
    import copy

    validation = copy.deepcopy(context["execution_validation"])
    validation["manifest"] = copy.deepcopy(execution_manifest)

    return prospective.build_prospective_manifest(
        race_date=RACE_DATE,
        data_root=context["root"],
        execution_manifest_path=context["execution_path"],
        execution_validation=validation,
        repository_commit=REPOSITORY_COMMIT,
        created_at=COMPLETED_AT,
    )


def _d1b4_pipeline_payload(context):
    return json.loads(
        context["artifact_paths"]["pipeline_manifest"].read_text(
            encoding="utf-8"
        )
    )


def test_d1b4_t01_manifest_copies_execution_deadline_digest(tmp_path):
    context = build_context(tmp_path)

    assert context["manifest"]["deadline_evidence_sha256"] == (
        DEADLINE_EVIDENCE_SHA256
    )
    assert context["manifest"]["deadline_evidence_sha256"] == (
        context["execution_validation"]["manifest"][
            "deadline_evidence_sha256"
        ]
    )


def test_d1b4_t02_exact_v2_top_level_key_set(tmp_path):
    context = build_context(tmp_path)

    assert set(context["manifest"]) == prospective.TOP_LEVEL_KEYS
    assert "deadline_evidence_sha256" in prospective.TOP_LEVEL_KEYS


def test_d1b4_t03_contract_version_is_v2(tmp_path):
    context = build_context(tmp_path)

    assert prospective.CONTRACT_VERSION == (
        "pre_night_prospective_dataset_v2"
    )
    assert context["manifest"]["contract_version"] == (
        "pre_night_prospective_dataset_v2"
    )


def test_d1b4_t04_digest_changes_run_id():
    artifacts = {
        name: {
            "path": f"/tmp/{name}",
            "size": 1,
            "sha256": "a" * 64,
        }
        for name in prospective.REQUIRED_ARTIFACTS
    }

    first = prospective._compute_run_id(
        race_date=RACE_DATE,
        repository_commit=REPOSITORY_COMMIT,
        deadline_evidence_sha256="d" * 64,
        artifacts=artifacts,
    )
    second = prospective._compute_run_id(
        race_date=RACE_DATE,
        repository_commit=REPOSITORY_COMMIT,
        deadline_evidence_sha256="e" * 64,
        artifacts=artifacts,
    )

    assert first != second


def test_d1b4_t05_missing_prospective_digest_fails_closed(tmp_path):
    context = build_context(tmp_path)
    manifest = dict(context["manifest"])
    manifest.pop("deadline_evidence_sha256")

    with pytest.raises(
        prospective.PreNightProspectiveContractError,
        match="keys mismatch|deadline_evidence_sha256",
    ):
        prospective.write_prospective_manifest(
            manifest,
            context["root"],
        )


def test_d1b4_t06_malformed_prospective_digest_fails_closed(tmp_path):
    context = build_context(tmp_path)
    manifest = dict(context["manifest"])
    manifest["deadline_evidence_sha256"] = "ABC"

    with pytest.raises(
        prospective.PreNightProspectiveContractError,
        match="deadline_evidence_sha256",
    ):
        prospective.write_prospective_manifest(
            manifest,
            context["root"],
        )


def test_d1b4_t07_missing_execution_digest_fails_closed(tmp_path):
    import copy

    context = build_context(tmp_path)
    execution = copy.deepcopy(
        context["execution_validation"]["manifest"]
    )
    execution.pop("deadline_evidence_sha256")

    with pytest.raises(
        prospective.PreNightProspectiveContractError,
        match="execution.deadline_evidence_sha256",
    ):
        _d1b4_build_with_execution(context, execution)


def test_d1b4_t08_malformed_execution_digest_fails_closed(tmp_path):
    import copy

    context = build_context(tmp_path)
    execution = copy.deepcopy(
        context["execution_validation"]["manifest"]
    )
    execution["deadline_evidence_sha256"] = "F" * 64

    with pytest.raises(
        prospective.PreNightProspectiveContractError,
        match="execution.deadline_evidence_sha256",
    ):
        _d1b4_build_with_execution(context, execution)


def test_d1b4_t09_prospective_execution_mismatch_fails_closed(tmp_path):
    context = build_context(tmp_path)
    manifest = dict(context["manifest"])
    manifest["deadline_evidence_sha256"] = "e" * 64

    with pytest.raises(
        prospective.PreNightProspectiveIntegrityError,
        match="deadline|SHA-256|differ",
    ):
        prospective.write_prospective_manifest(
            manifest,
            context["root"],
        )


def test_d1b4_t10_missing_pipeline_digest_fails_closed(tmp_path):
    import copy

    context = build_context(tmp_path)
    prospective_payload = copy.deepcopy(context["manifest"])
    execution = copy.deepcopy(
        context["execution_validation"]["manifest"]
    )
    pipeline = _d1b4_pipeline_payload(context)
    pipeline.pop("deadline_evidence_sha256")

    with pytest.raises(
        prospective.PreNightProspectiveContractError,
        match="pipeline.deadline_evidence_sha256",
    ):
        prospective._validate_pit_binding(
            prospective_payload,
            execution,
            pipeline,
        )


def test_d1b4_t11_malformed_pipeline_digest_fails_closed(tmp_path):
    import copy

    context = build_context(tmp_path)
    prospective_payload = copy.deepcopy(context["manifest"])
    execution = copy.deepcopy(
        context["execution_validation"]["manifest"]
    )
    pipeline = _d1b4_pipeline_payload(context)
    pipeline["deadline_evidence_sha256"] = "invalid"

    with pytest.raises(
        prospective.PreNightProspectiveContractError,
        match="pipeline.deadline_evidence_sha256",
    ):
        prospective._validate_pit_binding(
            prospective_payload,
            execution,
            pipeline,
        )


def test_d1b4_t12_three_way_digest_mismatch_fails_closed(tmp_path):
    import copy

    context = build_context(tmp_path)
    prospective_payload = copy.deepcopy(context["manifest"])
    execution = copy.deepcopy(
        context["execution_validation"]["manifest"]
    )
    pipeline = _d1b4_pipeline_payload(context)
    pipeline["deadline_evidence_sha256"] = "e" * 64

    with pytest.raises(
        prospective.PreNightProspectiveIntegrityError,
        match="deadline|SHA-256|differ",
    ):
        prospective._validate_pit_binding(
            prospective_payload,
            execution,
            pipeline,
        )


def test_d1b4_t13_matching_three_way_digest_validates(tmp_path):
    import copy

    context = build_context(tmp_path)
    prospective_payload = copy.deepcopy(context["manifest"])
    execution = copy.deepcopy(
        context["execution_validation"]["manifest"]
    )
    pipeline = _d1b4_pipeline_payload(context)

    assert prospective._validate_pit_binding(
        prospective_payload,
        execution,
        pipeline,
    ) is None


def test_d1b4_t14_cached_manifest_missing_digest_is_rejected(tmp_path):
    context = build_context(tmp_path)
    written = prospective.write_prospective_manifest(
        context["manifest"],
        context["root"],
    )
    path = written["manifest_path"]
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload.pop("deadline_evidence_sha256")
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(
        prospective.PreNightProspectiveContractError,
        match="keys mismatch|deadline_evidence_sha256",
    ):
        prospective.validate_prospective_manifest(
            path,
            context["root"],
        )


def test_d1b4_t15_cached_manifest_digest_mismatch_is_rejected(tmp_path):
    context = build_context(tmp_path)
    written = prospective.write_prospective_manifest(
        context["manifest"],
        context["root"],
    )
    path = written["manifest_path"]
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["deadline_evidence_sha256"] = "e" * 64
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(
        prospective.PreNightProspectiveIntegrityError,
        match="deadline|SHA-256|differ",
    ):
        prospective.validate_prospective_manifest(
            path,
            context["root"],
        )


def test_d1b4_t16_no_full_deadline_payload_is_serialized(tmp_path):
    context = build_context(tmp_path)
    manifest = context["manifest"]
    serialized = json.dumps(manifest, sort_keys=True)

    assert {
        "deadline_evidence",
        "raw_source_bytes",
        "raw_html",
        "canonical_deadline_evidence_bytes",
        "canonical_bytes",
        "eligibility_cutoff_at",
        "safety_margin_seconds",
    }.issubset(prospective.FORBIDDEN_KEYS)

    assert manifest["deadline_evidence_sha256"] == (
        DEADLINE_EVIDENCE_SHA256
    )
    assert "deadline_evidence" not in manifest
    assert "raw_source_bytes" not in serialized
    assert "raw_html" not in serialized
    assert "canonical_deadline_evidence_bytes" not in serialized
    assert "canonical_bytes" not in serialized
    assert "eligibility_cutoff_at" not in serialized
    assert "safety_margin_seconds" not in serialized


def test_d1b4_t17_public_function_signatures_are_preserved():
    import inspect

    expected = {
        "build_prospective_directory": [
            "race_date",
            "data_root",
        ],
        "build_prospective_manifest_path": [
            "manifest",
            "data_root",
        ],
        "build_prospective_manifest": [
            "race_date",
            "data_root",
            "execution_manifest_path",
            "execution_validation",
            "repository_commit",
            "created_at",
        ],
        "validate_prospective_manifest": [
            "manifest_path",
            "data_root",
            "race_date",
            "expected_repository_commit",
        ],
        "write_prospective_manifest": [
            "manifest",
            "data_root",
        ],
        "resolve_repository_commit": [
            "repository_root",
        ],
    }

    for name, parameters in expected.items():
        function = getattr(prospective, name)
        assert list(inspect.signature(function).parameters) == parameters


def test_d1b4_t18_return_container_shapes_are_preserved(tmp_path):
    context = build_context(tmp_path)

    assert set(context["manifest"]) == prospective.TOP_LEVEL_KEYS

    written = prospective.write_prospective_manifest(
        context["manifest"],
        context["root"],
    )

    assert set(written) == {
        "manifest_path",
        "manifest",
        "artifacts",
        "execution_manifest",
        "pipeline_manifest",
        "run_id",
        "race_date",
        "repository_commit",
        "cached",
        "skipped",
    }


def test_d1b4_t19_storage_directory_remains_v1(tmp_path):
    directory = prospective.build_prospective_directory(
        RACE_DATE,
        tmp_path,
    )

    assert directory == (
        tmp_path
        / "manifests"
        / "pre_night_prospective_v1"
        / RACE_DATE
    )


def test_d1b4_t20_validation_failure_precedes_publication(tmp_path):
    context = build_context(tmp_path)
    valid_manifest = context["manifest"]
    destination = prospective.build_prospective_manifest_path(
        valid_manifest,
        context["root"],
    )

    invalid_manifest = dict(valid_manifest)
    invalid_manifest.pop("deadline_evidence_sha256")

    with pytest.raises(
        prospective.PreNightProspectiveContractError,
        match="keys mismatch|deadline_evidence_sha256",
    ):
        prospective.write_prospective_manifest(
            invalid_manifest,
            context["root"],
        )

    assert not destination.exists()

    directory = prospective.build_prospective_directory(
        RACE_DATE,
        context["root"],
    )

    assert (
        not directory.exists()
        or list(directory.glob("*.tmp-*")) == []
    )


# END PHASE1_D1B4_TESTS
