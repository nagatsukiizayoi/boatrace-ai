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
