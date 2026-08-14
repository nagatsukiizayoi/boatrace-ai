from __future__ import annotations

import ast
import hashlib
import inspect
import json
from pathlib import Path

import pytest

from boatrace_ai.pipelines import pre_night_manifest_chain as chain
from boatrace_ai.pipelines.pre_night_deadline_collection import (
    COLLECTION_CONTRACT_VERSION,
)
from boatrace_ai.pipelines.pre_night_program_binding import (
    build_pre_night_program_entries_binding,
    canonical_program_entries_binding_bytes,
)


DATE = "2026-07-30"
RUN = "run-20260730-test"
D1 = "1" * 64
P1 = "a" * 64


def canonical(value):
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def install_inputs(root):
    collection = {
        "contract_version": COLLECTION_CONTRACT_VERSION,
        "race_date": DATE,
        "expected_venue_codes": ["01"],
        "entry_count": 1,
        "entries": [{
            "race_date": DATE,
            "venue_code": "01",
            "relative_path": (
                "prospective/pre_night/deadline_evidence/"
                "2026/07/30/01/deadline_evidence.json"
            ),
            "deadline_evidence_sha256": D1,
            "byte_length": 10,
            "contract_version": "test-stage1-v1",
        }],
    }
    collection_bytes = canonical(collection)
    collection_path = (
        root / "prospective/pre_night/deadline_evidence_collections/"
        "2026/07/30/deadline_evidence_collection.json"
    )
    collection_path.parent.mkdir(parents=True)
    collection_path.write_bytes(collection_bytes)
    collection_sha = hashlib.sha256(collection_bytes).hexdigest()

    entries = [
        {
            "race_date": DATE,
            "venue_code": "01",
            "race_no": race_no,
            "boat_no": boat_no,
        }
        for race_no in range(1, 13)
        for boat_no in range(1, 7)
    ]
    binding = build_pre_night_program_entries_binding(
        race_date=DATE,
        deadline_evidence_collection_sha256=collection_sha,
        deadline_evidence_sha256_by_venue={"01": D1},
        program_source_sha256_by_venue={"01": P1},
        program_entries=entries,
    )
    binding_bytes = canonical_program_entries_binding_bytes(binding)
    binding_path = (
        root / "prospective/pre_night/runs/2026/07/30"
        / RUN / "program_entries_binding.json"
    )
    binding_path.parent.mkdir(parents=True)
    binding_path.write_bytes(binding_bytes)

    snapshot = (
        root / "prospective/pre_night/runs/2026/07/30"
        / RUN / "snapshot.parquet"
    )
    snapshot.write_bytes(b"snapshot-exact-bytes")

    return {
        "collection": collection_path,
        "binding": binding_path,
        "snapshot": snapshot,
        "collection_sha": collection_sha,
        "binding_sha": hashlib.sha256(binding_bytes).hexdigest(),
    }


def publish(root, **overrides):
    values = {
        "race_date": DATE,
        "run_id": RUN,
        "snapshot_relative_path": (
            "prospective/pre_night/runs/2026/07/30/"
            f"{RUN}/snapshot.parquet"
        ),
        "pipeline_name": "pre-night",
        "pipeline_version": "v1",
        "branch": (
            "feature/pre-night-authoritative-deadline-pit-contract-v2"
        ),
        "head": "a" * 40,
        "started_at": "2026-07-30T10:00:00+09:00",
        "completed_at": "2026-07-30T10:01:00+09:00",
        "authorization_state": {"approved": True},
        "runtime": {"python": "3.12"},
        "test_state": {"focused": "PASSED"},
    }
    values.update(overrides)
    return chain.publish_pre_night_manifest_chain(root, **values)


def test_public_api_signature():
    assert list(
        inspect.signature(
            chain.publish_pre_night_manifest_chain
        ).parameters
    ) == [
        "data_root",
        "race_date",
        "run_id",
        "snapshot_relative_path",
        "pipeline_name",
        "pipeline_version",
        "branch",
        "head",
        "started_at",
        "completed_at",
        "authorization_state",
        "runtime",
        "test_state",
    ]


def test_creates_exact_manifest_chain(tmp_path):
    inputs = install_inputs(tmp_path)
    result = publish(tmp_path)

    assert result["publication_status"] == "CREATED"
    assert result["cached"] is False

    pipeline_bytes = result["paths"]["pipeline_manifest"].read_bytes()
    execution_bytes = result["paths"]["execution_manifest"].read_bytes()
    pipeline = json.loads(pipeline_bytes)
    execution = json.loads(execution_bytes)

    assert pipeline_bytes == canonical(pipeline)
    assert execution_bytes == canonical(execution)
    assert pipeline["deadline_evidence_collection_sha256"] == (
        inputs["collection_sha"]
    )
    assert pipeline["program_entries_binding_sha256"] == (
        inputs["binding_sha"]
    )
    assert execution["pipeline_manifest_sha256"] == (
        hashlib.sha256(pipeline_bytes).hexdigest()
    )
    assert execution["deadline_evidence_collection_sha256"] == (
        inputs["collection_sha"]
    )
    assert execution["program_entries_binding_sha256"] == (
        inputs["binding_sha"]
    )
    assert "manifest_sha256" not in pipeline
    assert "execution_manifest_sha256" not in execution
    assert "generated_at" not in pipeline
    assert "generated_at" not in execution


def test_receipt_digests_are_exact(tmp_path):
    install_inputs(tmp_path)
    result = publish(tmp_path)

    pipeline = result["paths"]["pipeline_manifest"].read_bytes()
    execution = result["paths"]["execution_manifest"].read_bytes()

    assert result["pipeline_manifest_sha256"] == (
        hashlib.sha256(pipeline).hexdigest()
    )
    assert result["execution_manifest_sha256"] == (
        hashlib.sha256(execution).hexdigest()
    )
    assert result["pipeline_manifest_byte_length"] == len(pipeline)
    assert result["execution_manifest_byte_length"] == len(execution)


def test_identical_cache_is_validated_reuse(tmp_path):
    install_inputs(tmp_path)
    first = publish(tmp_path)
    pipeline_mtime = first["paths"]["pipeline_manifest"].stat().st_mtime_ns

    second = publish(tmp_path)

    assert second["cached"] is True
    assert second["publication_status"] == "VALIDATED_REUSE"
    assert (
        second["paths"]["pipeline_manifest"].stat().st_mtime_ns
        == pipeline_mtime
    )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("race_date", "2026/07/30"),
        ("run_id", "../bad"),
        ("head", "A" * 40),
        ("started_at", "2026-07-30T10:00:00"),
        ("completed_at", "2026-07-30T09:59:00+09:00"),
    ],
)
def test_rejects_invalid_identity_and_time(tmp_path, field, value):
    install_inputs(tmp_path)
    with pytest.raises(chain.PreNightManifestChainContractError):
        publish(tmp_path, **{field: value})


def test_rejects_noncanonical_stage2(tmp_path):
    inputs = install_inputs(tmp_path)
    payload = json.loads(inputs["collection"].read_text())
    inputs["collection"].write_text(
        json.dumps(payload, indent=2) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(
        chain.PreNightManifestChainCacheError,
        match="non-canonical",
    ):
        publish(tmp_path)


def test_rejects_malformed_stage3(tmp_path):
    inputs = install_inputs(tmp_path)
    inputs["binding"].write_text("{bad", encoding="utf-8")

    with pytest.raises(chain.PreNightManifestChainCacheError):
        publish(tmp_path)


def test_rejects_stage2_stage3_digest_divergence(tmp_path):
    inputs = install_inputs(tmp_path)
    payload = json.loads(inputs["collection"].read_text())
    payload["entries"][0]["byte_length"] = 11
    inputs["collection"].write_bytes(canonical(payload))

    with pytest.raises(
        chain.PreNightManifestChainCacheError,
        match="collection digest mismatch",
    ):
        publish(tmp_path)


def test_rejects_missing_snapshot(tmp_path):
    inputs = install_inputs(tmp_path)
    inputs["snapshot"].unlink()

    with pytest.raises(
        chain.PreNightManifestChainCacheError,
        match="snapshot",
    ):
        publish(tmp_path)


def test_rejects_snapshot_path_escape(tmp_path):
    install_inputs(tmp_path)

    with pytest.raises(chain.PreNightManifestChainContractError):
        publish(tmp_path, snapshot_relative_path="../outside")


def test_rejects_conflicting_pipeline_cache(tmp_path):
    install_inputs(tmp_path)
    first = publish(tmp_path)
    first["paths"]["execution_manifest"].unlink()
    first["paths"]["pipeline_manifest"].write_text(
        '{"conflict":true}\n',
        encoding="utf-8",
    )

    with pytest.raises(chain.PreNightManifestChainCacheError):
        publish(tmp_path)


def test_rejects_execution_without_pipeline(tmp_path):
    inputs = install_inputs(tmp_path)
    directory = inputs["binding"].parent
    (directory / "execution_manifest.json").write_text(
        "{}\n",
        encoding="utf-8",
    )

    with pytest.raises(
        chain.PreNightManifestChainCacheError,
        match="without Pipeline",
    ):
        publish(tmp_path)


def test_resumes_pipeline_only_chain(tmp_path):
    install_inputs(tmp_path)
    first = publish(tmp_path)
    first["paths"]["execution_manifest"].unlink()

    resumed = publish(tmp_path)

    assert resumed["publication_status"] == (
        "RESUMED_CREATED_EXECUTION"
    )
    assert resumed["paths"]["pipeline_manifest"].is_file()
    assert resumed["paths"]["execution_manifest"].is_file()


def test_success_cleans_lock_and_temporaries(tmp_path):
    inputs = install_inputs(tmp_path)
    result = publish(tmp_path)
    directory = inputs["binding"].parent

    assert not (directory / ".manifest_chain.lock").exists()
    assert not list(directory.glob(".*manifest.json.*.tmp"))
    assert result["paths"]["pipeline_manifest"].is_file()


def test_foreign_lock_is_preserved(tmp_path):
    inputs = install_inputs(tmp_path)
    lock = inputs["binding"].parent / ".manifest_chain.lock"
    lock.write_text("foreign", encoding="utf-8")

    with pytest.raises(
        chain.PreNightManifestChainError,
        match="lock exists",
    ):
        publish(tmp_path)

    assert lock.read_text(encoding="utf-8") == "foreign"


def test_uses_link_and_not_replace():
    tree = ast.parse(
        Path(chain.__file__).read_text(encoding="utf-8")
    )
    calls = set()

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if isinstance(node.func, ast.Attribute):
            calls.add(node.func.attr)
        elif isinstance(node.func, ast.Name):
            calls.add(node.func.id)

    assert "link" in calls
    assert "replace" not in calls


def test_does_not_publish_later_stage_artifacts(tmp_path):
    install_inputs(tmp_path)
    publish(tmp_path)

    names = {
        path.name
        for path in tmp_path.rglob("*")
        if path.is_file()
    }

    assert "pipeline_manifest.json" in names
    assert "execution_manifest.json" in names
    assert "prospective_manifest.json" not in names
    assert "feature_matrix.parquet" not in names
    assert "prediction_manifest.json" not in names

def test_rejects_branch_other_than_authorized_branch(tmp_path):
    inputs = install_inputs(tmp_path)

    with pytest.raises(
        chain.PreNightManifestChainContractError,
        match="authorized branch",
    ):
        publish(
            tmp_path,
            branch="wrong-branch",
        )

    assert not (
        inputs["binding"].parent
        / "pipeline_manifest.json"
    ).exists()
    assert not (
        inputs["binding"].parent
        / "execution_manifest.json"
    ).exists()


def test_rejects_backslash_in_snapshot_relative_path(tmp_path):
    inputs = install_inputs(tmp_path)

    with pytest.raises(
        chain.PreNightManifestChainContractError,
        match="POSIX separators",
    ):
        publish(
            tmp_path,
            snapshot_relative_path="folder\\snapshot.parquet",
        )

    assert not (
        inputs["binding"].parent
        / "pipeline_manifest.json"
    ).exists()
    assert not (
        inputs["binding"].parent
        / "execution_manifest.json"
    ).exists()
