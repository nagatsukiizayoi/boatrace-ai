"""Immutable prospective PRE_NIGHT program-only dataset contract.

This module records only pre-race program artifacts that have already
passed execution-manifest and point-in-time validation. It performs no
collection, prediction, post-race join, evaluation, or scheduling.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import re
import subprocess
import uuid
from pathlib import Path
from typing import Any, Mapping


CONTRACT_VERSION = "pre_night_prospective_dataset_v2"
ARTIFACT_TYPE = "pre_night_program_only_prospective_run"

TOP_LEVEL_KEYS = {
    "contract_version",
    "artifact_type",
    "run_id",
    "race_date",
    "created_at",
    "repository_commit",
    "eligible_for_pre_night",
    "pit_eligibility",
    "artifacts",
    "deadline_evidence_sha256",
}

REQUIRED_ARTIFACTS = {
    "execution_manifest",
    "source_archive",
    "source_metadata",
    "output_parquet",
    "pipeline_manifest",
}

ARTIFACT_RECORD_KEYS = {
    "path",
    "size",
    "sha256",
}

FORBIDDEN_KEYS = {
    "result",
    "results",
    "payout",
    "payouts",
    "label",
    "labels",
    "prediction",
    "predictions",
    "probability",
    "probabilities",
    "odds",
    "exhibition",
    "recommendation",
    "recommendations",
    "ev",
    "canonical_bytes",
    "canonical_deadline_evidence_bytes",
    "deadline_evidence",
    "eligibility_cutoff_at",
    "raw_html",
    "raw_source_bytes",
    "safety_margin_seconds",
}

SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
GIT_SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")


class PreNightProspectiveError(RuntimeError):
    """Base error for the immutable prospective dataset contract."""


class PreNightProspectiveContractError(
    PreNightProspectiveError
):
    """Raised when a prospective payload violates the schema."""


class PreNightProspectiveIntegrityError(
    PreNightProspectiveError
):
    """Raised when a bound artifact fails validation."""


class PreNightProspectiveConflictError(
    PreNightProspectiveError
):
    """Raised when an immutable run ID has different content."""


def normalize_race_date(value) -> dt.date:
    if isinstance(value, dt.datetime):
        return value.date()

    if isinstance(value, dt.date):
        return value

    try:
        return dt.date.fromisoformat(str(value))
    except (TypeError, ValueError) as exc:
        raise PreNightProspectiveContractError(
            f"race_date is invalid: {value!r}"
        ) from exc


def parse_aware_timestamp(
    value,
    field_name: str,
) -> dt.datetime:
    if not isinstance(value, str) or not value.strip():
        raise PreNightProspectiveContractError(
            f"{field_name} must be a non-empty ISO-8601 string"
        )

    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError as exc:
        raise PreNightProspectiveContractError(
            f"{field_name} is invalid: {value!r}"
        ) from exc

    if (
        parsed.tzinfo is None
        or parsed.utcoffset() is None
    ):
        raise PreNightProspectiveContractError(
            f"{field_name} must include timezone"
        )

    return parsed


def require_sha256(
    value,
    field_name: str,
) -> str:
    if not isinstance(value, str):
        raise PreNightProspectiveContractError(
            f"{field_name} must be SHA-256 string"
        )

    normalized = value.strip().lower()

    if value != normalized:
        raise PreNightProspectiveContractError(
            f"{field_name} must contain exactly "
            "64 lowercase hex characters"
        )

    if SHA256_PATTERN.fullmatch(normalized) is None:
        raise PreNightProspectiveContractError(
            f"{field_name} must contain 64 lowercase hex characters"
        )

    return normalized


def require_git_sha(value) -> str:
    if not isinstance(value, str):
        raise PreNightProspectiveContractError(
            "repository_commit must be Git SHA string"
        )

    normalized = value.strip().lower()

    if GIT_SHA_PATTERN.fullmatch(normalized) is None:
        raise PreNightProspectiveContractError(
            "repository_commit must contain "
            "40 lowercase hex characters"
        )

    return normalized


def canonical_json_bytes(value) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        for block in iter(
            lambda: handle.read(1024 * 1024),
            b"",
        ):
            digest.update(block)

    return digest.hexdigest()


def _path_within_root(
    value,
    data_root,
    field_name: str,
) -> Path:
    root = Path(data_root).resolve()
    path = Path(value)

    if not str(path).strip():
        raise PreNightProspectiveContractError(
            f"{field_name} must be non-empty"
        )

    resolved = path.resolve()

    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise PreNightProspectiveIntegrityError(
            f"{field_name} is outside data_root: {resolved}"
        ) from exc

    return resolved


def _load_json_object(
    path: Path,
    field_name: str,
) -> dict:
    try:
        payload = json.loads(
            path.read_text(encoding="utf-8")
        )
    except Exception as exc:
        raise PreNightProspectiveIntegrityError(
            f"{field_name} is not valid JSON: {path}"
        ) from exc

    if not isinstance(payload, dict):
        raise PreNightProspectiveIntegrityError(
            f"{field_name} must be JSON object: {path}"
        )

    return payload


def _artifact_record(
    path,
    data_root,
    field_name: str,
) -> dict:
    resolved = _path_within_root(
        path,
        data_root,
        field_name,
    )

    if not resolved.is_file():
        raise PreNightProspectiveIntegrityError(
            f"{field_name} does not exist: {resolved}"
        )

    return {
        "path": str(resolved),
        "size": int(resolved.stat().st_size),
        "sha256": sha256_file(resolved),
    }


def _scan_forbidden_keys(
    value,
    location: str = "$",
) -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            normalized = str(key).strip().casefold()

            if normalized in FORBIDDEN_KEYS:
                raise PreNightProspectiveContractError(
                    "Forbidden prospective key: "
                    f"{location}.{key}"
                )

            _scan_forbidden_keys(
                child,
                f"{location}.{key}",
            )

    elif isinstance(value, list):
        for index, child in enumerate(value):
            _scan_forbidden_keys(
                child,
                f"{location}[{index}]",
            )


def build_prospective_directory(
    race_date,
    data_root,
) -> Path:
    date_value = normalize_race_date(race_date)

    return (
        Path(data_root)
        / "manifests"
        / "pre_night_prospective_v1"
        / date_value.isoformat()
    )


def build_prospective_manifest_path(
    manifest: Mapping[str, Any],
    data_root,
) -> Path:
    if not isinstance(manifest, Mapping):
        raise PreNightProspectiveContractError(
            "manifest must be mapping"
        )

    run_id = require_sha256(
        manifest.get("run_id"),
        "run_id",
    )

    return (
        build_prospective_directory(
            manifest.get("race_date"),
            data_root,
        )
        / f"{run_id}.json"
    )


def _run_id_material(
    *,
    race_date: str,
    repository_commit: str,
    deadline_evidence_sha256,
    artifacts: Mapping[str, Mapping[str, Any]],
) -> dict:
    return {
        "race_date": race_date,
        "repository_commit": repository_commit,
        "artifact_sha256": {
            name: artifacts[name]["sha256"]
            for name in sorted(REQUIRED_ARTIFACTS)
        },
        "deadline_evidence_sha256": deadline_evidence_sha256,
    }


def _compute_run_id(
    *,
    race_date: str,
    repository_commit: str,
    deadline_evidence_sha256,
    artifacts: Mapping[str, Mapping[str, Any]],
) -> str:
    material = _run_id_material(
        race_date=race_date,
        repository_commit=repository_commit,
        artifacts=artifacts,
        deadline_evidence_sha256=deadline_evidence_sha256,
    )

    return hashlib.sha256(
        canonical_json_bytes(material)
    ).hexdigest()


def _validate_pit_binding(
    prospective: Mapping[str, Any],
    execution_manifest: Mapping[str, Any],
    pipeline_manifest: Mapping[str, Any],
) -> None:
    prospective_deadline_sha256 = require_sha256(
        prospective.get("deadline_evidence_sha256"),
        "deadline_evidence_sha256",
    )
    execution_deadline_sha256 = require_sha256(
        execution_manifest.get("deadline_evidence_sha256"),
        "execution.deadline_evidence_sha256",
    )
    pipeline_deadline_sha256 = require_sha256(
        pipeline_manifest.get("deadline_evidence_sha256"),
        "pipeline.deadline_evidence_sha256",
    )

    if (
        prospective_deadline_sha256
        != execution_deadline_sha256
        or execution_deadline_sha256
        != pipeline_deadline_sha256
    ):
        raise PreNightProspectiveIntegrityError(
            "Prospective, execution, and pipeline deadline "
            "evidence SHA-256 values differ"
        )

    if prospective.get("eligible_for_pre_night") is not True:
        raise PreNightProspectiveContractError(
            "eligible_for_pre_night must be true"
        )

    prospective_pit = prospective.get("pit_eligibility")

    if not isinstance(prospective_pit, dict):
        raise PreNightProspectiveContractError(
            "pit_eligibility must be object"
        )

    if prospective_pit.get("status") != "ELIGIBLE":
        raise PreNightProspectiveContractError(
            "pit_eligibility.status must be ELIGIBLE"
        )

    if prospective_pit.get("eligible") is not True:
        raise PreNightProspectiveContractError(
            "pit_eligibility.eligible must be true"
        )

    race_date = prospective["race_date"]

    if str(prospective_pit.get("race_date")) != race_date:
        raise PreNightProspectiveContractError(
            "pit_eligibility race_date mismatch"
        )

    execution_pit = execution_manifest.get(
        "pit_eligibility"
    )
    pipeline_pit = pipeline_manifest.get(
        "pit_eligibility"
    )

    if not isinstance(execution_pit, dict):
        raise PreNightProspectiveIntegrityError(
            "Execution manifest PIT decision is missing"
        )

    if not isinstance(pipeline_pit, dict):
        raise PreNightProspectiveIntegrityError(
            "Pipeline manifest PIT decision is missing"
        )

    if prospective_pit != execution_pit:
        raise PreNightProspectiveIntegrityError(
            "Prospective and execution PIT decisions differ"
        )

    pit_fields = (
        "eligibility_status",
        "eligible_for_pre_night",
        "eligibility_reason",
        "pit_eligibility",
    )

    for field_name in pit_fields:
        if (
            execution_manifest.get(field_name)
            != pipeline_manifest.get(field_name)
        ):
            raise PreNightProspectiveIntegrityError(
                "Execution and pipeline PIT decisions differ: "
                f"{field_name}"
            )

    if execution_manifest.get(
        "eligible_for_pre_night"
    ) is not True:
        raise PreNightProspectiveIntegrityError(
            "Execution manifest is not PRE_NIGHT eligible"
        )

    if pipeline_manifest.get(
        "eligible_for_pre_night"
    ) is not True:
        raise PreNightProspectiveIntegrityError(
            "Pipeline manifest is not PRE_NIGHT eligible"
        )

    expected_as_of = execution_manifest.get(
        "as_of_time"
    )

    if prospective_pit.get(
        "as_of_time"
    ) != expected_as_of:
        raise PreNightProspectiveIntegrityError(
            "Prospective PIT as_of_time mismatch"
        )


def _validate_artifact_record(
    name: str,
    record,
    data_root,
) -> dict:
    if not isinstance(record, dict):
        raise PreNightProspectiveContractError(
            f"artifacts.{name} must be object"
        )

    actual_keys = set(record)

    if actual_keys != ARTIFACT_RECORD_KEYS:
        raise PreNightProspectiveContractError(
            f"artifacts.{name} keys mismatch: "
            f"{sorted(actual_keys)}"
        )

    resolved = _path_within_root(
        record.get("path"),
        data_root,
        f"artifacts.{name}.path",
    )

    if not resolved.is_file():
        raise PreNightProspectiveIntegrityError(
            f"Artifact missing: {name}: {resolved}"
        )

    expected_size = record.get("size")

    if (
        isinstance(expected_size, bool)
        or not isinstance(expected_size, int)
        or expected_size < 0
    ):
        raise PreNightProspectiveContractError(
            f"artifacts.{name}.size must be non-negative int"
        )

    expected_sha256 = require_sha256(
        record.get("sha256"),
        f"artifacts.{name}.sha256",
    )

    actual_size = int(resolved.stat().st_size)
    actual_sha256 = sha256_file(resolved)

    if actual_size != expected_size:
        raise PreNightProspectiveIntegrityError(
            f"Artifact size mismatch: {name}"
        )

    if actual_sha256 != expected_sha256:
        raise PreNightProspectiveIntegrityError(
            f"Artifact SHA-256 mismatch: {name}"
        )

    return {
        "path": resolved,
        "size": actual_size,
        "sha256": actual_sha256,
    }


def _validate_manifest_payload(
    manifest,
    data_root,
    *,
    race_date=None,
    expected_repository_commit=None,
) -> dict:
    if not isinstance(manifest, dict):
        raise PreNightProspectiveContractError(
            "Prospective manifest must be object"
        )

    _scan_forbidden_keys(manifest)

    actual_keys = set(manifest)

    if actual_keys != TOP_LEVEL_KEYS:
        missing = sorted(
            TOP_LEVEL_KEYS - actual_keys
        )
        unknown = sorted(
            actual_keys - TOP_LEVEL_KEYS
        )
        raise PreNightProspectiveContractError(
            "Prospective manifest keys mismatch: "
            f"missing={missing}, unknown={unknown}"
        )

    if manifest["contract_version"] != CONTRACT_VERSION:
        raise PreNightProspectiveContractError(
            "Unsupported prospective contract_version"
        )

    deadline_evidence_sha256 = require_sha256(
        manifest["deadline_evidence_sha256"],
        "deadline_evidence_sha256",
    )

    if manifest["artifact_type"] != ARTIFACT_TYPE:
        raise PreNightProspectiveContractError(
            "Unexpected prospective artifact_type"
        )

    date_value = normalize_race_date(
        manifest["race_date"]
    )
    date_text = date_value.isoformat()

    if manifest["race_date"] != date_text:
        raise PreNightProspectiveContractError(
            "race_date must use YYYY-MM-DD"
        )

    if race_date is not None:
        expected_date = normalize_race_date(
            race_date
        ).isoformat()

        if date_text != expected_date:
            raise PreNightProspectiveContractError(
                "Prospective race_date mismatch"
            )

    repository_commit = require_git_sha(
        manifest["repository_commit"]
    )

    if expected_repository_commit is not None:
        expected_commit = require_git_sha(
            expected_repository_commit
        )

        if repository_commit != expected_commit:
            raise PreNightProspectiveIntegrityError(
                "repository_commit mismatch"
            )

    created_at = parse_aware_timestamp(
        manifest["created_at"],
        "created_at",
    )

    artifacts = manifest["artifacts"]

    if not isinstance(artifacts, dict):
        raise PreNightProspectiveContractError(
            "artifacts must be object"
        )

    if set(artifacts) != REQUIRED_ARTIFACTS:
        raise PreNightProspectiveContractError(
            "Prospective artifact names mismatch: "
            f"{sorted(artifacts)}"
        )

    validated_artifacts = {
        name: _validate_artifact_record(
            name,
            artifacts[name],
            data_root,
        )
        for name in sorted(REQUIRED_ARTIFACTS)
    }

    execution_path = validated_artifacts[
        "execution_manifest"
    ]["path"]

    execution_manifest = _load_json_object(
        execution_path,
        "execution_manifest",
    )

    if execution_manifest.get("status") != "SUCCESS":
        raise PreNightProspectiveIntegrityError(
            "Execution manifest status must be SUCCESS"
        )

    if execution_manifest.get(
        "race_date"
    ) != date_text:
        raise PreNightProspectiveIntegrityError(
            "Execution manifest race_date mismatch"
        )

    if execution_manifest.get("dry_run") is not False:
        raise PreNightProspectiveIntegrityError(
            "Execution manifest must describe live run"
        )

    execution_artifacts = execution_manifest.get(
        "artifacts"
    )

    if not isinstance(execution_artifacts, dict):
        raise PreNightProspectiveIntegrityError(
            "Execution manifest artifacts are missing"
        )

    for name in (
        "source_archive",
        "source_metadata",
        "output_parquet",
        "pipeline_manifest",
    ):
        execution_record = execution_artifacts.get(name)

        if not isinstance(execution_record, dict):
            raise PreNightProspectiveIntegrityError(
                "Execution artifact record missing: "
                f"{name}"
            )

        prospective_record = artifacts[name]

        execution_resolved = _path_within_root(
            execution_record.get("path"),
            data_root,
            f"execution.artifacts.{name}.path",
        )

        prospective_resolved = validated_artifacts[
            name
        ]["path"]

        if execution_resolved != prospective_resolved:
            raise PreNightProspectiveIntegrityError(
                "Artifact path differs from execution manifest: "
                f"{name}"
            )

        if (
            execution_record.get("size")
            != prospective_record.get("size")
        ):
            raise PreNightProspectiveIntegrityError(
                "Artifact size differs from execution manifest: "
                f"{name}"
            )

        execution_sha256 = require_sha256(
            execution_record.get("sha256"),
            f"execution.artifacts.{name}.sha256",
        )

        if (
            execution_sha256
            != prospective_record["sha256"]
        ):
            raise PreNightProspectiveIntegrityError(
                "Artifact SHA-256 differs from execution manifest: "
                f"{name}"
            )

    pipeline_manifest = _load_json_object(
        validated_artifacts[
            "pipeline_manifest"
        ]["path"],
        "pipeline_manifest",
    )

    _validate_pit_binding(
        manifest,
        execution_manifest,
        pipeline_manifest,
    )

    completed_at = parse_aware_timestamp(
        execution_manifest.get("completed_at"),
        "execution.completed_at",
    )

    if created_at != completed_at:
        raise PreNightProspectiveContractError(
            "created_at must equal execution completed_at"
        )

    expected_run_id = _compute_run_id(
        race_date=date_text,
        repository_commit=repository_commit,
        artifacts=artifacts,
        deadline_evidence_sha256=deadline_evidence_sha256,
    )

    actual_run_id = require_sha256(
        manifest["run_id"],
        "run_id",
    )

    if actual_run_id != expected_run_id:
        raise PreNightProspectiveIntegrityError(
            "run_id does not match bound artifacts"
        )

    return {
        "manifest": manifest,
        "artifacts": validated_artifacts,
        "execution_manifest": execution_manifest,
        "pipeline_manifest": pipeline_manifest,
        "run_id": actual_run_id,
        "race_date": date_text,
        "repository_commit": repository_commit,
    }


def build_prospective_manifest(
    *,
    race_date,
    data_root,
    execution_manifest_path,
    execution_validation,
    repository_commit,
    created_at,
) -> dict:
    date_text = normalize_race_date(
        race_date
    ).isoformat()

    repository_commit = require_git_sha(
        repository_commit
    )

    if not isinstance(execution_validation, dict):
        raise PreNightProspectiveContractError(
            "execution_validation must be dict"
        )

    execution_manifest = execution_validation.get(
        "manifest"
    )
    execution_artifacts = execution_validation.get(
        "artifacts"
    )

    if not isinstance(execution_manifest, dict):
        raise PreNightProspectiveContractError(
            "execution_validation.manifest must be dict"
        )

    deadline_evidence_sha256 = require_sha256(
        execution_manifest.get("deadline_evidence_sha256"),
        "execution.deadline_evidence_sha256",
    )

    if not isinstance(execution_artifacts, dict):
        raise PreNightProspectiveContractError(
            "execution_validation.artifacts must be dict"
        )

    execution_path = _path_within_root(
        execution_manifest_path,
        data_root,
        "execution_manifest_path",
    )

    artifacts = {
        "execution_manifest": _artifact_record(
            execution_path,
            data_root,
            "execution_manifest",
        ),
    }

    for name in (
        "source_archive",
        "source_metadata",
        "output_parquet",
        "pipeline_manifest",
    ):
        record = execution_artifacts.get(name)

        if not isinstance(record, dict):
            raise PreNightProspectiveContractError(
                "execution_validation artifact missing: "
                f"{name}"
            )

        artifacts[name] = _artifact_record(
            record.get("path"),
            data_root,
            name,
        )

    created_timestamp = parse_aware_timestamp(
        created_at,
        "created_at",
    )

    run_id = _compute_run_id(
        race_date=date_text,
        repository_commit=repository_commit,
        artifacts=artifacts,
        deadline_evidence_sha256=deadline_evidence_sha256,
    )

    manifest = {
        "contract_version": CONTRACT_VERSION,
        "artifact_type": ARTIFACT_TYPE,
        "run_id": run_id,
        "race_date": date_text,
        "created_at": created_timestamp.isoformat(),
        "repository_commit": repository_commit,
        "eligible_for_pre_night": (
            execution_manifest.get(
                "eligible_for_pre_night"
            )
        ),
        "pit_eligibility": (
            execution_manifest.get(
                "pit_eligibility"
            )
        ),
        "artifacts": artifacts,
        "deadline_evidence_sha256": deadline_evidence_sha256,
    }

    _validate_manifest_payload(
        manifest,
        data_root,
        race_date=date_text,
        expected_repository_commit=repository_commit,
    )

    return manifest


def validate_prospective_manifest(
    manifest_path,
    data_root,
    *,
    race_date=None,
    expected_repository_commit=None,
) -> dict:
    resolved = _path_within_root(
        manifest_path,
        data_root,
        "manifest_path",
    )

    if not resolved.is_file():
        raise PreNightProspectiveIntegrityError(
            f"Prospective manifest does not exist: {resolved}"
        )

    manifest = _load_json_object(
        resolved,
        "prospective_manifest",
    )

    validated = _validate_manifest_payload(
        manifest,
        data_root,
        race_date=race_date,
        expected_repository_commit=(
            expected_repository_commit
        ),
    )

    expected_name = (
        validated["run_id"] + ".json"
    )

    if resolved.name != expected_name:
        raise PreNightProspectiveIntegrityError(
            "Prospective manifest filename mismatch"
        )

    expected_directory = (
        build_prospective_directory(
            validated["race_date"],
            data_root,
        ).resolve()
    )

    if resolved.parent != expected_directory:
        raise PreNightProspectiveIntegrityError(
            "Prospective manifest directory mismatch"
        )

    return {
        "manifest_path": resolved,
        **validated,
        "cached": True,
        "skipped": True,
    }


def write_prospective_manifest(
    manifest,
    data_root,
) -> dict:
    validated_payload = _validate_manifest_payload(
        manifest,
        data_root,
        race_date=manifest.get("race_date"),
        expected_repository_commit=(
            manifest.get("repository_commit")
        ),
    )

    destination = build_prospective_manifest_path(
        manifest,
        data_root,
    )
    destination.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    canonical = canonical_json_bytes(manifest)

    if destination.exists():
        existing = validate_prospective_manifest(
            destination,
            data_root,
            race_date=manifest["race_date"],
            expected_repository_commit=(
                manifest["repository_commit"]
            ),
        )

        if (
            canonical_json_bytes(existing["manifest"])
            != canonical
        ):
            raise PreNightProspectiveConflictError(
                "Immutable prospective run_id already "
                "exists with different content"
            )

        return existing

    temporary = destination.with_name(
        destination.name
        + "."
        + uuid.uuid4().hex
        + ".tmp"
    )

    published = False

    try:
        with temporary.open("xb") as handle:
            handle.write(canonical)
            handle.flush()
            os.fsync(handle.fileno())

        try:
            # Atomic, no-overwrite publication.
            os.link(temporary, destination)
            published = True
        except FileExistsError:
            existing = validate_prospective_manifest(
                destination,
                data_root,
                race_date=manifest["race_date"],
                expected_repository_commit=(
                    manifest["repository_commit"]
                ),
            )

            if (
                canonical_json_bytes(
                    existing["manifest"]
                )
                != canonical
            ):
                raise PreNightProspectiveConflictError(
                    "Concurrent prospective run_id conflict"
                )

            return existing

        result = validate_prospective_manifest(
            destination,
            data_root,
            race_date=manifest["race_date"],
            expected_repository_commit=(
                manifest["repository_commit"]
            ),
        )

        result["cached"] = False
        result["skipped"] = False
        return result

    except Exception:
        if published:
            destination.unlink(missing_ok=True)
        raise

    finally:
        temporary.unlink(missing_ok=True)


def resolve_repository_commit(
    repository_root=None,
) -> str:
    root = (
        Path(repository_root)
        if repository_root is not None
        else Path(__file__).resolve().parents[3]
    )

    result = subprocess.run(
        [
            "git",
            "-C",
            str(root),
            "rev-parse",
            "HEAD",
        ],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )

    if result.returncode != 0:
        raise PreNightProspectiveContractError(
            "Unable to resolve repository commit: "
            f"{result.stderr.strip()}"
        )

    return require_git_sha(
        result.stdout.strip()
    )


__all__ = [
    "ARTIFACT_TYPE",
    "CONTRACT_VERSION",
    "FORBIDDEN_KEYS",
    "PreNightProspectiveConflictError",
    "PreNightProspectiveContractError",
    "PreNightProspectiveError",
    "PreNightProspectiveIntegrityError",
    "REQUIRED_ARTIFACTS",
    "TOP_LEVEL_KEYS",
    "build_prospective_directory",
    "build_prospective_manifest",
    "build_prospective_manifest_path",
    "resolve_repository_commit",
    "validate_prospective_manifest",
    "write_prospective_manifest",
]
