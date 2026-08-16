"""Stage 4 immutable Pipeline/Execution Manifest digest chain."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from boatrace_ai.pipelines.pre_night_deadline_collection import (
    COLLECTION_CONTRACT_VERSION,
)
from boatrace_ai.pipelines.pre_night_program_binding import (
    PROGRAM_BINDING_CONTRACT_VERSION,
    canonical_program_entries_binding_bytes,
)


PIPELINE_MANIFEST_VERSION = "pre_night_pipeline_manifest_v1"
EXECUTION_MANIFEST_VERSION = "pre_night_execution_manifest_v1"
STAGE1_CONTRACT_ID = (
    "D1B5-STAGE1-DEADLINE-EVIDENCE-PUBLICATION-V2"
)
REPOSITORY_RELATIVE_PATH = (
    "src/boatrace_ai/pipelines/pre_night_manifest_chain.py"
)

REQUIRED_BRANCH = (
    "feature/pre-night-authoritative-deadline-pit-contract-v2"
)

AUTHORIZED_BRANCHES = frozenset({
    REQUIRED_BRANCH,
    "feature/pre-night-runtime-wiring-v1",
    "main",
})

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_HEAD_RE = re.compile(r"^[0-9a-f]{40}$")
_RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_VENUE_RE = re.compile(r"^\d{2}$")

_COLLECTION_KEYS = {
    "contract_version",
    "race_date",
    "expected_venue_codes",
    "entry_count",
    "entries",
}
_COLLECTION_ENTRY_KEYS = {
    "race_date",
    "venue_code",
    "relative_path",
    "deadline_evidence_sha256",
    "byte_length",
    "contract_version",
}
_PIPELINE_KEYS = {
    "manifest_version",
    "manifest_role",
    "pipeline_name",
    "pipeline_version",
    "race_date",
    "run_id",
    "branch",
    "head",
    "started_at",
    "completed_at",
    "authorization_state",
    "stage1_contract_id",
    "stage2_contract_version",
    "stage3_contract_version",
    "deadline_evidence_collection_sha256",
    "program_entries_binding_sha256",
    "input_artifacts",
    "output_artifacts",
}
_EXECUTION_KEYS = {
    "manifest_version",
    "manifest_role",
    "run_id",
    "race_date",
    "phase",
    "branch",
    "head",
    "repository_relative_path",
    "authorization_state",
    "runtime",
    "input_digests",
    "output_digests",
    "deadline_evidence_collection_sha256",
    "program_entries_binding_sha256",
    "pipeline_manifest_sha256",
    "test_state",
}
_ARTIFACT_KEYS = {
    "relative_path",
    "byte_length",
    "sha256",
    "contract_version",
}


class PreNightManifestChainError(Exception):
    """Base Stage 4 error."""


class PreNightManifestChainContractError(
    PreNightManifestChainError
):
    """Invalid caller input or unsafe path."""


class PreNightManifestChainCacheError(
    PreNightManifestChainError
):
    """Invalid or conflicting cached artifact."""


class PreNightManifestChainIntegrityError(
    PreNightManifestChainError
):
    """Publication or durability failure."""


def canonical_manifest_bytes(value: Mapping[str, Any]) -> bytes:
    if not isinstance(value, Mapping):
        raise PreNightManifestChainContractError(
            "manifest must be mapping"
        )

    try:
        text = json.dumps(
            dict(value),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as error:
        raise PreNightManifestChainContractError(
            "manifest is not canonical JSON compatible"
        ) from error

    return (text + "\n").encode("utf-8")


def _require_text(value: Any, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
    ):
        raise PreNightManifestChainContractError(
            f"{field} must be non-empty canonical string"
        )
    return value


def _require_race_date(value: Any) -> str:
    value = _require_text(value, "race_date")
    try:
        normalized = dt.date.fromisoformat(value).isoformat()
    except ValueError as error:
        raise PreNightManifestChainContractError(
            "race_date is invalid"
        ) from error
    if normalized != value:
        raise PreNightManifestChainContractError(
            "race_date must be canonical"
        )
    return value


def _require_run_id(value: Any) -> str:
    if (
        not isinstance(value, str)
        or value in {".", ".."}
        or _RUN_ID_RE.fullmatch(value) is None
    ):
        raise PreNightManifestChainContractError(
            "run_id is invalid"
        )
    return value


def _require_sha256(value: Any, field: str) -> str:
    if (
        not isinstance(value, str)
        or _SHA256_RE.fullmatch(value) is None
    ):
        raise PreNightManifestChainContractError(
            f"{field} must be lowercase SHA-256"
        )
    return value


def _require_head(value: Any) -> str:
    if not isinstance(value, str) or _HEAD_RE.fullmatch(value) is None:
        raise PreNightManifestChainContractError(
            "head must be lowercase 40-character Git SHA"
        )
    return value


def _timestamp(value: Any, field: str) -> tuple[dt.datetime, str]:
    if not isinstance(value, str) or not value:
        raise PreNightManifestChainContractError(
            f"{field} must be ISO-8601 string"
        )

    text = value
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError as error:
        raise PreNightManifestChainContractError(
            f"{field} is invalid"
        ) from error

    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise PreNightManifestChainContractError(
            f"{field} must be timezone-aware"
        )

    normalized = parsed.astimezone(dt.timezone.utc)

    if normalized.microsecond:
        output = normalized.isoformat(timespec="microseconds")
    else:
        output = normalized.isoformat(timespec="seconds")

    if not output.endswith("+00:00"):
        raise PreNightManifestChainContractError(
            f"{field} UTC normalization failed"
        )

    return normalized, output[:-6] + "Z"


def _json_mapping(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise PreNightManifestChainContractError(
            f"{field} must be mapping"
        )

    try:
        encoded = canonical_manifest_bytes(value)
        decoded = json.loads(encoded.decode("utf-8"))
    except Exception as error:
        raise PreNightManifestChainContractError(
            f"{field} is not canonical JSON compatible"
        ) from error

    if not isinstance(decoded, dict):
        raise PreNightManifestChainContractError(
            f"{field} must serialize as object"
        )

    return decoded


def _paths(root: Path, race_date: str, run_id: str) -> dict[str, Path]:
    date_value = dt.date.fromisoformat(race_date)
    date_parts = (
        f"{date_value.year:04d}",
        f"{date_value.month:02d}",
        f"{date_value.day:02d}",
    )
    run_directory = (
        root / "prospective" / "pre_night" / "runs"
        / date_parts[0] / date_parts[1] / date_parts[2] / run_id
    )
    collection = (
        root / "prospective" / "pre_night"
        / "deadline_evidence_collections"
        / date_parts[0] / date_parts[1] / date_parts[2]
        / "deadline_evidence_collection.json"
    )
    uid = os.urandom(16).hex()

    return {
        "root": root,
        "collection": collection,
        "binding": run_directory / "program_entries_binding.json",
        "directory": run_directory,
        "pipeline": run_directory / "pipeline_manifest.json",
        "execution": run_directory / "execution_manifest.json",
        "lock": run_directory / ".manifest_chain.lock",
        "pipeline_temporary": (
            run_directory / f".pipeline_manifest.json.{uid}.tmp"
        ),
        "execution_temporary": (
            run_directory / f".execution_manifest.json.{uid}.tmp"
        ),
    }


def _assert_safe(root: Path, target: Path, label: str) -> None:
    if root.exists() and root.is_symlink():
        raise PreNightManifestChainContractError(
            "data_root must not be symlink"
        )

    try:
        relative = target.relative_to(root)
    except ValueError as error:
        raise PreNightManifestChainContractError(
            f"{label} path escapes data_root"
        ) from error

    resolved_root = root.resolve(strict=False)
    resolved_target = target.resolve(strict=False)

    try:
        resolved_target.relative_to(resolved_root)
    except ValueError as error:
        raise PreNightManifestChainContractError(
            f"{label} resolved path escapes data_root"
        ) from error

    current = root
    for component in relative.parts:
        current = current / component
        if current.exists() and current.is_symlink():
            raise PreNightManifestChainContractError(
                f"{label} path contains symlink"
            )


def _assert_paths(paths: Mapping[str, Path]) -> None:
    for label in (
        "collection",
        "binding",
        "directory",
        "pipeline",
        "execution",
        "lock",
        "pipeline_temporary",
        "execution_temporary",
    ):
        _assert_safe(paths["root"], paths[label], label)

    for label in (
        "pipeline",
        "execution",
        "lock",
        "pipeline_temporary",
        "execution_temporary",
    ):
        if paths[label].parent != paths["directory"]:
            raise PreNightManifestChainContractError(
                f"{label} parent mismatch"
            )


def _load_exact_json(
    path: Path,
    *,
    root: Path,
    label: str,
) -> tuple[dict[str, Any], bytes]:
    _assert_safe(root, path, label)

    if (
        not path.exists()
        or path.is_symlink()
        or not path.is_file()
    ):
        raise PreNightManifestChainCacheError(
            f"{label} must be regular file"
        )

    stored = path.read_bytes()

    try:
        payload = json.loads(stored.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PreNightManifestChainCacheError(
            f"{label} is not valid UTF-8 JSON"
        ) from error

    if not isinstance(payload, dict):
        raise PreNightManifestChainCacheError(
            f"{label} must be JSON object"
        )

    return payload, stored


def _validate_collection(
    paths: Mapping[str, Path],
    race_date: str,
) -> dict[str, Any]:
    payload, stored = _load_exact_json(
        paths["collection"],
        root=paths["root"],
        label="Stage 2 collection",
    )

    if set(payload) != _COLLECTION_KEYS:
        raise PreNightManifestChainCacheError(
            "Stage 2 collection fields mismatch"
        )

    if canonical_manifest_bytes(payload) != stored:
        raise PreNightManifestChainCacheError(
            "Stage 2 collection is non-canonical"
        )

    if payload["contract_version"] != COLLECTION_CONTRACT_VERSION:
        raise PreNightManifestChainCacheError(
            "Stage 2 contract version mismatch"
        )

    if payload["race_date"] != race_date:
        raise PreNightManifestChainCacheError(
            "Stage 2 race_date mismatch"
        )

    venues = payload["expected_venue_codes"]
    entries = payload["entries"]

    if not isinstance(venues, list) or not venues:
        raise PreNightManifestChainCacheError(
            "Stage 2 venue list is invalid"
        )

    if not isinstance(entries, list):
        raise PreNightManifestChainCacheError(
            "Stage 2 entries must be list"
        )

    if (
        isinstance(payload["entry_count"], bool)
        or payload["entry_count"] != len(entries)
        or len(entries) != len(venues)
    ):
        raise PreNightManifestChainCacheError(
            "Stage 2 entry_count mismatch"
        )

    if (
        any(
            not isinstance(code, str)
            or _VENUE_RE.fullmatch(code) is None
            or not 1 <= int(code) <= 24
            for code in venues
        )
        or venues != sorted(set(venues), key=int)
    ):
        raise PreNightManifestChainCacheError(
            "Stage 2 venue order is invalid"
        )

    for index, entry in enumerate(entries):
        if (
            not isinstance(entry, dict)
            or set(entry) != _COLLECTION_ENTRY_KEYS
            or entry["race_date"] != race_date
            or entry["venue_code"] != venues[index]
        ):
            raise PreNightManifestChainCacheError(
                "Stage 2 entry identity mismatch"
            )

        try:
            _require_sha256(
                entry["deadline_evidence_sha256"],
                "deadline_evidence_sha256",
            )
        except PreNightManifestChainContractError as error:
            raise PreNightManifestChainCacheError(
                "Stage 2 entry digest is invalid"
            ) from error

    return {
        "payload": payload,
        "bytes": stored,
        "sha256": hashlib.sha256(stored).hexdigest(),
    }


def _validate_binding(
    paths: Mapping[str, Path],
    race_date: str,
    collection_sha256: str,
) -> dict[str, Any]:
    payload, stored = _load_exact_json(
        paths["binding"],
        root=paths["root"],
        label="Stage 3 binding",
    )

    try:
        canonical = canonical_program_entries_binding_bytes(payload)
    except Exception as error:
        raise PreNightManifestChainCacheError(
            "Stage 3 binding validation failed"
        ) from error

    if canonical != stored:
        raise PreNightManifestChainCacheError(
            "Stage 3 binding is non-canonical"
        )

    if payload.get("contract_version") != PROGRAM_BINDING_CONTRACT_VERSION:
        raise PreNightManifestChainCacheError(
            "Stage 3 contract version mismatch"
        )

    if payload.get("race_date") != race_date:
        raise PreNightManifestChainCacheError(
            "Stage 3 race_date mismatch"
        )

    if (
        payload.get("deadline_evidence_collection_sha256")
        != collection_sha256
    ):
        raise PreNightManifestChainCacheError(
            "Stage 3 collection digest mismatch"
        )

    return {
        "payload": payload,
        "bytes": stored,
        "sha256": hashlib.sha256(stored).hexdigest(),
    }


def _snapshot_record(
    root: Path,
    relative_text: Any,
) -> dict[str, Any]:
    relative_text = _require_text(
        relative_text,
        "snapshot_relative_path",
    )

    if "\\" in relative_text:
        raise PreNightManifestChainContractError(
            "snapshot_relative_path must use POSIX separators"
        )

    relative = Path(relative_text)

    if (
        relative.is_absolute()
        or relative.as_posix() != relative_text
        or ".." in relative.parts
        or "." in relative.parts
    ):
        raise PreNightManifestChainContractError(
            "snapshot_relative_path must be safe POSIX relative path"
        )

    path = root / relative
    _assert_safe(root, path, "snapshot")

    if not path.exists() or path.is_symlink() or not path.is_file():
        raise PreNightManifestChainCacheError(
            "snapshot must be regular file"
        )

    size = path.stat().st_size
    if size <= 0:
        raise PreNightManifestChainCacheError(
            "snapshot must not be empty"
        )

    digest = hashlib.sha256(path.read_bytes()).hexdigest()

    return {
        "path": path,
        "record": {
            "relative_path": relative_text,
            "byte_length": size,
            "sha256": digest,
            "contract_version": "snapshot_exact_bytes_v1",
        },
    }


def _artifact_record(
    path: Path,
    root: Path,
    contract_version: str,
) -> dict[str, Any]:
    stored = path.read_bytes()
    return {
        "relative_path": path.relative_to(root).as_posix(),
        "byte_length": len(stored),
        "sha256": hashlib.sha256(stored).hexdigest(),
        "contract_version": contract_version,
    }


def _validate_artifact_record(record: Any) -> None:
    if not isinstance(record, Mapping) or set(record) != _ARTIFACT_KEYS:
        raise PreNightManifestChainContractError(
            "artifact record fields mismatch"
        )

    _require_text(record["relative_path"], "relative_path")
    _require_sha256(record["sha256"], "artifact sha256")
    _require_text(record["contract_version"], "contract_version")

    if (
        isinstance(record["byte_length"], bool)
        or not isinstance(record["byte_length"], int)
        or record["byte_length"] <= 0
    ):
        raise PreNightManifestChainContractError(
            "artifact byte_length is invalid"
        )


def _validate_expected(
    path: Path,
    *,
    root: Path,
    expected_payload: Mapping[str, Any],
    expected_bytes: bytes,
    expected_keys: set[str],
    label: str,
) -> None:
    payload, stored = _load_exact_json(
        path,
        root=root,
        label=label,
    )

    if set(payload) != expected_keys:
        raise PreNightManifestChainCacheError(
            f"{label} fields mismatch"
        )

    if canonical_manifest_bytes(payload) != stored:
        raise PreNightManifestChainCacheError(
            f"{label} is non-canonical"
        )

    if payload != expected_payload or stored != expected_bytes:
        raise PreNightManifestChainCacheError(
            f"{label} content conflict"
        )


def _fsync_file(path: Path) -> None:
    try:
        with path.open("rb") as handle:
            os.fsync(handle.fileno())
    except OSError as error:
        raise PreNightManifestChainIntegrityError(
            "manifest destination fsync failed"
        ) from error


def _fsync_directory(path: Path) -> None:
    descriptor = None
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
        )
        os.fsync(descriptor)
    except OSError as error:
        raise PreNightManifestChainIntegrityError(
            "manifest directory fsync failed"
        ) from error
    finally:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError as error:
                raise PreNightManifestChainIntegrityError(
                    "manifest directory descriptor close failed"
                ) from error


def _publish_one(
    *,
    path: Path,
    temporary: Path,
    root: Path,
    payload: Mapping[str, Any],
    canonical: bytes,
    expected_keys: set[str],
    label: str,
) -> bool:
    if path.exists():
        _validate_expected(
            path,
            root=root,
            expected_payload=payload,
            expected_bytes=canonical,
            expected_keys=expected_keys,
            label=label,
        )
        return False

    created_temporary = False
    try:
        with temporary.open("xb") as handle:
            created_temporary = True
            handle.write(canonical)
            handle.flush()
            os.fsync(handle.fileno())

        if temporary.read_bytes() != canonical:
            raise PreNightManifestChainIntegrityError(
                f"{label} temporary byte mismatch"
            )

        try:
            os.link(temporary, path)
        except FileExistsError:
            _validate_expected(
                path,
                root=root,
                expected_payload=payload,
                expected_bytes=canonical,
                expected_keys=expected_keys,
                label=label,
            )
            return False
        except OSError as error:
            raise PreNightManifestChainIntegrityError(
                f"{label} atomic publication failed"
            ) from error

        _fsync_file(path)
        _fsync_directory(path.parent)

        _validate_expected(
            path,
            root=root,
            expected_payload=payload,
            expected_bytes=canonical,
            expected_keys=expected_keys,
            label=label,
        )
        return True

    finally:
        if created_temporary:
            try:
                temporary.unlink(missing_ok=True)
            except OSError as error:
                raise PreNightManifestChainIntegrityError(
                    f"{label} temporary cleanup failed"
                ) from error


def publish_pre_night_manifest_chain(
    data_root,
    *,
    race_date,
    run_id,
    snapshot_relative_path,
    pipeline_name,
    pipeline_version,
    branch,
    head,
    started_at,
    completed_at,
    authorization_state,
    runtime,
    test_state,
) -> dict[str, Any]:
    """Publish or validate one immutable Stage 4 manifest chain."""

    root = Path(data_root)
    race_date = _require_race_date(race_date)
    run_id = _require_run_id(run_id)
    pipeline_name = _require_text(pipeline_name, "pipeline_name")
    pipeline_version = _require_text(
        pipeline_version,
        "pipeline_version",
    )
    branch = _require_text(branch, "branch")

    if branch not in AUTHORIZED_BRANCHES:
        raise PreNightManifestChainContractError(
            "branch does not match the authorized branch"
        )

    head = _require_head(head)

    started_value, started_text = _timestamp(
        started_at,
        "started_at",
    )
    completed_value, completed_text = _timestamp(
        completed_at,
        "completed_at",
    )

    if completed_value < started_value:
        raise PreNightManifestChainContractError(
            "completed_at must not be before started_at"
        )

    authorization = _json_mapping(
        authorization_state,
        "authorization_state",
    )
    runtime = _json_mapping(runtime, "runtime")
    test_state = _json_mapping(test_state, "test_state")

    paths = _paths(root, race_date, run_id)
    _assert_paths(paths)

    collection = _validate_collection(paths, race_date)
    binding = _validate_binding(
        paths,
        race_date,
        collection["sha256"],
    )
    snapshot = _snapshot_record(
        root,
        snapshot_relative_path,
    )

    if snapshot["path"] in {
        paths["pipeline"],
        paths["execution"],
    }:
        raise PreNightManifestChainContractError(
            "snapshot path must not equal manifest path"
        )

    collection_record = _artifact_record(
        paths["collection"],
        root,
        COLLECTION_CONTRACT_VERSION,
    )
    binding_record = _artifact_record(
        paths["binding"],
        root,
        PROGRAM_BINDING_CONTRACT_VERSION,
    )
    snapshot_record = snapshot["record"]

    for record in (
        collection_record,
        binding_record,
        snapshot_record,
    ):
        _validate_artifact_record(record)

    pipeline_payload = {
        "manifest_version": PIPELINE_MANIFEST_VERSION,
        "manifest_role": "PIPELINE_MANIFEST",
        "pipeline_name": pipeline_name,
        "pipeline_version": pipeline_version,
        "race_date": race_date,
        "run_id": run_id,
        "branch": branch,
        "head": head,
        "started_at": started_text,
        "completed_at": completed_text,
        "authorization_state": authorization,
        "stage1_contract_id": STAGE1_CONTRACT_ID,
        "stage2_contract_version": COLLECTION_CONTRACT_VERSION,
        "stage3_contract_version": PROGRAM_BINDING_CONTRACT_VERSION,
        "deadline_evidence_collection_sha256": collection["sha256"],
        "program_entries_binding_sha256": binding["sha256"],
        "input_artifacts": {
            "deadline_evidence_collection": collection_record,
            "program_entries_binding": binding_record,
        },
        "output_artifacts": {
            "snapshot": snapshot_record,
        },
    }

    pipeline_bytes = canonical_manifest_bytes(pipeline_payload)
    pipeline_sha256 = hashlib.sha256(pipeline_bytes).hexdigest()

    execution_payload = {
        "manifest_version": EXECUTION_MANIFEST_VERSION,
        "manifest_role": "EXECUTION_MANIFEST",
        "run_id": run_id,
        "race_date": race_date,
        "phase": "PRE_NIGHT_MANIFEST_CHAIN",
        "branch": branch,
        "head": head,
        "repository_relative_path": REPOSITORY_RELATIVE_PATH,
        "authorization_state": authorization,
        "runtime": runtime,
        "input_digests": {
            "deadline_evidence_collection": collection["sha256"],
            "program_entries_binding": binding["sha256"],
        },
        "output_digests": {
            "snapshot": snapshot_record["sha256"],
            "pipeline_manifest": pipeline_sha256,
        },
        "deadline_evidence_collection_sha256": collection["sha256"],
        "program_entries_binding_sha256": binding["sha256"],
        "pipeline_manifest_sha256": pipeline_sha256,
        "test_state": test_state,
    }

    execution_bytes = canonical_manifest_bytes(execution_payload)

    pipeline_exists = paths["pipeline"].exists()
    execution_exists = paths["execution"].exists()

    if execution_exists and not pipeline_exists:
        raise PreNightManifestChainCacheError(
            "Execution Manifest exists without Pipeline Manifest"
        )

    if pipeline_exists:
        _validate_expected(
            paths["pipeline"],
            root=root,
            expected_payload=pipeline_payload,
            expected_bytes=pipeline_bytes,
            expected_keys=_PIPELINE_KEYS,
            label="Pipeline Manifest",
        )

    if execution_exists:
        _validate_expected(
            paths["execution"],
            root=root,
            expected_payload=execution_payload,
            expected_bytes=execution_bytes,
            expected_keys=_EXECUTION_KEYS,
            label="Execution Manifest",
        )

        return _receipt(
            paths,
            race_date,
            run_id,
            collection["sha256"],
            binding["sha256"],
            cached=True,
            publication_status="VALIDATED_REUSE",
        )

    paths["directory"].mkdir(parents=True, exist_ok=True)
    _assert_paths(paths)

    lock_fd = None
    lock_acquired = False
    lock_identity = None

    try:
        try:
            lock_fd = os.open(
                paths["lock"],
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
            )
            lock_acquired = True
            stat = os.fstat(lock_fd)
            lock_identity = (stat.st_dev, stat.st_ino)
        except FileExistsError as error:
            raise PreNightManifestChainError(
                "manifest-chain publication lock exists"
            ) from error
        except OSError as error:
            raise PreNightManifestChainError(
                "manifest-chain lock acquisition failed"
            ) from error

        os.close(lock_fd)
        lock_fd = None

        _assert_paths(paths)

        if paths["execution"].exists() and not paths["pipeline"].exists():
            raise PreNightManifestChainCacheError(
                "Execution Manifest exists without Pipeline Manifest"
            )

        pipeline_preexisting = paths["pipeline"].exists()

        pipeline_created = _publish_one(
            path=paths["pipeline"],
            temporary=paths["pipeline_temporary"],
            root=root,
            payload=pipeline_payload,
            canonical=pipeline_bytes,
            expected_keys=_PIPELINE_KEYS,
            label="Pipeline Manifest",
        )

        execution_created = _publish_one(
            path=paths["execution"],
            temporary=paths["execution_temporary"],
            root=root,
            payload=execution_payload,
            canonical=execution_bytes,
            expected_keys=_EXECUTION_KEYS,
            label="Execution Manifest",
        )

        if pipeline_preexisting and execution_created:
            status = "RESUMED_CREATED_EXECUTION"
        elif pipeline_created and execution_created:
            status = "CREATED"
        else:
            status = "VALIDATED_REUSE"

        return _receipt(
            paths,
            race_date,
            run_id,
            collection["sha256"],
            binding["sha256"],
            cached=(status == "VALIDATED_REUSE"),
            publication_status=status,
        )

    finally:
        cleanup_error = None

        if lock_fd is not None:
            try:
                os.close(lock_fd)
            except OSError as error:
                cleanup_error = error

        if lock_acquired:
            try:
                current = paths["lock"].lstat()
            except FileNotFoundError:
                current = None
            except OSError as error:
                current = None
                cleanup_error = cleanup_error or error

            if current is not None:
                identity = (current.st_dev, current.st_ino)
                if identity != lock_identity:
                    cleanup_error = cleanup_error or RuntimeError(
                        "manifest-chain lock identity changed"
                    )
                else:
                    try:
                        paths["lock"].unlink()
                    except OSError as error:
                        cleanup_error = cleanup_error or error

        if cleanup_error is not None:
            raise PreNightManifestChainIntegrityError(
                "manifest-chain owned-resource cleanup failed"
            ) from cleanup_error


def _receipt(
    paths: Mapping[str, Path],
    race_date: str,
    run_id: str,
    collection_sha256: str,
    binding_sha256: str,
    *,
    cached: bool,
    publication_status: str,
) -> dict[str, Any]:
    pipeline_bytes = paths["pipeline"].read_bytes()
    execution_bytes = paths["execution"].read_bytes()

    return {
        "race_date": race_date,
        "run_id": run_id,
        "pipeline_manifest_relative_path": (
            paths["pipeline"].relative_to(paths["root"]).as_posix()
        ),
        "pipeline_manifest_sha256": hashlib.sha256(
            pipeline_bytes
        ).hexdigest(),
        "pipeline_manifest_byte_length": len(pipeline_bytes),
        "execution_manifest_relative_path": (
            paths["execution"].relative_to(paths["root"]).as_posix()
        ),
        "execution_manifest_sha256": hashlib.sha256(
            execution_bytes
        ).hexdigest(),
        "execution_manifest_byte_length": len(execution_bytes),
        "deadline_evidence_collection_sha256": collection_sha256,
        "program_entries_binding_sha256": binding_sha256,
        "cached": cached,
        "publication_status": publication_status,
        "paths": {
            "directory": paths["directory"],
            "pipeline_manifest": paths["pipeline"],
            "execution_manifest": paths["execution"],
        },
    }


__all__ = [
    "PIPELINE_MANIFEST_VERSION",
    "EXECUTION_MANIFEST_VERSION",
    "PreNightManifestChainError",
    "PreNightManifestChainContractError",
    "PreNightManifestChainCacheError",
    "PreNightManifestChainIntegrityError",
    "canonical_manifest_bytes",
    "publish_pre_night_manifest_chain",
]
