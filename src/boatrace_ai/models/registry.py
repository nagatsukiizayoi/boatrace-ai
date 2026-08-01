"""Fail-closed approved model registry validation."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import os
from pathlib import Path
import re
import stat

from .contracts import (
    CONTRACT_ID,
    CONTRACT_SHA256,
    ModelAuthorityStateError,
    ModelContractError,
    ModelIntegrityError,
    ModelRegistryError,
    parse_model_json_bytes,
)


PACKAGE_FILES = (
    "model.artifact.json",
    "model.sha256",
    "model_contract.json",
    "feature_contract.json",
    "training_manifest.json",
    "validation_report.json",
    "approval.json",
    "rollback.json",
)

MAX_BYTES = {
    "model.artifact.json": 33554432,
    "model.sha256": 65,
    "model_contract.json": 4194304,
    "feature_contract.json": 8388608,
    "training_manifest.json": 4194304,
    "validation_report.json": 4194304,
    "approval.json": 4194304,
    "rollback.json": 4194304,
    "authority_event.json": 1048576,
    "authority_event_authorization.json": 1048576,
}

MODEL_ID_RE = re.compile(r"^[a-z][a-z0-9-]{2,63}$")
SHA_RE = re.compile(r"^[0-9a-f]{64}$")
EVENT_FILE_RE = re.compile(r"^([0-9]{20})-([0-9a-f]{64})\.json$")
ID_RE = MODEL_ID_RE
TIME_RE = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T"
    r"[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z$"
)


def _raise(cls, stage, artifact):
    raise cls(
        error_code=stage,
        validation_stage=stage,
        artifact_name=artifact,
    )


def _root(value) -> Path:
    if type(value) is not str:
        _raise(ModelRegistryError, "S7L-001_ROOT_ARGUMENT_TYPE", "package")
    path = Path(value)
    if not path.is_absolute() or ".." in path.parts or "." in path.parts:
        _raise(ModelRegistryError, "S7L-002_ROOT_ABSOLUTE_GRAMMAR", "package")
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current = current / part
        try:
            info = os.lstat(current)
        except OSError:
            _raise(ModelRegistryError, "S7L-003_ROOT_DESCRIPTOR_OPEN", "package")
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            _raise(ModelRegistryError, "S7L-003_ROOT_DESCRIPTOR_OPEN", "package")
    return path


def _identity(model_id, digest):
    if type(model_id) is not str or MODEL_ID_RE.fullmatch(model_id) is None:
        _raise(ModelContractError, "S7L-004_MODEL_ID_GRAMMAR", "package")
    if type(digest) is not str or SHA_RE.fullmatch(digest) is None:
        _raise(ModelContractError, "S7L-005_MODEL_SHA256_GRAMMAR", "package")


def _safe_dir(path: Path, stage: str, artifact: str) -> tuple:
    try:
        info = os.lstat(path)
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            raise OSError
        entries = tuple(sorted(os.listdir(path)))
        return (info.st_dev, info.st_ino, info.st_mtime_ns, entries)
    except OSError:
        _raise(ModelRegistryError, stage, artifact)


def _stable_read(path: Path, name: str, stage: str, artifact: str) -> bytes:
    try:
        before_l = os.lstat(path)
        if stat.S_ISLNK(before_l.st_mode) or not stat.S_ISREG(before_l.st_mode):
            raise OSError
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(path, flags)
        try:
            before = os.fstat(fd)
            limit = MAX_BYTES[name]
            data = b""
            while True:
                block = os.read(fd, min(1048576, limit + 1 - len(data)))
                if not block:
                    break
                data += block
                if len(data) > limit:
                    _raise(ModelIntegrityError, stage, artifact)
            after = os.fstat(fd)
        finally:
            os.close(fd)
        if (
            before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns
        ) != (
            after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns
        ):
            _raise(ModelIntegrityError, stage, artifact)
        return data
    except ModelIntegrityError:
        raise
    except OSError:
        _raise(ModelIntegrityError, stage, artifact)


def _digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _timestamp(value, artifact):
    if type(value) is not str or TIME_RE.fullmatch(value) is None:
        _raise(ModelContractError, "S7L-036_EVENT_CHAIN", artifact)
    try:
        return datetime.strptime(
            value, "%Y-%m-%dT%H:%M:%S.%fZ"
        ).replace(tzinfo=timezone.utc)
    except ValueError:
        _raise(ModelContractError, "S7L-036_EVENT_CHAIN", artifact)


def _exact_fields(value, fields, stage, artifact):
    if type(value) is not dict or set(value) != set(fields):
        _raise(ModelContractError, stage, artifact)


def _package(model_root, model_id, expected_model_sha256):
    root = _root(model_root)
    _identity(model_id, expected_model_sha256)
    package = root / "models" / "approved" / model_id / expected_model_sha256
    first_identity = _safe_dir(
        package, "S7L-006_PACKAGE_DESCRIPTOR_OPEN", "package"
    )
    if first_identity[3] != tuple(sorted(PACKAGE_FILES)):
        _raise(ModelRegistryError, "S7L-009_EXACT_PACKAGE_SET", "package")

    raw = {}
    parsed = {}
    for name in PACKAGE_FILES:
        raw[name] = _stable_read(
            package / name,
            name,
            "S7L-012_STABLE_FILE_READ",
            name if name != "model.sha256" else "model.sha256",
        )

    digest_text = raw["model.sha256"]
    if (
        len(digest_text) != 65
        or digest_text[-1:] != b"\n"
        or SHA_RE.fullmatch(digest_text[:64].decode("ascii", "ignore")) is None
    ):
        _raise(ModelIntegrityError, "S7L-013_MODEL_SHA256_TEXT", "model.sha256")

    for name in PACKAGE_FILES:
        if name != "model.sha256":
            parsed[name] = parse_model_json_bytes(
                raw[name], artifact_name=name
            )

    actual_model_sha = _digest(raw["model.artifact.json"])
    if (
        actual_model_sha != expected_model_sha256
        or digest_text != (expected_model_sha256 + "\n").encode("ascii")
    ):
        _raise(
            ModelIntegrityError,
            "S7L-017_MODEL_ARTIFACT_DIGEST",
            "model.artifact.json",
        )

    model = parsed["model.artifact.json"]
    model_contract = parsed["model_contract.json"]
    feature = parsed["feature_contract.json"]
    approval = parsed["approval.json"]
    rollback = parsed["rollback.json"]

    if model.get("model_id") != model_id:
        _raise(ModelContractError, "S7L-016_ARTIFACT_SCHEMA",
               "model.artifact.json")
    if model_contract.get("model_id") != model_id:
        _raise(ModelContractError, "S7L-016_ARTIFACT_SCHEMA",
               "model_contract.json")
    if model_contract.get("model_sha256") != expected_model_sha256:
        _raise(ModelContractError, "S7L-018_CHILD_DIGEST_BINDINGS", "package")

    bindings = (
        ("feature_contract_sha256", "feature_contract.json"),
        ("training_manifest_sha256", "training_manifest.json"),
        ("validation_report_sha256", "validation_report.json"),
    )
    for field, name in bindings:
        if model_contract.get(field) != _digest(raw[name]):
            _raise(ModelContractError, "S7L-018_CHILD_DIGEST_BINDINGS",
                   "package")

    if tuple(model.get("raw_feature_order", ())) != tuple(
        feature.get("raw_feature_order", ())
    ):
        _raise(ModelContractError, "S7L-020_RAW_FEATURE_ORDER",
               "feature_contract.json")
    if tuple(model.get("encoded_feature_order", ())) != tuple(
        feature.get("encoded_feature_order", ())
    ):
        _raise(ModelContractError, "S7L-022_ENCODED_FEATURE_ORDER",
               "feature_contract.json")

    if (
        approval.get("decision") != "APPROVED"
        or approval.get("model_id") != model_id
        or approval.get("model_sha256") != expected_model_sha256
        or approval.get("authority_contract_id") != CONTRACT_ID
        or approval.get("authority_contract_sha256") != CONTRACT_SHA256
    ):
        _raise(ModelAuthorityStateError, "S7L-040_APPROVAL_AUTHORITY",
               "approval.json")

    if (
        rollback.get("authority_status") != "ACTIVE"
        or rollback.get("reason_code") != "INITIAL_APPROVAL"
        or rollback.get("rollback_target_model_id") is not None
        or rollback.get("rollback_target_model_sha256") is not None
        or rollback.get("approval_sha256") != _digest(raw["approval.json"])
    ):
        _raise(
            ModelAuthorityStateError,
            "S7L-023_INITIAL_AUTHORITY_SNAPSHOT",
            "rollback.json",
        )

    second_identity = _safe_dir(
        package, "S7L-043_FINAL_DIRECTORY_STABILITY", "package"
    )
    if second_identity != first_identity:
        _raise(ModelIntegrityError, "S7L-043_FINAL_DIRECTORY_STABILITY",
               "package")
    return root, package, raw, parsed


def _authority_paths(root, model_id, model_sha):
    event_dir = (
        root / "models" / "manifests" / "authority-events"
        / model_id / model_sha
    )
    auth_dir = (
        root / "models" / "manifests" / "authority-event-authorizations"
        / model_id / model_sha
    )
    return event_dir, auth_dir


def validate_model_authority_events(
    model_root, *, model_id, expected_model_sha256
) -> dict:
    root = _root(model_root)
    _identity(model_id, expected_model_sha256)
    event_dir, auth_dir = _authority_paths(
        root, model_id, expected_model_sha256
    )
    event_identity = _safe_dir(
        event_dir, "S7L-024_AUTHORITY_DIRECTORY_OPEN", "authority_event.json"
    )
    auth_identity = _safe_dir(
        auth_dir, "S7L-024_AUTHORITY_DIRECTORY_OPEN",
        "authority_event_authorization.json",
    )
    event_names = event_identity[3]
    auth_ids = auth_identity[3]

    if not event_names and not auth_ids:
        return {
            "model_id": model_id,
            "model_sha256": expected_model_sha256,
            "authority_status": "ACTIVE",
            "authority_event_head_sha256": None,
            "cached": False,
            "validation_status": "VALIDATED_MODEL_AUTHORITY_EVENTS",
        }
    if not event_names or not auth_ids:
        _raise(ModelRegistryError, "S7L-025_AUTHORITY_DIRECTORY_SET",
               "authority_event.json")

    previous = None
    previous_published = None
    previous_effective = None
    used_event_ids = set()
    used_auth_ids = set()
    status = "ACTIVE"
    head = None

    for expected_number, filename in enumerate(event_names, start=1):
        match = EVENT_FILE_RE.fullmatch(filename)
        if match is None or int(match.group(1)) != expected_number:
            _raise(ModelAuthorityStateError, "S7L-036_EVENT_CHAIN",
                   "authority_event.json")
        sequence, filename_digest = match.groups()
        raw = _stable_read(
            event_dir / filename,
            "authority_event.json",
            "S7L-026_EVENT_SIZE_READ",
            "authority_event.json",
        )
        if _digest(raw) != filename_digest:
            _raise(ModelIntegrityError, "S7L-028_EVENT_CANONICAL_DIGEST",
                   "authority_event.json")
        event = parse_model_json_bytes(raw, artifact_name="authority_event.json")
        if (
            event.get("sequence") != sequence
            or event.get("model_id") != model_id
            or event.get("model_sha256") != expected_model_sha256
            or event.get("previous_event_sha256") != previous
        ):
            _raise(ModelAuthorityStateError, "S7L-036_EVENT_CHAIN",
                   "authority_event.json")

        event_id = event.get("event_id")
        auth_id = event.get("authorization_id")
        auth_sha = event.get("authorization_sha256")
        if (
            type(event_id) is not str or ID_RE.fullmatch(event_id) is None
            or event_id in used_event_ids
            or type(auth_id) is not str or ID_RE.fullmatch(auth_id) is None
            or auth_id in used_auth_ids
            or type(auth_sha) is not str or SHA_RE.fullmatch(auth_sha) is None
        ):
            _raise(ModelAuthorityStateError, "S7L-036_EVENT_CHAIN",
                   "authority_event.json")

        auth_event_dir = auth_dir / event_id
        auth_identity_one = _safe_dir(
            auth_event_dir, "S7L-030_AUTHORIZATION_PATH",
            "authority_event_authorization.json",
        )
        if auth_identity_one[3] != (auth_sha + ".json",):
            _raise(ModelRegistryError, "S7L-030_AUTHORIZATION_PATH",
                   "authority_event_authorization.json")
        auth_raw = _stable_read(
            auth_event_dir / (auth_sha + ".json"),
            "authority_event_authorization.json",
            "S7L-031_AUTHORIZATION_SIZE_READ",
            "authority_event_authorization.json",
        )
        if _digest(auth_raw) != auth_sha:
            _raise(ModelIntegrityError,
                   "S7L-033_AUTHORIZATION_CANONICAL_DIGEST",
                   "authority_event_authorization.json")
        authorization = parse_model_json_bytes(
            auth_raw, artifact_name="authority_event_authorization.json"
        )
        if authorization.get("decision") != "AUTHORIZED":
            _raise(ModelAuthorityStateError, "S7L-034_AUTHORIZATION_SCHEMA",
                   "authority_event_authorization.json")

        binding_fields = (
            "authorized_at", "published_at", "effective_at",
            "event_id", "model_id", "model_sha256", "sequence",
            "previous_event_sha256", "event_type", "reason_code",
            "rollback_target_model_id", "rollback_target_model_sha256",
            "authority_contract_id", "authority_contract_sha256",
            "branch", "head",
        )
        if any(event.get(field) != authorization.get(field)
               for field in binding_fields):
            _raise(ModelAuthorityStateError,
                   "S7L-035_EVENT_AUTHORIZATION_BINDING",
                   "authority_event.json")

        authorized_at = _timestamp(event.get("authorized_at"),
                                   "authority_event.json")
        published_at = _timestamp(event.get("published_at"),
                                  "authority_event.json")
        effective_at = _timestamp(event.get("effective_at"),
                                  "authority_event.json")
        if not authorized_at <= published_at <= effective_at:
            _raise(ModelAuthorityStateError, "S7L-036_EVENT_CHAIN",
                   "authority_event.json")
        if previous_published is not None and published_at < previous_published:
            _raise(ModelAuthorityStateError, "S7L-036_EVENT_CHAIN",
                   "authority_event.json")
        if previous_effective is not None and effective_at < previous_effective:
            _raise(ModelAuthorityStateError, "S7L-036_EVENT_CHAIN",
                   "authority_event.json")
        if status != "ACTIVE":
            _raise(ModelAuthorityStateError, "S7L-036_EVENT_CHAIN",
                   "authority_event.json")

        event_type = event.get("event_type")
        if event_type == "REVOKE":
            status = "REVOKED"
        elif event_type == "ROLLBACK":
            status = "ROLLED_BACK"
        else:
            _raise(ModelAuthorityStateError, "S7L-036_EVENT_CHAIN",
                   "authority_event.json")

        used_event_ids.add(event_id)
        used_auth_ids.add(auth_id)
        previous = filename_digest
        previous_published = published_at
        previous_effective = effective_at
        head = filename_digest

    return {
        "model_id": model_id,
        "model_sha256": expected_model_sha256,
        "authority_status": status,
        "authority_event_head_sha256": head,
        "cached": False,
        "validation_status": "VALIDATED_MODEL_AUTHORITY_EVENTS",
    }


def validate_model_authority_event_authorization(
    model_root, *, model_id, expected_model_sha256,
    event_id, expected_authorization_sha256
) -> dict:
    if type(event_id) is not str or ID_RE.fullmatch(event_id) is None:
        _raise(ModelContractError, "S7L-030_AUTHORIZATION_PATH",
               "authority_event_authorization.json")
    if (
        type(expected_authorization_sha256) is not str
        or SHA_RE.fullmatch(expected_authorization_sha256) is None
    ):
        _raise(ModelContractError, "S7L-030_AUTHORIZATION_PATH",
               "authority_event_authorization.json")
    root = _root(model_root)
    _identity(model_id, expected_model_sha256)
    event_dir, auth_dir = _authority_paths(
        root, model_id, expected_model_sha256
    )
    _safe_dir(event_dir, "S7L-024_AUTHORITY_DIRECTORY_OPEN",
              "authority_event.json")
    target = auth_dir / event_id
    identity = _safe_dir(
        target, "S7L-030_AUTHORIZATION_PATH",
        "authority_event_authorization.json",
    )
    filename = expected_authorization_sha256 + ".json"
    if identity[3] != (filename,):
        _raise(ModelRegistryError, "S7L-030_AUTHORIZATION_PATH",
               "authority_event_authorization.json")
    raw = _stable_read(
        target / filename,
        "authority_event_authorization.json",
        "S7L-031_AUTHORIZATION_SIZE_READ",
        "authority_event_authorization.json",
    )
    if _digest(raw) != expected_authorization_sha256:
        _raise(ModelIntegrityError,
               "S7L-033_AUTHORIZATION_CANONICAL_DIGEST",
               "authority_event_authorization.json")
    value = parse_model_json_bytes(
        raw, artifact_name="authority_event_authorization.json"
    )
    if value.get("event_id") != event_id or value.get("decision") != "AUTHORIZED":
        _raise(ModelAuthorityStateError, "S7L-034_AUTHORIZATION_SCHEMA",
               "authority_event_authorization.json")
    return {
        "model_id": model_id,
        "model_sha256": expected_model_sha256,
        "event_id": event_id,
        "event_sha256": value.get("event_sha256"),
        "sequence": value.get("sequence"),
        "authorization_sha256": expected_authorization_sha256,
        "decision": "AUTHORIZED",
        "cached": False,
        "validation_status": "VALIDATED_AUTHORITY_EVENT_AUTHORIZATION",
    }


def validate_approved_model_package(
    model_root, *, model_id, expected_model_sha256
) -> dict:
    _root_value, package, raw, parsed = _package(
        model_root, model_id, expected_model_sha256
    )
    authority = validate_model_authority_events(
        model_root,
        model_id=model_id,
        expected_model_sha256=expected_model_sha256,
    )
    if authority["authority_status"] != "ACTIVE":
        _raise(ModelAuthorityStateError, "S7L-039_ACTIVE_AUTHORITY_REQUIRED",
               "approval.json")
    return {
        "model_id": model_id,
        "model_sha256": expected_model_sha256,
        "package_relative_path":
            f"models/approved/{model_id}/{expected_model_sha256}",
        "model_contract_sha256": _digest(raw["model_contract.json"]),
        "feature_contract_sha256": _digest(raw["feature_contract.json"]),
        "training_manifest_sha256": _digest(raw["training_manifest.json"]),
        "validation_report_sha256": _digest(raw["validation_report.json"]),
        "approval_sha256": _digest(raw["approval.json"]),
        "rollback_sha256": _digest(raw["rollback.json"]),
        "authority_event_head_sha256":
            authority["authority_event_head_sha256"],
        "model_format": parsed["model.artifact.json"]["model_format"],
        "authority_status": "ACTIVE",
        "cached": False,
        "validation_status": "VALIDATED_APPROVED_MODEL",
    }


def resolve_approved_model(
    model_root, *, model_id, expected_model_sha256
) -> dict:
    return validate_approved_model_package(
        model_root,
        model_id=model_id,
        expected_model_sha256=expected_model_sha256,
    )
