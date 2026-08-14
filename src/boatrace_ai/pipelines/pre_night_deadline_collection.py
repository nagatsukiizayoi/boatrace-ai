"""Stage 2 deterministic multi-venue deadline-evidence collection."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
from pathlib import Path

from boatrace_ai.ingestion.pre_night_deadlines import (
    canonical_deadline_evidence_bytes,
    validate_deadline_evidence,
)


COLLECTION_CONTRACT_VERSION = (
    "pre_night_deadline_evidence_collection_v1"
)


class PreNightDeadlineCollectionError(Exception):
    """Base Stage 2 collection error."""


class PreNightDeadlineCollectionContractError(
    PreNightDeadlineCollectionError
):
    """Invalid Stage 2 caller input or unsafe path."""


class PreNightDeadlineCollectionCacheError(
    PreNightDeadlineCollectionError
):
    """Existing Stage 1 or Stage 2 artifact is invalid."""


class PreNightDeadlineCollectionIntegrityError(
    PreNightDeadlineCollectionError
):
    """Durability or owned-resource operation failed."""


def _require_race_date(value) -> str:
    if not isinstance(value, str):
        raise PreNightDeadlineCollectionContractError(
            "race_date must be string"
        )

    try:
        normalized = dt.date.fromisoformat(value).isoformat()
    except (TypeError, ValueError) as error:
        raise PreNightDeadlineCollectionContractError(
            "race_date is invalid"
        ) from error

    if normalized != value:
        raise PreNightDeadlineCollectionContractError(
            "race_date must be canonical ISO date"
        )

    return value


def _require_venue_code(value) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 2
        or not value.isascii()
        or not value.isdigit()
        or not 1 <= int(value) <= 24
    ):
        raise PreNightDeadlineCollectionContractError(
            "venue_code must be 01 through 24"
        )

    return value


def _normalize_expected_venue_codes(values) -> list[str]:
    if isinstance(values, (str, bytes)) or not isinstance(
        values,
        (list, tuple),
    ):
        raise PreNightDeadlineCollectionContractError(
            "expected_venue_codes must be list or tuple"
        )

    if not values:
        raise PreNightDeadlineCollectionContractError(
            "expected_venue_codes must not be empty"
        )

    normalized = [
        _require_venue_code(value)
        for value in values
    ]

    if len(set(normalized)) != len(normalized):
        raise PreNightDeadlineCollectionContractError(
            "expected_venue_codes contains duplicate"
        )

    return sorted(normalized, key=int)


def _collection_paths(
    data_root,
    race_date: str,
) -> dict[str, Path]:
    root = Path(data_root)
    date_value = dt.date.fromisoformat(race_date)

    stage1_date_directory = (
        root
        / "prospective"
        / "pre_night"
        / "deadline_evidence"
        / f"{date_value.year:04d}"
        / f"{date_value.month:02d}"
        / f"{date_value.day:02d}"
    )

    directory = (
        root
        / "prospective"
        / "pre_night"
        / "deadline_evidence_collections"
        / f"{date_value.year:04d}"
        / f"{date_value.month:02d}"
        / f"{date_value.day:02d}"
    )

    destination = (
        directory / "deadline_evidence_collection.json"
    )
    uid = os.urandom(16).hex()

    return {
        "root": root,
        "stage1_date_directory": stage1_date_directory,
        "directory": directory,
        "destination": destination,
        "lock": (
            directory
            / ".deadline_evidence_collection.lock"
        ),
        "temporary": (
            directory
            / (
                ".deadline_evidence_collection.json."
                f"{uid}.tmp"
            )
        ),
    }


def _assert_safe_path(
    *,
    root: Path,
    target: Path,
    label: str,
) -> None:
    if root.exists() and root.is_symlink():
        raise PreNightDeadlineCollectionContractError(
            "data_root must not be symlink"
        )

    try:
        target.relative_to(root)
    except ValueError as error:
        raise PreNightDeadlineCollectionContractError(
            f"{label} path escapes data_root"
        ) from error

    root_resolved = root.resolve(strict=False)
    target_resolved = target.resolve(strict=False)

    try:
        target_resolved.relative_to(root_resolved)
    except ValueError as error:
        raise PreNightDeadlineCollectionContractError(
            f"{label} resolved path escapes data_root"
        ) from error

    current = root

    for component in target.relative_to(root).parts:
        current = current / component

        if current.exists() and current.is_symlink():
            raise PreNightDeadlineCollectionContractError(
                f"{label} path contains symlink: {current}"
            )


def _assert_safe_paths(paths: dict[str, Path]) -> None:
    root = paths["root"]
    directory = paths["directory"]

    for label in (
        "stage1_date_directory",
        "directory",
        "destination",
        "lock",
        "temporary",
    ):
        _assert_safe_path(
            root=root,
            target=paths[label],
            label=label,
        )

    if paths["destination"].parent != directory:
        raise PreNightDeadlineCollectionContractError(
            "destination parent mismatch"
        )

    if paths["lock"].parent != directory:
        raise PreNightDeadlineCollectionContractError(
            "lock parent mismatch"
        )

    if paths["temporary"].parent != directory:
        raise PreNightDeadlineCollectionContractError(
            "temporary parent mismatch"
        )


def _canonical_collection_bytes(payload: dict) -> bytes:
    text = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return (text + "\n").encode("utf-8")


def _fsync_file(path: Path) -> None:
    try:
        with path.open("rb") as handle:
            os.fsync(handle.fileno())
    except OSError as error:
        raise PreNightDeadlineCollectionIntegrityError(
            "collection destination fsync failed"
        ) from error


def _fsync_directory(directory: Path) -> None:
    descriptor = None
    close_error = None

    try:
        flags = os.O_RDONLY | getattr(
            os,
            "O_DIRECTORY",
            0,
        )
        descriptor = os.open(directory, flags)
        os.fsync(descriptor)
    except OSError as error:
        raise PreNightDeadlineCollectionIntegrityError(
            "collection parent-directory fsync failed"
        ) from error
    finally:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError as error:
                close_error = error

        if close_error is not None:
            raise PreNightDeadlineCollectionIntegrityError(
                "collection directory descriptor close failed"
            ) from close_error


def _stage1_path(
    paths: dict[str, Path],
    venue_code: str,
) -> Path:
    return (
        paths["stage1_date_directory"]
        / venue_code
        / "deadline_evidence.json"
    )


def _inventory_stage1_venues(
    paths: dict[str, Path],
) -> set[str]:
    directory = paths["stage1_date_directory"]

    if not directory.exists() or not directory.is_dir():
        raise PreNightDeadlineCollectionCacheError(
            "Stage 1 race-date directory does not exist"
        )

    if directory.is_symlink():
        raise PreNightDeadlineCollectionContractError(
            "Stage 1 race-date directory must not be symlink"
        )

    venues: set[str] = set()

    stage1_children = tuple(directory.iterdir())
    for child in stage1_children:
        if child.is_symlink():
            raise PreNightDeadlineCollectionContractError(
                "stage1 venue path symlink is forbidden"
            )
    for child in stage1_children:
        if child.is_symlink():
            raise PreNightDeadlineCollectionContractError(
                "Stage 1 venue path must not be symlink"
            )

        if not child.is_dir():
            raise PreNightDeadlineCollectionCacheError(
                "unexpected file in Stage 1 race-date directory"
            )

        venue_code = _require_venue_code(child.name)
        artifact = child / "deadline_evidence.json"

        if not artifact.exists():
            raise PreNightDeadlineCollectionCacheError(
                "Stage 1 venue artifact is missing"
            )

        venues.add(venue_code)

    return venues


def _load_stage1_entry(
    *,
    paths: dict[str, Path],
    race_date: str,
    venue_code: str,
) -> dict:
    artifact = _stage1_path(paths, venue_code)

    _assert_safe_path(
        root=paths["root"],
        target=artifact,
        label="Stage 1 artifact",
    )

    if (
        not artifact.exists()
        or artifact.is_symlink()
        or not artifact.is_file()
    ):
        raise PreNightDeadlineCollectionCacheError(
            "Stage 1 artifact must be regular file"
        )

    stored_bytes = artifact.read_bytes()

    try:
        payload = json.loads(stored_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PreNightDeadlineCollectionCacheError(
            "Stage 1 artifact is not valid UTF-8 JSON"
        ) from error

    if not isinstance(payload, dict):
        raise PreNightDeadlineCollectionCacheError(
            "Stage 1 artifact must be JSON object"
        )

    try:
        validated = validate_deadline_evidence(payload)
        canonical = canonical_deadline_evidence_bytes(
            validated
        )
    except Exception as error:
        raise PreNightDeadlineCollectionCacheError(
            "Stage 1 artifact validation failed"
        ) from error

    if canonical != stored_bytes:
        raise PreNightDeadlineCollectionCacheError(
            "Stage 1 artifact is non-canonical"
        )

    if validated.get("race_date") != race_date:
        raise PreNightDeadlineCollectionCacheError(
            "Stage 1 artifact race_date mismatch"
        )

    if validated.get("venue_code") != venue_code:
        raise PreNightDeadlineCollectionCacheError(
            "Stage 1 artifact venue_code mismatch"
        )

    contract_version = validated.get("contract_version")

    if not isinstance(contract_version, str) or not contract_version:
        raise PreNightDeadlineCollectionCacheError(
            "Stage 1 contract_version is invalid"
        )

    relative_path = artifact.relative_to(
        paths["root"]
    ).as_posix()

    return {
        "race_date": race_date,
        "venue_code": venue_code,
        "relative_path": relative_path,
        "deadline_evidence_sha256": (
            hashlib.sha256(stored_bytes).hexdigest()
        ),
        "byte_length": len(stored_bytes),
        "contract_version": contract_version,
    }


def _validate_cached_collection(
    *,
    paths: dict[str, Path],
    expected_payload: dict,
    expected_bytes: bytes,
    expected_sha256: str,
) -> dict:
    destination = paths["destination"]

    _assert_safe_paths(paths)

    if (
        not destination.exists()
        or destination.is_symlink()
        or not destination.is_file()
    ):
        raise PreNightDeadlineCollectionCacheError(
            "collection cache must be regular file"
        )

    stored_bytes = destination.read_bytes()

    try:
        stored_payload = json.loads(
            stored_bytes.decode("utf-8")
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PreNightDeadlineCollectionCacheError(
            "collection cache is not valid UTF-8 JSON"
        ) from error

    if not isinstance(stored_payload, dict):
        raise PreNightDeadlineCollectionCacheError(
            "collection cache must be JSON object"
        )

    allowed_keys = {
        "contract_version",
        "race_date",
        "expected_venue_codes",
        "entry_count",
        "entries",
    }

    if set(stored_payload) != allowed_keys:
        raise PreNightDeadlineCollectionCacheError(
            "collection cache fields conflict"
        )

    stored_canonical = _canonical_collection_bytes(
        stored_payload
    )

    if stored_canonical != stored_bytes:
        raise PreNightDeadlineCollectionCacheError(
            "collection cache is non-canonical"
        )

    if stored_payload != expected_payload:
        raise PreNightDeadlineCollectionCacheError(
            "collection cache payload conflict"
        )

    if stored_bytes != expected_bytes:
        raise PreNightDeadlineCollectionCacheError(
            "collection cache byte conflict"
        )

    stored_sha256 = hashlib.sha256(
        stored_bytes
    ).hexdigest()

    if stored_sha256 != expected_sha256:
        raise PreNightDeadlineCollectionCacheError(
            "collection cache digest conflict"
        )

    return _receipt(
        paths=paths,
        payload=stored_payload,
        digest=stored_sha256,
        byte_length=len(stored_bytes),
        cached=True,
    )


def _receipt(
    *,
    paths: dict[str, Path],
    payload: dict,
    digest: str,
    byte_length: int,
    cached: bool,
) -> dict:
    return {
        "race_date": payload["race_date"],
        "relative_path": (
            paths["destination"]
            .relative_to(paths["root"])
            .as_posix()
        ),
        "deadline_evidence_collection_sha256": digest,
        "byte_length": byte_length,
        "entry_count": payload["entry_count"],
        "expected_venue_codes": list(
            payload["expected_venue_codes"]
        ),
        "paths": {
            "directory": paths["directory"],
            "deadline_evidence_collection": (
                paths["destination"]
            ),
        },
        "cached": cached,
        "publication_status": (
            "VALIDATED_REUSE" if cached else "CREATED"
        ),
    }


def collect_pre_night_deadline_evidence(
    data_root,
    *,
    race_date,
    expected_venue_codes,
) -> dict:
    """Collect exact Stage 1 artifacts into one immutable daily artifact."""

    race_date = _require_race_date(race_date)
    expected = _normalize_expected_venue_codes(
        expected_venue_codes
    )

    paths = _collection_paths(data_root, race_date)
    _assert_safe_paths(paths)

    actual = _inventory_stage1_venues(paths)
    expected_set = set(expected)

    missing = sorted(expected_set - actual, key=int)
    extra = sorted(actual - expected_set, key=int)

    if missing:
        raise PreNightDeadlineCollectionCacheError(
            f"missing Stage 1 venues: {missing}"
        )

    if extra:
        raise PreNightDeadlineCollectionCacheError(
            f"extra Stage 1 venues: {extra}"
        )

    entries = [
        _load_stage1_entry(
            paths=paths,
            race_date=race_date,
            venue_code=venue_code,
        )
        for venue_code in expected
    ]

    payload = {
        "contract_version": COLLECTION_CONTRACT_VERSION,
        "race_date": race_date,
        "expected_venue_codes": expected,
        "entry_count": len(entries),
        "entries": entries,
    }

    canonical_bytes = _canonical_collection_bytes(
        payload
    )
    digest = hashlib.sha256(
        canonical_bytes
    ).hexdigest()

    destination = paths["destination"]

    if destination.exists():
        return _validate_cached_collection(
            paths=paths,
            expected_payload=payload,
            expected_bytes=canonical_bytes,
            expected_sha256=digest,
        )

    directory = paths["directory"]
    directory.mkdir(parents=True, exist_ok=True)
    _assert_safe_paths(paths)

    lock_path = paths["lock"]
    temporary = paths["temporary"]

    lock_fd = None
    lock_acquired = False
    lock_identity = None
    temporary_created = False

    try:
        try:
            lock_fd = os.open(
                lock_path,
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
            )
            lock_acquired = True
            lock_stat = os.fstat(lock_fd)
            lock_identity = (
                lock_stat.st_dev,
                lock_stat.st_ino,
            )
        except FileExistsError as error:
            raise PreNightDeadlineCollectionError(
                "collection publication lock exists"
            ) from error
        except OSError as error:
            raise PreNightDeadlineCollectionError(
                "collection lock acquisition failed"
            ) from error

        try:
            os.close(lock_fd)
        except OSError as error:
            raise PreNightDeadlineCollectionIntegrityError(
                "collection lock descriptor close failed"
            ) from error
        else:
            lock_fd = None

        _assert_safe_paths(paths)

        if destination.exists():
            return _validate_cached_collection(
                paths=paths,
                expected_payload=payload,
                expected_bytes=canonical_bytes,
                expected_sha256=digest,
            )

        try:
            with temporary.open("xb") as handle:
                temporary_created = True
                handle.write(canonical_bytes)
                handle.flush()
                os.fsync(handle.fileno())
        except Exception as error:
            raise PreNightDeadlineCollectionIntegrityError(
                "collection temporary write failed"
            ) from error

        temporary_bytes = temporary.read_bytes()

        if temporary_bytes != canonical_bytes:
            raise PreNightDeadlineCollectionIntegrityError(
                "collection temporary byte mismatch"
            )

        if (
            hashlib.sha256(temporary_bytes).hexdigest()
            != digest
        ):
            raise PreNightDeadlineCollectionIntegrityError(
                "collection temporary digest mismatch"
            )

        _assert_safe_paths(paths)

        if destination.exists():
            return _validate_cached_collection(
                paths=paths,
                expected_payload=payload,
                expected_bytes=canonical_bytes,
                expected_sha256=digest,
            )

        try:
            os.link(temporary, destination)
        except FileExistsError:
            return _validate_cached_collection(
                paths=paths,
                expected_payload=payload,
                expected_bytes=canonical_bytes,
                expected_sha256=digest,
            )
        except OSError as error:
            raise PreNightDeadlineCollectionIntegrityError(
                "collection atomic publication failed"
            ) from error

        _fsync_file(destination)
        _fsync_directory(directory)

        verified = _validate_cached_collection(
            paths=paths,
            expected_payload=payload,
            expected_bytes=canonical_bytes,
            expected_sha256=digest,
        )

        return {
            **verified,
            "cached": False,
            "publication_status": "CREATED",
        }

    finally:
        cleanup_error = None

        if temporary_created:
            try:
                temporary.unlink(missing_ok=True)
            except OSError as error:
                cleanup_error = error

        if lock_fd is not None:
            try:
                os.close(lock_fd)
            except OSError as error:
                if cleanup_error is None:
                    cleanup_error = error
            else:
                lock_fd = None

        if lock_acquired:
            try:
                current_stat = lock_path.lstat()
            except FileNotFoundError:
                current_stat = None
            except OSError as error:
                current_stat = None
                if cleanup_error is None:
                    cleanup_error = error

            if current_stat is not None:
                current_identity = (
                    current_stat.st_dev,
                    current_stat.st_ino,
                )

                if current_identity != lock_identity:
                    if cleanup_error is None:
                        cleanup_error = RuntimeError(
                            "collection lock identity changed"
                        )
                else:
                    try:
                        lock_path.unlink()
                    except OSError as error:
                        if cleanup_error is None:
                            cleanup_error = error

        if cleanup_error is not None:
            raise PreNightDeadlineCollectionIntegrityError(
                "collection owned-resource cleanup failed"
            ) from cleanup_error
